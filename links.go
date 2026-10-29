package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"log/slog"
	mrand "math/rand/v2"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

const (
	alphabet    = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	codeLen     = 7
	cacheTTL    = 24 * time.Hour
	negativeTTL = time.Minute
	loadTimeout = 5 * time.Second
)

var (
	errNotFound = errors.New("link not found")
	errTaken    = errors.New("alias is taken")
)

var cacheLookups = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "redshortener_cache_lookups_total",
	Help: "Redirect lookups in Redis by result: hit, negative_hit, miss or error.",
}, []string{"result"})

type Link struct {
	Code      string     `json:"code"`
	URL       string     `json:"url"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	Clicks    int64      `json:"clicks"`
	Token     string     `json:"token,omitempty"` // only when created
}

// Links stores links in Postgres and caches lookups in Redis.
type Links struct {
	db  *pgxpool.Pool
	ro  *pgxpool.Pool // a read replica, or db itself
	rdb redis.UniversalClient
	sf  singleflight.Group
}

func cacheKey(code string) string { return "link:" + code }

// readCode scans the row a query by code returns, from the replica. If the
// replica has no row or fails, it runs the query again on the primary: a link
// created a moment ago may not have reached the replica yet, and a "not found"
// from a lagging replica would be cached for a minute.
func (l *Links) readCode(ctx context.Context, sql, code string, dest ...any) error {
	err := l.ro.QueryRow(ctx, sql, code).Scan(dest...)
	if err != nil && l.ro != l.db {
		err = l.db.QueryRow(ctx, sql, code).Scan(dest...)
	}
	return err
}

// Create stores url under alias, or under a new random code when alias is
// empty. A zero expiresIn means the link never expires. The returned link
// carries the token that can change or delete it; only its hash is stored.
func (l *Links) Create(ctx context.Context, url string, expiresIn time.Duration, alias string) (Link, error) {
	for range 3 {
		link := Link{Code: alias, URL: url, Token: rand.Text()}
		hash := sha256.Sum256([]byte(link.Token))
		if alias == "" {
			link.Code = newCode()
		}
		// The primary key decides who gets a code, so two requests for the
		// same alias cannot both succeed. A code whose link has expired but
		// not been deleted yet is taken over.
		err := l.db.QueryRow(ctx, `
			INSERT INTO links (code, url, expires_at, token_hash)
			VALUES ($1, $2, now() + nullif($3::float8, 0) * interval '1 second', $4)
			ON CONFLICT (code) DO UPDATE
				SET url = excluded.url, created_at = now(), expires_at = excluded.expires_at,
					clicks = 0, token_hash = excluded.token_hash
				WHERE links.expires_at <= now()
			RETURNING created_at, expires_at`, link.Code, url, expiresIn.Seconds(), hash[:]).Scan(&link.CreatedAt, &link.ExpiresAt)
		if errors.Is(err, pgx.ErrNoRows) {
			if alias != "" {
				return Link{}, errTaken
			}
			continue // the code is taken
		}
		if err != nil {
			return Link{}, err
		}

		// Writers set the cache entry unconditionally and lookups only with
		// NX, so a lookup that read Postgres before this insert cannot
		// overwrite the entry, and this replaces any "not found" left by one.
		ttl := ttlFor(cacheTTL, expiresIn)
		if err := l.rdb.Set(ctx, cacheKey(link.Code), url, ttl).Err(); err != nil {
			slog.WarnContext(ctx, "caching new link", "code", link.Code, "err", err)
		}
		return link, nil
	}
	return Link{}, errors.New("no free code after 3 attempts")
}

// Update points code at url, if token is the link's token.
func (l *Links) Update(ctx context.Context, code, token, url string) error {
	return l.change(ctx, code, token, url)
}

// Delete makes code expire now, if token is the link's token. The row stays
// until the hourly purge, so deleting again succeeds, and an alias is free
// for a new link at once.
func (l *Links) Delete(ctx context.Context, code, token string) error {
	return l.change(ctx, code, token, "")
}

// change updates the row and writes the new cache entry, url or "not found"
// for an empty url, before committing. The UPDATE holds the row lock until
// then, so concurrent changes of one link reach Redis in the order Postgres
// applies them. If Redis fails, the change is rolled back and the caller can
// retry: otherwise the old URL could stay cached for a day.
func (l *Links) change(ctx context.Context, code, token, url string) error {
	hash := sha256.Sum256([]byte(token))
	err := pgx.BeginFunc(ctx, l.db, func(tx pgx.Tx) error {
		var left *float64
		sql := `UPDATE links SET url = $3 WHERE code = $1 AND token_hash = $2
			AND (expires_at IS NULL OR expires_at > now())
			RETURNING extract(epoch FROM expires_at - now())::float8`
		args := []any{code, hash[:], url}
		if url == "" {
			sql = `UPDATE links SET expires_at = now() WHERE code = $1 AND token_hash = $2
				RETURNING NULL::float8`
			args = args[:2]
		}
		err := tx.QueryRow(ctx, sql, args...).Scan(&left)
		if errors.Is(err, pgx.ErrNoRows) {
			return errNotFound
		}
		if err != nil {
			return err
		}
		ttl := negativeTTL
		if url != "" {
			ttl = ttlFor(cacheTTL, remaining(left))
		}
		return l.rdb.Set(ctx, cacheKey(code), url, ttl).Err()
	})
	if err != nil && !errors.Is(err, errNotFound) {
		// The commit can fail after the cache was written.
		l.rdb.Del(context.WithoutCancel(ctx), cacheKey(code))
	}
	return err
}

// Resolve returns the URL behind code, from Redis when cached and from
// Postgres otherwise. If Redis fails, lookups go to Postgres.
func (l *Links) Resolve(ctx context.Context, code string) (string, error) {
	url, err := l.rdb.Get(ctx, cacheKey(code)).Result()
	switch {
	case err == nil && url == "":
		cacheLookups.WithLabelValues("negative_hit").Inc()
		return "", errNotFound
	case err == nil:
		cacheLookups.WithLabelValues("hit").Inc()
		return url, nil
	case errors.Is(err, redis.Nil):
		cacheLookups.WithLabelValues("miss").Inc()
	default:
		cacheLookups.WithLabelValues("error").Inc()
		slog.WarnContext(ctx, "reading cache", "code", code, "err", err)
	}

	// Concurrent misses on one code share a single query, so a popular link
	// dropping out of the cache sends one query per instance, not one per
	// request. The query outlives the first caller's context, because the
	// other callers are waiting on it too.
	v, err, _ := l.sf.Do(code, func() (any, error) {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), loadTimeout)
		defer cancel()
		return l.load(ctx, code)
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

// load reads code from Postgres and caches the answer, including "not found".
func (l *Links) load(ctx context.Context, code string) (string, error) {
	var url string
	var left *float64 // seconds until expiry, read from the Postgres clock
	err := l.readCode(ctx, `
		SELECT url, extract(epoch FROM expires_at - now())::float8 FROM links
		WHERE code = $1 AND (expires_at IS NULL OR expires_at > now())`, code, &url, &left)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := l.rdb.SetNX(ctx, cacheKey(code), "", negativeTTL).Err(); err != nil {
			slog.WarnContext(ctx, "caching missing link", "code", code, "err", err)
		}
		return "", errNotFound
	}
	if err != nil {
		return "", err
	}

	// NX: if a writer has set the entry since this lookup read Postgres, the
	// writer's value is newer than url.
	if ttl := ttlFor(cacheTTL, remaining(left)); ttl >= time.Millisecond {
		if err := l.rdb.SetNX(ctx, cacheKey(code), url, ttl).Err(); err != nil {
			slog.WarnContext(ctx, "caching link", "code", code, "err", err)
		}
	}
	return url, nil
}

// Stats returns the stored link. Clicks lag behind by up to one flush interval.
func (l *Links) Stats(ctx context.Context, code string) (Link, error) {
	link := Link{Code: code}
	err := l.readCode(ctx, `
		SELECT url, created_at, expires_at, clicks FROM links
		WHERE code = $1 AND (expires_at IS NULL OR expires_at > now())`, code,
		&link.URL, &link.CreatedAt, &link.ExpiresAt, &link.Clicks)
	if errors.Is(err, pgx.ErrNoRows) {
		return Link{}, errNotFound
	}
	return link, err
}

func (l *Links) deleteExpired(ctx context.Context) error {
	// ponytail: one DELETE for every expired row; batch it with LIMIT if a
	// backlog ever makes the statement hold locks for too long.
	_, err := l.db.Exec(ctx, `DELETE FROM links WHERE expires_at <= now()`)
	return err
}

// remaining converts seconds left until expiry, NULL for never, to a duration.
func remaining(left *float64) time.Duration {
	if left == nil {
		return 0
	}
	return time.Duration(*left * float64(time.Second))
}

// ttlFor jitters base by ±10%, so links cached together do not all expire
// and miss together, and caps it at expiresIn when that is set.
func ttlFor(base, expiresIn time.Duration) time.Duration {
	ttl := base - base/10 + mrand.N(base/5)
	if expiresIn > 0 && expiresIn < ttl {
		return expiresIn
	}
	return ttl
}

func newCode() string {
	code := make([]byte, 0, codeLen)
	buf := make([]byte, 16)
	for len(code) < codeLen {
		rand.Read(buf)
		for _, b := range buf {
			// 248 is the largest multiple of 62 below 256. Dropping the bytes
			// above it keeps every character equally likely.
			if b < 248 && len(code) < codeLen {
				code = append(code, alphabet[b%62])
			}
		}
	}
	return string(code)
}

// validCode reports whether code is a generated code or a valid alias: 3 to
// 32 characters from the alphabet, '-' and '_'. Anything else is rejected
// before it reaches Redis or Postgres.
func validCode(code string) bool {
	return len(code) >= 3 && len(code) <= 32 && strings.Trim(code, alphabet+"-_") == ""
}

func checkURL(raw string, self *url.URL) error {
	if len(raw) > 2048 {
		return errors.New("url is longer than 2048 bytes")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("url must be an absolute http or https URL")
	}
	if u.User != nil {
		return errors.New("url must not contain a user name or password")
	}
	if strings.EqualFold(u.Hostname(), self.Hostname()) {
		return errors.New("url points to this shortener")
	}
	return nil
}
