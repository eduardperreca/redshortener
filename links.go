package main

import (
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	mrand "math/rand/v2"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	codeLen  = 7
	cacheTTL = 24 * time.Hour
)

var errNotFound = errors.New("link not found")

type Link struct {
	Code      string     `json:"code"`
	URL       string     `json:"url"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	Clicks    int64      `json:"clicks"`
}

// Links stores links in Postgres and caches lookups in Redis.
type Links struct {
	db  *pgxpool.Pool
	rdb redis.UniversalClient
}

func cacheKey(code string) string { return "link:" + code }

// Create stores url under a new random code. A zero expiresIn means the link
// never expires.
func (l *Links) Create(ctx context.Context, url string, expiresIn time.Duration) (Link, error) {
	var secs *float64
	if expiresIn > 0 {
		s := expiresIn.Seconds()
		secs = &s
	}
	for range 3 {
		link := Link{Code: newCode(), URL: url}
		err := l.db.QueryRow(ctx, `
			INSERT INTO links (code, url, expires_at)
			VALUES ($1, $2, now() + make_interval(secs => $3))
			ON CONFLICT (code) DO NOTHING
			RETURNING created_at, expires_at`, link.Code, url, secs).Scan(&link.CreatedAt, &link.ExpiresAt)
		if errors.Is(err, pgx.ErrNoRows) {
			continue // the code is taken
		}
		if err != nil {
			return Link{}, err
		}
		return link, nil
	}
	return Link{}, errors.New("no free code after 3 attempts")
}

// Resolve returns the URL behind code, from Redis when cached and from
// Postgres otherwise. If Redis fails, lookups go to Postgres.
func (l *Links) Resolve(ctx context.Context, code string) (string, error) {
	url, err := l.rdb.Get(ctx, cacheKey(code)).Result()
	switch {
	case err == nil:
		return url, nil
	case errors.Is(err, redis.Nil):
		// not cached
	default:
		slog.WarnContext(ctx, "reading cache", "code", code, "err", err)
	}
	return l.load(ctx, code)
}

// load reads code from Postgres and caches the answer.
func (l *Links) load(ctx context.Context, code string) (string, error) {
	var url string
	var left *float64 // seconds until expiry, read from the Postgres clock
	err := l.db.QueryRow(ctx, `
		SELECT url, extract(epoch FROM expires_at - now())::float8 FROM links
		WHERE code = $1 AND (expires_at IS NULL OR expires_at > now())`, code).Scan(&url, &left)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", errNotFound
	}
	if err != nil {
		return "", err
	}

	var expiresIn time.Duration
	if left != nil {
		expiresIn = time.Duration(*left * float64(time.Second))
	}
	if ttl := ttlFor(cacheTTL, expiresIn); ttl >= time.Millisecond {
		if err := l.rdb.Set(ctx, cacheKey(code), url, ttl).Err(); err != nil {
			slog.WarnContext(ctx, "caching link", "code", code, "err", err)
		}
	}
	return url, nil
}

// Stats returns the stored link.
func (l *Links) Stats(ctx context.Context, code string) (Link, error) {
	link := Link{Code: code}
	err := l.db.QueryRow(ctx, `
		SELECT url, created_at, expires_at, clicks FROM links
		WHERE code = $1 AND (expires_at IS NULL OR expires_at > now())`, code).
		Scan(&link.URL, &link.CreatedAt, &link.ExpiresAt, &link.Clicks)
	if errors.Is(err, pgx.ErrNoRows) {
		return Link{}, errNotFound
	}
	return link, err
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

// validCode reports whether code could have been made by newCode. Anything
// else is rejected before it reaches Redis or Postgres.
func validCode(code string) bool {
	return len(code) == codeLen && strings.Trim(code, alphabet) == ""
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
