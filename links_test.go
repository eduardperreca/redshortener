package main

import (
	"context"
	"errors"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func newLinks(t *testing.T) *Links {
	dbURL, addr := os.Getenv("DATABASE_URL"), os.Getenv("REDIS_ADDR")
	if dbURL == "" || addr == "" {
		t.Skip("DATABASE_URL or REDIS_ADDR not set")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	return &Links{db: db, ro: db, rdb: rdb}
}

// Walks one code through every state the cache can be in.
func TestCacheAside(t *testing.T) {
	l := newLinks(t)
	ctx := context.Background()

	// Unknown codes are cached as "not found".
	code := newCode()
	if _, err := l.Resolve(ctx, code); !errors.Is(err, errNotFound) {
		t.Fatalf("unknown code: got %v, want errNotFound", err)
	}
	if v, err := l.rdb.Get(ctx, cacheKey(code)).Result(); err != nil || v != "" {
		t.Fatalf("negative entry: got %q, %v", v, err)
	}

	// A new link is cached on creation, with a TTL capped at its expiry.
	link, err := l.Create(ctx, "https://example.com/", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if ttl := l.rdb.PTTL(ctx, cacheKey(link.Code)).Val(); ttl <= 0 || ttl > time.Hour {
		t.Fatalf("cache TTL %v, want at most 1h", ttl)
	}

	// A miss reads Postgres and fills the cache again.
	l.rdb.Del(ctx, cacheKey(link.Code))
	if got, err := l.Resolve(ctx, link.Code); err != nil || got != link.URL {
		t.Fatalf("after miss: got %q, %v", got, err)
	}
	if got := l.rdb.Get(ctx, cacheKey(link.Code)).Val(); got != link.URL {
		t.Fatalf("cache after miss: got %q", got)
	}
}

// Ten instances flushing at once must count every click exactly once.
func TestConcurrentFlush(t *testing.T) {
	l := newLinks(t)
	ctx := context.Background()

	link, err := l.Create(ctx, "https://example.com/", 0)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range 10 {
				l.Hit(ctx, link.Code)
				if err := l.flushClicks(ctx); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	if err := l.flushClicks(ctx); err != nil {
		t.Fatal(err)
	}

	got, err := l.Stats(ctx, link.Code)
	if err != nil {
		t.Fatal(err)
	}
	if got.Clicks != 100 {
		t.Fatalf("got %d clicks, want 100", got.Clicks)
	}
}

func TestCheckURL(t *testing.T) {
	self, _ := url.Parse("https://sho.rt")
	for raw, ok := range map[string]bool{
		"https://example.com/path?q=1":                      true,
		"HTTP://example.com":                                true,
		"example.com":                                       false,
		"javascript:alert(1)":                               false,
		"ftp://example.com":                                 false,
		"https://user:pw@example.com":                       false,
		"https://SHO.RT/abc1234":                            false,
		"https://example.com/" + string(make([]byte, 2048)): false,
	} {
		if err := checkURL(raw, self); (err == nil) != ok {
			t.Errorf("checkURL(%.40q) = %v, want ok=%v", raw, err, ok)
		}
	}
}

// A replica is usually a millisecond behind the primary, so a link read
// right after it is created is often missing there. It must still resolve,
// and must not be cached as not found.
func TestReadAfterCreate(t *testing.T) {
	l := newLinks(t)
	u := os.Getenv("DATABASE_READ_URL")
	if u == "" {
		t.Skip("DATABASE_READ_URL not set")
	}
	ctx := context.Background()
	ro, err := pgxpool.New(ctx, u)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	l.ro = ro

	for range 100 {
		link, err := l.Create(ctx, "https://example.com/", 0)
		if err != nil {
			t.Fatal(err)
		}
		l.rdb.Del(ctx, cacheKey(link.Code)) // as if caching the new link had failed
		if _, err := l.Resolve(ctx, link.Code); err != nil {
			t.Fatalf("resolving %s right after creating it: %v", link.Code, err)
		}
	}
}
