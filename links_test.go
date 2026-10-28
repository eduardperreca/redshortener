package main

import (
	"context"
	"net/url"
	"os"
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
	return &Links{db: db, rdb: rdb}
}

// A miss reads Postgres and fills the cache, with a TTL capped at the
// link's expiry.
func TestCacheAside(t *testing.T) {
	l := newLinks(t)
	ctx := context.Background()

	link, err := l.Create(ctx, "https://example.com/", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := l.Resolve(ctx, link.Code); err != nil || got != link.URL {
		t.Fatalf("after miss: got %q, %v", got, err)
	}
	if got := l.rdb.Get(ctx, cacheKey(link.Code)).Val(); got != link.URL {
		t.Fatalf("cache after miss: got %q", got)
	}
	if ttl := l.rdb.PTTL(ctx, cacheKey(link.Code)).Val(); ttl <= 0 || ttl > time.Hour {
		t.Fatalf("cache TTL %v, want at most 1h", ttl)
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
