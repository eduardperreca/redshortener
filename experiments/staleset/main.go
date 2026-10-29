// Command staleset changes one link over and over while readers keep its
// cache entry filled, reading Postgres from a replica on a miss, and counts
// how often the cache is left holding an old URL.
//
// With -mode naive, a change is UPDATE then DEL, and readers fill the cache
// with SET. With -mode app, it works like the app: the change SETs the new
// URL before committing, and readers fill the cache with SET NX.
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {
	mode := flag.String("mode", "app", "naive or app")
	primaryURL := flag.String("primary", "postgres://redshortener:redshortener@postgres,postgres2,postgres3/redshortener?target_session_attrs=read-write", "primary URL")
	replicaURL := flag.String("replica", "postgres://redshortener:redshortener@postgres,postgres2,postgres3/redshortener?target_session_attrs=prefer-standby", "replica URL")
	redisAddr := flag.String("redis", "redis:6379", "Redis address")
	n := flag.Int("n", 500, "changes")
	readers := flag.Int("readers", 8, "readers")
	flag.Parse()

	ctx := context.Background()
	primary, err := pgxpool.New(ctx, *primaryURL)
	if err != nil {
		panic(err)
	}
	replica, err := pgxpool.New(ctx, *replicaURL)
	if err != nil {
		panic(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})

	const code, key = "staleset", "link:staleset"
	if _, err := primary.Exec(ctx, `INSERT INTO links (code, url) VALUES ($1, 'https://example.com/start')
		ON CONFLICT (code) DO UPDATE SET url = excluded.url, expires_at = NULL`, code); err != nil {
		panic(err)
	}
	defer primary.Exec(ctx, `DELETE FROM links WHERE code = $1`, code)
	rdb.Del(ctx, key)
	defer rdb.Del(ctx, key)

	stop, cancel := context.WithCancel(ctx)
	defer cancel()
	for range *readers {
		go func() {
			for stop.Err() == nil {
				if rdb.Get(ctx, key).Err() != redis.Nil {
					continue
				}
				var url string
				if replica.QueryRow(ctx, `SELECT url FROM links WHERE code = $1`, code).Scan(&url) != nil {
					continue
				}
				if *mode == "naive" {
					rdb.Set(ctx, key, url, time.Hour)
				} else {
					rdb.SetNX(ctx, key, url, time.Hour)
				}
			}
		}()
	}

	stale := 0
	for i := range *n {
		url := fmt.Sprintf("https://example.com/%d", i)
		if *mode == "naive" {
			if _, err := primary.Exec(ctx, `UPDATE links SET url = $2 WHERE code = $1`, code, url); err != nil {
				panic(err)
			}
			rdb.Del(ctx, key)
		} else {
			err := pgx.BeginFunc(ctx, primary, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, `UPDATE links SET url = $2 WHERE code = $1`, code, url); err != nil {
					return err
				}
				return rdb.Set(ctx, key, url, time.Hour).Err()
			})
			if err != nil {
				panic(err)
			}
		}
		// Long enough for the replica to catch up and a reader to refill.
		time.Sleep(20 * time.Millisecond)
		if got := rdb.Get(ctx, key).Val(); got != "" && got != url {
			stale++
		}
	}
	fmt.Printf("%s: %d of %d changes left an old URL in the cache\n", *mode, stale, *n)
}
