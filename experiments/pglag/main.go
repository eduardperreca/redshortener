// Command pglag inserts rows on a Postgres primary and reads each one back
// from a replica right away, counting how often the replica does not have it
// yet. It uses a table of its own and drops it at the end.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	primaryURL := flag.String("primary", "postgres://redshortener:redshortener@postgres:5432/redshortener", "primary URL")
	replicaURL := flag.String("replica", "postgres://redshortener:redshortener@postgres-replica:5432/redshortener", "replica URL")
	n := flag.Int("n", 2000, "rounds")
	writers := flag.Int("writers", 0, "connections inserting 10 KB rows on the primary meanwhile")
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
	if _, err := primary.Exec(ctx, `DROP TABLE IF EXISTS pglag; CREATE TABLE pglag (id bigint PRIMARY KEY, pad text)`); err != nil {
		panic(err)
	}
	defer primary.Exec(ctx, `DROP TABLE pglag`)
	for w := range *writers {
		go func() {
			for j := int64(0); ; j++ {
				primary.Exec(ctx, `INSERT INTO pglag VALUES ($1, repeat('x', 10000))`, int64(w+1)<<40+j)
			}
		}()
	}

	missing := func(id int) bool {
		var got int64
		return errors.Is(replica.QueryRow(ctx, `SELECT id FROM pglag WHERE id = $1`, id).Scan(&got), pgx.ErrNoRows)
	}
	var waits []time.Duration
	for i := range *n {
		if _, err := primary.Exec(ctx, `INSERT INTO pglag VALUES ($1, '')`, i); err != nil {
			panic(err)
		}
		start := time.Now()
		if missing(i) {
			for missing(i) {
			}
			waits = append(waits, time.Since(start))
		}
	}

	fmt.Printf("%d of %d reads right after the insert found no row on the replica\n", len(waits), *n)
	if len(waits) > 0 {
		slices.Sort(waits)
		fmt.Printf("time until visible: p50 %v, p99 %v, max %v\n",
			waits[len(waits)/2], waits[len(waits)*99/100], waits[len(waits)-1])
	}
}
