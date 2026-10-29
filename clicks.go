package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
)

const (
	clickShards   = 16
	flushInterval = 10 * time.Second
	purgeInterval = time.Hour
)

var clicksFlushed = promauto.NewCounter(prometheus.CounterOpts{
	Name: "redshortener_clicks_flushed_total",
	Help: "Clicks moved from Redis to Postgres.",
})

func clicksKey(shard int) string { return "clicks:pending:" + strconv.Itoa(shard) }

// Hit counts a click on code. Counts collect in Redis hashes and reach
// Postgres in batches, so a redirect never writes to Postgres.
//
// There are 16 hashes, and each click goes to a random one. With a single
// hash, every click in a Redis Cluster would go to the primary that owns its
// slot. Picking the hash at random instead of by code also spreads the clicks
// of one popular link.
func (l *Links) Hit(ctx context.Context, code string) {
	if err := l.rdb.HIncrBy(ctx, clicksKey(rand.N(clickShards)), code, 1).Err(); err != nil {
		slog.WarnContext(ctx, "counting click", "code", code, "err", err)
	}
}

// flushClicks moves the pending counts from Redis to Postgres.
//
// Each hash is emptied with takeClicks, so when several instances flush at
// once, each count is taken by exactly one of them. If that fails for some
// hashes, the counts taken from the others are still written. If Postgres
// rejects the batch, the counts go back into Redis for the next flush. If the
// process dies between taking the counts and writing them, they are lost: a
// click is counted at most once.
func (l *Links) flushClicks(ctx context.Context) error {
	// The same code can be pending in several hashes. UPDATE ... FROM would
	// apply only one of its rows, so the counts are summed first.
	sum := map[string]int64{}
	var rerr error
	for i := range clickShards {
		pending, err := l.takeClicks(ctx, clicksKey(i))
		if err != nil {
			rerr = errors.Join(rerr, err)
			continue
		}
		for code, s := range pending {
			n, _ := strconv.ParseInt(s, 10, 64)
			sum[code] += n
		}
	}
	if len(sum) == 0 {
		return rerr
	}
	codes := make([]string, 0, len(sum))
	counts := make([]int64, 0, len(sum))
	var total int64
	for code, n := range sum {
		codes = append(codes, code)
		counts = append(counts, n)
		total += n
	}

	// Codes that expired or never existed match no row and are dropped.
	_, err := l.db.Exec(ctx, `
		UPDATE links SET clicks = clicks + c.n
		FROM unnest($1::text[], $2::bigint[]) AS c(code, n)
		WHERE links.code = c.code`, codes, counts)
	if err != nil {
		_, perr := l.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
			for i, code := range codes {
				p.HIncrBy(ctx, clicksKey(rand.N(clickShards)), code, counts[i])
			}
			return nil
		})
		if perr != nil {
			slog.ErrorContext(ctx, "clicks lost", "count", total, "err", perr)
		}
		return errors.Join(rerr, err)
	}
	clicksFlushed.Add(float64(total))
	return rerr
}

// takeClicks empties one pending hash with MULTI { HGETALL, DEL } and returns
// its counts once a replica has the DEL.
//
// If the primary died before a replica got the DEL, the replica would still
// have the counts after it was promoted, and they would be flushed twice.
// WAIT asks the primary how many replicas have every write of this connection
// so far, so the MULTI and the WAIT must share a connection. If the primary is
// gone before it answers, the counts are dropped: either the replica still has
// them, or they are lost, and in both cases no click is counted twice.
//
// A WAIT that times out still lets the counts through. Without replicas it
// always does, after 50 ms, and with replicas it means they are behind, not
// that the primary is gone.
func (l *Links) takeClicks(ctx context.Context, key string) (map[string]string, error) {
	// One MULTI per hash: in a Redis Cluster, a MULTI can only use keys of
	// one slot.
	// Sentinel and Cluster clients are both ClusterClients here.
	var primary *redis.Client
	switch c := l.rdb.(type) {
	case *redis.Client:
		primary = c
	case *redis.ClusterClient:
		var err error
		if primary, err = c.MasterForKey(ctx, key); err != nil {
			return nil, err
		}
	}
	conn := primary.Conn()
	defer conn.Close()

	var pending *redis.MapStringStringCmd
	_, err := conn.TxPipelined(ctx, func(p redis.Pipeliner) error {
		pending = p.HGetAll(ctx, key)
		p.Del(ctx, key)
		return nil
	})
	if err != nil || len(pending.Val()) == 0 {
		return nil, err
	}
	if err := conn.Wait(ctx, 1, 50*time.Millisecond).Err(); err != nil {
		return nil, fmt.Errorf("dropped %d pending codes from %s: %w", len(pending.Val()), key, err)
	}
	return pending.Val(), nil
}

// background flushes clicks and deletes expired links until ctx is done,
// then flushes one last time.
func (l *Links) background(ctx context.Context) {
	flush := time.NewTicker(flushInterval)
	defer flush.Stop()
	purge := time.NewTicker(purgeInterval)
	defer purge.Stop()

	// Each run gets its own timeout instead of ctx, so that shutting down does
	// not cancel a flush halfway, after the counts have left Redis.
	run := func(name string, f func(context.Context) error) {
		ctx, cancel := context.WithTimeout(context.Background(), loadTimeout)
		defer cancel()
		if err := f(ctx); err != nil {
			slog.Error(name, "err", err)
		}
	}
	for {
		select {
		case <-flush.C:
			run("flushing clicks", l.flushClicks)
		case <-purge.C:
			run("deleting expired links", l.deleteExpired)
		case <-ctx.Done():
			run("flushing clicks", l.flushClicks)
			return
		}
	}
}
