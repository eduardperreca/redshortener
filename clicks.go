package main

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	clicksKey     = "clicks:pending"
	flushInterval = 10 * time.Second
	purgeInterval = time.Hour
)

// Hit counts a click on code. Counts collect in a Redis hash and reach
// Postgres in batches, so a redirect never writes to Postgres.
func (l *Links) Hit(ctx context.Context, code string) {
	// ponytail: one hash holds every pending count, so in Redis Cluster it is
	// one hot slot. Shard it into clicks:pending:{0..n} when that shows up.
	if err := l.rdb.HIncrBy(ctx, clicksKey, code, 1).Err(); err != nil {
		slog.WarnContext(ctx, "counting click", "code", code, "err", err)
	}
}

// flushClicks moves the pending counts from Redis to Postgres.
//
// HGETALL and DEL run in one MULTI, so when several instances flush at once,
// each count is taken by exactly one of them. If Postgres rejects the batch,
// the counts go back into Redis for the next flush. If the process dies
// between taking the counts and writing them, they are lost: a click is
// counted at most once.
func (l *Links) flushClicks(ctx context.Context) error {
	var pending *redis.MapStringStringCmd
	_, err := l.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		pending = p.HGetAll(ctx, clicksKey)
		p.Del(ctx, clicksKey)
		return nil
	})
	if err != nil || len(pending.Val()) == 0 {
		return err
	}

	codes := make([]string, 0, len(pending.Val()))
	counts := make([]int64, 0, len(pending.Val()))
	var total int64
	for code, s := range pending.Val() {
		n, _ := strconv.ParseInt(s, 10, 64)
		codes = append(codes, code)
		counts = append(counts, n)
		total += n
	}

	// Codes that expired or never existed match no row and are dropped.
	_, err = l.db.Exec(ctx, `
		UPDATE links SET clicks = clicks + c.n
		FROM unnest($1::text[], $2::bigint[]) AS c(code, n)
		WHERE links.code = c.code`, codes, counts)
	if err != nil {
		_, perr := l.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
			for i, code := range codes {
				p.HIncrBy(ctx, clicksKey, code, counts[i])
			}
			return nil
		})
		if perr != nil {
			slog.ErrorContext(ctx, "clicks lost", "count", total, "err", perr)
		}
		return err
	}
	return nil
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
