// Command redshortener is a URL shortener that keeps links in Postgres and
// caches them in Redis.
package main

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

//go:embed migrations/*.sql
var migrations embed.FS

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	base, err := url.Parse(env("BASE_URL", "http://localhost:8080"))
	if err != nil || base.Host == "" {
		return fmt.Errorf("BASE_URL must be an absolute URL, got %q", os.Getenv("BASE_URL"))
	}

	db, err := pgxpool.New(ctx, env("DATABASE_URL", "postgres://redshortener:redshortener@localhost:5432/redshortener"))
	if err != nil {
		return err
	}
	defer db.Close()
	if err := migrate(ctx, db); err != nil {
		return fmt.Errorf("migrating: %w", err)
	}

	opts := &redis.UniversalOptions{
		Addrs:                 strings.Split(env("REDIS_ADDR", "localhost:6379"), ","),
		MasterName:            os.Getenv("REDIS_MASTER"),
		MaxRetries:            1,
		ContextTimeoutEnabled: true,
	}
	var rdb redis.UniversalClient
	if opts.MasterName != "" {
		// Through Sentinel, with read-only commands such as GET sent to a
		// replica and everything else to the primary. It has to be a
		// failover cluster client: NewUniversalClient with ReadOnly would
		// send writes to a replica too.
		fo := opts.Failover()
		fo.ReplicaOnly = true
		rdb = redis.NewFailoverClusterClient(fo)
	} else {
		rdb = redis.NewUniversalClient(opts)
	}
	rdb.AddHook(redisBudget(300 * time.Millisecond))
	defer rdb.Close()

	links := &Links{db: db, rdb: rdb}
	srv := &http.Server{
		Addr:              env("ADDR", ":8080"),
		Handler:           newHandler(links, base, os.Getenv("CLIENT_IP_HEADER")),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       time.Minute,
	}

	done := make(chan struct{})
	go func() {
		links.background(ctx)
		close(done)
	}()

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	slog.Info("listening", "addr", srv.Addr, "base_url", base.String())

	select {
	case err := <-errc:
		stop()
		<-done
		return err
	case <-ctx.Done():
	}
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	<-done
	return err
}

// migrate applies the files in migrations/ that have not been applied yet, in
// name order, all in one transaction. The advisory lock makes instances that
// start at the same time wait for each other instead of racing.
func migrate(ctx context.Context, db *pgxpool.Pool) error {
	files, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('redshortener migrate'))`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
			name       text PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
			return err
		}
		for _, name := range files {
			tag, err := tx.Exec(ctx, `INSERT INTO schema_migrations (name) VALUES ($1) ON CONFLICT DO NOTHING`, name)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				continue // already applied
			}
			sql, err := migrations.ReadFile(name)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			slog.Info("applied migration", "name", name)
		}
		return nil
	})
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// redisBudget gives every Redis command and pipeline a deadline that covers
// dialing and retries too. Per-step timeouts do not add up to a bound: with
// a dead primary, go-redis dials 5 times per attempt, and each dial gets the
// full dial timeout. When the budget runs out, the caller goes on without
// Redis.
type redisBudget time.Duration

func (b redisBudget) DialHook(next redis.DialHook) redis.DialHook { return next }

func (b redisBudget) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		ctx, cancel := context.WithTimeout(ctx, time.Duration(b))
		defer cancel()
		return next(ctx, cmd)
	}
}

func (b redisBudget) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		ctx, cancel := context.WithTimeout(ctx, time.Duration(b))
		defer cancel()
		return next(ctx, cmds)
	}
}
