package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/eduardperreca/redlimit"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//go:embed index.html
var indexHTML []byte

const maxExpiresIn = 10 * 365 * 24 * time.Hour

var requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "redshortener_http_request_duration_seconds",
	Help:    "HTTP request latency by route and status code.",
	Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14), // 0.5ms to 4s
}, []string{"route", "code"})

// newHandler serves the API and the web page. Client addresses for rate
// limiting come from ipHeader when it is set, and from the TCP connection
// otherwise.
func newHandler(links *Links, base *url.URL, ipHeader string) http.Handler {
	// 10 links at once, then one a second, per client address.
	limit := redlimit.Middleware(redlimit.NewTokenBucket(links.rdb, 1, 10), func(r *http.Request) string {
		if ip := r.Header.Get(ipHeader); ipHeader != "" && ip != "" {
			return ip
		}
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		return host
	})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	mux.Handle("POST /api/links", limit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			URL       string `json:"url"`
			ExpiresIn int64  `json:"expires_in"` // seconds; 0 never expires
		}
		r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "body must be JSON with a url field")
			return
		}
		if err := checkURL(req.URL, base); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		expiresIn := time.Duration(req.ExpiresIn) * time.Second
		if req.ExpiresIn < 0 || expiresIn > maxExpiresIn {
			writeError(w, http.StatusBadRequest, "expires_in must be between 0 and 315360000 seconds")
			return
		}

		link, err := links.Create(r.Context(), req.URL, expiresIn)
		if err != nil {
			slog.ErrorContext(r.Context(), "creating link", "err", err)
			writeError(w, http.StatusInternalServerError, "could not create link")
			return
		}
		writeJSON(w, http.StatusCreated, struct {
			Link
			ShortURL string `json:"short_url"`
		}{link, base.JoinPath(link.Code).String()})
	})))

	mux.HandleFunc("GET /api/links/{code}", func(w http.ResponseWriter, r *http.Request) {
		code := r.PathValue("code")
		if !validCode(code) {
			writeError(w, http.StatusNotFound, errNotFound.Error())
			return
		}
		link, err := links.Stats(r.Context(), code)
		if errors.Is(err, errNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		if err != nil {
			slog.ErrorContext(r.Context(), "reading link", "code", code, "err", err)
			writeError(w, http.StatusInternalServerError, "could not read link")
			return
		}
		writeJSON(w, http.StatusOK, link)
	})

	mux.HandleFunc("GET /{code}", func(w http.ResponseWriter, r *http.Request) {
		code := r.PathValue("code")
		if !validCode(code) {
			http.NotFound(w, r)
			return
		}
		target, err := links.Resolve(r.Context(), code)
		if errors.Is(err, errNotFound) {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			slog.ErrorContext(r.Context(), "resolving link", "code", code, "err", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		// Counting the click must not delay the redirect. When the Redis node
		// holding its hash is down, the write takes the whole 300 ms budget.
		go links.Hit(context.WithoutCancel(r.Context()), code)
		// 302, not 301: browsers cache a 301 forever, so later clicks would
		// not be counted and an expired link would keep redirecting.
		http.Redirect(w, r, target, http.StatusFound)
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {})
	// Ready while either store answers: without Redis, lookups go to
	// Postgres, and without Postgres, cached links still redirect. Every
	// instance shares both stores, so failing on one of them would take all
	// instances out of a load balancer at once, during a Postgres failover
	// for example.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		perr := links.db.Ping(r.Context())
		if perr == nil {
			return
		}
		if rerr := links.rdb.Ping(r.Context()).Err(); rerr != nil {
			http.Error(w, "postgres: "+perr.Error()+"\nredis: "+rerr.Error(), http.StatusServiceUnavailable)
		}
	})
	mux.Handle("GET /metrics", promhttp.Handler())

	return instrument(mux)
}

// instrument records request latency. It reads r.Pattern after the mux has
// routed the request, so the route label is the pattern, not the raw path,
// and does not grow with every code.
func instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		requestDuration.WithLabelValues(r.Pattern, strconv.Itoa(rec.status)).Observe(time.Since(start).Seconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
