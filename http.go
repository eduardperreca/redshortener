package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/eduardperreca/redlimit"
)

const maxExpiresIn = 10 * 365 * 24 * time.Hour

// newHandler serves the API. Client addresses for rate
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
		links.Hit(r.Context(), code)
		// 302, not 301: browsers cache a 301 forever, so later clicks would
		// not be counted and an expired link would keep redirecting.
		http.Redirect(w, r, target, http.StatusFound)
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {})
	// Redis is only a cache: without it lookups go to Postgres. So only
	// Postgres decides whether this instance is ready.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := links.db.Ping(r.Context()); err != nil {
			http.Error(w, "postgres: "+err.Error(), http.StatusServiceUnavailable)
		}
	})

	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
