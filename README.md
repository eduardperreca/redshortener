# redshortener

A URL shortener written in Go. Links are stored in Postgres, and Redis caches
lookups in front of it.

The project exists to study how a cache and a database work together, and what
changes when each of them gets replicas. The design notes below explain every
choice and the failure it guards against.

## Running it

```sh
docker compose up -d --build
```

Open http://localhost:8080. To run the app outside Docker, start only the
stores:

```sh
docker compose up -d postgres redis
go run .
```

## API

Create a link. `expires_in` is in seconds and optional; without it the link
never expires.

```sh
curl -X POST localhost:8080/api/links -d '{"url": "https://go.dev/doc/", "expires_in": 3600}'
```

```json
{
  "code": "tsLRfhS",
  "url": "https://go.dev/doc/",
  "created_at": "2026-10-28T19:22:34.511066Z",
  "expires_at": "2026-10-28T20:22:34.511066Z",
  "clicks": 0,
  "short_url": "http://localhost:8080/tsLRfhS"
}
```

| Request                  | Response                                                     |
| ------------------------ | ------------------------------------------------------------ |
| `POST /api/links`        | `201` with the link, `400` with `{"error": ...}`, `429` when rate limited |
| `GET /{code}`            | `302` to the URL, `404` if the code is unknown or expired    |
| `GET /api/links/{code}`  | `200` with the link and its click count, `404`               |
| `GET /healthz`           | `200` while the process is up                                |
| `GET /readyz`            | `200` when Postgres answers, `503` otherwise                 |
| `GET /metrics`           | Prometheus metrics                                           |

Accepted URLs are absolute `http` or `https` URLs of at most 2048 bytes,
without a user name or password, and not pointing to the shortener itself.

## Configuration

| Variable           | Default                                                        |
| ------------------ | -------------------------------------------------------------- |
| `ADDR`             | `:8080`                                                        |
| `BASE_URL`         | `http://localhost:8080`, used to build short URLs              |
| `DATABASE_URL`     | `postgres://redshortener:redshortener@localhost:5432/redshortener` |
| `REDIS_ADDR`       | `localhost:6379`                                               |
| `CLIENT_IP_HEADER` | empty: rate limit by the TCP peer address                      |

Behind a reverse proxy, every request comes from the proxy, so all clients
would share one rate limit. Set `CLIENT_IP_HEADER` to the header the proxy
writes the client address into, such as `X-Real-IP`, and only when the proxy
overwrites it: otherwise clients can set it themselves.

## How it works

### Codes

A code is 7 random characters from `[0-9A-Za-z]`, read from `crypto/rand`.
That is 62⁷, about 3.5·10¹², possible codes. The code is the primary key, and
the insert uses `ON CONFLICT DO NOTHING`, so Postgres rejects a duplicate and
the app draws again.

A counter encoded in base 62 would give shorter codes and never collide, but
anyone could walk through every link by counting. Random codes cannot be
guessed and need no shared counter.

Requests for anything that is not a well-formed code get a `404` before they
reach Redis or Postgres.

### Reading a link

Lookups follow the cache-aside pattern:

```
GET link:{code}                  hit: redirect
                                 miss:
SELECT url ... WHERE code = $1
SET link:{code} <url> EX <ttl>   then redirect
```

Postgres stays the only source of truth. Everything in Redis can be thrown away
and rebuilt from it.

The TTL is 24 hours with 10% random jitter, so links cached at the same time do
not all expire, and miss, at the same moment. For a link that expires sooner,
the TTL is cut to the time it has left, computed by Postgres from its own clock,
so that app servers with skewed clocks cannot keep an expired link alive.

### Links that do not exist

A client probing random codes would miss the cache on every request and send
every request to Postgres. To stop that, "not found" is cached too, as an empty
string, for 60 seconds.

Negative entries open a race. A lookup can miss in Postgres just before the
link is created, and write its "not found" just after:

```
lookup                          create
SELECT -> no row
                                INSERT
                                DEL link:{code}
SET link:{code} ""              (the new link is now hidden for 60s)
```

This is the stale set problem described in the Memcache at Facebook paper. The
fix here relies on links never changing once created:

- Creating a link writes it into the cache with a plain `SET`, which replaces
  any negative entry.
- Lookups write negative entries with `SET NX`, which does nothing if the key
  exists, so a late negative entry cannot replace a real one.

One window remains: if the `SET` on creation fails because Redis is
unreachable, a negative entry written earlier can hide the link for up to 60
seconds.

### When a popular link drops out of the cache

When a link that gets thousands of requests per second expires from Redis,
all of those requests miss at once and query Postgres for the same row. This
is a cache stampede.

Each instance runs lookups through `singleflight`: concurrent misses on the
same code wait for one query and share its result. Sending 300 requests, 100
at a time, at a link just removed from the cache gave 8 misses and 292 hits:
the requests that arrived while the query ran waited for it, and the rest
found the link back in the cache.

The protection is per instance, so with N instances Postgres still sees up to
N queries. Two known ways to go further were left out:

- Leases (Memcache at Facebook): the cache hands out one token per key, and
  only the holder may query the database. It is cluster-wide, but needs either
  a cache server that supports it or a lock emulated in Redis.
- Probabilistic early expiration (XFetch, Vattani et al.): each request may
  refresh an entry before it expires, with a probability that rises as expiry
  approaches. Hot keys are refreshed before they miss, without coordination.

The shared query runs with its own 5 second timeout instead of the first
caller's context, because if that client disconnects, the others waiting on
the same query should not fail with it.

### Counting clicks

Writing to Postgres on every redirect would put the database back on the path
the cache is there to protect. Instead, each redirect runs

```
HINCRBY clicks:pending <code> 1
```

and every 10 seconds each instance moves the pending counts to Postgres:

```
MULTI
HGETALL clicks:pending
DEL clicks:pending
EXEC

UPDATE links SET clicks = clicks + c.n
FROM unnest($codes, $counts) AS c(code, n)
WHERE links.code = c.code
```

`MULTI` runs both commands with nothing in between, so when several instances
flush at the same moment, each count is taken by exactly one of them. If the
`UPDATE` fails, the counts go back into the hash with `HINCRBY` and the next
flush retries them. That includes deadlocks between two instances updating the
same rows in a different order: Postgres aborts one of them, and its counts are
retried.

The cost is that a click is counted at most once, not exactly once. Counts are
lost if the process dies after taking them and before writing them, or if Redis
loses the hash. Click counts shown by the API lag behind by up to 10 seconds.

A first version used `RENAME clicks:pending clicks:flushing` instead of
`MULTI`. It works, but a crash between the rename and the delete leaves a
`clicks:flushing` key behind, and recovering it safely with several instances
is harder than not creating it.

### When Redis is down

Redis is a cache, so its failure should make the service slower, not
unavailable:

- Lookups that fail in Redis go to Postgres, still through `singleflight`.
- The Redis client gives up after 200 ms per read or write and 1 second to
  connect, with one retry. With the go-redis defaults, a hung Redis would delay
  every redirect by several seconds.
- `/readyz` only checks Postgres, so a load balancer keeps sending traffic.
- The rate limiter, [redlimit](https://github.com/eduardperreca/redlimit),
  fails open: link creation is not limited while Redis is down.
- Clicks are not counted.

### Redis configuration

`compose.yaml` runs Redis as a cache: 100 MB of memory, no RDB snapshots, no
AOF. Nothing in it needs to survive a restart except the pending click counts,
and losing those is accepted above.

The eviction policy is `volatile-lfu`. It only evicts keys that have a TTL,
which keeps `clicks:pending` safe, and it prefers to evict rarely used links
over recently used ones, which suits traffic where a few links get most of the
clicks.

### Postgres

- The table is created with `fillfactor = 90`. Click updates change no indexed
  column, so with free space on the page Postgres can make them HOT updates:
  the new row version stays on the same page and no index is touched.
- A partial index on `expires_at` covers only links that expire. Expired links
  are deleted once an hour.
- Migrations in `migrations/` run at startup in one transaction, behind an
  advisory lock, so instances that start together wait for each other.

### Redirect status

Redirects use `302`. Browsers cache a `301` indefinitely, so later clicks would
never reach the server and would not be counted, and an expired link would keep
working for anyone who had opened it.

## Metrics

| Metric                                        | Labels          |
| --------------------------------------------- | --------------- |
| `redshortener_cache_lookups_total`            | `result`: `hit`, `negative_hit`, `miss`, `error` |
| `redshortener_clicks_flushed_total`           |                 |
| `redshortener_http_request_duration_seconds`  | `route`, `code` |

The cache hit ratio is `hit / (hit + miss)`. The route label is the matched
pattern, such as `GET /{code}`, so it does not grow with the number of links.

## Development

```sh
docker compose up -d postgres redis
DATABASE_URL=postgres://redshortener:redshortener@localhost:5432/redshortener \
REDIS_ADDR=localhost:6379 go test -race ./...
```

Without the two variables, only the tests that need no database run.

## Roadmap

- v0.2: Redis Sentinel, with redirects read from Redis replicas.
- v0.3: a Postgres streaming replica for reads, and the read-your-writes
  problem it brings: a link created on the primary and missed on a lagging
  replica reads as not found.
- v0.4: several app instances behind Caddy.
- v0.5: Redis Cluster.
- v1.0: failover experiments, written up in `docs/`.

## References

- Nishtala et al., [Scaling Memcache at Facebook](https://www.usenix.org/conference/nsdi13/technical-sessions/presentation/nishtala), NSDI 2013. Leases, stale sets and thundering herds.
- Vattani, Chierichetti, Lowenstein, [Optimal Probabilistic Cache Stampede Prevention](https://doi.org/10.14778/2757807.2757813), PVLDB 8(8), 2015.
- Redis, [Key eviction](https://redis.io/docs/latest/develop/reference/eviction/).
- PostgreSQL, [Heap-Only Tuples](https://www.postgresql.org/docs/current/storage-hot.html).

## License

MIT
