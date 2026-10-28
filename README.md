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

With a Redis primary, two replicas and three Sentinels instead of a single
Redis:

```sh
docker compose -f compose.yaml -f compose.sentinel.yaml up -d --build
```

With a Postgres streaming replica for reads, alone or together with the
Sentinel setup:

```sh
docker compose -f compose.yaml -f compose.pgreplica.yaml up -d --build
docker compose -f compose.yaml -f compose.sentinel.yaml -f compose.pgreplica.yaml up -d --build
```

With three app instances behind Caddy, which then serves port 8080. Add
`-f compose.scale.yaml` to any of the commands above:

```sh
docker compose -f compose.yaml -f compose.scale.yaml up -d --build
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
| `REDIS_ADDR`       | `localhost:6379`; comma-separated Sentinels with `REDIS_MASTER` |
| `REDIS_MASTER`     | empty; the Sentinel master name to go through Sentinel         |
| `DATABASE_READ_URL` | empty; a Postgres replica for cache misses and stats          |
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
- Every Redis command or pipeline gets 300 ms in total, dialing and the one
  retry included. The budget is a go-redis hook that sets a context deadline,
  and `ContextTimeoutEnabled` makes the deadline apply to socket reads and
  writes as well.
- `/readyz` only checks Postgres, so a load balancer keeps sending traffic.
- The rate limiter, [redlimit](https://github.com/eduardperreca/redlimit),
  fails open: link creation is not limited while Redis is down.
- Clicks are not counted.

A budget is needed because per-step timeouts do not add up to a bound. The
first version set a 1 second dial timeout, 200 ms read and write timeouts and
one retry, which looks like at most a few hundred milliseconds. But go-redis
also retries dialing 5 times per attempt, each time with the full dial
timeout. When the Redis primary was stopped during a test, a rate-limited
create spent 5 seconds dialing, retried, spent 5 more, and was cut off by the
server's 10 second write timeout.

### Redis replicas

With `REDIS_MASTER` set, the app asks the Sentinels in `REDIS_ADDR` where the
primary is and reads links from the replicas. Redis replicates asynchronously:
the primary answers a write before the replicas have it.

#### Which command goes where

The client is a go-redis failover cluster client with `ReplicaOnly`. It treats
the primary and its replicas as one shard of a cluster, sends commands that
Redis flags as read-only to a replica, and everything else to the primary.

| Command                        | Node    |
| ------------------------------ | ------- |
| `GET link:{code}`              | replica |
| `SET`, `SET NX`, `HINCRBY`     | primary |
| `MULTI` (click flush)          | primary |
| `EVALSHA` (rate limiter)       | primary |

This was checked with `INFO commandstats` on each node: 50 redirects gave 25
`GET`s on each replica and none on the primary.

The obvious configuration does something else. `NewUniversalClient` with a
`MasterName` and `ReadOnly: true` returns a plain failover client with
`ReplicaOnly`, which connects only to replicas and sends them the writes too.
Those fail with `READONLY`. Routing by command needs
`NewFailoverClusterClient`, or `RouteRandomly`, which spreads reads over the
primary and the replicas.

#### Stale reads

A read from a replica can return an older value than the primary holds. Here
that matters in two places:

- A link read right after it is created can miss on the replica. That costs
  one Postgres query, then the link is cached.
- If a code was probed in the last 60 seconds, the replica can still hold the
  negative entry after the primary has replaced it with the new link. The link
  then reads as not found until the replica catches up.

In practice the window is small. 10,000 rounds of `SET` on the primary followed
right away by `GET` on a replica, from the same client, found the key on the
replica every time, also with 8 other connections writing 100 KB values to the
primary. Redis sends a write to its replicas in the same event loop iteration
as the reply to the client, so a stale read needs a replication link that is
slower than the client's own round trip to the replica: a replica in another
zone, a congested network, or a replica that is still resyncing.

Replicas never delete expired keys on their own; they wait for the `DEL` from
the primary. They do answer reads of a key that is past its TTL as if it did
not exist, so a link whose cache entry has expired is not served from a
replica.

#### Failover

Stopping the primary while sending a redirect and a create every 100 ms:

- The Sentinels agreed the primary was down after 5 seconds
  (`down-after-milliseconds`) and promoted a replica 1.2 seconds later.
- All 202 redirects and all 202 creates succeeded. Redirects kept hitting the
  cache on the replicas.
- From 2 to 8 seconds after the stop, each write to Redis used its whole 300
  ms budget, so redirects took up to 310 ms, creates up to 630 ms, and the loop
  went from 6 iterations a second to 1. Clicks and rate limiting stopped for
  those seconds. The app needed about 2 seconds after the promotion to send
  writes to the new primary.
- When the old primary came back, Sentinel made it a replica of the new one.

Two things can go wrong in a failover that did not show in this test:

- Writes the old primary accepted but had not sent to a replica are lost. For
  the cache that means misses. For the click flush it is worse: if the primary
  answers `MULTI { HGETALL, DEL }` and dies before the `DEL` reaches a replica,
  the new primary still has the counts, and they are flushed a second time.
  `WAIT 1 <timeout>` after the flush would make the app wait for a replica to
  confirm the `DEL`, which narrows this window but does not close it.
- Redis runs without persistence here. If a primary restarts fast enough that
  Sentinel has not yet failed over, it comes back empty, and the replicas
  resync from it and empty themselves too. For a cache that means a cold start
  and lost click counts. If the data mattered, the primary would need
  persistence, or must not be restarted automatically.

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

### Postgres replica

With `DATABASE_READ_URL` set, cache misses and `GET /api/links/{code}` read
from a streaming replica. Creating links, flushing clicks and deleting expired
links stay on the primary.

#### How far behind the replica is

Postgres confirms a commit once the WAL is flushed on the primary. The
replica receives the WAL after that, writes it and replays it, so a read on
the replica right after a commit usually comes too early. 2,000 rounds of
`INSERT` on the primary followed by `SELECT` of the same row on the replica:

| Primary load        | Rows missing on the replica | Time until visible |
| ------------------- | --------------------------- | ------------------ |
| none                | 1,995 of 2,000              | p50 0.9 ms, p99 2.2 ms, max 6.5 ms |
| 8 concurrent writers | 1,999 of 2,000             | p50 2.3 ms, p99 5.6 ms, max 31 ms  |

That is the opposite of the Redis result above, where the replica had every
key by the time the client asked.

#### Read-your-writes

Creating a link writes it to the cache, so it is normally read from Redis,
not from the replica. When that write did not happen, because Redis was
unreachable or evicted the key, a redirect right after the create misses the
cache and asks the replica, which most likely does not have the row yet. On
its own that is a 404 for a few milliseconds. Combined with negative caching
it is worse: the "not found" is written to Redis and the link stays hidden for
a minute.

So a read that finds no row on the replica, or fails there, runs again on the
primary, and only a "not found" from the primary is cached. Unknown codes cost
two queries the first time and are then served from the negative cache.
`TestReadAfterCreate` creates a link, removes it from the cache and resolves it
right away, 100 times. Without the retry on the primary it fails in the first
round.

Other ways to get read-your-writes, not used here:

- Remember the primary's WAL position after the insert
  (`pg_current_wal_lsn()`) and read from the replica only once
  `pg_last_wal_replay_lsn()` there has reached it. Exact, but the position has
  to travel with the client, in a cookie for example.
- `synchronous_commit = remote_apply` with the replica listed in
  `synchronous_standby_names`: the commit returns only after the replica has
  applied it. Every insert then waits for the replica, and stops if the replica
  is down.

#### When the replica is down

Reads go to the primary after the replica fails, and `/readyz` only checks the
primary. A replica that hangs instead of refusing connections makes each read
wait for the connection attempt first; set `connect_timeout` in
`DATABASE_READ_URL` to bound that.

The app does not fail over the primary. That needs a tool such as Patroni and
is out of scope here.

### Several app instances

The app keeps no state of its own between requests: the cache, the pending
click counts and the rate limits live in Redis, the links in Postgres. So
instances can be added behind a load balancer without changes.
`compose.scale.yaml` runs three behind Caddy, with all the other compose files
included, and the following was checked there.

**Migrations run once.** The three instances started at the same moment on an
empty database. One applied the migration; the other two waited on the
advisory lock and then found it applied.

**The rate limit is shared.** Twelve creates from one client gave ten `201`s
and two `429`s. The `201`s were served by the instances 2, 4 and 4 times: the
limit belongs to the client, not to the instance that happens to answer.

Caddy overwrites `X-Real-IP` with the address it sees. A test that sent a
different `X-Real-IP` with every request was still limited as a single client,
so the header cannot be used to dodge the limit.

**Stampede protection is per instance.** `singleflight` only merges misses
inside one process. Removing a hot link from the cache and sending 300
requests, 100 at a time, gave between 1 and 5 misses over the three instances,
never more than 2 on one.

**Stopping an instance loses no requests.** Four clients sent a redirect and a
create in a loop for 30 seconds, 5,842 requests in all. During that time one
instance was stopped with `SIGTERM`, another killed with `SIGKILL`, and both
were started again. No request failed. The slowest took 290 ms, at the moment
of the stop: Caddy could not reach the stopping instance and retried on
another after its default `lb_try_interval` of 250 ms. An instance that gets
`SIGTERM` finishes the requests it has, flushes the click counts and exits.

**Clicks are counted once.** Those 2,921 redirects all went to one link, and
after the next flush it had 2,921 clicks in Postgres. Pending counts are kept
in Redis, not in the process, so killing an instance loses none of them, except
for a batch it has taken from Redis and not written yet. Two instances
flushing at once can update the same rows in different orders and deadlock;
Postgres then aborts one, whose counts go back to Redis. That did not happen
here: `pg_stat_database.deadlocks` stayed at 0.

Metrics are per instance. Prometheus has to scrape each instance, not the load
balancer, which would return a different instance's numbers on each scrape.

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
`TestReadAfterCreate` also needs `DATABASE_READ_URL`, pointing to a replica of
the same database; CI has no replica and skips it.

## Roadmap

- v0.2: Redis Sentinel, with redirects read from Redis replicas. Done.
- v0.3: a Postgres streaming replica for reads, and the read-your-writes
  problem it brings. Done.
- v0.4: several app instances behind Caddy. Done.
- v0.5: Redis Cluster.
- v1.0: failover experiments, written up in `docs/`.

## References

- Nishtala et al., [Scaling Memcache at Facebook](https://www.usenix.org/conference/nsdi13/technical-sessions/presentation/nishtala), NSDI 2013. Leases, stale sets and thundering herds.
- Vattani, Chierichetti, Lowenstein, [Optimal Probabilistic Cache Stampede Prevention](https://doi.org/10.14778/2757807.2757813), PVLDB 8(8), 2015.
- Redis, [Key eviction](https://redis.io/docs/latest/develop/reference/eviction/).
- Redis, [Replication](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/), including the safety of replication when the primary has persistence turned off.
- Redis, [High availability with Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/).
- PostgreSQL, [Log-Shipping Standby Servers](https://www.postgresql.org/docs/current/warm-standby.html) and [synchronous_commit](https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-SYNCHRONOUS-COMMIT).
- PostgreSQL, [Heap-Only Tuples](https://www.postgresql.org/docs/current/storage-hot.html).

## License

MIT
