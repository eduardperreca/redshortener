# redshortener

A URL shortener in Go: links live in Postgres, Redis caches them. Built to
learn how a cache and a database behave together, with replicas and failures.

```
redirect:  app ── GET ──────► Redis ── miss ─────────► Postgres
click:     app ── HINCRBY ──► Redis ── every 10 s ───► Postgres
```

## Run

```sh
docker compose up -d --build     # http://localhost:8080
```

Extra compose files add replicas. Combine them with `COMPOSE_FILE`:

```sh
COMPOSE_FILE=compose.yaml:compose.patroni.yaml:compose.scale.yaml docker compose up -d --build
```

| File                   | Adds                                                   |
| ---------------------- | ------------------------------------------------------ |
| `compose.sentinel.yaml`  | Redis primary, 2 replicas, 3 Sentinels                 |
| `compose.cluster.yaml`   | Redis Cluster, 3 primaries and 3 replicas              |
| `compose.pgreplica.yaml` | a Postgres read replica                                |
| `compose.patroni.yaml`   | 3 Postgres nodes with automatic failover (Patroni)     |
| `compose.scale.yaml`     | 3 app instances behind Caddy                           |

Use at most one Redis file and one Postgres file.

## API

```sh
curl -X POST localhost:8080/api/links -d '{"url": "https://go.dev", "alias": "go", "expires_in": 3600}'
```

`alias` and `expires_in` (seconds) are optional. Without an alias the link
gets a random 7-character code. The response includes a `token`, shown only
once, that changes or deletes the link:

```sh
curl -X PATCH localhost:8080/api/links/go -H "Authorization: Bearer $TOKEN" -d '{"url": "https://go.dev/doc"}'
curl -X DELETE localhost:8080/api/links/go -H "Authorization: Bearer $TOKEN"
```

| Request                 | Response                                                        |
| ----------------------- | --------------------------------------------------------------- |
| `POST /api/links`       | `201` link, `400` bad input, `409` alias taken, `429` rate limited |
| `GET /{code}`           | `302` to the URL, `404`                                         |
| `GET /api/links/{code}` | `200` link with click count, `404`                              |
| `PATCH /api/links/{code}` | `204`, `404` if unknown or wrong token, `503` retry           |
| `DELETE /api/links/{code}` | `204`, `404` if unknown or wrong token, `503` retry          |
| `GET /healthz`          | `200` while the process runs                                    |
| `GET /readyz`           | `200` if Postgres or Redis answers                              |
| `GET /metrics`          | Prometheus                                                      |

## Configuration

| Variable            | Meaning                                                    |
| ------------------- | ---------------------------------------------------------- |
| `DATABASE_URL`      | Postgres primary                                           |
| `DATABASE_READ_URL` | optional Postgres replica for reads                        |
| `REDIS_ADDR`        | one address, or several for a Cluster or for Sentinels     |
| `REDIS_MASTER`      | Sentinel master name; set it to use Sentinel               |
| `BASE_URL`          | used to build short URLs, default `http://localhost:8080`  |
| `ADDR`              | listen address, default `:8080`                            |
| `CLIENT_IP_HEADER`  | header with the client IP, behind a proxy that overwrites it |

## How it works

- **Redirect.** `GET link:{code}` in Redis. On a miss, read Postgres and cache
  the URL for 24 h ±10%, so entries cached together do not expire together.
- **Unknown codes** are cached as "not found" for 60 s, so probing random codes
  does not hit Postgres. Creating a link overwrites that entry; lookups write
  it with `SET NX`, so a late "not found" cannot hide a new link.
- **Stampede.** Concurrent misses on one code share one query (`singleflight`).
- **Clicks** never touch Postgres on the redirect. Each one is an `HINCRBY` on
  one of 16 hashes. Every 10 s each instance takes a hash with
  `MULTI { HGETALL, DEL }`, waits for a replica with `WAIT`, and writes all
  counts in one `UPDATE`. A click is counted at most once.
- **Codes and aliases** share the primary key, so `INSERT ... ON CONFLICT`
  settles duplicates, with no check-then-insert race.
- **Changing a link.** Only the SHA-256 of the token is stored. Writers
  `SET` the cache entry, lookups fill it with `SET NX`, so a lookup that read
  the old URL cannot cache it after the change. The `SET` runs before the
  commit, under the row lock: concurrent changes reach Redis in commit order,
  and if Redis fails the change rolls back (`503`, retry). Deleting makes the
  link expire now, which frees an alias at once.
- **Redis down.** Each Redis command gets 300 ms in total; after that, lookups
  go to Postgres.
- **Postgres replica.** A row missing on the replica is read again from the
  primary, so a new link is never cached as "not found".
- **`302`, not `301`**: browsers cache a `301` forever, so clicks would not be
  counted and expired links would keep working.

## What the experiments showed

Each line is a measurement; the scripts are in [`experiments/`](experiments/).

| Test                                                | Result                                                  |
| --------------------------------------------------- | ------------------------------------------------------- |
| Read a Redis replica right after a write            | 0 stale reads out of 10,000                             |
| Read a Postgres replica right after an insert       | 1,995 of 2,000 rows not there yet (about 1 ms behind)    |
| Stop the Redis primary (Sentinel)                   | no failed request; writes slow for about 6 s            |
| Stop a Redis Cluster primary                        | all 7,690 redirects `302`; 843 of 7,700 clicks lost     |
| Kill the Postgres primary (Patroni)                 | all 8,600 redirects `302`; creates fail for 19 s; 0 links lost |
| Stop etcd under Patroni                             | writes continue with `failsafe_mode`, all fail without it |
| Stop and kill app instances behind Caddy            | 0 of 5,842 requests failed                              |
| Fill Redis until it evicts                          | `volatile-lfu` kept 16 of 16 click hashes, `allkeys-lru` 7 |
| Change a link 2,000 times, readers on the replica   | `UPDATE` + `DEL` left the old URL cached 22 times, the app's way 0 |

## Bugs the experiments found

- `NewUniversalClient` with `MasterName` and `ReadOnly` sends writes to
  replicas, and they fail. `NewFailoverClusterClient` routes per command.
- go-redis timeouts multiply: 5 dial retries per attempt kept one create
  waiting over 10 s. Fixed with one 300 ms deadline per command.
- After a Cluster failover, go-redis kept using the dead node until the test
  ended, 26 s later: it reloads the slot map every 60 s. Fixed with a reload
  every 2 s and a 200 ms dial timeout.
- With `cluster-require-full-coverage yes`, one dead primary stops the whole
  cluster. A cache wants `no`.
- One click hash put every click on one Cluster node; hence 16 random hashes.
  A `MULTI` over all of them fails with `CROSSSLOT`; hence one per hash.
- Redirects waited for the click write and slowed from 200 to 40 a second
  during a failover. The click is now written in the background.
- `WAIT` only counts writes made on its own connection, so it must run on the
  connection that ran the `MULTI`.
- Invalidating with `UPDATE` then `DEL` caches old URLs: a reader misses
  right after the `DEL` and reads a replica that is a millisecond behind.
  Reading the primary, it did not happen in 2,000 changes.
- `/readyz` checked only Postgres, so every instance reported unready during a
  Postgres failover while redirects worked fine.
- Patroni refuses a `ttl` under 20 s, and a new replica waited minutes for a
  spread checkpoint until `basebackup: checkpoint: fast`.

## Left out

- Cross-instance stampede protection (leases, XFetch). A load is a 1 ms lookup
  by primary key, so there is little to save.
- Three etcd nodes and network partition tests. Everything runs on one Docker
  host.

## Development

```sh
docker compose up -d postgres redis
DATABASE_URL=postgres://redshortener:redshortener@localhost:5432/redshortener \
REDIS_ADDR=localhost:6379 go test -race ./...
```

## References

- Nishtala et al., [Scaling Memcache at Facebook](https://www.usenix.org/conference/nsdi13/technical-sessions/presentation/nishtala), NSDI 2013.
- Vattani et al., [Optimal Probabilistic Cache Stampede Prevention](https://doi.org/10.14778/2757807.2757813), PVLDB 2015.
- Redis: [replication](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/), [Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/), [Cluster spec](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/), [eviction](https://redis.io/docs/latest/develop/reference/eviction/), [WAIT](https://redis.io/docs/latest/commands/wait/).
- PostgreSQL: [standby servers](https://www.postgresql.org/docs/current/warm-standby.html), [`target_session_attrs`](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-TARGET-SESSION-ATTRS).
- Patroni: [replication modes](https://patroni.readthedocs.io/en/latest/replication_modes.html), [failsafe mode](https://patroni.readthedocs.io/en/latest/dcs_failsafe_mode.html).

## License

MIT
