# Experiments

The measurements quoted in the main README come from these programs and
scripts. Run them from the repository root. Numbers depend on the machine; the
ones in the README were taken with Docker on an Apple Silicon laptop.

The scripts read the compose files from `COMPOSE_FILE`, so start the stack
with the same variable set:

```sh
export COMPOSE_FILE=compose.yaml:compose.sentinel.yaml:compose.pgreplica.yaml
docker compose up -d --build
```

The Go programs run inside the compose network, where the Redis and Postgres
containers are reachable by name. The network is called `redshortener_default`
when the repository directory is called `redshortener`.

## Replication lag

[`redislag`](redislag/main.go) sets a key on the Redis primary and reads it
from a replica right away, 10,000 times. [`pglag`](pglag/main.go) does the
same with an `INSERT` on the Postgres primary and a `SELECT` on the replica,
2,000 times. `-writers 8` adds background write load on the primary.

Needs `compose.sentinel.yaml` for Redis and `compose.pgreplica.yaml` for
Postgres.

```sh
docker run --rm --network redshortener_default -v "$PWD":/src -w /src golang:1.26 \
  go run ./experiments/redislag -writers 8
docker run --rm --network redshortener_default -v "$PWD":/src -w /src golang:1.26 \
  go run ./experiments/pglag -writers 8
```

Expect the Redis replica to have the key nearly every time, and the Postgres
replica to miss nearly every row by a millisecond or two. See
[What the experiments showed](../README.md#what-the-experiments-showed).

## Failover

[`failover.sh`](failover.sh) creates 10 links, sends redirects to them from 4
clients for 35 seconds, breaks something 5 seconds in, and prints the status
codes, redirects per second with the slowest of each second, and the errors
the app logged.

```sh
experiments/failover.sh sentinel   # needs compose.sentinel.yaml
experiments/failover.sh cluster    # needs compose.cluster.yaml
experiments/failover.sh postgres   # needs compose.patroni.yaml
experiments/failover.sh app        # needs compose.scale.yaml
```

`sentinel` and `cluster` stop the current Redis primary and print the command
to start it again; it comes back as a replica. `postgres` kills the Postgres
primary with `SIGKILL`, also creates a link every second, and checks
afterwards that every link it created is still there. `app` stops one app instance
with `SIGTERM`, kills another with `SIGKILL` 5 seconds later, and starts both
5 seconds after that.

Expect no failed redirects in any of them. With `postgres`, creates fail for
about 20 seconds and no created link is missing. During a Redis failover, click
writes to the missing primary fail and are logged as `counting click` until a
replica takes over. See
[What the experiments showed](../README.md#what-the-experiments-showed).

## Eviction

[`eviction.sh`](eviction.sh) shrinks the plain setup's Redis to 4 MB, adds the
16 pending click hashes, and writes 40,000 cached links. It does this once
with `volatile-lfu` and once with `allkeys-lru`, then puts the settings back.

```sh
COMPOSE_FILE=compose.yaml experiments/eviction.sh
```

```
volatile-lfu: evicted 17979 keys, click hashes left: 16 of 16
allkeys-lru: evicted 17997 keys, click hashes left: 7 of 16
```

Only keys with a TTL can be evicted under `volatile-lfu`, and the click hashes
have none. Under `allkeys-lru` about half of them were evicted, and the counts
in them lost.
