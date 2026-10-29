#!/usr/bin/env bash
# Fills a small Redis with cached links until it evicts, once per eviction
# policy, and counts how many of the 16 pending click hashes survive. The
# hashes have no TTL, the links do.
#
# Run it from the repository root against the plain compose setup. It flushes
# the redis service and restores its settings afterwards.
set -euo pipefail

docker compose exec -T redis sh -s <<'EOF'
for policy in volatile-lfu allkeys-lru; do
	redis-cli FLUSHALL >/dev/null
	redis-cli CONFIG SET maxmemory 4mb >/dev/null
	redis-cli CONFIG SET maxmemory-policy $policy >/dev/null
	redis-cli CONFIG RESETSTAT >/dev/null
	for i in $(seq 0 15); do redis-cli HINCRBY clicks:pending:$i somecode 1 >/dev/null; done
	seq 1 40000 | awk '{printf "SET link:%07d https://example.com/a/long/path/%d EX 86400\r\n", $1, $1}' |
		redis-cli --pipe >/dev/null
	evicted=$(redis-cli INFO stats | grep evicted_keys: | tr -d '\r' | cut -d: -f2)
	left=$(redis-cli --scan --pattern 'clicks:*' | wc -l)
	echo "$policy: evicted $evicted keys, click hashes left: $left of 16"
done
redis-cli FLUSHALL >/dev/null
redis-cli CONFIG SET maxmemory 100mb >/dev/null
redis-cli CONFIG SET maxmemory-policy volatile-lfu >/dev/null
EOF
