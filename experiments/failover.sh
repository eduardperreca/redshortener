#!/usr/bin/env bash
# Sends redirects to 10 links from 4 clients for 35 seconds, breaks something
# 5 seconds in, and reports what the clients saw.
#
#   experiments/failover.sh sentinel   stops the Redis primary behind Sentinel
#   experiments/failover.sh cluster    stops one Redis Cluster primary
#   experiments/failover.sh app        stops one app instance, kills another
#
# Run it from the repository root, with COMPOSE_FILE naming the compose files
# the stack was started with, for example
#   export COMPOSE_FILE=compose.yaml:compose.cluster.yaml:compose.scale.yaml
set -euo pipefail
export LC_ALL=C

mode=${1:?usage: experiments/failover.sh sentinel|cluster|app}
url=${URL:-http://localhost:8080}
out=$(mktemp)
trap 'rm -f "$out"' EXIT

ip_of() { docker compose exec -T "$1" hostname -i | tr -d '\r'; }

redis_primary() {
	case $mode in
	sentinel)
		local ip
		ip=$(docker compose exec -T sentinel1 redis-cli -p 26379 sentinel get-master-addr-by-name mymaster | head -1 | tr -d '\r')
		for s in redis-primary redis-replica1 redis-replica2; do
			[ "$(ip_of $s)" = "$ip" ] && echo $s && return
		done ;;
	cluster)
		for n in 1 2 3 4 5 6; do
			docker compose exec -T node$n redis-cli role 2>/dev/null | head -1 | grep -q master && echo node$n && return
		done ;;
	esac
}

# Creating links is rate limited to 10 at once, then one a second.
codes=()
while [ ${#codes[@]} -lt 10 ]; do
	code=$(curl -s -X POST "$url/api/links" -d '{"url":"https://example.com/"}' | sed -nE 's/.*"code":"([^"]+)".*/\1/p')
	if [ -n "$code" ]; then codes+=("$code"); else sleep 1; fi
done
for c in "${codes[@]}"; do curl -s -o /dev/null "$url/$c"; done # fill the cache

client() {
	local end=$(($(date +%s) + 35))
	while [ "$(date +%s)" -lt $end ]; do
		for c in "${codes[@]}"; do
			echo "$(date +%s) $(curl -s -o /dev/null -w '%{http_code} %{time_total}' "$url/$c")" >>"$out"
		done
	done
}
for _ in 1 2 3 4; do client & done

start=$(date +%s)
since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sleep 5
case $mode in
sentinel | cluster)
	target=$(redis_primary)
	echo "+5s stopping Redis primary $target"
	docker compose stop "$target" >/dev/null 2>&1 ;;
app)
	apps=($(docker compose ps -q app))
	echo "+5s stopping one app instance (SIGTERM)"
	docker stop "${apps[0]}" >/dev/null
	sleep 5
	echo "+10s killing another (SIGKILL)"
	docker kill "${apps[1]}" >/dev/null
	sleep 5
	echo "+15s starting both again"
	docker start "${apps[0]}" "${apps[1]}" >/dev/null ;;
esac
wait

echo
echo "redirects by status:"
awk '{print $2}' "$out" | sort | uniq -c
echo "per second: redirects / slowest"
awk -v t="$start" '{s = $1 - t; n[s]++; if ($3 > m[s]) m[s] = $3}
	END {for (s in n) printf "%d %d %.2f\n", s, n[s], m[s]}' "$out" |
	sort -n | awk '{printf "+%ss %4d / %.2fs   ", $1, $2, $3} NR % 5 == 0 {print ""}'
echo
echo "errors the app logged, by message:"
docker compose logs --since "$since" app 2>/dev/null | grep -oE '"level":"(WARN|ERROR)","msg":"[^"]+' |
	sed -E 's/.*"msg":"//' | sort | uniq -c | sort -rn || true
if [ "$mode" != app ]; then
	echo
	echo "start $target again with: docker compose start $target"
fi
