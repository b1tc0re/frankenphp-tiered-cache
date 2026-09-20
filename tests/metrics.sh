#!/usr/bin/env bash

set -euo pipefail

: "${IMAGE:?IMAGE must be set}"
: "${REDIS_NETWORK:?REDIS_NETWORK must be set}"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
container="franken-cache-metrics-$RANDOM"
port=""

cleanup() {
	docker rm -f "$container" >/dev/null 2>&1 || true
}

trap cleanup EXIT

if ! docker run -d \
	--name "$container" \
	--network "$REDIS_NETWORK" \
	-p 127.0.0.1::2019 \
	--env FRANKEN_CACHE_REDIS_ADDR=redis:6379 \
	--entrypoint frankenphp \
	"$IMAGE" \
	run --config /opt/frankenphp-tiered-cache/tests/metrics.Caddyfile --adapter caddyfile >/dev/null; then
	docker logs "$container" 2>&1 || true
	exit 1
fi

port="$(docker port "$container" 2019/tcp | sed -n 's/.*:\([0-9][0-9]*\)$/\1/p' | head -n 1)"
if [[ -z "$port" ]]; then
	echo "Could not determine the published Caddy admin port." >&2
	docker logs "$container" 2>&1 || true
	exit 1
fi

scrape_metrics() {
	local attempt
	metrics=""
	for attempt in $(seq 1 60); do
		if metrics="$(curl --silent --show-error --fail "http://127.0.0.1:${port}/metrics" 2>/dev/null)"; then
			return 0
		fi
		metrics=""
		sleep 0.25
	done
	return 1
}

assert_metrics() {
	if ! grep -q '^franken_cache_build_info{version="[^"]*"} 1$' <<<"$metrics"; then
		echo "franken_cache_build_info is missing from /metrics." >&2
		printf '%s\n' "$metrics" >&2
		return 1
	fi

	if ! grep -q '^franken_cache_invalidation_ready 1$' <<<"$metrics"; then
		echo "franken_cache_invalidation_ready is not healthy in /metrics." >&2
		printf '%s\n' "$metrics" >&2
		return 1
	fi
}

if ! scrape_metrics; then
	echo "Caddy metrics endpoint did not become ready." >&2
	docker logs "$container" 2>&1 || true
	exit 1
fi
if ! assert_metrics; then
	docker logs "$container" 2>&1 || true
	exit 1
fi

if ! curl --silent --show-error --fail \
	-X POST "http://127.0.0.1:${port}/load?adapter=caddyfile" \
	-H 'Content-Type: text/caddyfile' \
	--data-binary "@${script_dir}/metrics.Caddyfile" >/dev/null; then
	echo "Caddy reload request failed." >&2
	docker logs "$container" 2>&1 || true
	exit 1
fi

if ! scrape_metrics; then
	echo "Caddy metrics endpoint did not recover after reload." >&2
	docker logs "$container" 2>&1 || true
	exit 1
fi
if ! assert_metrics; then
	docker logs "$container" 2>&1 || true
	exit 1
fi

echo "franken_cache Prometheus metrics smoke and reload test passed."
