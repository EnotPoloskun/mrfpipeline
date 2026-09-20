#!/bin/sh
# Scale consumer replicas. Does not change MRF resident capacity or recreate
# existing mrf containers.
# Usage: scripts/scale-consumer.sh <N>
set -eu

cd "$(dirname "$0")/.."

if [ ! -f docker-compose.yml ]; then
	echo "run from the mrfpipeline repository (docker-compose.yml is missing)" >&2
	exit 1
fi

n=${1:-}
if ! printf '%s' "$n" | grep -Eq '^[1-9][0-9]*$'; then
	echo "usage: scripts/scale-consumer.sh <positive-integer>" >&2
	exit 1
fi

mrf_n=$(docker compose ps --format '{{.Service}}' | grep -c '^mrf$' || true)
if [ "$mrf_n" -lt 1 ]; then
	mrf_n=1
fi

docker compose --profile workers up -d --no-recreate --scale "mrf=$mrf_n" --scale "consumer=$n" mrf consumer

echo "consumer replicas=$n mrf replicas=$mrf_n"
