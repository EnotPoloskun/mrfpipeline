#!/bin/sh
# Scale MRF parser replicas and resident capacity together.
# Does not recreate existing mrf containers (in-flight parses keep running).
# Usage: scripts/scale-mrf.sh <N>
set -eu

cd "$(dirname "$0")/.."

if [ ! -f docker-compose.yml ]; then
	echo "run from the mrfpipeline repository (docker-compose.yml is missing)" >&2
	exit 1
fi

n=${1:-}
if ! printf '%s' "$n" | grep -Eq '^[1-9][0-9]*$'; then
	echo "usage: scripts/scale-mrf.sh <positive-integer>" >&2
	exit 1
fi

if [ ! -f .env ]; then
	cp .env.example .env
fi

tmp=$(mktemp)
awk -v n="$n" '
	BEGIN { done = 0 }
	/^MRFPIPELINE_MRF_RESIDENT_CAPACITY=/ {
		print "MRFPIPELINE_MRF_RESIDENT_CAPACITY=" n
		done = 1
		next
	}
	{ print }
	END { if (!done) print "MRFPIPELINE_MRF_RESIDENT_CAPACITY=" n }
' .env >"$tmp"
mv "$tmp" .env

consumer_n=$(docker compose ps --format '{{.Service}}' | grep -c '^consumer$' || true)
if [ "$consumer_n" -lt 1 ]; then
	consumer_n=1
fi

docker compose up -d --force-recreate --no-deps control
docker compose --profile workers up -d --no-recreate --scale "mrf=$n" --scale "consumer=$consumer_n" mrf consumer

echo "mrf replicas=$n resident_capacity=$n consumer replicas=$consumer_n"
echo "wait for control worker_started, then: docker compose --profile operator run --rm cli month status"
