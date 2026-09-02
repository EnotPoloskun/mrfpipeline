#!/bin/sh
# Measure one fresh private filter-catalog build.
# Configuration is read from the existing MRFPIPELINE_* environment. The only
# arguments are the exact payer and collection month (YYYY-MM).
set -eu

if [ "$#" -ne 2 ]; then
    exit 2
fi
payer=$1
month=$2
case "$payer" in
    ''|*[!a-z0-9._-]*) exit 2 ;;
esac
case "$month" in
    ????-0[1-9]|????-1[0-2]) : ;;
    *) exit 2 ;;
esac

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM
metrics="$work/metrics.json"
usage="$work/time.txt"

case "$(uname -s)" in
    Linux)
        /usr/bin/time -v mrfpipeline filters measure \
            --payer "$payer" --collection-month "$month" >"$metrics" 2>"$usage"
        rss_kib=$(sed -n 's/^[[:space:]]*Maximum resident set size (kbytes):[[:space:]]*//p' "$usage")
        case "$rss_kib" in ''|*[!0-9]*) exit 1 ;; esac
        rss_bytes=$((rss_kib * 1024))
        ;;
    Darwin)
        /usr/bin/time -l mrfpipeline filters measure \
            --payer "$payer" --collection-month "$month" >"$metrics" 2>"$usage"
        rss_bytes=$(sed -n 's/^[[:space:]]*\([0-9][0-9]*\)[[:space:]]*maximum resident set size$/\1/p' "$usage")
        case "$rss_bytes" in ''|*[!0-9]*) exit 1 ;; esac
        ;;
    *)
        exit 2
        ;;
esac

python3 - "$metrics" "$rss_bytes" <<'PY'
import json
import sys

fields = [
    "warehouse_schema_version",
    "provider_catalog_schema_version",
    "provider_catalog_release_month",
    "publication_generation",
    "candidate_output_count",
    "standard_fact_count",
    "billing_code_count",
    "code_filter_value_count",
    "plan_count",
    "plan_output_count",
    "output_code_network_count",
    "provider_filter_value_count",
    "duckdb_wall_time_ms",
    "postgres_population_wall_time_ms",
    "total_wall_time_ms",
    "peak_rss_bytes",
    "catalog_database_bytes",
]
with open(sys.argv[1], encoding="utf-8") as source:
    value = json.load(source)
if set(value) != set(fields):
    raise SystemExit(1)
value["peak_rss_bytes"] = int(sys.argv[2])
for name in fields:
    if name == "warehouse_schema_version" or name == "provider_catalog_release_month":
        if not isinstance(value[name], str):
            raise SystemExit(1)
    else:
        if isinstance(value[name], bool) or not isinstance(value[name], int) or value[name] < 0:
            raise SystemExit(1)
print(json.dumps({name: value[name] for name in fields}, separators=(",", ":")))
PY
