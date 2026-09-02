#!/bin/sh
# Explain fixed catalog lookup queries into an operator-selected private file.
# Arguments: payer, collection month (YYYY-MM), billing-code type, billing code,
# plan ID, and a private output path. Configuration comes from the existing
# MRFPIPELINE_DATABASE_URL environment variable.
set -eu
umask 077

if [ "$#" -ne 6 ]; then
    exit 2
fi
payer=$1
month=$2
code_type=$3
code=$4
plan_id=$5
output=$6
case "$payer" in ''|*[!a-z0-9._-]*) exit 2 ;; esac
case "$month" in ????-0[1-9]|????-1[0-2]) : ;; *) exit 2 ;; esac
case "$code_type" in CPT|HCPCS) : ;; *) exit 2 ;; esac
case "$code" in ''|*[!A-Za-z0-9._-]*) exit 2 ;; esac
case "$plan_id" in ''|*[!0-9]*) exit 2 ;; esac
case "$output" in ''|*[!A-Za-z0-9._/-]*) exit 2 ;; esac

month_date="$month-01"
: "${MRFPIPELINE_DATABASE_URL:?}"

psql "$MRFPIPELINE_DATABASE_URL" --no-psqlrc --set ON_ERROR_STOP=1 \
    --set payer="$payer" --set month_date="$month_date" \
    --set code_type="$code_type" --set code="$code" --set plan_id="$plan_id" \
    >"$output" <<'SQL'
BEGIN READ ONLY;
SELECT COUNT(*)::int AS active_found
FROM mrfweb.active_release_catalogs
WHERE payer_id = :'payer' AND collection_month = :'month_date'::date
\gset
\if :active_found = 0
\quit 3
\endif

SELECT catalog_id AS active_catalog_id,
       provider_catalog_schema_version AS active_schema,
       provider_catalog_release_month AS active_month
FROM mrfweb.active_release_catalogs
WHERE payer_id = :'payer' AND collection_month = :'month_date'::date
\gset

SELECT MIN(id) AS plan_lo, MAX(id) AS plan_hi
FROM mrfweb.release_plans
WHERE catalog_id = :active_catalog_id
\gset

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT billing_code_type, billing_code, observation_count, unmodified_observation_count
FROM mrfweb.release_billing_codes
WHERE catalog_id = :active_catalog_id
  AND billing_code_type = :'code_type'
  AND billing_code LIKE :'code' || '%'
ORDER BY billing_code_type COLLATE "C", billing_code COLLATE "C";

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text
FROM mrfweb.release_plans
WHERE catalog_id = :active_catalog_id
  AND search_text COLLATE "C" LIKE '' || '%' ESCAPE E'\\'
  AND (search_text COLLATE "C", id) > ('' COLLATE "C", 0::bigint)
ORDER BY search_text COLLATE "C", id
LIMIT 50;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT filter_kind, filter_value, display_label, observation_count
FROM mrfweb.release_code_filter_values
WHERE catalog_id = :active_catalog_id
  AND billing_code_type = :'code_type'
  AND billing_code = :'code'
  AND filter_kind = 'modifier'
ORDER BY filter_value COLLATE "C";

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT filter_kind, filter_value, display_label, observation_count
FROM mrfweb.release_code_filter_values
WHERE catalog_id = :active_catalog_id
  AND billing_code_type = :'code_type'
  AND billing_code = :'code'
  AND filter_kind = 'place_of_service'
ORDER BY filter_value COLLATE "C";

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT filter_kind, filter_value, display_label, observation_count
FROM mrfweb.release_code_filter_values
WHERE catalog_id = :active_catalog_id
  AND billing_code_type = :'code_type'
  AND billing_code = :'code'
  AND filter_kind = 'setting'
ORDER BY filter_value COLLATE "C";

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT network_name, SUM(observation_count)::bigint AS observation_count
FROM mrfweb.release_output_code_networks
WHERE catalog_id = :active_catalog_id
  AND billing_code_type = :'code_type'
  AND billing_code = :'code'
GROUP BY network_name
ORDER BY network_name COLLATE "C";

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH selected_outputs AS (
    SELECT DISTINCT output_id
    FROM mrfweb.release_plan_outputs
    WHERE catalog_id = :active_catalog_id
      AND plan_id = ANY (ARRAY[:plan_id]::bigint[])
)
SELECT n.network_name, SUM(n.observation_count)::bigint AS observation_count
FROM selected_outputs AS selected
JOIN mrfweb.release_output_code_networks AS n
  ON n.catalog_id = :active_catalog_id AND n.output_id = selected.output_id
WHERE n.billing_code_type = :'code_type' AND n.billing_code = :'code'
GROUP BY n.network_name
ORDER BY network_name COLLATE "C";

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH selected_outputs AS (
    SELECT DISTINCT output_id
    FROM mrfweb.release_plan_outputs
    WHERE catalog_id = :active_catalog_id
      AND plan_id = ANY (ARRAY[:plan_lo, :plan_hi]::bigint[])
)
SELECT n.network_name, SUM(n.observation_count)::bigint AS observation_count
FROM selected_outputs AS selected
JOIN mrfweb.release_output_code_networks AS n
  ON n.catalog_id = :active_catalog_id AND n.output_id = selected.output_id
WHERE n.billing_code_type = :'code_type' AND n.billing_code = :'code'
GROUP BY n.network_name
ORDER BY network_name COLLATE "C";

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT filter_value, provider_count
FROM mrfweb.release_provider_filter_values
WHERE catalog_id = :active_catalog_id
  AND filter_kind = 'state' AND parent_value = ''
ORDER BY filter_value COLLATE "C";

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT filter_value, provider_count
FROM mrfweb.release_provider_filter_values
WHERE catalog_id = :active_catalog_id
  AND filter_kind = 'city'
  AND parent_value = (
      SELECT filter_value
      FROM mrfweb.release_provider_filter_values
      WHERE catalog_id = :active_catalog_id
        AND filter_kind = 'state' AND parent_value = ''
      ORDER BY filter_value COLLATE "C"
      LIMIT 1
  )
ORDER BY filter_value COLLATE "C";

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT filter_value, provider_count
FROM mrfweb.release_provider_filter_values
WHERE catalog_id = :active_catalog_id
  AND filter_kind = 'taxonomy' AND parent_value = ''
ORDER BY filter_value COLLATE "C";
COMMIT;
SQL
