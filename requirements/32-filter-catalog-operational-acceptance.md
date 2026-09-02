# Story 32: Filter catalog operational acceptance

## Status

Implemented.

## User story

As a pipeline operator, I want permanent synthetic acceptance and optional real
warehouse evidence for release filter catalogs so I can trust publication,
rollback, lookup semantics, resource use, and failure behavior before a public
web process depends on them.

## Context

Stories 28–31 add a new schema, pinned DuckDB extraction, explicit build CLI,
and catalog-aware release publication. Story 32 completes the permanent
operational acceptance boundary; the implementation is not a future placeholder.

```text
pipeline release state
→ exact consumer warehouse outputs
→ DuckDB compact extraction
→ PostgreSQL catalog
→ publication generation
→ stable read-only serving views
```

This story supplies deterministic end-to-end acceptance, packaging checks,
operator documentation, and descriptive real-data performance evidence. It
adds no web server/UI and no new product behavior beyond convergence needed to
satisfy the approved contracts.

## Dependencies

Completed implementations of Stories 28–32 plus the existing Story 22 local
Compose topology and Story 27 incremental publication fixture behavior.

## Goal

1. Add one permanent synthetic filter-catalog acceptance that creates real
   parser/consumer publications through supported pipeline paths, builds
   catalogs with pinned DuckDB, publishes two incremental generations, verifies
   exact PostgreSQL serving queries, switches months, and rolls back.
2. Add one opt-in real-warehouse measurement command that produces only
   sanitized counts/timings/resource evidence.
3. Verify Docker packaging, migration, crash/retry, redaction, and operator
   commands on Linux CI and supported local architectures.
4. Converge README, DESIGN, CLI help, Docker guidance, failure-code docs, and
   story status/authority after implementation.

## Product decisions

- Permanent CI acceptance uses small deterministic synthetic data with hand-
  computed expected rows. It does not depend on private payer files, the local
  184 MB warehouse, external URLs, or licensed code definitions.
- Real warehouse evidence is opt-in, local, read-only, sanitized, and not a
  performance SLA.
- Use exact DuckDB `v1.5.5` in tests and runtime packaging.
- Do not mock DuckDB SQL semantics in the end-to-end acceptance.
- Do not build final consumer Parquet directly to bypass supported publication
  paths where existing consumer-owned fixtures can produce it.
- Tests may create private temporary warehouses/databases and must remove only
  their own temporary paths.
- No test mutates a developer/operator warehouse supplied for real evidence.
- No HTTP/browser/UI test is added.

## Permanent synthetic fixture

Create a compact scenario with at least:

```text
payer-a / 2026-08
  output A published generation 1
  output B becomes plan-ready and is published generation 2
  output C shares payer/month but is never selected/published

payer-a / 2026-09
  output D published generation 1

payer-b / 2026-07
  output E published generation 1
```

Fixture construction deliberately separates orchestration rows from publication
bytes:

- filters/release commands accept any valid lower-ASCII payer identifier;
- create synthetic monthly releases, sources, selected membership, snapshots,
  plans, and attachment state for `payer-a`/`payer-b` directly in the
  disposable test database; do not invent a non-UHC discovery adapter;
- produce A, B, D, and E through real public parser → consumer publication and
  plan-attachment paths, using output IDs derived from their inserted pipeline
  snapshot IDs; and
- produce C by calling public standalone `mrfconsumer.Ingest` with payer-a,
  August, and an unrelated valid output ID, then public
  `mrfconsumer.AttachPlans` for that same output; C therefore has real warehouse
  facts/plan-association parts but no pipeline snapshot, `mrf_plans` row, or
  `monthly_release_outputs` membership.

Tests may construct domain prerequisites directly but never hand-write final
consumer Parquet. This preserves multi-payer release coverage and proves that
an unrelated warehouse output is excluded without expanding production
discovery beyond UHC.

The provider catalog contains several exact NPIs proving:

- one NPI with psychiatry taxonomy in FL/miami;
- one NPI with another taxonomy in FL/miami;
- one NPI with psychiatry taxonomy in another state;
- one group containing several reachable NPIs;
- repeated membership paths for one NPI; and
- catalog providers not reachable from any selected output.

Facts include:

- CPT and HCPCS codes;
- standard `negotiated` non-null rates;
- one `percentage` row;
- one null-rate negotiated row;
- no-modifier, repeated modifier, and two-distinct-modifier rows;
- numeric POS, `CSTM-00`, repeated POS, and malformed composite stored POS;
- several billing classes/settings/arrangements;
- one past expiration that still participates;
- one code/network present only in output A;
- one code/network present only in B;
- same network in A and B;
- conflicting/empty service labels; and
- equal-frequency label candidates requiring lexical tie-break.

Plans include:

- one canonical plan shared by A and B;
- another plan only on A;
- another plan only on B;
- same plan ID with another issuer/name;
- sponsor variants that collapse under the five-field identity; and
- one plan on excluded output C that must never appear.

Every exact expected catalog row/count is written by hand in test code. Do not
calculate expected output through the production extractor or another SQL query
that repeats its logic.

## End-to-end acceptance sequence

### Schema and first build

1. Migrate a fresh PostgreSQL database through migration 0009.
2. Produce candidate output A through supported consumer ingest/plan attachment
   fixture paths.
3. Run `filters build` for payer-a/August.
4. Verify one ready generation-1 catalog and every exact child row.
5. Verify active serving views remain empty before activation.
6. Run first `month activate`.
7. Verify output membership and catalog become published in one transaction.
8. Verify active views expose exactly A/generation 1.

### Incremental generation

1. Complete output B while August is active.
2. Verify existing generation-1 catalog remains unchanged.
3. Run activation before build and require missing/not-ready failure with no
   output/catalog mutation.
4. Build prospective generation 2 over A+B.
5. Activate and verify only B is newly inserted, release generation becomes 2,
   and generation-2 catalog publishes atomically.
6. Run a separate disposable stale-race fixture: build one ready candidate,
   make another output publishable, prove activation rejects the stale catalog,
   then discard that fixture. Do not attempt to reverse a supported additive
   publishability transition.
7. Verify generation-1 catalog remains byte/value unchanged.
8. Run idempotent build and activation; require `unchanged`, no DuckDB on build,
   and unchanged timestamps/generation/membership.

### Month switch and rollback

1. Build/publish payer-a September output D.
2. Verify August becomes inactive and September active.
3. Verify active views select only September catalog/output.
4. Reactivate August without rebuilding.
5. Verify exact generation-2 August catalog/output membership returns and
   September becomes inactive.
6. Prove no greatest-month or all-payer/month warehouse inference.

### Multiple payers

Publish payer-b independently and verify the active views queried without a
payer predicate contain the complete union of each payer's active
catalog/output relation while every filter query remains catalog-scoped.

## Required serving-query assertions

Execute these through read-only PostgreSQL transactions using only `mrfweb`
serving tables/views.

Direct parameterized SQL over the published catalog tables is the normative
serving surface; do not add one view per filter. Every query first obtains
`catalog_id` from globally fail-closed `active_release_catalogs` and binds that
ID. SQL never accepts a caller-provided column, table, operator, order, or raw
fragment.

Required query/result contracts follow.

### Active catalog and outputs

```sql
SELECT catalog_id, payer_id, collection_month, publication_generation,
       provider_catalog_schema_version, provider_catalog_release_month
FROM mrfweb.active_release_catalogs
WHERE ($1::text IS NULL OR payer_id = $1)
ORDER BY payer_id COLLATE "C";
```

Then read exact outputs from `active_release_outputs` by bound `catalog_id`,
ordered by `output_id COLLATE "C"`. Zero rows always produce the public
unavailable state, both when no payer is active and when global fail-closed
suppression detects one inconsistent active payer. The web role does not
distinguish those causes and never serves remaining payers.

### Billing-code lookup

```sql
SELECT billing_code_type, billing_code, billing_code_type_version,
       warehouse_service_name, warehouse_service_description,
       observation_count, unmodified_observation_count
FROM mrfweb.release_billing_codes
WHERE catalog_id = $1
  AND ($2::text IS NULL OR billing_code_type = $2)
  AND billing_code LIKE $3 || '%'
ORDER BY billing_code_type COLLATE "C", billing_code COLLATE "C"
LIMIT $4;
```

`$3` is a validated alphanumeric code prefix without SQL wildcard characters;
`$4` is a positive bounded result limit. Result columns are exact table types.

### Code option lookup

```sql
SELECT filter_kind, filter_value, display_label, observation_count
FROM mrfweb.release_code_filter_values
WHERE catalog_id = $1
  AND billing_code_type = $2
  AND billing_code = $3
  AND filter_kind = $4
ORDER BY filter_value COLLATE "C";
```

`filter_kind` is one of Story 28's five fixed values.

### Plan prefix pagination

```sql
SELECT id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type,
       search_text
FROM mrfweb.release_plans
WHERE catalog_id = $1
  AND search_text COLLATE "C" LIKE ($2 || '%') ESCAPE E'\\'
  AND (search_text COLLATE "C", id) >
      ($3::text COLLATE "C", $4::bigint)
ORDER BY search_text COLLATE "C", id
LIMIT $5;
```

Search prefix and cursor text are normalized with Story 28's exact Go
`strings.ToLower`/ASCII-trim rules. Before binding `$2`, escape backslash as
`\\`, percent as `\%`, and underscore as `\_`; those characters remain literal
search text. First page uses empty cursor text and `0`; later pages return the
last `(search_text, id)` as cursor. Limit is positive and bounded. No OFFSET,
fuzzy search, wildcard input, or locale order is normative.

### Networks without selected plans

```sql
SELECT network_name, SUM(observation_count)::bigint AS observation_count
FROM mrfweb.release_output_code_networks
WHERE catalog_id = $1
  AND billing_code_type = $2
  AND billing_code = $3
GROUP BY network_name
ORDER BY network_name COLLATE "C";
```

### Networks for selected plans

```sql
WITH selected_outputs AS (
    SELECT DISTINCT output_id
    FROM mrfweb.release_plan_outputs
    WHERE catalog_id = $1
      AND plan_id = ANY($4::bigint[])
)
SELECT n.network_name,
       SUM(n.observation_count)::bigint AS observation_count
FROM selected_outputs AS selected
JOIN mrfweb.release_output_code_networks AS n
  ON n.catalog_id = $1 AND n.output_id = selected.output_id
WHERE n.billing_code_type = $2
  AND n.billing_code = $3
GROUP BY n.network_name
ORDER BY n.network_name COLLATE "C";
```

The selected plan-ID array is nonempty, deduplicated, and resolved within the
same catalog. The distinct-output CTE prevents one output reached through
several plans from multiplying counts.

### Provider filter lookup

```sql
SELECT filter_value, provider_count
FROM mrfweb.release_provider_filter_values
WHERE catalog_id = $1
  AND filter_kind = $2
  AND parent_value = $3
ORDER BY filter_value COLLATE "C";
```

Taxonomy/state bind empty parent; city binds the exact selected state. Result
city identity remains lowercase.

### Billing codes

- exact code pair identity;
- standard observation/no-modifier counts;
- percentage/null-rate exclusion;
- expired row inclusion;
- deterministic version/name/description winner and null fallback.

### Code option values

- exact modifier/POS/context rows and counts;
- repeated list element counted once per fact;
- two distinct values each counted once;
- no synthetic modifier-mode rows;
- exact `CSTM-00` label; and
- malformed composite POS preserved as one identity.

### Plans

- complete five-field canonical identity;
- sponsor excluded;
- shared plan has two plan/output rows;
- excluded output/plan absent;
- deterministic catalog-scoped prefix search and pagination;
- a canonical plan/attachment change after catalog build causes exact stale
  rejection even when output rows/fingerprint are unchanged; and
- no tuple/value from inactive/other catalog leaks into the selected catalog.

### Plan-dependent networks

For no selected plan, return all distinct networks for exact catalog/code.
For one selected plan, use:

```text
release_plans
→ release_plan_outputs
→ release_output_code_networks
```

For several selected plan IDs, first select distinct output IDs, then aggregate
network rows. Assert:

- only networks related through selected plan output(s) and exact code appear;
- one output reached through several selected plans is counted once;
- union semantics across several selected plans;
- network present only for another code is absent;
- same network from distinct outputs sums observation counts; and
- no direct materialized plan/network cross-product exists.

### Provider filters

- taxonomy/state/city values include only release-reachable NPIs;
- provider counts are distinct NPIs despite repeated paths;
- city uses exact lowercase and state parent;
- a non-lowercase stored city fails extraction and is never normalized or
  preserved as another identity;
- cross-NPI taxonomy/geography does not invent a value relationship;
- unreachable catalog providers are absent; and
- postal code/NPI/expiration rows do not exist.

## Failure and crash matrix

Permanent tests cover each externally meaningful boundary:

- missing/wrong DuckDB version;
- invalid/unsupported/damaged warehouse;
- candidate output missing/mismatched;
- DuckDB nonzero/malformed/truncated output;
- graceful context cancellation terminates/waits for DuckDB, uses a detached
  five-second cleanup context, and records failed/cancelled when cleanup can
  commit;
- cancellation cleanup database failure and simulated process death leave a
  replaceable `building` header with no required failure code;
- PostgreSQL failure before header, after building header, during COPY, before
  ready, and during activation;
- process crash releases the advisory lock and next explicit build replaces
  abandoned building state;
- same-release concurrent build returns busy;
- failed retry replacement;
- stale ready replacement;
- published replacement rejection;
- catalog error precedence exactly follows missing → not-ready → inconsistent
  → stale → existing warehouse/preflight classification;
- activation rejects missing/building/failed/stale/inconsistent catalog;
- one invalid active payer makes the complete no-payer handoff fail closed;
- reconciliation reports exact backfill-required and sealed-inconsistency
  counts without command failure or repair; and
- CLI interruption/recreate behavior without automatic retry.

For each case assert exact database rows/timestamps, release/output membership,
warehouse checksums, fixed failure code, sanitized logs/result, and absence of
orphan DuckDB processes.

## Read-only and mutation evidence

Before and after each DuckDB extraction, compare:

- fixed warehouse marker/catalog/snapshot manifest bytes or checksums;
- every fixture Parquet file size and modification time/checksum;
- plan association part inventory; and
- absence of new files below warehouse root.

Provision one disposable non-owner/non-superuser test role with:

```text
CONNECT
USAGE ON SCHEMA mrfweb
SELECT ON mrfweb.active_release_catalogs
SELECT ON mrfweb.active_release_outputs
SELECT ON mrfweb.release_billing_codes
SELECT ON mrfweb.release_code_filter_values
SELECT ON mrfweb.release_plans
SELECT ON mrfweb.release_plan_outputs
SELECT ON mrfweb.release_output_code_networks
SELECT ON mrfweb.release_provider_filter_values
```

Run every fixed serving query above through this role. Prove direct SELECT on
`mrfweb.release_catalogs`, `mrfweb.release_outputs`, every `mrfpipeline`/River
operational table, and every INSERT/UPDATE/DELETE/DDL operation is rejected.
Role/password provisioning remains test/deployment setup, not a hard-coded
production credential or migration secret.

## Docker and Compose acceptance

- Build the ordinary image on Linux amd64 in CI.
- Verify pipeline and DuckDB exact versions inside the final runtime image.
- Compile the Go binary with `CGO_ENABLED=0`.
- Packaging tests prove architecture mapping/checksum logic includes amd64 and
  arm64.
- Run `filters status/build` through the existing operator-profile CLI with the
  same PostgreSQL and artifact/warehouse mounts as other one-shots.
- No DuckDB port, extra service, persistent DuckDB volume, host database port,
  secret build argument, or catalog-builder worker is added.
- Existing control/MRF/consumer role ownership and scaling remain unchanged.

## Opt-in real warehouse measurement

Add one explicit script/command documented for a private local config. It
accepts existing normalized pipeline database/warehouse/provider/services
configuration and an exact payer/month. It never accepts a raw SQL string,
output list override, or alternate publication membership.

The operator runs it against an unmodified private warehouse and a disposable
PostgreSQL database/catalog schema with no catalog for the target generation.
An existing ready/published target is a precondition failure; the metric run
never replaces it. The command performs one fresh build and must not activate a
release unless the operator runs separate activation.

Sanitized output is one JSON object with these exact fields, types, and units:

```text
warehouse_schema_version            string
provider_catalog_schema_version     integer
provider_catalog_release_month      string YYYY-MM
publication_generation              integer
candidate_output_count              integer rows
standard_fact_count                 integer rows
billing_code_count                  integer rows
code_filter_value_count             integer rows
plan_count                          integer rows
plan_output_count                   integer rows
output_code_network_count           integer rows
provider_filter_value_count         integer rows
duckdb_wall_time_ms                 integer milliseconds
postgres_population_wall_time_ms    integer milliseconds
total_wall_time_ms                  integer milliseconds
peak_rss_bytes                      integer bytes
catalog_database_bytes              integer bytes
```

All integers are JSON numbers representing nonnegative signed 64-bit values;
durations and byte sizes never use floating point or formatted unit strings.
Every field is required and no additional field is allowed.

Metric scope is exact:

- `duckdb_wall_time_ms` is monotonic elapsed time from starting the one
  extraction process through successful `Wait`; it excludes the version probe;
- `postgres_population_wall_time_ms` is monotonic elapsed time from beginning
  Story 30 Phase 4 through the ready transaction commit;
- `total_wall_time_ms` is monotonic elapsed time immediately before the
  advisory-lock attempt through successful ready commit, including candidate
  selection, preflight, version probe, extraction, plan projection, and
  population;
- `peak_rss_bytes` is maximum resident set for the complete wrapped build
  command including waited child processes, reported by GNU `/usr/bin/time -v`
  on Linux (KiB multiplied by 1024) or `/usr/bin/time -l` on Darwin (bytes);
  unsupported platforms fail the opt-in measurement rather than inventing a
  value; and
- `catalog_database_bytes` is the nonnegative before/after delta of
  `SUM(pg_total_relation_size(c.oid))` over base/partitioned tables
  (`relkind IN ('r','p')`) in schema `mrfweb`. `pg_total_relation_size`
  includes each table's indexes and TOAST, so indexes are not separately summed.
  The disposable database permits no concurrent catalog write during this
  measurement.

`standard_fact_count` is persisted in the ready catalog header by Stories
28–30 and must equal the Story 29 summary row. After recording the fresh-build
JSON, run ordinary `filters build` once more and assert `unchanged:true`, same
catalog/counts, and no DuckDB extraction. The repeated no-op is correctness
evidence only and does not emit another performance JSON object.

Do not print paths, output IDs, labels, plan/network/provider/filter values,
rates, source URLs, SQL, credentials, or DuckDB stderr.

Record descriptive evidence in implementation review/docs:

- machine/OS/architecture;
- DuckDB exact version;
- publication generation/output count;
- first and repeated build behavior;
- peak memory; and
- catalog lookup timings.

No fixed subsecond HTTP claim, activation SLA, or production capacity promise
is made from one warehouse. Fail the harness only on correctness, process
failure, invalid resource values, or an explicit generous safety timeout chosen
to detect hangs—not on a narrow benchmark target.

## Lookup performance measurements

Measure warm PostgreSQL queries for:

- billing-code prefix/exact lookup;
- plan catalog prefix search with deterministic pagination;
- all networks for catalog/code;
- networks for one plan/code;
- networks for several plans/code with output deduplication;
- states;
- cities for one state;
- taxonomies; and
- code-scoped modifiers/POS/context.

Use `EXPLAIN (ANALYZE, BUFFERS)` only in private test output, never public CLI
JSON/logs. Verify intended indexes are usable. Do not add Redis, Elasticsearch,
materialized search service, or speculative denormalization unless measured
PostgreSQL evidence requires a later story.

## Reconciliation and backup/restore acceptance

- Backup PostgreSQL and warehouse after two published generations.
- Restore both into a disposable environment.
- Run catalog-aware reconciliation, then targeted `month status` and exact
  catalog queries; verify values match pre-backup state.
- After a PostgreSQL-only restore against a wrong/missing warehouse, `filters
  status` may still succeed because it is intentionally database-only and is
  not restore validation.
- In that mismatch state, reconciliation with full filesystem configuration
  remains a successful command but increments
  `sealed_release_inconsistency_count`; `month activate` rejects through
  existing warehouse/catalog failure classification. Neither path mutates
  release, catalog, or warehouse state.
- Restoring warehouse without PostgreSQL remains insufficient because release
  and catalog identity live in PostgreSQL.

Documentation identifies reconciliation plus warehouse-aware activation—not
`filters status`—as the restore consistency check. No automatic backup,
retention, or catalog deletion is added.


## Documentation convergence

After implementation:

- mark Stories 28–32 implemented;
- update README version/current command sections;
- update DESIGN active architecture/story sequence;
- document exact `filters build/status` commands and Compose forms;
- document build-before-activate, stale rebuild, incremental publication,
  rollback, cutover, backup/restore, DuckDB packaging, and read-only web grants;
- update CLI help/failure code/operator recovery documentation;
- retain historical Stories 01–13 sections as historical, not current; and
- do not claim the pipeline implements HTTP/query UI or `mrfweb switch`.

## Required tests and commands

Ordinary project verification remains:

```text
go test ./...
go test -race ./...
go vet ./...
CGO_ENABLED=0 go build ./cmd/mrfpipeline
```

DuckDB/filter acceptance uses the final image or exact pinned binary and a
fresh disposable database. Database integration tests remain serialized
according to existing project convention and never target a populated operator
database.

Database integration setup resets the new `mrfweb` schema together with
application/River test schemas. Migration assertions advance the contiguous
latest application migration from version 8 to 9. CLI parsing tests explicitly
reject duplicate `--payer` and `--collection-month` in separate and
`--flag=value` forms. Release tests reject active/inactive generation `0`.

The exact new focused scripts/test names are implementation choices, but one CI
path must execute the complete synthetic extraction/population/publication
scenario rather than only detached helpers.

## Acceptance criteria

- Synthetic end-to-end expected rows/counts are exact and hand-computed.
- Two incremental generations, month switch, rollback, and multiple payers use
  matching immutable catalogs.
- Plan-dependent code/network discovery has correct union/deduplication
  semantics.
- Every failure/crash boundary is fail-closed, retryable only through explicit
  commands, and sanitized.
- Warehouse is demonstrably read-only.
- Future web credentials can read required serving data and cannot mutate it.
- Final Docker image carries verified DuckDB 1.5.5 without changing static Go
  build or service topology.
- Real-data evidence is descriptive and private.
- Documentation is complete and does not claim web/UI implementation.

## Non-goals

- Implementing web server, API, HTML, CSS, JavaScript, charts, authentication,
  query execution, or manual web-process switch.
- Adding a polling/notification system.
- Adding external CPT/HCPCS reference data.
- Adding ZIP/NPI/expiration catalogs.
- Adding materialized dashboard statistics or arbitrary filter combinations.
- Automatically building, activating, retaining, compacting, or deleting
  catalogs.
- Changing consumer warehouse schema or writing to a real operator warehouse.

## Implementation notes

Prefer one end-to-end fixture plus focused failure tests. Reuse existing
migration, pipeline worker/publication fixtures, Docker/Compose, redaction, and
real-acceptance patterns. Do not create a general benchmark framework, test DSL,
warehouse faker, service harness, or duplicate pipeline implementation in test
code.
