# Story 32: Filter catalog operational acceptance

## Status

Proposed.

## User story

As a pipeline operator, I want permanent synthetic acceptance and optional real
warehouse evidence for release filter catalogs so I can trust publication,
rollback, lookup semantics, resource use, and failure behavior before a public
web process depends on them.

## Context

Stories 28–31 add a new schema, pinned DuckDB extraction, explicit build CLI,
and catalog-aware release publication. Individual unit tests do not prove the
complete boundary:

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

Completed implementations of Stories 28–31 plus the existing Story 22 local
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
- cross-NPI taxonomy/geography does not invent a value relationship;
- unreachable catalog providers are absent; and
- postal code/NPI/expiration rows do not exist.

## Failure and crash matrix

Permanent tests cover each externally meaningful boundary:

- missing/wrong DuckDB version;
- invalid/unsupported/damaged warehouse;
- candidate output missing/mismatched;
- DuckDB nonzero/malformed/truncated output;
- context cancellation and process cleanup;
- PostgreSQL failure before header, after building header, during COPY, before
  ready, and during activation;
- process crash leaving building catalog with released advisory lock;
- concurrent build busy;
- failed retry replacement;
- stale ready replacement;
- published replacement rejection;
- activation missing/building/failed/stale/inconsistent catalog;
- active handoff missing published catalog;
- reconciliation sealed catalog mismatch; and
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

PostgreSQL read-only serving-query tests run under a role granted only:

```text
CONNECT
USAGE ON SCHEMA mrfweb
SELECT on required mrfweb views/tables
```

Prove INSERT/UPDATE/DELETE/DDL and direct access to operational tables are
rejected. Role/password provisioning remains test/deployment setup, not a
hard-coded production credential or migration secret.

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

The operator runs it against an unmodified private warehouse only after taking
or selecting a disposable PostgreSQL database/catalog schema. It may build a
real ready catalog but must not activate a release unless the operator runs the
separate activation command.

Sanitized output contains only:

```text
warehouse schema version
provider catalog schema/release month
candidate output count
standard fact count
billing code count
code filter value count
plan count
plan/output count
output/code/network count
provider filter value count
DuckDB wall time
PostgreSQL population wall time
total wall time
peak RSS
catalog database bytes
```

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
- Run catalog-aware reconciliation/status.
- Verify active views and exact catalog queries match pre-backup values.
- Restoring PostgreSQL without matching warehouse fails warehouse/catalog
  validation and does not mutate serving state.
- Restoring warehouse without PostgreSQL remains insufficient because release
  and catalog identity live in PostgreSQL.

No automatic backup, retention, or catalog deletion is added.

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
