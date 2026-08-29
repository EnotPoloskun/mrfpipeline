# Story 31: Filter-aware release publication

## Status

Proposed.

## User story

As a pipeline operator, I want activation to publish an output generation only
when its exact filter catalog is ready so the active PostgreSQL handoff can
never identify September outputs with August plans, networks, or code options.

## Context

Story 27 recomputes the exact published-plus-publishable target under release
row locks and atomically persists a new output generation. Story 30 builds a
catalog outside that activation transaction because DuckDB extraction may take
minutes. Workers can finish another output between those operations.

This story joins the two boundaries: activation requires one exact ready
catalog and compares its complete output set under the same lock used to
publish membership. A stale/missing/failed/building catalog blocks the
checkpoint. The operator rebuilds explicitly; activation never runs DuckDB.

The existing incremental publication model remains. There is no force flag:
terminal TOC/MRF file failures may be omitted, pending known MRF work may
continue, and repeated activation appends newly plan-ready outputs only after a
matching next-generation catalog is ready.

## Dependencies

- Story 27 incremental publication transaction and rollback behavior;
- Story 28 catalog schema, published state, and active serving views;
- Story 30 exact candidate selection, output fingerprint, and build/status
  commands; and
- existing activation warehouse/plan preflight and sealed-publication audits.

## Goal

Change publication so one successful checkpoint atomically commits:

```text
exact monthly_release_outputs membership
monthly_releases publication_generation/status pointer
matching mrfweb release catalog ready → published
```

No mutation occurs unless all three represent the same exact output set.

Add catalog readiness to targeted status, activation results, active-output
validation, reconciliation, rollback, and operator documentation. Do not add a
web server, poller, notification, or `mrfweb switch` implementation.

## Product decisions

- `filters build` is mandatory before any checkpoint that would publish a new
  generation.
- Activation never generates, repairs, replaces, or partially accepts a
  catalog.
- Exact set comparison is authoritative. Count and SHA-256 fingerprint are
  additional evidence, not substitutes.
- A catalog is published in the same PostgreSQL transaction as output
  membership/generation changes.
- A published catalog is immutable and retained when a later generation or
  month becomes active.
- Inactive rollback reuses its exact published current-generation catalog.
- Existing releases created before catalog-aware cutover must be explicitly
  backfilled with `filters build` and promoted through `month activate`.
- A future query service reads stable `mrfweb` views with read-only credentials
  and refreshes only by explicit operator action. That service is out of scope.
- Do not weaken Story 27 upstream failure policy, output plan readiness,
  first-activation inventory freeze, additive generation behavior, or exact
  warehouse preflight.

## Activation catalog selection

After activation locks every monthly release row for the payer and recomputes
Story 27's exact `targets`, derive:

```text
has_new_outputs
expected_publication_generation
expected output rows
expected output fingerprint
```

Rules:

### Building release

- At least one target is required.
- Every target is new.
- Expected generation is current generation plus one.
- Require one `ready` catalog for exact payer/month/expected generation.

### Active release with new output

- Expected targets are durable published outputs plus currently publishable
  unpublished outputs.
- Expected generation is current generation plus one.
- Require one `ready` catalog for that exact generation.

### Active release with no new output

- Expected generation remains current.
- If an exact `published` current catalog exists, activation retains Story 27
  idempotency: no generation, timestamp, membership, or catalog change.
- During one-time cutover, an exact `ready` backfill for the current generation
  may be promoted to `published` without changing generation, output
  membership, `sealed_at`, or `last_activated_at`.
- Missing/building/failed/mismatched catalog rejects the command.

### Inactive rollback/reactivation

- Targets are exact persisted membership; no unpublished output is admitted.
- Expected generation is the inactive release's stored current generation.
- Require an exact `published` catalog, or allow a one-time exact `ready`
  backfill to be promoted in the rollback transaction.
- Reactivation restores the frozen membership/catalog and does not derive a
  next generation.

No branch chooses a latest catalog ID, greatest generation, ready catalog from
another month, or catalog sharing only a fingerprint.

## Exact catalog validation under lock

Activation locks the selected `mrfweb.release_catalogs` row and requires:

- exact payer and collection month;
- exact expected publication generation;
- status allowed by the branch above;
- exact provider-catalog identity matching the recognized warehouse;
- header output count equal to target length;
- header output fingerprint equal to the freshly calculated fingerprint;
- exact `release_outputs` count equal to target length;
- no target missing from `release_outputs` by both snapshot ID and output ID;
- no extra `release_outputs` row;
- each output ID equals `mrf-<snapshot-id>`;
- all ready/published header count fields are non-null and equal actual child
  table counts;
- for every target output, the exact current sponsor-independent
  `(output_id, plan_name, issuer_name, plan_id_type, plan_id,
  plan_market_type)` set derived from authoritative `mrf_plans` equals the set
  represented by `release_plan_outputs → release_plans` in both directions;
- current attachment batches and plan assignments still satisfy Story 27
  plan-readiness under the release lock; and
- at least one billing code, plan, plan/output row, and catalog output.

Use exact set-difference queries or sorted typed comparisons. Do not repair with
`DISTINCT`, grouping, `ON CONFLICT DO NOTHING`, or partial intersection. A hash
collision cannot cause acceptance because rows are compared.

Run existing warehouse/plan-part preflight against the same `targets` before
catalog publication. A catalog does not replace filesystem validation.

Story 27 stage claims use the same release-row ordering. The locked activation
recheck therefore closes the plan/attachment race: a batch that is already
pending/running makes the output non-publishable, and no new claim can pass the
locked release boundary before membership/catalog publication commits.

## Publication transaction

For a checkpoint with new outputs:

1. retain Story 27 payer release-row lock order;
2. calculate targets/generation under lock;
3. run existing warehouse preflight on those targets;
4. lock and validate exact ready catalog;
5. insert only new `monthly_release_outputs` with expected generation;
6. require inserted count to equal the computed new-target count;
7. switch the payer's previous active month inactive when applicable;
8. update target monthly release status/generation/timestamps exactly as Story
   27 requires;
9. change catalog `ready → published` and set `published_at`;
10. requery authoritative output and child counts; and
11. commit once.

Any failure rolls back membership, release pointer, and catalog status
together. A ready catalog may remain ready after rollback and may be retried if
its output set remains exact.

For a cutover/rollback promotion with no new outputs, perform only the required
release-pointer changes and `ready → published`; do not rewrite existing
membership or child rows.

For an idempotent active checkpoint with an already published exact catalog,
return without updating `last_activated_at`, catalog `published_at`, or any row.

## Stale catalog behavior

A ready catalog is stale when any exact activation target or authoritative
plan/output property differs, including one newly publishable output that
completed after build or a plan/attachment change after the catalog became
ready.

Activation returns fixed `filter_catalog_stale` and leaves:

- current active month;
- release status/generation/timestamps;
- output membership;
- ready catalog and all children; and
- warehouse files

unchanged. The operator reruns `filters build`, which replaces the unpublished
ready catalog for that prospective generation, then retries activation.

Do not automatically launch a rebuild, ignore the new output, publish only the
catalog intersection, or delete the stale catalog from activation.

## Missing and failed catalog behavior

Use fixed failures:

```text
filter_catalog_missing
filter_catalog_not_ready
filter_catalog_stale
filter_catalog_inconsistent
```

- missing: no exact generation header;
- not ready: exact header is building or failed where ready/published is
  required;
- stale: complete header identity/output set differs from current targets;
- inconsistent: a ready/published header contradicts its child rows/counts or
  immutable database invariants.

All errors are sanitized. Do not include raw catalog values, output IDs, SQL,
paths, DuckDB details, or credentials.

## Catalog-aware active handoff

After cutover, one active release is publicly handoff-ready only when:

- `monthly_releases.status = 'active'`;
- `publication_generation > 0`;
- matching `mrfweb.release_catalogs.status = 'published'`; and
- exact authoritative output membership equals catalog output membership.

`mrfweb.active_release_catalogs` and
`mrfweb.active_release_outputs` remain the stable read-only web contract.

Update `release.ListActiveOutputs` and no-selector `month status` to fail closed
with fixed catalog inconsistency when an active monthly release lacks an exact
published catalog or set equality. They may still return the existing exact
`Target`/`active_outputs` public shape; do not add catalog child values. The
future web process reads catalog ID/generation directly from `mrfweb` views.

Payers with no active release remain valid and contribute no rows. A database
with an active release but no matching catalog is invalid cutover state, not an
empty active relation.

## Status behavior

Extend targeted `month status` JSON with:

```text
filter_catalog_ready              boolean
filter_catalog_id                 integer or null
filter_catalog_generation         integer
filter_catalog_status             string or null
filter_catalog_output_count       integer or null
```

`filter_catalog_generation` is the generation required for the next checkpoint
when currently publishable new outputs exist; otherwise current generation.

Add at most one targeted blocker from this ordered catalog group:

```text
filter_catalog_missing
filter_catalog_building
filter_catalog_failed
filter_catalog_stale
filter_catalog_inconsistent
```

Status uses database candidate/catalog rows only and does not run DuckDB or
warehouse extraction. `filter_catalog_ready` means an exact ready prospective
catalog or exact published current catalog exists according to database state;
activation still recomputes and validates under lock.

Strict `database_ready` requires filter catalog readiness in addition to its
existing all-work conditions. Incremental activation may still succeed while
`database_ready:false` for Story 27-permitted pending/terminal upstream work,
but it never succeeds with a catalog blocker.

## Activation result

Extend successful `month activate` JSON by one required field:

```text
filter_catalog_id
```

The existing `publication_generation` identifies the published catalog
generation. Do not duplicate catalog counts/values in activation JSON; operators
use `filters status/build` for those.

For an idempotent checkpoint, return the existing published catalog ID. For
backfill promotion, return the promoted catalog ID.

## Cutover for existing release state

Migration 0009 is additive and may be deployed while an existing Story 27
release is active without a catalog. The sequential story landing provides a
simple cutover without a compatibility flag:

1. deploy through Story 30 so migration, DuckDB, and `filters build` are
   available while Story 27 activation/listing behavior is still current;
2. build an exact ready catalog for every active release's current generation;
3. stop the control role for the short Story 31 binary cutover;
4. deploy the Story 31 binary;
5. run `month activate` for each same active payer/month to promote its exact
   backfill without changing output membership, generation, or activation
   timestamps;
6. verify catalog-aware active views/status;
7. optionally build catalogs for inactive releases before rollback is needed;
   and
8. restart control.

A fresh database simply builds before first activation.

Do not auto-mark existing releases/catalogs published in migration, infer
filter rows, or temporarily let the web read a catalog-less active generation.
The current live Compose stack is not migrated as part of tests or source
landing.

## Repeated activation and rollback

- Generation 1 catalog remains published after generation 2 is appended. It is
  not current but may be used by an in-flight future query state until an
  explicit retention story exists.
- The active handoff selects only the catalog matching the release's current
  generation.
- Switching to another month leaves prior month catalogs published.
- Inactive rollback selects that month's current stored generation and exact
  published catalog.
- Repeated rollback/reactivation does not rebuild or republish catalog rows.
- No automatic catalog retention/deletion is added.

## Reconciliation and audits

Extend reconciliation with read-only catalog audits:

- every active release requires a matching published current-generation
  header;
- an inactive release with a published current-generation catalog is audited
  by the same exact membership/fingerprint/count rules;
- an inactive legacy release with no published current-generation catalog, or
  only an exact ready backfill, is reported through a sanitized
  `filter_catalog_backfill_required` count and is not a sealed inconsistency
  until rollback is attempted;
- authoritative output membership equals catalog outputs;
- catalog plans reference only catalog outputs;
- output/code/networks reference valid catalog output/code rows; and
- provider-catalog identity matches warehouse recognition where existing
  reconciliation already has the warehouse open.

A mismatch in any existing published catalog returns one fixed sealed/catalog
inconsistency and does not mutate release, membership, catalog, plans, or
warehouse. Reconciliation never builds, promotes, deletes, or repairs a
catalog. Missing inactive backfill remains an explicit operator condition;
rollback still rejects until `filters build` supplies an exact ready catalog.

Building releases and their unpublished catalogs are not sealed audit
failures. Failed/building abandoned catalogs remain operator-visible through
`filters status` and are replaced only by explicit `filters build`.

## Concurrency and request consistency contract

Activation retains Story 27 lock order and must not wait for a running catalog
build. It attempts the exact catalog row lock with PostgreSQL `NOWAIT`; lock
contention maps to `filter_catalog_not_ready` and changes no row. If the row is
observably building, activation fails without locking its children. Catalog
build may encounter only ordinary short conflicts with activation after its
long extraction phase; neither operation polls or cancels the other.

The stable handoff is generation-scoped so a future query process can:

1. load active catalog/output views;
2. validate the complete candidate against the warehouse;
3. load matching filter values;
4. atomically replace one immutable serving-state pointer; and
5. let in-flight requests finish with their captured prior catalog.

That future process and manual switch control socket are explicitly out of
scope; this story supplies the database invariants they require.

## Required tests

### Activation branches

- First building activation requires ready generation 1 catalog.
- Active append requires ready current+1 catalog.
- Active no-new-output with published current catalog is fully idempotent.
- Active current-generation ready backfill promotes without changing release
  timestamps/generation/membership.
- Inactive rollback uses published exact catalog; ready backfill promotion is
  atomic with rollback.
- Switching months publishes target catalog and preserves prior catalogs.

### Exact-set failures

- Missing, building, failed, wrong generation, wrong payer/month, wrong
  fingerprint/count, missing output, extra output, snapshot/output mismatch,
  bad provider identity, and child count inconsistency all reject without any
  release/output/catalog mutation.
- A new output becoming publishable after build causes stale rejection.
- Hash equality with row inequality still rejects.
- Warehouse preflight failure retains prior active state and ready catalog.

### Incremental behavior

- Generation 1 publishes first ready subset.
- MRF work continues.
- Build generation 2 over old+new outputs.
- Generation 2 activation appends only new output and publishes matching
  catalog.
- Idempotent third activation changes no row/timestamp.
- Generation 1 catalog remains immutable.

### Status/handoff/reconcile

- Targeted status reports exact catalog fields/blocker precedence without
  DuckDB.
- Strict database readiness includes catalog readiness.
- No-selector active output status fails closed on catalog-less/inconsistent
  active release and succeeds on exact published state.
- Active views expose only current generation and exact outputs.
- Reconcile reports sealed catalog inconsistency without repair.
- Multi-payer active months select independent catalogs/generations.

### Cutover

- Existing active release can build/promote exact current-generation backfill.
- Promotion changes no membership/generation/activation timestamp.
- Missing cutover catalog fails closed after catalog-aware checks land.
- Existing inactive release can be prepared and rolled back exactly.

## Acceptance criteria

- No new output becomes active before its exact plans/networks/filter values are
  ready in PostgreSQL.
- Output membership, publication generation, and catalog publication are one
  atomic transaction.
- Stale build/activation races fail explicitly and never narrow or repair the
  candidate.
- Incremental Story 27 behavior, failure omissions, plan readiness, and
  rollback remain intact.
- Active handoff is complete and generation-scoped.
- Existing state has one explicit safe cutover path.
- No automatic builder, query server, or direct-SQL operator bypass is added.

## Non-goals

- Running DuckDB during activation.
- Automatically building/rebuilding catalogs.
- Adding `--force` or an environment-specific publication path.
- Modifying filter values or published catalogs.
- Implementing the web server, UI, query endpoints, polling, PostgreSQL
  LISTEN/NOTIFY, Unix control socket, or manual web switch command.
- Adding catalog retention/compaction/deletion.
- Applying changes to the current live Compose database/warehouse.

## Documentation

Update README and design release procedures to require:

```text
filters build
month activate
future mrfweb switch
```

Clearly label the final command as a future query-service operation until that
repository implements it. Document catalog-aware cutover, incremental
rebuild-on-stale, rollback reuse, and fail-closed active views.

## Implementation notes

Expected changes:

```text
internal/release/release.go
internal/release/status.go
internal/cli/cli.go
internal/cli/help.go
internal/reconcile/status.go
internal/reconcile/sealed.go
```

Use existing release target helpers, transactions, and concrete queries. Add
one focused catalog validation function; do not introduce an activation hook
framework, generic publisher, distributed transaction abstraction, or event
bus.
