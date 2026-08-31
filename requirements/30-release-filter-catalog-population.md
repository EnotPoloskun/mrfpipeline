# Story 30: Release filter catalog population

## Status

Implemented.

## User story

As a pipeline operator, I want an explicit idempotent command to build and
inspect the complete filter catalog for the exact next publication checkpoint
so activation can later publish only outputs whose matching web filters are
already ready.

## Context

Story 28 creates versioned PostgreSQL catalog tables. Story 29 extracts compact
filter rows from a caller-supplied exact output relation. This story chooses the
candidate relation through Story 27's release rules, populates all PostgreSQL
rows, and exposes operator commands. It does not change active output
membership; Story 31 owns publication.

Because an active monthly release can append newly ready outputs, a build may
target either the release's current generation or its prospective next
generation. Workers may finish another output during a long DuckDB scan. The
built header therefore records the exact candidate output rows and fingerprint;
Story 31 rejects it if activation later computes a different set.

## Dependencies

- Story 27 publication checkpoint selection and durable membership;
- Story 28 exact PostgreSQL schema and lifecycle;
- Story 29 typed read-only warehouse extraction; and
- existing plan-readiness, warehouse preflight, database lease, redaction, CLI
  parsing, and JSON output conventions.

## Goal

Add these exact commands:

```text
mrfpipeline filters build --payer <payer> --collection-month <YYYY-MM>
mrfpipeline filters status --payer <payer> --collection-month <YYYY-MM>
mrfpipeline filters build --help
mrfpipeline filters status --help
```

`filters build`:

1. serializes one build for the payer/month;
2. computes the exact current or prospective Story 27 publication target;
3. creates/replaces one unpublished catalog header for that target generation;
4. runs Story 29 extraction;
5. projects canonical plans and plan/output relationships from PostgreSQL;
6. writes every child catalog row and validates exact counts in one database
   transaction;
7. changes the header from `building` to `ready`; and
8. prints one sanitized JSON result.

`filters status` reads PostgreSQL only. It reports current publication and
catalog state without running DuckDB, validating warehouse files, repairing
state, or modifying a row.

## Product decisions

- Catalog build is explicit and synchronous. Do not create a River job, worker
  queue, scheduler, daemon, webhook, or automatic background build.
- The command may run while ordinary workers remain active. It does not freeze
  or block MRF work. A later output can make the catalog stale; Story 31 detects
  that exact race.
- Do not hold a release-row transaction lock during DuckDB extraction.
- Hold one PostgreSQL session advisory lock dedicated to filter-catalog build
  for the exact payer/month. The lock releases automatically on process exit.
- Build from the same published/publishable predicates as activation. Do not
  create a second approximation of plan readiness.
- The command never changes `monthly_releases`,
  `monthly_release_outputs`, jobs, attempts, plans, attachments, or warehouse
  files.
- Plans come from pipeline PostgreSQL; fact/context/provider values come from
  Story 29 DuckDB extraction.
- No external CPT/HCPCS reference import is part of this command.
- A published catalog is immutable. A failed/building/ready catalog for a
  generation not yet published may be replaced under the build lock.
- Do not retain multiple unpublished attempts for one payer/month/generation.
  The catalog header `id` is private staging identity, not attempt history.

## CLI parsing and configuration

Add `filters` as one root command with exact actions `build` and `status`.

Both require exactly:

```text
--payer <payer>
--collection-month <YYYY-MM>
```

Requirements:

- accept separate or `--flag=value` forms according to existing CLI rules;
- reject missing, duplicated, empty, unknown, or positional arguments;
- validate payer/month through existing config/release contracts;
- `--help` succeeds without opening PostgreSQL or inspecting local paths;
- usage errors use the existing root/command hint style; and
- do not add `--force`, `--replace`, output path, DuckDB path, threads, memory,
  timeout, or format flags.

`filters status` requires only the pipeline database URL. `filters build`
requires the database URL, warehouse path, provider catalog path, artifact
root, and services path through the same normalization/overlap/catalog checks
used by activation. It does not require workers to run.

The command resolves exact DuckDB `v1.5.5` as Story 29 requires.

## Candidate target selection

Introduce one purpose-specific release operation that returns a concrete
`CatalogCandidate`:

```text
payer_id
collection_month
release_status
current_publication_generation
target_publication_generation
[]release.Target
output_fingerprint
has_new_outputs
```

It reuses the current release package's existing checkpoint inspection,
published target, publishable target, and deterministic merge logic. Do not
export SQL strings, create a general release planner, or duplicate predicates
in the CLI/filter package.

Selection rules follow.

### Building release

- Run the same first-publication readiness checks as Story 27: source target
  configured and selected, discovery/TOC inventory terminal, no consumer or
  attachment publication failure, and at least one publishable plan-ready
  output.
- Candidate targets are all currently publishable outputs.
- Target generation is current generation plus one, normally `1`.
- `has_new_outputs` is true.

The build does not freeze discovery/TOC inventory. First activation still owns
that state transition. If inventory or target output membership changes after
build, Story 31 rejects the stale catalog.

### Active release with newly publishable outputs

- Candidate targets are the union of durable published outputs and currently
  publishable unpublished outputs.
- Target generation is current generation plus one.
- `has_new_outputs` is true.

### Active release with no new output

- Candidate targets are the exact durable published outputs.
- Target generation is the current generation.
- `has_new_outputs` is false.
- If the exact current generation already has a published catalog, build is an
  idempotent no-op and returns it.
- If migration/cutover left the current active generation without a catalog,
  build backfills that exact generation and may make it `ready`. Any exact
  current-generation ready catalog is eligible for Story 31 promotion when
  activation finds no new output; no hidden cutover flag/state is required.

### Inactive release

- Candidate targets are its exact durable published outputs.
- Target generation is its current stored generation.
- No new output is admitted.
- If a matching published catalog exists, build is an idempotent no-op.
- If a catalog is absent during migration/cutover, build may backfill it as
  `ready` so Story 31 can authorize exact rollback.

### Invalid candidate

Fail without catalog mutation when:

- release does not exist;
- target set is empty;
- existing release status is outside building/active/inactive;
- an active or inactive release has publication generation `0`;
- first-publication inventory requirements fail;
- consumer or attachment publication failure exists;
- target rows violate output identity/uniqueness; or
- existing published membership fails consumed, plan-ready, warehouse-valid,
  or catalog lexical invariants.

Terminal TOC/MRF download/parse failures retain Story 27 behavior and do not by
themselves block a candidate.

## Build serialization

Use a domain-separated stable 64-bit hash for one session advisory lock per
exact payer/month. Hash the UTF-8 bytes
`mrfpipeline/filter-catalog`, unit separator, payer, unit separator, and
`YYYY-MM` with SHA-256; interpret the first eight digest bytes as signed
big-endian `int64`. It does not reuse snapshot IDs or the existing MRF/consumer
execution-lock keys.

A mathematical hash collision is allowed to cause a false
`filter_catalog_busy`; it can never permit concurrent mutation or incorrect
publication. Do not claim the mapping is collision-free and do not hold a
database transaction for extraction merely to avoid that safe false-busy case.

The build obtains one pool connection, tries the session advisory lock, and
retains that same connection until completion. A second live build with the
same lock key fails immediately with fixed `filter_catalog_busy`; it does not
wait, poll, cancel, or replace the live build. Different payer/month builds
normally proceed independently but a hash collision may conservatively
serialize them. One CLI process starts only one build.

A crashed process releases the advisory lock. On the next build, an existing
unpublished `building` header is treated as an abandoned attempt and replaced.
No heartbeat, lease-renewal row, timeout scanner, or cleanup daemon is added.

## Build lifecycle

### Phase 1: candidate and header

Under the advisory lock:

1. calculate the complete database candidate from PostgreSQL;
2. reject active/inactive generation `0` and validate candidate identity,
   separate ASCII-output fingerprint copy, and historical membership
   invariants without repairing membership;
3. capture the exact sorted semantic plan snapshot described below;
4. run existing warehouse/catalog/path recognition and activation preflight on
   every candidate target;
5. inspect and fully validate an existing catalog for target generation;
6. return an exact published/ready match as idempotent success without
   resolving or invoking DuckDB;
7. reject any attempt to replace a published mismatch;
8. only when a new build is required, resolve DuckDB and complete the exact
   Story 29 version probe before inserting a catalog header; and
9. in one short transaction, delete a replaceable unpublished catalog for that
   generation and insert a new `building` header plus exact `release_outputs`.

At this point no filter child row exists. `building` is never visible through
active serving views.

### Phase 2: extraction

Run Story 29 outside a database transaction while retaining the advisory lock.
The candidate output relation and fingerprint are immutable inputs to the
extractor.

If extraction fails or the command is gracefully canceled:

- terminate and wait for the DuckDB process first;
- create a detached `context.Background()` cleanup context with a fixed
  five-second timeout;
- under that cleanup context, begin a short transaction, lock the building
  header, delete child rows defensively, set status `failed`, set
  `completed_at`, and record the fixed extraction or
  `filter_catalog_cancelled` code;
- return the original fixed failure/nonzero CLI result; and
- if detached cleanup cannot connect or commit, leave the header `building`.

A process crash or uncatchable termination always leaves `building` and cannot
be expected to record a failure code. The next explicit build replaces that
abandoned header after the session advisory lock is released. Do not add a
cleanup worker or heartbeat.

### Phase 3: plan projection

The Phase 1 semantic plan snapshot is the exact sorted relation:

```text
output_id
plan_name
issuer_name
plan_id_type
plan_id
plan_market_type
```

plus, per output, the boolean/count facts proving at least one plan, every plan
assigned through a succeeded attachment batch, and no pending/running/failed
batch. It is derived semantically from current rows, never from timestamps or a
new plan-set revision column.

After DuckDB extraction, reread the same relation/readiness for exactly the
candidate snapshots. If any row or readiness fact differs from Phase 1, fail
the complete build with `filter_catalog_plan_invalid` and perform the same
detached short failure cleanup. This detects a meaningful concurrent change
without treating an irrelevant timestamp update as stale.

Validate every exact plan identity field as nonempty, valid UTF-8, free of CR/LF,
and compliant with Story 28 lexical constraints before insertion. Deduplicate
the sponsor-independent five-field tuple across candidate outputs. For each
canonical plan, retain every candidate output that supplied that tuple.
Sponsor remains outside catalog identity.

Build `search_text` exactly as Story 28 specifies. Sort canonical plans by the
five identity fields using Go `strings.Compare` field-by-field, which is UTF-8
byte lexical order, and sort plan/output rows by that identity then ASCII
output ID before persistence. PostgreSQL catalog pagination/order uses
`COLLATE "C"` so locale does not change the result.

Story 31 compares the exact current authoritative plan/output relation and
attachment readiness again under activation locks. A semantic change after
ready publication therefore makes the catalog stale.

### Phase 4: atomic population and readiness

Begin one transaction and lock the building header. Re-read its exact identity,
status, output rows, and fingerprint. Reject any surprise.

Insert concrete typed rows into child tables using `pgx.CopyFrom` or bounded
explicit batches. The implementation may stream each sorted slice into the
transaction; it must not create a generic table writer or reflection-based row
mapper.

Insertion order:

1. `release_billing_codes`;
2. `release_code_filter_values`;
3. `release_plans` and map generated IDs to canonical tuples;
4. `release_plan_outputs`;
5. `release_output_code_networks`; and
6. `release_provider_filter_values`.

After insertion, query exact database counts and validate:

- child counts equal the typed extraction/projection counts;
- every catalog output has at least one canonical plan/output relationship;
- every code-filter and output-network code has one billing-code row;
- every output-network output has one catalog output row;
- header output rows/count/fingerprint still equal candidate;
- at least one billing code and plan exist;
- no duplicate key was repaired or ignored; and
- provider catalog schema version and exact `YYYY-MM-01` release-month date
  equal the recognized warehouse identity.

Update header standard fact count, child counts, and provider-catalog identity
from the exact extraction result; set status `ready`, set
`completed_at = transaction_timestamp()`, and commit. No reader can observe a
ready partial catalog.

On a synchronous database error, roll back Phase 4, then mark the header failed
in a new short transaction when the connection remains usable. If failure
status cannot be recorded, return a fixed database failure; the next explicit
build replaces the abandoned building header after obtaining the advisory
lock.

## Idempotency and replacement

An `exact match` means exact catalog header identity, candidate fingerprint,
provider-catalog identity, `(output_id, mrf_snapshot_id)` set equality, exact
current semantic plan/readiness snapshot equality, and the same actual
child-count/referential validation required before publication.
Header/fingerprint equality alone is insufficient. An inconsistent ready
unpublished catalog is replaceable; an inconsistent published catalog fails
`filter_catalog_inconsistent` and is never rebuilt or repaired.

- Exact published match: return `unchanged`; no DuckDB or database mutation.
- Exact ready match for an unpublished target: return `unchanged`; no DuckDB.
- Failed/building unpublished target: replace and rebuild.
- Ready unpublished target with different output fingerprint: replace and
  rebuild.
- Internally consistent published target whose exact candidate output/plan set
  differs: fail `filter_catalog_published_mismatch`. Internal
  header/child/count/referential corruption always takes the preceding
  `filter_catalog_inconsistent` classification.
- A catalog for another generation is never deleted by this build.
- Repeating a successful build returns the same catalog ID and counts.

Do not use `ON CONFLICT DO NOTHING`, `DISTINCT` repair, last-row-wins, or
partial row replacement to hide logical duplicates.

## `filters build` result

Success JSON has exactly:

```text
payer_id
collection_month
publication_generation
catalog_id
catalog_status
output_count
standard_fact_count
billing_code_count
code_filter_value_count
plan_count
plan_output_count
output_code_network_count
provider_filter_value_count
unchanged
```

`catalog_status` is `ready` for a prospective/backfilled catalog or `published`
for an idempotent current published catalog. Counts are nonnegative integers.
Do not include output IDs, plan/network/provider/filter values, paths, SQL,
DuckDB stderr, database URL, or durations in public JSON. Wall time and phase
may be emitted only in sanitized structured operator logs.

## `filters status` result

Return one JSON object with:

```text
payer_id
collection_month
release_status
current_publication_generation
current_catalog_id nullable
current_catalog_status nullable
current_catalog_output_count nullable
prospective_candidate_valid
prospective_publication_generation nullable
prospective_catalog_id nullable
prospective_catalog_status nullable
prospective_catalog_output_count nullable
```

Definitions:

- current catalog means exact current stored publication generation;
- active/inactive generation `0` is an invariant failure, not a null catalog;
- a building/active prospective candidate is valid only when the complete
  database candidate gate succeeds, including target selection,
  discovery/TOC requirements where applicable, publication-failure checks,
  output identity, plan readiness, and a nonempty exact target set;
- when that complete gate succeeds with new outputs, prospective generation is
  current plus one;
- active with no new output and an inactive release report current generation
  only when their complete database candidate gate succeeds;
- when any building/active/inactive database candidate gate fails,
  `prospective_candidate_valid` is false and every prospective
  generation/catalog field is JSON null;
- status is explicitly database-only: it does not calculate a warehouse
  fingerprint, inspect filesystem/catalog files, run DuckDB, or validate a
  PostgreSQL/warehouse restore; and
- absent catalog fields are JSON null, not omitted or fabricated.

For a nonexistent release, return the existing release-not-found failure rather
than an all-null object.

## Failure codes and redaction

Add fixed codes for:

```text
filter_catalog_busy
filter_catalog_release_not_ready
filter_catalog_published_mismatch
filter_catalog_plan_invalid
filter_catalog_population_failed
filter_catalog_database_failed
filter_catalog_inconsistent
```

Reuse Story 29 fixed extraction failures. Map errors through existing sanitized
CLI behavior. Logs identify safe command/action, fixed phase, catalog ID where
already generated, and fixed failure code. Never log raw SQL, paths, DuckDB
stderr, plan/provider/filter values, source URL, or credentials.

## Transaction and concurrency invariants

- No transaction spans DuckDB extraction.
- Advisory build lock spans candidate selection through final ready/failed
  state.
- Workers do not acquire the build lock and may progress concurrently.
- Catalog build does not acquire the singleton control lease or release-row
  `FOR UPDATE` locks for its full duration.
- Short database phases lock only the exact catalog/release rows they inspect.
- Story 31 activation may occur concurrently; it either finds an exact ready
  catalog or rejects without publication. It never waits for a building
  catalog to finish.
- A build started against a release that becomes inactive/changes generation
  may finish ready, but Story 31 accepts it only if exact target state still
  matches. No automatic deletion is required.

## Required tests

### Candidate selection

- Building first checkpoint targets generation 1 and exact publishable outputs.
- Active release with one new output targets current+1 and union of old/new.
- Active with no new output targets current and returns current published
  catalog unchanged.
- Inactive release uses exact persisted membership/current generation and
  never adds output.
- Terminal file failures are omitted; consumer/attachment failure blocks.
- Planless and incomplete outputs never enter the candidate.
- Candidate ordering and fingerprint are deterministic.

### Lifecycle/idempotency

- Successful build creates one ready header and exact child rows/counts.
- Repetition returns unchanged without invoking DuckDB.
- Failed extraction records failed state with no child rows.
- Retry replaces failed state and succeeds.
- Simulated crash leaves building; after session-lock release, retry replaces
  it without a heartbeat.
- Concurrent same-release build returns busy; different releases do not
  collide.
- Stale unpublished ready catalog is replaceable.
- Published catalog is never replaceable/deletable.
- Database failure at each phase exposes no ready partial rows.

### Plans

- Identical five-field tuples from several outputs become one plan plus several
  plan/output rows.
- Same plan ID with another issuer/name remains distinct.
- Sponsor differences do not split canonical identity.
- Missing/unassigned/nonterminal/failed attachment state fails build.
- Search text is exact and deterministic.

### CLI

- Help, missing/duplicate/unknown flags, invalid payer/month, status/build
  success, busy, cancel, and sanitized failures follow existing conventions.
- Status is database-only and never invokes warehouse/DuckDB.
- Build JSON has the exact fields and no sensitive values.

## Acceptance criteria

- One explicit command produces one complete ready PostgreSQL catalog for the
  exact current/prospective publication generation.
- Candidate selection and plan readiness reuse Story 27 authority rather than
  diverging SQL.
- Build is safe with active workers because output-set races become exact stale
  catalog rejection in Story 31.
- Ready state is atomic, counts reconcile, and no partial child rows are
  visible.
- Retry/crash behavior needs only an advisory lock and catalog state; no worker,
  queue, heartbeat, or repair daemon is added.
- Published catalogs remain immutable and usable for rollback/in-flight query
  state.

## Non-goals

- Publishing outputs or changing release status/generation.
- Automatically invoking build from a worker or scheduler.
- Adding a force flag or treating failed catalog build as a skippable warning.
- Modifying consumer/warehouse files.
- Adding web server/UI/switch commands.
- Adding external code definitions, ZIP/NPI/expiration filters, or analytical
  aggregate caches.
- Retaining catalog attempt history or introducing a generic workflow engine.

## Documentation

Document the proposed build/status command and required build-before-activate
runbook in clearly planned README/DESIGN sections until Story 31 is implemented.
Do not place proposed commands in the current implemented command list.

## Implementation notes

Expected implementation areas:

```text
internal/filtercatalog/build.go
internal/filtercatalog/status.go
internal/filtercatalog/plans.go
internal/filtercatalog/fingerprint.go
internal/cli/cli.go
internal/cli/help.go
```

Reuse exact release helpers through one purpose-built candidate operation. Use
concrete structs, explicit queries, and existing pgx pool/transaction patterns.
Do not introduce repositories, a unit-of-work abstraction, reflection-based
COPY, or general job lifecycle machinery.
