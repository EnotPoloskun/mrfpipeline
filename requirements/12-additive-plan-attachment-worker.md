# Story 12: Additive plan attachment worker

## Status

Implemented.

## User story

As a pipeline operator, I want newly discovered plans attached additively to an
already ingested MRF snapshot so that overlapping TOCs converge without
re-parsing the MRF or rewriting rate/provider data.

## Goal

Create immutable plan batches from unassigned PostgreSQL plans, project each
batch to the consumer's exact six-property JSON input, execute
`mrfconsumer.AttachPlans` on the serialized consumer queue, and continue
batching later plans until the snapshot has no unresolved known association.

This story completes the normal production pipeline. It does not remove or
correct a plan, retry terminal failed batches automatically, query DuckDB in
production, or add a UI.

## Dependencies and scope

This story depends on Stories 01 through 11 and uses the already pinned public
module:

```text
github.com/enotpoloskun/mrfconsumer
```

Call the package in process:

```go
report, err := mrfconsumer.AttachPlans(ctx, cfg)
```

Do not execute the consumer CLI, import consumer `internal` packages, write
Parquet in the pipeline, or copy the consumer's set-difference logic. Consumer
Story 21 is normative for plan JSON validation, sponsor-independent identity,
global plan-store validation, immutable part publication, and exact warehouse
`1.5.0` behavior.

## Product decisions

- Plan identity is the exact five fields already stored uniquely in
  `mrf_plans`; sponsor remains validation input but not identity.
- One plan can belong to only one frozen batch, enforced by the Story 02
  database constraint.
- Consumer `PlanBatchID` is exactly
  `plan-batch-<plan_attachment_batches.id>`. It is a database-formatted
  publication attempt ID, not a River job ID and not plan identity.
- A distinct batch always receives a new database ID. Retry of the same batch
  reuses its existing database ID, frozen items, JSON, and consumer batch ID.
- At most one pending, running, or failed unresolved batch exists for a
  snapshot under supported application behavior.
- A pending/running batch leaves later plans unassigned. Its success schedules
  the next batch. A failed batch blocks later batches until Story 13 explicitly
  retries that same batch.
- Each batch contains all plans currently unassigned for that snapshot. There
  is no arbitrary plan-count limit in version 1. Add a cap only after
  representative measurements show it is necessary. A zero-count batch is
  never created.
- Attachments are additive only. No plan part, rate snapshot, parser output,
  or database plan identity is removed, replaced, merged, or revised.
- A consumer success with `AddedPlanCount=0` is a valid idempotent success.
  `added_plan_count` stores the last acknowledged consumer report, not an
  independently reconstructed warehouse total.
- All `Ingest` and `AttachPlans` calls share the one-worker `consumer` queue.
- DuckDB is test-only. Production pipeline code does not query it.
- No `is_plan_ready` column is added. Serving derives the Story 12 readiness
  rule from PostgreSQL.

## Worker registration

The final queue registration is:

| Queue | Maximum workers | Registered kinds |
|---|---:|---|
| `consumer` | 1 | `consumer.ingest`, `consumer.attach_plans` |

Do not create a second attachment queue. Queue serialization plus the Story 13
single-process lease protects the one local warehouse from overlapping writers.

## Common plan-batch scheduler

Add one internal helper used only inside a caller-owned PostgreSQL transaction.
For one positive snapshot ID, it:

1. Locks `mrf_snapshots` first.
2. Requires the snapshot to exist and validates its lifecycle consistency.
3. Returns no-op unless `consume_status=succeeded`.
4. Checks `plan_attachment_batches` for that snapshot under the lock.
5. If any batch is pending, running, or failed, returns no-op. A failed batch
   is deliberately unresolved and must not be bypassed.
6. Selects all `mrf_plans` for the snapshot that have no
   `plan_attachment_batch_items` row, ordered by the five identity fields and
   then numeric plan ID, and locks those plan rows.
7. Returns no-op if no unassigned plan exists.
8. Inserts one pending `plan_attachment_batches` row with
   `requested_plan_count` equal to the selected row count.
9. Inserts one batch-item row for every selected plan.
10. Inserts one `consumer.attach_plans` job through River `InsertTx` with the
    new batch ID and stores its River job ID.

The batch, frozen item set, count, River job, and job ID commit together. A
uniqueness or transaction conflict is retried from a fresh snapshot lock; it
does not create a second logical batch.

The helper never selects plan values into River arguments, never changes a
succeeded batch, never moves a plan between batches, and never derives batch
identity from plan content.

## Eligibility integration

Story 12 extends three normal paths with the common scheduler:

### Consumer ingest success

Extend Story 11's success transaction. After setting the snapshot consume
stage succeeded, call the scheduler for that same locked snapshot before
commit. Ingest success and initial batch/job creation therefore become visible
together when at least one plan is already known.

### TOC import finalization

Extend Story 08's finalization transaction. After all rows have been imported,
select the distinct snapshots referenced by that TOC, order their IDs, lock
each snapshot in order, and call the scheduler. Then mark the TOC import
succeeded and commit the finalization plus any new batches/jobs together.

This ensures plans imported after a snapshot was consumed become eligible.
It does not wait for another TOC, discovery run, or payer-wide completion
signal.

### Deployment backlog sweep

Before the first Story 12 River client starts accepting work, scan consumed
snapshots in ascending ID pages and invoke the scheduler in one short
transaction per snapshot. This schedules plans left unassigned by snapshots
that completed under Story 11 before the attachment worker existed.

The sweep processes only valid consumed snapshots and unassigned plans. It
does not reset running/failed stages, replace jobs, or repair inconsistent
states; those are Story 13 responsibilities. Repeating the sweep is safe. A
large backlog may delay startup. The sweep remains serial and transactionally
bounded per snapshot.

### Batch completion

After one attachment succeeds, mark it succeeded and call the scheduler for
the same snapshot in the same transaction. Plans imported while the batch was
pending/running are thereby frozen into the next batch and job.

## Job argument and claim

Exact argument:

```json
{"plan_attachment_batch_id":230}
```

First read the batch's snapshot ID without changing state. In one short claim
transaction, lock the snapshot, then lock the batch, and:

- require snapshot consume succeeded;
- require `requested_plan_count > 0` and exactly that many frozen items;
- return stale no-op for a different River job ID;
- return completed no-op for batch succeeded;
- return terminal no-op for batch failed;
- change assigned pending/same-job running to running, setting `started_at`
  only on its first transition; or
- return safe invariant failure for missing/inconsistent state.

Also require every item to reference an `mrf_plan` owned by the batch's
snapshot. Do not hold database locks while generating JSON or calling the
consumer. The success transaction repeats all ownership/count checks.

## Exact plans JSON projection

The generated path is:

```text
<artifact-root>/plan-batches/plan-batch-<batch-id>/plans.json
```

Load only the frozen batch items, joined to their snapshot plans, ordered by:

```text
plan_name
issuer_name
plan_id_type
plan_id
plan_market_type
```

Emit one uncompressed UTF-8 JSON array from a typed struct in exact field
order. Use a pinned encoder configuration: compact JSON, `SetEscapeHTML(false)`,
and one trailing newline. Every element has exactly these six members in this
order:

```json
{"plan_name":"...","issuer_name":"...","plan_sponsor_name":"... or null","plan_id_type":"ein or hios","plan_id":"...","plan_market_type":"group or individual"}
```

The illustrative string above does not relax JSON typing. EIN emits the
stored required nonempty sponsor as a JSON string. HIOS always emits JSON
null sponsor (`"plan_sponsor_name":null`); never emit an empty string. All
other values are preserved from PostgreSQL through that pinned encoder. Do not
trim, normalize, infer, case-fold, or rely on `encoding/json` default HTML
escaping.

The document contains exactly `requested_plan_count` distinct objects, ends
with one newline, and has no wrapper, metadata, TOC fields, MRF location,
output ID, batch ID, timestamps, additional fields, or hashes.

Encoder behavior is part of the pipeline artifact contract. Existing
`plans.json` must equal the newly rendered canonical bytes exactly.

## Plan document publication and reuse

Build the complete private file under the artifact root `.staging` directory,
close it, and atomically rename a completed private batch directory into the
exact final path. Temporary names do not include plan values and do not end in
the final `plans.json` path.

Preflight is exact:

- absent target: generate and publish it;
- empty target or target without `plans.json`: treat it as manifest-absent
  pipeline-owned partial work, remove/recreate only that exact generated batch
  leaf, then publish the frozen document;
- existing real `plans.json`: require its bytes to equal the newly rendered
  canonical projection exactly, including order, sponsor typing, compact
  encoding, HTML-escape setting, and trailing newline;
- symlink, wrong type, extra entry, malformed JSON, duplicate/unknown member,
  count mismatch, or content mismatch: preserve it and fail closed.

Strict comparison uses the database rows directly. There is no file digest,
content-addressed path, JSON hash, or batch identity derived from contents.

Version 1 retains a valid `plans.json` after attachment success until Story 13
applies the documented cleanup policy. A retry may therefore reuse it without
regeneration.

## Consumer configuration

Build exactly:

```go
cfg := mrfconsumer.AttachPlansConfig{
    PlansPath:   plansPath,
    OutputPath:  configuredWarehousePath,
    OutputID:    formatSnapshotOutputID(snapshot.ID),
    PlanBatchID: formatPlanBatchID(batch.ID),
}
report, err := mrfconsumer.AttachPlans(ctx, cfg)
```

`formatSnapshotOutputID` emits `mrf-<positive snapshot ID>` and
`formatPlanBatchID` emits `plan-batch-<positive batch ID>`, without padding.
Do not use the River job ID. Pass the River context unchanged.

Before invocation, re-run Story 11's warehouse `1.5.0` recognition and require
exactly one completed rate snapshot for the expected output ID. The consumer
then performs authoritative validation of the plan JSON, complete global plan
store, existing plan identities, and publication destination.

## Report and publication validation

On success require:

- `AttachPlansReport.OutputID` equals `mrf-<snapshot-id>`;
- `AttachPlansReport.PlanBatchID` equals `plan-batch-<batch-id>`; and
- `0 <= AddedPlanCount <= requested_plan_count`.

`AddedPlanCount` may be smaller than the batch size because the consumer is
the final warehouse idempotency authority. Zero is success, including retry
after the same plans were already published.

When `AddedPlanCount > 0`, require the exact consumer-owned part:

```text
<warehouse>/plan_associations/output_id=mrf-<snapshot-id>/plan-batch-<batch-id>-part-00000.parquet
```

to exist as a real private regular non-symlink file. Let the consumer validate
the complete plan store and exact Parquet schema; the pipeline does not decode
or rewrite the part.

When `AddedPlanCount == 0`, the batch part may be absent or may already exist
from an earlier successful publication whose acknowledgement was lost. Do not
infer a different count from its presence and do not remove it.

## Success transaction

After report validation:

1. Lock the snapshot, then the batch.
2. Verify the same River job remains assigned.
3. Return no-op if the batch already succeeded.
4. Require snapshot consume succeeded, batch running, unchanged ownership,
   exact requested item count, and all items owned by the snapshot.
5. Set batch status succeeded, store the exact nonnegative
   `report.AddedPlanCount`, clear failure code, set `completed_at`, and update
   `updated_at`.
6. Invoke the common scheduler for the same already-locked snapshot so plans
   that arrived during execution receive the next batch/job.
7. Commit all changes together.

If the consumer published a part but the transaction fails, River retry uses
the same frozen JSON and batch ID. `AttachPlans` sees those identities already
present and returns a successful no-op; the database may consequently record
`added_plan_count=0` for this recovered acknowledgement. That field is the
acknowledged report count, not an independent warehouse audit total.

## A/B then B/C behavior

The required end-to-end sequence is:

```text
TOC/import yields plans A,B
snapshot ingest succeeds
batch 1 freezes A,B
consumer publishes A,B; batch 1 succeeds

later TOC/import yields plans B,C
B conflicts with existing mrf_plans identity; C is inserted unassigned
batch 2 freezes only C
consumer publishes only C; batch 2 succeeds

redelivery of either completed River job is a domain no-op
warehouse exposes A,B,C without another MRF parse or rate ingest
```

If a deliberately constructed attachment input overlaps warehouse state, the
consumer still performs the exact set difference. The pipeline never edits an
old plan part or replaces one Parquet file with an A/B/C aggregate.

## Retry, failure, and cancellation

Before attempt eight, JSON generation/reuse, warehouse preflight, consumer,
report, output, or database failure returns the assigned batch from running to
pending and leaves `started_at` intact. Preserve frozen items, valid JSON,
warehouse snapshots, and every published plan part.

Use fixed safe classifications:

```text
plan_attach_config_invalid
plan_attach_input_invalid
plan_attach_output_failed
plan_attach_output_invalid
plan_attach_database_failed
plan_attach_invariant
```

Mapping is exact:

- `mrfconsumer.ErrInvalidConfig` -> `plan_attach_config_invalid`;
- `mrfconsumer.ErrInvalidInput` -> `plan_attach_input_invalid`;
- `mrfconsumer.ErrOutput` -> `plan_attach_output_failed`;
- frozen batch/JSON/report/publication mismatch -> the fixed input, output, or
  invariant code;
- database failure -> `plan_attach_database_failed`; and
- context cancellation/deadline remains the context error.

Do not classify by consumer error strings. On the eighth attempt, mark this
same batch failed with the most specific code and set `completed_at`. Its
items remain assigned to it, newer plans remain unassigned, and the scheduler
must not create a bypass batch. Story 13 provides explicit retry of the same
batch ID.

Never include consumer errors, paths, plan values, output/batch IDs, counts,
warehouse entries, manifest contents, or database diagnostics in production
logs or `failure_code`.

## Readiness rule

No mutable ready flag is added. A snapshot is plan-ready only when:

- consume status is succeeded;
- at least one attachment batch for it is succeeded;
- no canonical plan is unassigned; and
- no batch for it is pending, running, or failed.

A planless snapshot is valid warehouse state but not plan-ready. A later UI or
query may derive this rule from PostgreSQL; it must not use directory presence,
River retention, or consumer `AddedPlanCount` as readiness identity.

## Required tests

### Unit tests

- Scheduler locks the snapshot, creates at most one unresolved batch, freezes
  all unassigned plans, never creates a zero-count batch, and inserts its
  River job atomically.
- Pending/running/failed batches block a new batch; succeeded batches do not.
- Batch and output formatting uses numeric IDs, not River IDs or hashes.
- JSON has exact fields/order/types, deterministic plan ordering, HIOS null,
  EIN sponsor, compact encoder with `SetEscapeHTML(false)`, one newline, and
  no domain extras.
- Existing valid JSON is compared as exact canonical bytes; partial work is
  targeted; invalid/mismatched final input is preserved.
- Claim/success use snapshot-then-batch lock order and verify exact item count.
- Typed consumer errors and report mismatches map to fixed redacted codes.
- Readiness derivation covers planless, unassigned, pending, failed, and fully
  attached states.

### PostgreSQL/River/consumer integration tests

- Ingest success atomically creates the first batch/job for known plans.
- TOC import after ingest creates a batch for later unassigned plans.
- Story 12 startup sweep schedules a Story 11-era consumed backlog once.
- Plans arriving during a running batch remain unassigned, then receive one
  next batch when it succeeds.
- A failed batch retains its items and blocks bypass batching.
- A/B then B/C publishes A/B and then only C; warehouse association union is
  A/B/C and rate/provider snapshot bytes remain unchanged.
- Repeating identical/overlapping plans returns a successful zero-add no-op.
- Killing before consumer rename exposes no plan part and retry succeeds.
- Killing after consumer rename before database commit reuses the same batch
  and converges without a duplicate plan row or new batch ID.
- Reused batch ID with genuinely new remaining additions fails closed through
  the consumer.
- Empty output-ID directories are tolerated; stray or malformed plan-store
  entries fail without mutation.
- HIOS empty sponsor is never emitted and is rejected if injected.
- All ingest and attach calls are serialized on one consumer queue.
- A later query on one supported already-open DuckDB connection sees newly
  published associations without view recreation, while rate counts stay
  unchanged. This is an acceptance test of the consumer contract, not a
  production pipeline DuckDB call.

Run:

```text
go test ./...
go test -race ./...
go vet ./...
```

## Acceptance criteria

- Every consumed snapshot with known plans converges through immutable frozen
  batches and additive consumer calls.
- The orchestrator creates the exact projected six-property JSON artifact;
  it never passes TOC Parquet directly to the consumer.
- A/B followed by B/C yields A/B/C while only C is written on the second
  normal attachment.
- New plans never trigger MRF download, MRF parse, or consumer rate ingestion.
- Same-batch retry is idempotent; distinct work receives a new numeric batch
  ID; failed work is not bypassed.
- Consumer report success, including zero additions, is recorded durably and
  later unassigned plans are scheduled atomically.
- The warehouse remains append-only and all writers remain serialized.

## Non-goals

- Plan removal, replacement, revision, expiration, or in-place correction.
- Rebuilding a snapshot merely because its plan set grew.
- Passing plan lists to `mrfparser` or `mrfconsumer.Ingest`.
- A mutable consolidated plans Parquet file or manual DuckDB refresh.
- External warehouse writers, several worker processes, or distributed locks.
- Automatic retry of a terminal failed batch; Story 13 owns that operator act.
- Deleting successful plan JSON artifacts before Story 13 retention handling.
- A plan UI, API, or content hashes.
