# Story 11: Consumer snapshot ingest worker

## Status

Implemented.

## User story

As a pipeline operator, I want each eligible payer/feed/month snapshot ingested
once into the shared warehouse so that parsed rates become queryable without
coupling ingestion to the current TOC plan set.

## Goal

Implement the `consumer.ingest` worker through the public `mrfconsumer` Go
package, validate exact consumer `1.5.0` publication, recover safely when the
warehouse publication succeeded before PostgreSQL acknowledgement, and mark
the durable snapshot stage succeeded.

This story does not create plan batches, write `plans.json`, invoke
`AttachPlans`, run provider enrichment, or expose a snapshot as plan-ready.

## Dependencies and scope

This story depends on Stories 01 through 10 and adds the pinned module:

```text
github.com/enotpoloskun/mrfconsumer
```

Do not commit a workspace-relative `replace` directive. Call the public package
in process:

```go
report, err := mrfconsumer.Ingest(ctx, cfg)
```

Do not execute the consumer CLI, import consumer `internal` packages, copy its
transformations, or open DuckDB. Consumer Story 21 is normative: parser input
`1.1.0`, warehouse/snapshot `1.5.0`, six snapshot datasets, a pinned provider
catalog, a warehouse-level plan-association schema seed, and plan-independent
ingestion.

## Product decisions

- One `consumer.ingest` job owns one `mrf_snapshots.id`.
- Consumer output ID is exactly `mrf-<mrf_snapshots.id>`.
- The consumer receives payer and feed identity from `mrf_feeds`, and the
  snapshot's stored collection month; none is inferred from a URL or parser
  output.
- Plans are never passed to `Ingest`. A snapshot may publish before its first
  plan attachment and is then valid warehouse state but not plan-ready.
  Planless rates may be queryable. Serving that must not expose planless rates
  waits on the PostgreSQL-derived plan-ready rule; the worker does not hide
  warehouse files.
- The manually produced provider catalog remains an operator input. The
  pipeline never invokes `mrfenricher` or mutates the configured catalog.
- The first successful ingestion pins the catalog into the warehouse according
  to the consumer contract. Later ingestions must match that pinned identity.
  A pinned-catalog identity mismatch returned as consumer `ErrOutput` maps to
  `consumer_ingest_output_failed`. `consumer_ingest_provider_changed` applies
  only to the pipeline's configured-path metadata guard.
- The `consumer` queue has maximum concurrency one and is the only supported
  writer path to the configured warehouse.
- A valid already-published exact snapshot is reusable after lost database
  acknowledgement. An invalid or conflicting publication is preserved and
  fails closed.
- Parsed MRF output is shared and retained. Ingestion never deletes or changes
  it.
- Story 11 tests create no attachment batches.
- Wire `OnProgress` to a throttled Story 03 `progress` logger. Do not leave it
  nil. Do not persist percent in PostgreSQL.

## Worker registration

Add:

| Queue | Maximum workers | Registered kinds |
|---|---:|---|
| `consumer` | 1 | `consumer.ingest` |

All prior queues remain registered. `consumer.attach_plans` is still not
registered, and Story 11 does not create attachment jobs. Story 12 adds that
worker and schedules the plan backlog.

## Provider catalog and warehouse startup contract

Before River starts, complete the Story 04 local path validation and require:

- `MRFPIPELINE_PROVIDER_CATALOG_PATH` is a readable real directory rather than
  a symlink;
- `MRFPIPELINE_WAREHOUSE_PATH` is absent, an empty real directory, or a real
  directory whose `warehouse.json` is strict JSON with exact warehouse schema
  `1.5.0`;
- the configured catalog is lexically separate from the artifact root and
  service selector; and
- the configured catalog is lexically separate from the warehouse except for
  the consumer-supported exact `<warehouse>/provider_catalog` path.

Startup recognition is shallow: a real warehouse directory plus strict
`warehouse.json` with exact `1.5.0`. Do not require the provider-catalog copy
or plan-schema seed at startup; the consumer owns that documented recovery.
Unexpected warehouse structure is rejected later by `Ingest`.

The warehouse-owned catalog exception is valid only for an already recognized
consumer `1.5.0` warehouse. A missing or new warehouse requires an external
catalog path.

At startup, record the configured external catalog root's device/inode where
available, nanosecond modification time, and its manifest file metadata. Check
those values before every ingest. This is an operator-change guard, not catalog
identity and not a checksum. The consumer remains the authority that validates
the catalog manifest, Parquet, relationships, release, and compatibility with
the warehouse-pinned catalog.

Startup does not create, repair, or migrate a warehouse. A nonempty directory
whose `warehouse.json` is absent, malformed, or not exact `1.5.0` fails worker
startup. Detailed provider-catalog validation, seed presence, and the
consumer's documented recoverable initialization work occur inside `Ingest`.

## Job argument and claim

Exact argument:

```json
{"mrf_snapshot_id":211}
```

In one short transaction, lock the snapshot row, its source row, and its feed
row in that order and:

- require source parse succeeded;
- return stale no-op for a different consume job ID;
- return completed no-op for consume succeeded;
- return terminal no-op for consume failed;
- change assigned pending/same-job running to running when consistent; or
- return safe invariant failure for blocked/missing/inconsistent state.

Set `started_at` only on the first claim if the schema later provides such a
field; version 1 `mrf_snapshots` has no separate start timestamp, so Story 11
updates only `consume_status`, `failure_code`, and `updated_at`.

Do not hold a PostgreSQL transaction or row lock while validating files or
running the consumer. After claim, derive all values from the locked rows that
were read; a later success transaction rechecks them before committing.

## Input preflight

Require the source's exact Story 10 parsed directory and its validated
completion boundary:

```text
<artifact-root>/mrf/mrf-source-<source-id>/parsed/manifest.json
```

Re-run Story 10's strict layout and manifest validation before consumer
invocation. Require exact parser `1.1.0`, eight datasets, no `plans` dataset or
count, the configured service selector, and the expected generated source
path. A missing, partial, changed, symlinked, or unsupported parser output is a
consumer-input orchestration failure; do not repair it from this worker.

Recheck provider-catalog root metadata from startup. Do not inspect, log, or
copy provider NPIs or taxonomy values in the pipeline.

## Consumer configuration

When no completed consumer publication is recognized, build exactly:

```go
cfg := mrfconsumer.Config{
    InputPath:          parsedPath,
    ProviderCatalogPath: configuredProviderCatalogPath,
    OutputPath:         configuredWarehousePath,
    PayerID:            feed.PayerID,
    FeedID:             feed.FeedID,
    CollectionMonth:    formatMonth(snapshot.CollectionMonth),
    OutputID:           formatSnapshotOutputID(snapshot.ID),
    OnProgress:         throttledIngestProgress,
}
report, err := mrfconsumer.Ingest(ctx, cfg)
```

The production field order follows the consumer's public type and `gofmt`.
`formatMonth` emits exact `YYYY-MM`; output ID emits `mrf-<positive base-10
ID>` with no zero padding.

`throttledIngestProgress` is a Story 03 `progress` adapter. It logs the
consumer's fixed phase tokens (`validating_input`, `provider_relationships`,
`rate_facts`, `publishing`) and integer `percent`. It applies the 30-second
throttle. It must not log `OutputID`, paths, payer, feed, month, catalog
identity, or report counts.

Do not pass plans, a TOC path, taxonomy values, a DuckDB handle, PostgreSQL,
job identity, batch identity, callbacks that expose domain values, or generic
storage options. Pass the River context unchanged. Do not leave `OnProgress`
nil.

## Consumer initialization and recovery

Rely on consumer `1.5.0` for its owned initialization protocol:

- an absent or empty warehouse is initialized with `warehouse.json`;
- the first accepted external provider catalog is copied and pinned under
  `<warehouse>/provider_catalog`;
- a recognized warehouse whose metadata was published before its provider
  catalog copy completed has that deterministic missing copy completed;
- a recognized warehouse missing only
  `plan_associations/_schema/part-00000.parquet` has the valid zero-row seed
  completed before a snapshot is published; and
- a present corrupt catalog copy, corrupt seed, unexpected warehouse content,
  unsupported version, or catalog identity conflict returns `ErrOutput` and is
  never overwritten.

The pipeline does not reproduce, bypass, or manually repair these consumer
operations. A retry calls `Ingest` again when no final snapshot exists, so the
consumer can finish its documented initialization recovery.

## Exact completed snapshot recognition

The expected final path is:

```text
<warehouse>/snapshots/collection_month=<YYYY-MM>/payer_id=<payer-id>/output_id=mrf-<snapshot-id>
```

Before calling `Ingest`, inspect only the exact generated target and the fixed
warehouse metadata. This recognizer exists for the crash window where the
consumer atomically published a snapshot but PostgreSQL still says running or
pending.

If the target is absent, proceed to `Ingest`. Empty month/payer parents are not
completion. If any entry exists at the exact target, do not call `Ingest`;
preserve it and require all of the following:

- target and its ancestors beneath the warehouse are real directories rather
  than symlinks;
- `warehouse.json` is strict JSON with exact warehouse schema `1.5.0` and a
  valid provider-catalog identity;
- target `manifest.json` is a real private regular file, strict JSON, and has
  exact manifest/output schema `1.5.0`;
- manifest `output_id`, payer, feed, collection month, and provider-catalog
  identity exactly match the database-derived values and warehouse metadata;
- the manifest has exactly the six datasets `rate_facts`,
  `rate_provider_groups`, `provider_groups`, `provider_group_memberships`,
  `ingestions`, and `network_names`, with nonnegative row/part counts;
- target contains exactly those six real dataset directories plus
  `manifest.json`, with no snapshot-local plans directory; and
- each dataset contains exactly the manifest-declared contiguous private
  regular non-symlink parts named
  `mrf-<snapshot-id>-part-<ordinal>.parquet`, using the consumer's canonical
  formatting: a minimum of five decimal digits, contiguous ordinals from zero,
  and wider ordinals such as `100000` valid. Do not impose a five-digit
  maximum. There are no gaps or extra entries.

Use the consumer's documented JSON size bounds and strict duplicate/unknown
member rules. This recovery recognizer validates the immutable publication
envelope; it does not rescan rate/provider rows or reimplement consumer
transformations.

A manifest-absent or invalid target is not reset or deleted. It is
`consumer_ingest_output_invalid` and requires Story 13 operator
reconciliation. Consumer publication is atomic, so a normal interrupted
ingestion leaves work only under the consumer-owned `.staging` directory and
does not create this final target.

## Report validation

After `Ingest` returns success, require:

- `Report.OutputID` equals the expected `mrf-<snapshot-id>`;
- `Report.FinalPath`, after the consumer's documented absolute cleaning,
  equals the expected final path without symlink resolution;
- all four reported fact/provider row counts are nonnegative; and
- the completed snapshot recognizer above succeeds.

Do not store the report's row counts in version 1 domain tables. Do not log
them as routine job fields. The warehouse manifest remains the durable count
record.

## Success transaction

After a successful new ingestion or exact completed-output recognition:

1. Lock and re-read the snapshot, source, and feed in the claim order.
2. Verify the same consume job remains assigned.
3. Return no-op if consume already succeeded.
4. Require source parse succeeded, consume running, and unchanged source/feed/
   month relationships.
5. Set snapshot consume succeeded, clear failure code, update `updated_at`, and
   commit.

Story 11 by itself does not insert a plan batch or attachment job. Story 12
supersedes this success boundary by calling its common scheduler before commit,
so ingest success and the initial batch/job become visible together when plans
are known. Story 12 also sweeps snapshots that completed before its deployment.

If the database commit fails after publication, retry recognizes the exact
completed output and executes only this success transaction. It never calls
`Ingest` with an already-published output ID.

## Retry, failure, and cancellation

Before attempt eight, input, provider metadata, consumer, output-recognition,
report, or database failure returns the assigned consume stage to pending and
returns a safe error to River. Preserve all consumer and parser artifacts.

Use fixed safe classifications based on typed boundaries:

```text
consumer_ingest_config_invalid
consumer_ingest_input_invalid
consumer_ingest_output_failed
consumer_ingest_output_invalid
consumer_ingest_provider_changed
consumer_ingest_database_failed
```

Mapping is exact:

- `mrfconsumer.ErrInvalidConfig` -> `consumer_ingest_config_invalid`;
- `mrfconsumer.ErrInvalidInput` -> `consumer_ingest_input_invalid`;
- `mrfconsumer.ErrOutput` -> `consumer_ingest_output_failed`, including a
  pinned-catalog identity mismatch returned by the consumer;
- pipeline configured-path provider-catalog metadata mismatch ->
  `consumer_ingest_provider_changed` only;
- pipeline preflight/recognition/report mismatch other than that metadata
  guard -> the corresponding fixed input or output-invalid code;
- database failure -> `consumer_ingest_database_failed`; and
- context cancellation/deadline remains the context error.

Do not classify by matching consumer error strings. On the eighth attempt,
mark only the snapshot consume stage failed with the most specific safe code.
The source parse and all TOC/plan rows remain intact.

Never include consumer errors, paths, URLs, plan/provider/service values,
catalog identity values, report counts, manifest contents, or database
diagnostics in logs or `failure_code`.

## Required tests

### Unit tests

- Configuration derives only the exact public consumer fields from database
  identity and configured roots; plans and batch values cannot enter it.
- Output/month formatting is deterministic numeric formatting without hashes.
- Claim and success transitions require the assigned job and source parse.
- Startup accepts only absent/empty or a real directory whose
  `warehouse.json` is exact `1.5.0`, without requiring seed or catalog copy,
  and enforces the warehouse-owned catalog exception.
- Provider metadata change detection uses filesystem metadata, not a digest,
  and is the only path to `consumer_ingest_provider_changed`.
- Completed-output recognition is strict, redacted, and does not scan rows.
  Part ordinals use canonical consumer formatting with a five-digit minimum,
  not a five-digit maximum.
- Typed consumer errors map to fixed failure codes without string inspection.
  Catalog identity `ErrOutput` maps to `consumer_ingest_output_failed`.
- Ingest `OnProgress` is wired, throttled, redacted, and skipped when a
  completed snapshot is recognized without calling `Ingest`.

### Integration tests

Using PostgreSQL/River, a small exact parser 1.1.0 fixture, and a real provider
catalog fixture:

- First ingest initializes a 1.5.0 warehouse, pins the catalog, repairs the
  plan seed when needed, and publishes six datasets with no snapshot plans.
- A recognized warehouse missing its catalog copy or seed is repaired through
  `Ingest`; a corrupt present copy/seed fails closed.
- Two eligible snapshots execute serially on the one consumer queue.
- A successful snapshot uses exact payer/feed/month/output identity from the
  database and does not read `mrf_plans`.
- Cancellation before publication leaves no final target and retry succeeds.
- Killing after final rename but before database commit causes retry to
  recognize the publication and skip a second `Ingest` call.
- A conflicting, partial, symlinked, wrong-version, or identity-mismatched
  final target is preserved and rejected.
- Database rollback after publication leaves state retryable and the next
  attempt converges without rewriting the warehouse.
- Final failure leaves parsed output, provenance, and plans intact.
- No consumer writers overlap.
- Story 11 tests create no attachment batches.

Run:

```text
go test ./...
go test -race ./...
go vet ./...
```

## Acceptance criteria

- Every eligible snapshot is ingested plan-independently through the public
  consumer API at maximum warehouse concurrency one.
- Consumer input is exact parser 1.1.0 output and publication is exact
  warehouse/snapshot 1.5.0 with six datasets.
- The configured manual provider catalog is validated and pinned by the
  consumer; the pipeline never runs enrichment.
- Consumer-owned catalog/seed initialization crash windows recover through the
  consumer contract.
- Lost PostgreSQL acknowledgement after atomic snapshot publication converges
  by exact recognition without a second ingestion.
- A completed snapshot with no plan attachment is valid warehouse state and
  may be queryable. Serving waits on the PostgreSQL-derived plan-ready rule.
- Failures do not delete or rewrite parser output or a published warehouse.

## Non-goals

- Creating attachment batches, plan JSON, or calling `AttachPlans`.
- Querying TOC output or passing any plans to `Ingest`.
- Automatically running or refreshing `mrfenricher`.
- Migrating a pre-1.5.0 warehouse or parser 1.0.0 output.
- Adding a second warehouse, S3 warehouse, DuckDB call, or concurrent writer.
- Persisting consumer row counts in PostgreSQL.
- Deleting parsed MRF output or calculating hashes.
