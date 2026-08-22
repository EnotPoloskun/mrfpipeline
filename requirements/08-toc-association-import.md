# Story 08: TOC association import

## Status

Planned.

## User story

As a pipeline operator, I want completed TOC associations imported
idempotently so that shared MRF URLs are scheduled once, feed/month snapshots
are created independently, and overlapping plans are retained without waiting
for every payer TOC.

## Goal

Implement `toc.import`: independently validate the two TOC Parquet datasets,
stream associations into PostgreSQL, assign the conservative version 1 UHC
feed policy, create shared MRF sources and consumer snapshots, project canonical
plans, and schedule newly eligible MRF downloads or consumer ingests.

This story does not execute MRF download/parse, consumer ingest, or plan
attachment. It creates their durable eligible work only where specified.

## Dependencies and scope

This story depends on Stories 01 through 07 and adds:

```text
github.com/parquet-go/parquet-go v0.30.1
```

Use typed, sequential Parquet readers. Do not introduce DuckDB as an import
runtime, load complete datasets into memory, shell out to a query process, or
import `mrftocparser/internal` packages.

Story 02 remains authoritative for all table constraints and sponsor rules.
Story 03 owns atomic job insertion. Story 07 owns parser invocation and root
layout/manifest validation; this story independently verifies physical
Parquet schemas and row-level contracts before mutating application data.

## Product decisions

- Import is per TOC and starts as soon as that TOC parses. There is no
  payer-wide or discovery-run-wide barrier.
- One exact `mrf_location` creates or reuses one global `mrf_sources` row.
- Different exact locations remain different sources even when filenames or
  bytes might match.
- All association rows are validated in a first bounded pass before the first
  domain mutation. Import uses a second bounded pass for writes.
- Database uniqueness makes a replay idempotent. No import checkpoint table,
  row hash, or content hash is added.
- New sources receive `mrf.download` jobs immediately.
- New snapshots for already parsed sources receive `consumer.ingest` jobs
  immediately; other snapshots remain blocked until Story 10 parse success.
- Plans are inserted immediately but attachment batches are not created until
  Story 12. Once Story 12 is implemented, its common scheduler extends import
  finalization for consumed snapshots without changing the row-import rules in
  this story.
- TOC parser output remains after import in version 1. Story 13 owns retention.

## Conservative UHC feed policy

Version 1 does not have trustworthy payer metadata that maps monthly UHC MRF
URLs into a stable named network series. It therefore does not guess from a
filename, plan name, issuer, reporting entity, URL path, or stripped date.

For each UHC MRF source, assign:

```text
feed_id = mrf-source-<mrf_sources.id>
```

Examples:

```text
mrf-source-1
mrf-source-9021
```

The feed row is unique by `(payer_id, feed_id)`. Its numeric suffix is the
PostgreSQL-generated source ID, formatted without padding. It is not a River
job ID and is not a hash.

Consequences are explicit:

- Every source has a deterministic consumer-compatible feed ID.
- Reusing the same exact source URL in another collection month reuses the feed
  and produces a new monthly snapshot.
- A changed monthly URL creates a different source and therefore a different
  feed, even if a human suspects it is the next version of the same network.
- Version 1 does not claim cross-URL monthly feed continuity. Old distinct
  feeds are not automatically superseded by a newer URL.

This conservative policy avoids silently grouping unrelated rate files. A
future curated feed-mapping story may introduce explicit aliases and a
warehouse rebuild/migration strategy. It must not rewrite this version 1
history by guessing after publication.

## Worker registration

Extend production queues:

| Queue | Maximum workers | Registered kinds |
|---|---:|---|
| `discovery` | 1 | `discovery.run` |
| `toc_download` | 4 | `toc.download` |
| `toc_parse` | 2 | `toc.parse` |
| `toc_import` | 2 | `toc.import` |

MRF download and consumer jobs inserted by this story remain pending until
their worker stories register those queues.

## Job argument and claim

The exact argument is:

```json
{"toc_file_id":72}
```

Lock the TOC row and apply the common lifecycle:

- Require `download_status=succeeded` and `parse_status=succeeded`.
- Different `import_river_job_id`: stale no-op.
- `import_status=succeeded`: completed no-op.
- `import_status=failed`: terminal no-op.
- Assigned `pending`/same-job `running`: set/retain `running` and update
  `updated_at`.
- Missing, blocked, or inconsistent state: safe invariant failure.

Read payer, collection month, and formatted `toc-<id>` output identity during
claim. Version 1 accepts only stored payer `uhc` for feed assignment.

After claim, re-run Story 07 completed-output validation. Missing, altered,
symlinked, unsupported, or malformed output is an import input failure; do not
reset or reparse it from this worker.

## Exact Parquet schemas

Validate physical column order, names, repetition, physical types, logical
types, converted types, and field IDs against `mrftocparser` output schema
`1.0.0`.

### `toc_files`

Exact columns:

```text
toc_output_id
payer_id
collection_month
source_uri
reporting_entity_name
reporting_entity_type
last_updated_on
last_updated_on_raw
source_schema_version
additional_fields_json
```

Read exactly one row across all contiguous parts. Validate its complete
documented semantics and require:

- output ID, payer, and collection month equal the TOC domain row;
- source URI equals the generated former download data path recorded by the
  manifest, even though the download leaf has been removed;
- reporting entity values are valid nonempty strings; and
- row count equals manifest `counts.toc_files == 1`.

The local source path is validation-only and is not copied into PostgreSQL or
logged.

### `mrf_plan_associations`

Exact columns:

```text
toc_output_id
payer_id
collection_month
mrf_location
mrf_filename
plan_name
issuer_name
plan_sponsor_name
plan_id_type
plan_id
plan_market_type
reporting_structure_additional_fields_json
plan_additional_fields_json
file_additional_fields_json
```

Validate each typed row:

- Caller metadata equals the TOC domain row.
- `mrf_location` is the exact valid HTTPS source string from the parser output.
- Nullable `mrf_filename` matches the parser's documented derivation.
- Required plan strings are nonempty valid UTF-8.
- `plan_id_type` is exact `ein` or `hios`.
- `plan_market_type` is exact `group` or `individual`.
- EIN has a nonempty sponsor.
- HIOS sponsor is null or nonempty; empty is invalid.
- Optional additional-field values remain valid canonical JSON objects.

The three additional-field JSON columns are validated but not copied to the
pipeline database.

## First validation pass

Before any insert/update/job transaction:

1. Validate manifest and dataset layout.
2. Validate both physical schemas.
3. Read and validate the one `toc_files` row.
4. Stream every association row across parts in order.
5. Require strict ascending order by the parser's complete association key:
   exact location, plan name, issuer name, nullable sponsor with null first,
   ID type, plan ID, and market type. Extension payloads are retained row data,
   not ordering or association identity.
6. Reject an exact duplicate or out-of-order row.
7. Count rows with overflow checks.
8. Require the final count to equal manifest
   `counts.mrf_plan_associations`.
9. Honor cancellation between row groups and rows.

Use bounded reader buffers and retain only the preceding order key/current row.
Do not build a full association slice or a global URL/plan map.

If any validation fails, no MRF source, feed, snapshot, provenance, plan, or
downstream job may have been inserted by this attempt.

A valid zero-row association dataset completes both passes and marks the TOC
import succeeded without creating downstream records.

## Second import pass

Reopen the validated association parts and stream them again in batches of at
most 1,000 rows. Each batch uses one short PostgreSQL transaction. Do not hold
the transaction while reading the next batch from disk.

Within a batch, process rows in their existing order. Database uniqueness is
the final concurrency arbiter.

### MRF source upsert

For each distinct exact `mrf_location` in the batch:

1. Select or insert `mrf_sources(source_url)`.
2. On insert, set `first_mrf_filename` to the nonempty filename when present,
   download pending, and parse blocked.
3. Insert one `mrf.download` job through `InsertTx` for a new source and store
   `download_river_job_id`.
4. On an existing source, never change its URL or stage state and never insert
   another normal download job.
5. If its `first_mrf_filename` is null and the current association has a
   nonempty filename, set it once with `COALESCE`; never replace an existing
   value.

A uniqueness conflict after attempted insert is handled as an existing source
and does not create a duplicate job.

### Feed and snapshot upsert

For the source ID:

1. Lock the `mrf_sources` row before reading its parse state; this lock is the
   race boundary shared with Story 10 parse finalization.
2. Format `feed_id=mrf-source-<source-id>`.
3. Select or insert `mrf_feeds(payer_id, feed_id)`.
4. Select or insert the unique snapshot `(mrf_source_id, mrf_feed_id,
   collection_month)`.
5. For a new snapshot:
   - set `consume_status=pending` and insert `consumer.ingest` through
     `InsertTx` when the source parse is already `succeeded`; or
   - leave `consume_status=blocked` when source parse is not succeeded.
6. For an existing blocked snapshot whose source parse is now succeeded and
   whose consume job ID is null, set pending and insert its ingest job.
7. Never reopen a failed snapshot or duplicate pending/running/succeeded
   consume work.

The normal import path does not repair suspicious states such as pending with
no job ID or blocked with a nonnull job ID. It returns `domain_invariant` and
leaves Story 13 to reconcile them explicitly.

### Provenance insert

Insert `toc_mrf_plan_associations` with:

- current TOC ID;
- resolved snapshot ID;
- exact location and nullable filename; and
- all six source plan fields, including sponsor.

Use the Story 02 `UNIQUE NULLS NOT DISTINCT` constraint. An exact replay does
nothing. Do not update an existing provenance row or merge sponsor variants.

### Canonical plan projection

Project to `mrf_plans` for the resolved snapshot:

- Identity is exact `(plan_name, issuer_name, plan_id_type, plan_id,
  plan_market_type)`.
- EIN stores the current valid sponsor only when the identity is first
  inserted; later sponsor variants do not replace it.
- HIOS always stores null sponsor, even if provenance contains a sponsor.
- Conflict on the sponsor-independent identity is a no-op.

Story 08 by itself does not create a plan attachment batch or River attachment
job. Story 12 supersedes that finalization boundary: after all rows are
imported it schedules one batch for each touched consumed snapshot that has
unassigned plans and no unresolved batch. Until then, unassigned `mrf_plans`
rows are durable eligibility.

### Batch commit

Source/feed/snapshot upserts, association/plan inserts, and any initial
download/consume jobs created for those rows commit together for that batch.
On transaction failure, retry the whole batch. Earlier committed batches may
remain and are safely replayed from the beginning after a worker retry.

## Import finalization

After the second pass completes, use one final transaction:

1. Lock the TOC row.
2. Verify the same import job remains assigned.
3. Require parse succeeded and import running.
4. With Story 12 present, select distinct snapshot IDs referenced by this TOC,
   lock them in ascending order, and invoke its common plan-batch scheduler.
5. Set `import_status=succeeded`, clear any import failure code, and update
   `updated_at`.
6. Commit the final state and any attachment batches/jobs together.

There is no single successor job because association import may create several
independent MRF sources/snapshots or none. MRF/consumer jobs are inserted with
their owning rows in batch transactions; Story 12 attachment jobs are inserted
with frozen batches during finalization.

A retry after final commit is a no-op. A crash between batches or before
finalization repeats both passes and converges through uniqueness.

## Independence from other TOCs

This worker never queries whether every TOC in a discovery run or payer listing
has parsed. For shared location `L`:

```text
TOC A imports L + plans A,B -> one source, one snapshot, plans A,B
TOC B imports L + plans B,C -> same source/snapshot, add only plan C
```

TOC B does not schedule another MRF download or parse. If it arrives after the
snapshot is consumed, Story 12 later batches plan C for additive attachment.

There is no reparse caused by plan growth and no update to parser output.

## Error, retry, and terminal failure

Fixed safe classifications include:

```text
toc_import_manifest_invalid
toc_import_schema_invalid
toc_import_row_invalid
toc_import_order_invalid
toc_import_database_failed
toc_import_invariant
```

Before attempt eight, return assigned import state to pending and leave
`failure_code` null. Already committed valid batches and jobs remain; retry
replays idempotently.

On the final attempt, set import failed with the most specific safe code.
Previously committed sources/snapshots/jobs are not deleted or canceled because
they are valid projections from rows that passed the complete first validation
pass. Story 13 exposes this partial-import terminal condition for operator
review/retry.

Never store or log Parquet values, URLs, plan data, extension JSON, physical
paths, raw schema text, or database diagnostics.

## Required tests

### Unit tests

- Exact physical Parquet descriptors match published 1.0.0 schemas.
- Typed row validation enforces metadata, filename, plan, sponsor, and
  additional-JSON contracts.
- First pass checks row count/order with bounded memory and performs no writes.
- Feed formatting is exactly `mrf-source-<positive-id>` and needs no source
  string/hash.
- HIOS sponsor projects to null; first EIN sponsor is stable across variants.
- Batch planning never creates attachment batches in this story.
- All error paths redact row values and paths.

### PostgreSQL/Parquet/River integration tests

Prove:

- A valid TOC output imports exact provenance and canonical plans.
- Zero associations succeed with no downstream records/jobs.
- Invalid second or last part causes no mutation because validation completes
  before import.
- Several parts stream in bounded batches and counts match the manifest.
- A new exact location creates one source, one source-based feed, one snapshot,
  and one MRF download job.
- Reimport creates no duplicate source, feed, snapshot, association, plan, or
  job.
- Two TOCs with overlapping plans for one location reuse parse lifecycle and
  produce the union of canonical plans.
- Sponsor variants preserve provenance but not duplicate canonical identities.
- Same source in a different collection month reuses feed and creates a new
  snapshot.
- Different source URLs, including strings differing only by query/case,
  create distinct sources and feeds.
- A new snapshot for an already parsed source gets one consumer ingest job;
  an unparsed source snapshot remains blocked.
- Crash between write batches and retry converges exactly.
- Concurrent imports resolve uniqueness without duplicate jobs.
- Final import success is a no-op on redelivery.
- Queue concurrency does not exceed two.

Run:

```text
go test ./...
go test -race ./...
go vet ./...
```

## Acceptance criteria

- Completed TOC outputs are independently schema/row/order validated before
  application mutation.
- Associations import with bounded memory and retry-safe batch transactions.
- Exact shared locations create one MRF source/download lifecycle across TOCs.
- The conservative source-based UHC feed policy is deterministic and explicit
  about its cross-URL limitation.
- Snapshot, provenance, and sponsor-independent plan rows converge under
  replay and overlap.
- New MRF downloads and already-eligible consumer ingests are scheduled
  atomically with their owning records.
- Import never waits for unrelated TOCs and never reparses an MRF because plans
  were added.
- No consumer plan batch is created before Story 12.

## Non-goals

- Inferring named UHC network/feed continuity across different URLs.
- Reading TOC additional-field JSON into application tables.
- MRF download/parse execution.
- Consumer ingest or plan attachment execution.
- Plan replacement/removal, attachment batches, or correction workflows.
- Deleting imported TOC parser output.
- DuckDB runtime use, unbounded in-memory association maps, URL normalization,
  filenames as identity, or hashes.
