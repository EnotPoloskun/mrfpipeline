# Story 17: Consumer 2.0 feed-free integration

## Status

Implemented.

## User story

As a pipeline operator, I want snapshot ingestion and plan attachment to use
the exact feed-free `mrfconsumer 2.0.0` contract so that pipeline database
identity, consumer manifests, Parquet schemas, and recovery recognition agree.

## Dependencies and compatibility

This story depends on Stories 14–16, parser `1.1.0`, and consumer Stories
22–24.

This completes the atomic Stories 14–17 breaking delivery batch defined by
Story 14. None of those four pipeline stories is independently mergeable,
releasable, or deployable. There is no supported intermediate runtime state.

Pin the exact sibling module revision that implements the approved consumer
contract. The integration accepts exactly:

```text
parser manifest/output              1.1.0
consumer warehouse                  2.0.0
consumer snapshot manifest/output   2.0.0
provider catalog schema             1
```

Before pinning, align consumer Story 22 so unknown nonlegacy manifest metadata
is allowed while required fields/datasets remain exact and any `feed_id` member
is rejected. Pipeline and consumer must use the same recognition boundary.

Consumer `1.5.0` warehouses and publications are unsupported. There is no
pipeline compatibility reader, manifest adapter, Parquet rewrite, dual module
version, feature flag, or migration. Operators create a new pipeline database
and warehouse as required by Story 14.

## Supersession

This story supersedes Stories 11–13 wherever they:

- pin or recognize consumer `1.5.0`;
- lock/read an `mrf_feeds` row;
- pass `FeedID` to `mrfconsumer.Config`;
- expect a manifest `feed_id`;
- validate feed-bearing `ingestions` or rate facts; or
- describe current views as the serving boundary.

It preserves the one-worker consumer queue, provider-catalog guard, exact
output-ID/path formatting, crash-window recognition, additive plan batches,
and immutable publication. Operational context follows the revised Story 14
redaction boundary.

## Worker startup contract

Before River starts, continue validating all configured paths and the pinned
provider-catalog metadata guard. Recognize only:

- an absent warehouse;
- an empty real warehouse directory; or
- a real directory with strict `warehouse.json` declaring exact `2.0.0` and a
  syntactically valid provider-catalog identity.

The consumer owns detailed warehouse/catalog/plan-seed recovery. Pipeline
startup remains shallow and does not modify the warehouse.

A nonempty missing/malformed/unsupported warehouse fails closed. The exact
`<warehouse>/provider_catalog` configured-path exception is allowed only for a
recognized `2.0.0` warehouse.

## Feed-free consumer ingestion

The consume claim locks snapshot then source, reads payer/month directly from
the snapshot, and performs all existing prerequisite/job-ID/status checks.
There is no feed lookup or lock.

When no completed publication is recognized, build the public consumer
configuration with a keyed literal:

```go
cfg := mrfconsumer.Config{
    InputPath:           parsedPath,
    ProviderCatalogPath: configuredProviderCatalogPath,
    OutputPath:          configuredWarehousePath,
    PayerID:             snapshot.PayerID,
    CollectionMonth:     formatMonth(snapshot.CollectionMonth),
    OutputID:            formatSnapshotOutputID(snapshot.ID),
    OnProgress:          throttledIngestProgress,
}
report, err := mrfconsumer.Ingest(ctx, cfg)
```

Never pass a feed, plan list, TOC path, payer-derived network, active month,
release status, database handle, DuckDB handle, job ID, or generic storage
option. Pass the River context unchanged.

Progress retains the consumer's fixed phases and throttle. Validated payer,
month, derived output ID, numeric IDs, and aggregate counts may be structured
operational context. Paths, catalog values, provider/service values, plans,
credentials, raw errors, and response content remain redacted.

## Completed snapshot recognition

The expected final path remains:

```text
<warehouse>/snapshots/
  collection_month=<YYYY-MM>/
    payer_id=<payer-id>/
      output_id=mrf-<snapshot-id>/
```

Crash-window recognition requires:

- exact warehouse `2.0.0`;
- exact snapshot manifest/output `2.0.0`;
- manifest `output_id`, `payer_id`, and `collection_month` equal database
  values;
- no `feed_id` member;
- exact provider-catalog identity agreement;
- exactly the six Story 21 datasets and counts;
- exact real non-symlink directory/part layout; and
- no snapshot-local plan dataset.

The recognizer checks the immutable publication envelope and canonical parts,
not full Parquet row values and not every consumer schema rule. Missing,
duplicate, or wrongly typed required JSON members fail closed. Unknown
nonlegacy metadata is allowed. `feed_id` is explicitly rejected even when its
value is null or empty. JSON object order remains nonsemantic.

A target with absent/invalid manifest is preserved and reported as output
failure; it is never automatically deleted or overwritten. A valid target lets
the worker acknowledge success without calling `Ingest` again.

## Success and failure transitions

Consumer report validation remains exact:

- `OutputID` equals `mrf-<snapshot-id>`;
- `FinalPath` equals the expected cleaned final path;
- counts are nonnegative and compatible with the manifest; and
- report contains no feed field.

The success transaction re-locks snapshot then source, rechecks identity and
job ownership, marks consume succeeded, and invokes the common plan-batch
scheduler. No feed invariant is checked.

Existing retryable/terminal error mapping remains, with version tokens updated:

- invalid parser input -> safe consumer-input failure;
- invalid configured/provider input -> existing provider/input failure;
- unsupported/conflicting warehouse or publication -> output failure;
- cancellation/retryable I/O -> existing River retry behavior; and
- repeated terminal exhaustion -> exact failed stage.

## Additive plan attachment

The public attachment configuration remains:

```go
cfg := mrfconsumer.AttachPlansConfig{
    PlansPath:   plansPath,
    OutputPath:  configuredWarehousePath,
    OutputID:    formatSnapshotOutputID(snapshot.ID),
    PlanBatchID: formatPlanBatchID(batch.ID),
}
```

No feed change is needed in the JSON plan document or plan identity. Update
warehouse/snapshot recognition and consumer error expectations to `2.0.0`.
Attachment still writes only absent plans and never reads/reingests rates.

## Query boundary

Production pipeline workers do not run DuckDB. Consumer Story 23's nine views
and explicit active payer/month relation are integration documentation for the
future query service and Story 19 acceptance, not worker behavior.

Do not write an active-month file into the consumer warehouse or alter
`warehouse.json` during pipeline activation.

## Reconciliation and cleanup

Reconciliation uses feed-free snapshot/source claims and exact `2.0.0`
recognition. It may restore missing consumer jobs or acknowledge valid completed
publications under existing rules. It never adopts `1.5.0` output.

Cleanup policies remain:

- consumer warehouse and staging are never cleaned by the pipeline;
- parsed MRF output remains shared and retained;
- successful frozen plan JSON may be removed only through existing safe
  cleanup; and
- database rows are never deleted automatically.

## Required tests

Add or revise tests proving:

- startup accepts absent/empty or exact `2.0.0` warehouse and rejects `1.5.0`;
- consume claims require only snapshot and source rows;
- exact consumer config contains payer/month/output and no feed;
- completed recognition accepts exact feed-free `2.0.0` manifests;
- any legacy `feed_id` member is rejected while unrelated unknown metadata is
  accepted;
- valid crash-window publication is acknowledged without reingestion;
- invalid/conflicting publication is preserved;
- success transaction remains idempotent and schedules initial plans;
- the same exact URL captured and parsed independently in two months produces
  two valid `2.0.0` outputs;
- multiple outputs in one payer/month remain independent by output ID;
- attachment A/B then B/C behavior remains additive under `2.0.0`;
- provider-catalog pinning and metadata guard remain unchanged;
- structured operational context and redaction follow the boundary above; and
- exact sibling versions compile and all cross-package contract tests pass.

Tests exercise required manifest fields, identity agreement, dataset inventory,
layout, and legacy-field rejection. They do not recreate the complete consumer
schema validator or scan source text for forbidden words.

## Acceptance criteria

- Pipeline consumer calls compile against and use only the feed-free public API.
- Warehouse and snapshot recovery recognize exact `2.0.0` only.
- No feed value appears in configuration, claims, manifests, reports, or logs.
- Rate ingestion and additive plan attachment preserve their existing atomic
  and idempotent behavior.
- Consumer history contains independent immutable payer/month outputs ready for
  Story 18 activation.

## Out of scope

- Implementing consumer transformations or Parquet writing in the pipeline.
- Active-month lifecycle and activation commands.
- Query-service implementation or DuckDB execution in production.
- Migrating consumer `1.5.0` warehouses.
- Plan correction/removal or MRF content deduplication.
