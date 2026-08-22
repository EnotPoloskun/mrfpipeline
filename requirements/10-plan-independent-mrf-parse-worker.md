# Story 10: Plan-independent MRF parse worker

## Status

Planned.

## User story

As a pipeline operator, I want one shared MRF source parsed independently from
all TOC plans so that later TOCs can add associations without repeating the
largest CPU, memory, and disk stage.

## Goal

Implement `mrf.parse` through the public `mrfparser` Go package, validate exact
plan-independent output `1.1.0`, delete the completed download, and atomically
unblock every waiting consumer snapshot for that source.

This story does not pass plans to the parser, ingest a consumer snapshot, build
plan JSON, or attach plans to the warehouse.

## Dependencies and scope

This story depends on Stories 01 through 09 and adds the pinned module:

```text
github.com/EnotPoloskun/mrfparser
```

Do not commit a workspace-relative `replace` directive. Use:

```go
cfg := mrfparser.DefaultConfig()
err := mrfparser.Parse(ctx, cfg)
```

The pipeline does not execute the parser CLI, import its `internal` packages,
or fork its parsing logic. The public package emits no CLI lifecycle logs.

Parser Story 28 is normative: schema/manifest `1.1.0`, eight datasets, no
`plans` input, no `plans` dataset, and nullable root plan fields retained only
as source audit metadata.

## Product decisions

- One `mrf.parse` job owns one `mrf_sources.id`.
- Parser configuration contains input, output, service selector, resource
  defaults, and temporary root only. It contains no TOC, payer, feed, month,
  snapshot, consumer output, plan, or batch value.
- The pipeline always supplies `MRFPIPELINE_SERVICES_PATH`; it does not run the
  all-services parser mode.
- Use the parser's pinned `DefaultConfig` resource values. Version 1 does not
  add duplicate memory/row-group/temp-limit environment knobs.
- Set parser `TempDir` to the artifact root `.staging` directory.
- Only one MRF parse executes in the process. This satisfies the parser's
  prohibition on concurrent `Parse` calls.
- `mrfparser.Parse` temporarily changes the process-wide Go memory limit.
  Other small River work may continue, but no other MRF parse may overlap.
- Manifest-absent partial output is reset. Valid completed output is reused.
- A manifest-present invalid/unsupported output is preserved and fails closed.
- Source download bytes are deleted after output validation and before domain
  success.
- Parse success unblocks every currently blocked snapshot for the source and
  inserts one consumer ingest job per snapshot in the same transaction.
- Parsed MRF output is retained indefinitely in version 1 because later TOCs
  may create another snapshot for the shared source.

## Worker registration

Add:

| Queue | Maximum workers | Registered kinds |
|---|---:|---|
| `mrf_parse` | 1 | `mrf.parse` |

All prior queues remain registered. `consumer` is still not consumed, so
ingest jobs inserted here remain pending until Story 11.

## Service selector startup contract

Story 10 completes worker validation for
`MRFPIPELINE_SERVICES_PATH`. Before River starts:

- require a real, regular, non-symlink, readable local file;
- require it to remain outside the artifact root, warehouse, and provider
  catalog per Story 04 path separation;
- require file size at most the parser's 16 MiB selector limit; and
- record its device/inode where available, size, and nanosecond modification
  time as process-local immutability metadata.

Before each MRF parse, re-stat the selector and require those metadata values
to match startup. This is an operator-change guard, not content identity or a
checksum. The parser still validates the CSV contents and detects concurrent
modification only according to its own immutable-input contract.

Changing selector contents requires stopping the worker and intentionally
starting a new warehouse/pipeline run plan. Existing parser and consumer
outputs are not retroactively rebuilt.

## Job argument and claim

Exact argument:

```json
{"mrf_source_id":151}
```

Lock the source row and:

- require `download_status=succeeded`;
- return stale no-op for a different parse job ID;
- return completed no-op for parse succeeded;
- return terminal no-op for parse failed;
- change assigned pending/same-job running to running when consistent; or
- return safe invariant failure for blocked/missing/inconsistent state.

During this claim, `mrf_snapshots` are not locked. Snapshot coordination occurs
in the success transaction under the source lock.

After claim, validate the completed Story 04 MRF download. A missing or damaged
artifact despite succeeded download state is an invariant/reconciliation
failure; this worker does not change download state or issue HTTP.

## Source/snapshot race rule

To prevent a snapshot from missing ingest scheduling:

- Story 10 locks `mrf_sources` before scanning/updating its snapshots on parse
  success.
- Story 08 must lock that same source row before inserting a snapshot and
  deciding whether source parse is succeeded.

Thus either:

1. Import inserts the blocked snapshot first and parse success sees/unblocks
   it; or
2. Parse success commits first and import sees succeeded source state and
   inserts the snapshot as pending with its ingest job.

There is no timing gap that leaves a valid new snapshot blocked after source
parse success.

## Output preflight

Inspect:

```text
<artifact-root>/mrf/mrf-source-<id>/parsed
```

- Absent/empty: prepare exact empty real directory.
- Nonempty without manifest: remove/recreate that exact `parsed` leaf.
- Manifest present: preserve and validate without invoking parser.
- Symlink/wrong type: artifact error.

Preflight finishes before selector or source input is opened. Never call the
parser with a nonempty output.

## Parser configuration

When no completed manifest is present:

```go
cfg := mrfparser.DefaultConfig()
cfg.Input = downloadDataPath
cfg.Output = parsedPath
cfg.Services = configuredServicesPath
cfg.TempDir = artifactStagingPath
err := mrfparser.Parse(ctx, cfg)
```

Retain pinned defaults unless the parser dependency itself changes through an
explicit story:

```text
MemoryLimit    = 1 GiB
TempDiskLimit  = 10 GiB
TargetFileSize = 256 MiB
RowGroupRows   = 100000
```

Do not set a plan field—none exists in parser 1.1.0. Do not generate a dummy
plan, read `mrf_plans`, query TOC output, or copy root plan metadata into
configuration.

Pass the River context unchanged. `mrfparser.Parse` returns only error/success;
the durable result is validated from output.

## Completed-output layout

Require exactly these root entries:

```text
mrf_files/
services/
service_relations/
rate_groups/
negotiated_prices/
rate_provider_groups/
provider_groups/
providers/
processing_stats.json
manifest.json
```

There is no `plans/` directory or any ninth dataset.

Each dataset is a real directory containing contiguous regular non-symlink
parts `part-00000.parquet`, `part-00001.parquet`, and so on, with no gaps or
extra entries. Every dataset has at least part zero even when it contains no
rows. Root entries are real/non-symlink and no unexpected root content exists.

Story 11 independently validates exact Parquet descriptors and relationships
through `mrfconsumer`. Story 10 validates publication layout and metadata but
does not scan all rate rows a second time.

## Manifest validation

Strictly decode `manifest.json` with the documented parser 1.1.0 schema,
duplicate/unknown-property rejection, no trailing data, and a 4 MiB size cap.
Validate:

- exact manifest/output schema `1.1.0`;
- exact `status=complete`;
- `source.kind=local`;
- source URI equals the normalized generated download data path;
- selection mode is exact `service_csv`;
- selector URI equals normalized configured services path;
- selector requested/matched/unmatched counts are present, nonnegative, and
  satisfy their equations;
- source/retained/filtered service counts are nonnegative and consistent;
- all eight dataset counts are present and satisfy manifest invariants;
- `counts.mrf_files == 1`;
- warning totals/examples satisfy the bounded warning schema; and
- `counts.plans` is absent.

Paths in manifest are compared but never logged or stored in the pipeline
database.

Strictly decode `processing_stats.json` with its exact six fields and 64 KiB
limit. Require nonnegative stored/decoded bytes, valid ordered UTC timestamps,
finite nonnegative wall seconds and throughput, and no unknown/duplicate data.
Stats are validation/operational artifacts and are not copied to PostgreSQL.

## Download cleanup

After all output validation, remove the exact source `download` leaf through
Story 04 before finalizing database success.

If cleanup fails, retry without re-running parser. If cleanup succeeds but the
success transaction fails, retry uses completed parser output. Never remove
the shared `parsed` output.

## Success and snapshot scheduling transaction

Use one transaction:

1. Lock and re-read the source row.
2. Verify the same parse job remains assigned.
3. Return no-op if parse already succeeded.
4. Require download succeeded and parse running.
5. Lock all `mrf_snapshots` for this source in ascending ID order.
6. For each snapshot:
   - if consume is blocked with null job ID, set pending, insert one
     `consumer.ingest` job through `InsertTx`, and store its ID;
   - if consume is pending/running/succeeded/failed, leave it unchanged;
   - if lifecycle/job-ID state is inconsistent, roll back with
     `domain_invariant`.
7. Set source parse succeeded, clear parse failure code, update source and
   changed snapshot timestamps, and commit.

All waiting snapshot jobs and parse success become visible together. Ingest is
plan-independent, so it need not wait for attachment batches or for all TOCs.

A source with no snapshots still marks parse succeeded. Story 08 can later add
a snapshot and schedule ingest under the source lock race rule.

## Retry, failure, and cancellation

Before attempt eight, parser execution, resource, output, validation, selector,
or cleanup failure returns assigned parse state to pending and leaves snapshots
blocked. A manifest-absent partial output is reset on retry; a valid completed
output is reused.

Because the public parser API intentionally exposes no stable error taxonomy,
the pipeline does not inspect error strings. Fixed safe codes are based on the
pipeline-observed boundary:

```text
mrf_parse_execution_failed
mrf_parse_output_invalid
mrf_parse_selector_changed
mrf_parse_cleanup_failed
mrf_parse_database_failed
```

On the eighth attempt, mark only source parse failed with the most specific
code. Dependent snapshots remain blocked and their plans remain stored.

Cancellation passes through the parser. Manifest publication remains the
completion boundary even if acknowledgement/cleanup races with cancellation;
retry validates the actual output before deciding whether to parse again.

Never include parser error text, manifest content, warnings, source/root plan
fields, service selector values, paths, URLs, or dataset counts in logs or
`failure_code`.

## Required tests

### Unit tests

- Parser config starts from exact defaults and sets only input/output/services/
  temp root.
- No plan, payer, feed, month, TOC, snapshot, or batch value enters config.
- Selector startup/immutability checks use metadata only and no digest.
- Output layout requires eight datasets and rejects `plans/`.
- Strict manifest validation requires exact 1.1.0, service CSV selection, eight
  counts, and no plans count.
- Processing stats validation is exact and redacted.
- Snapshot scheduling locks/orders rows and uses transactional inserts.
- Error strings from the parser never escape classification.

### Integration tests

Using small real parser fixtures plus PostgreSQL/River:

- Raw and gzip MRF inputs produce valid plan-independent 1.1.0 outputs.
- An MRF with no usable root plan succeeds without any supplied plans.
- Output has eight dataset directories, no plans directory/count, and nullable
  root audit columns as published by the parser.
- The configured selector filters services and manifest selection metadata
  matches it.
- A partial parse is reset and retried; a completed parse is reused without a
  second parser invocation.
- Invalid manifest-present output is preserved and fails closed.
- Download removal happens only after complete output validation.
- One parse success unblocks and inserts jobs for several waiting snapshots in
  one transaction.
- Concurrent TOC import and parse finalization cannot leave a valid snapshot
  blocked or create duplicate ingest jobs.
- A source with no snapshot succeeds; a later import schedules ingest.
- Transaction rollback leaves all snapshots blocked until retry commits.
- Final parse failure leaves snapshot/plan rows intact and blocked.
- Queue concurrency never executes two parser calls simultaneously.

Run:

```text
go test ./...
go test -race ./...
go vet ./...
```

## Acceptance criteria

- One shared exact MRF source is parsed once at maximum concurrency one.
- Parser invocation is strictly plan-independent and always uses the configured
  service selector.
- Only exact eight-dataset parser 1.1.0 output is accepted and reused.
- Partial output is safely reset; invalid manifest-present output is preserved.
- Download bytes are deleted only after parse completion is proven; parsed
  output remains for future snapshots.
- Parse success atomically unblocks every current snapshot, while concurrent
  later imports cannot miss scheduling.
- Failures and cancellation do not lose snapshot associations or expose source
  data.

## Non-goals

- Passing plans to `mrfparser` or recreating its removed plans dataset.
- Consumer ingest or plan attachment execution.
- Dynamic parser resource configuration.
- Supporting parser 1.0.0, S3 pipeline input/output, or output migration.
- Reading every produced Parquet row in the orchestrator.
- Deleting shared parsed output, inferring feed identity, or calculating
  hashes.
