# Story 07: TOC parse worker

## Status

Implemented.

## User story

As a pipeline operator, I want each completed TOC download parsed and validated
in the background so that incomplete parser output is retried safely and only
published associations reach the import stage.

## Goal

Implement the production `toc.parse` River worker, invoke the importable
`mrftocparser` package, validate exact output version `1.0.0`, remove the
download after validated parse publication, and atomically schedule
`toc.import`.

This story does not read association Parquet rows into PostgreSQL, create MRF
sources or feeds, download an MRF, or call `mrfconsumer`.

## Dependencies and scope

This story depends on Stories 01 through 06 and adds the pinned module:

```text
github.com/EnotPoloskun/mrftocparser
```

Do not commit a workspace-relative `replace` directive. Invoke the public root
package API rather than executing its CLI or duplicating its parser:

```go
mrftocparser.Parse(ctx, mrftocparser.Config{...})
```

Story 04 owns parser-output structural states and exact-leaf reset. Story 06
owns the completed input download. `mrftocparser` owns strict TOC parsing,
association expansion, Parquet publication, and its own manifest semantics.

## Product decisions

- One `toc.parse` job owns one `toc_files.id` and uses the same numeric ID for
  stable `toc_output_id` formatting.
- The parser receives the extensionless completed download path. It detects
  JSON versus gzip from bytes.
- The pipeline invokes `mrftocparser` in-process through its public Go API.
- A process-wide `TMPDIR` of `<artifact-root>/.staging` is set once before
  River starts, because `mrftocparser.Config` has no temp-directory field.
  Never mutate `TMPDIR` around individual calls.
- Two concurrent TOC parser calls are supported.
- A nonempty `parsed` directory without `manifest.json` is incomplete and is
  reset before retry.
- Manifest presence alone is not enough. The pipeline validates the exact
  supported manifest, metadata, layout, and part naming before domain success.
- A manifest-present invalid or unsupported output is not automatically
  deleted. It fails closed for operator/reconciliation review.
- A valid completed parser output is reusable after a crash and is never
  parsed again.
- The downloaded TOC bytes are removed only after completed parser output is
  validated.
- Parse success, import eligibility, and one `toc.import` job commit together.
- An empty valid association dataset is a successful TOC parse and import
  input; it is not a parser failure.

## Worker registration

Extend the production client:

| Queue | Maximum workers | Registered kinds |
|---|---:|---|
| `discovery` | 1 | `discovery.run` |
| `toc_download` | 4 | `toc.download` |
| `toc_parse` | 2 | `toc.parse` |

`toc.import` jobs remain pending until Story 08. Do not configure unimplemented
queues or no-op workers.

Amend `work` so that, after Story 04 workspace validation and before starting
River, it sets process `TMPDIR` once to the validated
`<artifact-root>/.staging` directory. Never restore or mutate `TMPDIR` around
individual parser calls.

## Job argument and owned fields

The exact argument is:

```json
{"toc_file_id":72}
```

The worker owns:

- `parse_status`
- `parse_river_job_id`
- shared `failure_code` when parse terminally fails
- `updated_at`
- successor `import_status`
- successor `import_river_job_id`

It must not change the completed download state, source URL, caller metadata,
discovery relationship, or import result.

## Claim and prerequisite validation

Lock the `toc_files` row and apply Story 03:

- Missing row: `missing_domain_record`.
- Different parse job ID: stale no-op.
- `parse_status=succeeded`: completed no-op.
- `parse_status=failed`: terminal no-op.
- Assigned `pending`/same-job `running`: require
  `download_status=succeeded` and `import_status=blocked`, then set/retain
  `running` and update `updated_at`.
- `parse_status=blocked` or another inconsistent combination:
  `domain_invariant`.

Read payer ID and collection month during the claim. Format:

```text
toc_output_id = toc-<toc_files.id>
collection_month = YYYY-MM
```

The formatting uses the stored positive ID and first-of-month date only.

After claim commit, validate the Story 04 completed download and obtain its
regular `data` path. Always pass that exact absolute-clean generated
`download/data` path to the parser. Missing, incomplete, malformed, or
wrong-type download content is an artifact invariant failure; the parse worker
does not silently redownload or change download status. Story 13 owns repair of
a succeeded download whose artifact was later lost.

Once valid parser output exists, source-path validation compares the recorded
`source.uri` string to the generated path. The deleted input file need not
exist.

## Output preflight

Inspect the generated `parsed` directory through Story 04:

- `absent` or `empty`: prepare an empty real directory for parsing.
- `incomplete`: remove and recreate only this exact `parsed` leaf.
- `manifest_present`: do not mutate it; proceed directly to completed-output
  validation.
- Symlink/wrong type: fail as an artifact error.

The preflight runs before opening the input through `mrftocparser`. The worker
never asks the parser to append, resume, overwrite, or repair output.

## Parser invocation

When no manifest is present after preflight, call:

```go
ctx = mrftocparser.WithProgress(ctx, throttledTOCStageProgress)
report, err := mrftocparser.Parse(ctx, mrftocparser.Config{
    InputPath:       downloadDataPath,
    OutputPath:      parsedPath,
    TOCOutputID:     "toc-<id>",
    PayerID:         storedPayerID,
    CollectionMonth: "YYYY-MM",
})
```

Pass the River context unchanged. Do not install process signal handlers, set
package globals, or redirect package output.

Install public `mrftocparser.WithProgress` so the three fixed stages log as
Story 03 `progress` events with phases `toc_destination_prepared`,
`toc_input_parsed`, and `toc_parquet_closed`. Do not invent a percent for TOC
parse. Do not write the parser CLI's human progress lines. Do not import
`internal/app`. If the pinned TOC parser has no root `WithProgress`, add that
thin public wrapper in `mrftocparser` and pin the release; it already exists
unexported for the CLI.

The package returns no successful report unless it published its final
manifest. On success, require:

- `report.TOCOutputID` equals the formatted ID;
- `report.FinalPath` equals the normalized generated output path;
- every report count is nonnegative and matches the final manifest; and
- every warning total is nonnegative and matches the final manifest.

Do not store the report in PostgreSQL. It is a cross-check for a fresh
invocation. Recovery from an already completed output relies on independent
manifest validation because no prior report is available.

## Completed-output validation

Implement one pipeline-owned, read-only validator for the documented
`mrftocparser` 1.0.0 local contract. It does not parse TOC source JSON or
rewrite output.

### Root layout

Require exactly:

```text
parsed/
  toc_files/
  mrf_plan_associations/
  manifest.json
```

All three are real, non-symlink entries with the expected types. There are no
extra root entries. Any extra, including `.DS_Store`, `Thumbs.db`, or similar
noise, fails closed.

Each dataset directory contains only contiguous regular files named:

```text
part-00000.parquet
part-00001.parquet
...
```

At least `part-00000.parquet` exists in each dataset, including for zero
associations. There are no gaps, subdirectories, temporary files, symlinks, or
other extensions.

Story 08 validates Parquet schemas and row contents. Story 07 does not count
all Parquet rows or duplicate the parser's semantic processing.

### Manifest

Strictly decode the documented manifest JSON with exact properties, duplicate
and unknown property rejection, no trailing data, and fixed size limit 1 MiB.
Validate:

- `manifest_schema_version == "1.0.0"`.
- `output_schema_version == "1.0.0"`.
- `toc_output_id` equals `toc-<id>`.
- `payer_id` equals the stored payer.
- `collection_month` equals the stored `YYYY-MM`.
- `source.uri` equals the exact normalized download data path.
- `source.encoding` is exact `json` or `gzip`.
- The required TOC metadata, nullable fields, counts, warning totals/examples,
  and count equations satisfy the published schema.
- `counts.toc_files == 1`.
- `counts.mrf_plan_associations >= 0`.

Source URI is compared but never reported. Exact schema validation may be
implemented with a small private typed model or an embedded copy of the
published JSON Schema. Do not import sibling `internal` packages.

An unsupported or corrupt manifest beside otherwise present output is a
completed-output contract failure. Generic reset refuses it because deleting
a manifest-present directory could destroy successfully published evidence.
The operator must stop the worker, inspect and remove or quarantine that exact
generated `parsed` directory, then issue `retry`. A fresh parser
report/manifest mismatch is the same class: preserve the manifest-present
output and fail closed.

## Download cleanup

After completed-output validation and before the database success transaction,
call Story 04's idempotent exact-leaf removal for this TOC `download` directory.

This ordering is safe:

- The validated parser output no longer needs source bytes for import.
- If cleanup fails, return a retryable artifact error. Retry validates the same
  completed parser output, retries cleanup, and does not invoke the parser.
- If cleanup succeeds but database finalization fails, retry uses the completed
  parser output; it does not need the deleted download.

Never remove the TOC record directory or `parsed` output.

## Success and successor transaction

In one transaction:

1. Lock and re-read the TOC row.
2. Verify this parse River job remains assigned.
3. If parse already succeeded, return a no-op without another import job.
4. Require `download_status=succeeded`, `parse_status=running`, and
   `import_status=blocked`.
5. Set `parse_status=succeeded` and `import_status=pending`.
6. Insert `toc.import` with the same `toc_file_id` through `InsertTx`.
7. Store its ID in `import_river_job_id`.
8. Clear any parse terminal failure code, update `updated_at`, and commit.

A crash after manifest publication or download cleanup but before commit is
recovered through output validation. A crash after commit finds parse success
and the single import job atomically present.

## Error and retry mapping

All non-cancellation failures are returned through River's shared attempt
policy. Map sibling classifications safely:

| Sibling/result | Pipeline safe classification |
|---|---|
| `mrftocparser.ErrInvalidConfig` | `domain_invariant` |
| `mrftocparser.ErrInvalidInput` | `toc_parse_input_invalid` |
| `mrftocparser.ErrResource` | `toc_parse_resource_failed` |
| `mrftocparser.ErrOutput` | `toc_parse_output_failed` |
| Completed-output validation failure | `toc_parse_output_invalid` |
| Artifact cleanup failure | `toc_parse_cleanup_failed` |

Before the final attempt, return assigned `running` to `pending`, keep import
blocked, and leave `failure_code` null. Partial manifest-absent output remains
for the next preflight to reset.

On the eighth attempt, set parse `failed`, leave import blocked, and persist the
most specific fixed classification above. Do not store `error.Error()`, parser
paths, manifest content, reporting entity, warnings, or counts.

Cancellation remains the context error, leaves no manifest unless the parser
already published one, and follows common retry/shutdown behavior.

## Required tests

### Unit tests

- Config formatting uses exact stored ID, payer, month, and generated paths.
- Output-state branching never invokes the parser for a manifest-present
  output and resets only manifest-absent partial output.
- Strict manifest decoding and metadata/count validation match the 1.0.0
  contract.
- Root/part layout validation rejects extras, gaps, symlinks, and wrong types.
- Fresh parser report and manifest mismatches fail closed and preserve
  manifest-present output.
- After download cleanup, source-path validation compares the recorded string
  and does not require the deleted file to exist.
- `TMPDIR` is set once before River and is not mutated around `Parse`.
- The three TOC parser stages log as throttled/fixed `progress` phases without
  percent, URLs, or paths.
- Sibling errors map to fixed safe classifications without raw values.
- Success finalization atomically inserts one `toc.import` job.

### Integration tests

Using disposable PostgreSQL/River state and local fixtures, prove:

- A real raw-JSON TOC and gzip TOC parse successfully through the package.
- A valid empty-association TOC succeeds with a readable zero-row association
  part and is scheduled for import.
- Parser output contains exact 1.0.0 metadata for the database row.
- Download bytes are removed only after completed-output validation.
- An injected parser failure leaves no manifest; retry resets only the partial
  output and succeeds.
- A crash after manifest publication causes retry to reuse output without
  calling the parser again.
- A cleanup failure causes retry without reparse.
- Unsupported/corrupt manifest is preserved and ultimately marks parse failed.
- Success transaction rollback retains reusable output and creates no visible
  import job until retry commits.
- Completed/stale jobs are no-ops and cannot delete another stage's artifacts.
- Queue concurrency is two and two parser calls may overlap.

Run:

```text
go test ./...
go test -race ./...
go vet ./...
```

## Acceptance criteria

- TOC parse jobs run with maximum concurrency two through the public
  `mrftocparser` package.
- Parser input/output and caller metadata are derived exactly from the domain
  row and artifact workspace.
- Manifest-absent partial outputs are reset; valid completed 1.0.0 outputs are
  independently validated and reused.
- Download bytes are deleted only after parse publication is proven.
- Parse success and one import successor commit together.
- Empty valid association datasets still advance to import.
- Failures, cancellation, crashes, and stale jobs preserve artifacts and
  domain state according to the shared contracts.

## Non-goals

- Reading association Parquet rows into PostgreSQL.
- Feed assignment, MRF source/snapshot/plan creation, or downstream scheduling.
- Supporting a TOC parser version other than exact 1.0.0.
- Parser compatibility mode, output repair, or manifest-present deletion.
- Persisting parser reports, counts, warning examples, or TOC metadata in the
  pipeline database.
- MRF download/parse, consumer ingest, plan attachment, or content hashes.
