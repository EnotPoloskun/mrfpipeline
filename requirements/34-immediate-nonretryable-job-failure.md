# Story 34: Immediate failure for non-retryable job errors

## Status

Implemented.

## User story

As an operator, I want a job whose next attempt cannot succeed to fail on this
attempt and run the existing terminal occupancy cleanup, so poison files and
frozen identity mismatches do not burn the remaining retry budget or pin an
MRF resident slot.

## Context

Story 03 originally used one retry budget for every classified work error,
including HTTP 4xx and URL-policy failures, and said there was no fail-fast
taxonomy. Later stories carved out a small immediate list. Current
`jobs.isImmediateFail` is:

```text
invalid_job_arguments
missing_domain_record
domain_invariant
toc_download_not_found
mrf_download_not_found
sealed_release_inconsistent
sealed_release_retry_forbidden
```

HTTP 404 already maps to the two `*_not_found` codes. Claim-time
`JobCancel` already stops missing-record, invariant, invalid-argument, and
sealed-release deliveries. Everything else, including parse/import/ingest
validation against frozen bytes, returns to `pending` and retries until
`MaxAttempts` (8, or 4 for `toc.download` / `mrf.download` / `mrf.parse`).

That wastes attempts and, for `mrf.parse`, holds a resident slot for the
whole budget. A manifest-present invalid parse output is reused
(`runParse=false`) and fails the same validation on every attempt. Invalid
TOC import parquet, a changed `services.csv` / provider-catalog identity,
and a missing consumer input are the same class: waiting does not change
the input.

`jobs.Run` already treats immediate failure like attempt exhaustion:
`MarkFailed`, then the worker `Terminal` hook, then `river.JobCancel`.
Stories 23 and 26 already delete unpublished download bytes and release an
empty MRF slot from that hook. This story expands the immediate taxonomy;
it does not invent a second occupancy path.

`control.schedule` does not call `jobs.Run`, so even `invalid_job_arguments`
retries today.

## Dependencies

Implemented Stories 03, 13, 21, 23, 25, and 26. In particular:

- Story 03 `jobs.Run` retry vs `failAssignedTerminal`.
- Story 23 terminal `mrf.parse` cleanup (unpublished parsed output, parser
  temp, raw download, download `blocked`, `ReleaseSlotTx`, `WakeTx`).
- Story 26 terminal `mrf.download` / `toc.download` unpublished-file delete
  and empty MRF slot release.

Do not rewrite those historical stories as if they originally listed this
taxonomy. Current DESIGN.md, README, and `failure_test.go` must describe
the new list.

## Goal

1. Fail the assigned stage on the first work attempt that returns a
   non-retryable code. Do not `MarkRetryable`. Persist that code, run the
   existing `Terminal` hook, and `JobCancel` the River delivery.
2. Keep automatic retries for errors that can clear without an operator
   changing bytes, URL, catalog, or process identity.
3. Reuse Stories 23/26 occupancy cleanup. Immediate `mrf.parse` /
   `mrf.download` / `toc.download` failure must free slots and delete
   unpublished download/parse materialization files exactly as exhaustion
   and HTTP 404 already do.
4. Classify HTTP 410 Gone after redirects as the existing
   `toc_download_not_found` / `mrf_download_not_found` codes.
5. Make `control.schedule` cancel permanent errors instead of retrying them
   for eight attempts.
6. After deploy, the next execution of a still-retryable River job that
   now returns an immediate code becomes terminal on that attempt. No
   extra reconcile pass is required for that conversion.

## Non-goals

- Changing `MaxAttempts` (8 default; 4 on `toc.download`, `mrf.download`,
  `mrf.parse`).
- Fail-fast of `mrf_parse_execution_failed`. The public parser has no
  stable error taxonomy, so that code mixes poison files with OOM/disk.
  Immediate fail would also run Story 23 terminal cleanup (delete raw and
  release the slot) on a maybe-transient error. Keep the four-attempt
  budget; exhaustion still runs Terminal.
- Fail-fast of generic `toc_download_failed` / `mrf_download_failed`
  (HTTP 400/401/403/429/5xx, DNS, TLS, copy, disk, private-IP policy,
  https→http redirect). Private-IP rejection is not 100% futile: DNS can
  change. Split those into new codes only in a later story.
- Fail-fast of `discovery_result_invalid`. A retry re-fetches the live
  listing; a one-shot garbage page can clear.
- Fail-fast of database, cleanup, resource, and `*_output_failed` codes.
- Fail-fast of the whole `consumer_ingest_output_invalid` or
  `plan_attach_output_invalid` codes. Those mix frozen validation with
  transient filesystem / consumer I/O. This story only splits the
  already-published ingest recognition path (below).
- A new `Terminal` hook for `toc.parse`, `toc.import`, discovery, ingest,
  or attach. TOC invalid parsed output stays on disk for operator
  inspection (Story 07). Consumer/warehouse publications stay (Stories
  11/12).
- Unselecting a failed source, substituting another URL, or changing
  numeric targets.
- River UI retry/delete, recreate/SIGTERM meaning, or claim-time
  `JobCancel` without `MarkFailed` (that path must still skip Terminal).
- Filter-catalog CLI errors (not River jobs).
- A migration.

## Product decisions

Locked for this story:

- **One list.** Work-time fail-fast is `isImmediateFail` plus HTTP 410
  mapping onto the existing 404 codes. Production workers that already
  call `jobs.Run` must not each call `JobCancel`.
- **Occupancy is existing Terminal.** Immediate fail after a successful
  claim is the same `failAssignedTerminal` path as last-attempt
  exhaustion. `mrf.parse` Terminal remains Story 23 cleanup.
  `mrf.download` / `toc.download` Terminal remains Story 26
  `RemoveUnpublishedDownload` (then empty-slot release for MRF). Claim-time
  cancel without `MarkFailed` still must not run Terminal.
- **HTTP 410 is 404.** After redirects, Gone is `ErrHTTPNotFound` (or an
  equivalent sentinel that workers already map to `*_download_not_found`).
  Do not add `*_download_gone` codes.
- **Frozen identity mismatches fail now.** `mrf_parse_selector_changed` and
  `consumer_ingest_provider_changed` cannot succeed until the operator
  restores `services.csv` / provider-catalog metadata and restarts the
  worker, then `retry`. Waiting inside the same process cannot help.
- **Config invalid fails now.** `consumer_ingest_config_invalid` and
  `plan_attach_config_invalid` are process wiring (`mrfconsumer.ErrInvalidConfig`).
  Fix the worker config and `retry`.
- **TOC invalid parsed output is not auto-deleted.** Immediate
  `toc_parse_output_invalid` / `toc_parse_input_invalid` leaves the
  download and any manifest-present parsed directory. Operator inspection
  and `retry --stage toc.parse` stay Story 07/13. MRF parse is different
  because Story 23 already deletes unpublished parsed output on terminal
  so the slot can free.
- **Ingest publication split.** When the exact final snapshot leaf
  already exists, `consumer.ingest` only re-validates and never calls
  `Ingest`. That validation failure is immediate. `Lstat` errors other
  than not-exist, and report/layout failures after a fresh `Ingest` call,
  stay `consumer_ingest_output_invalid` and retryable. Implement the
  existing-leaf path as a dedicated allowed code
  `consumer_ingest_publication_invalid` (add it to `allowedFailureCode`
  and `isImmediateFail`).
- **Attach snapshot preflight.** `InspectCompletedSnapshot` failure on
  `consumer.attach_plans` is a missing/invalid frozen snapshot, not
  consumer I/O. Map it to `plan_attach_input_invalid` (already fail-fast
  in this story). Missing plan-association parts after
  `AddedPlanCount > 0` stay `plan_attach_output_invalid` and retryable.
- **`control.schedule`.** Wrap permanent errors with `river.JobCancel`.
  Retry `reconciliation_database_failed` and
  `artifact_reconciliation_failed`. Recreate/SIGTERM of control remains
  an interruption.

## Required behavior

### Immediate work codes

Add these existing codes to `isImmediateFail`:

| Kind | Code |
|---|---|
| `toc.parse` | `toc_parse_input_invalid` |
| `toc.parse` | `toc_parse_output_invalid` |
| `mrf.parse` | `mrf_parse_output_invalid` |
| `mrf.parse` | `mrf_parse_selector_changed` |
| `toc.import` | `toc_import_manifest_invalid` |
| `toc.import` | `toc_import_schema_invalid` |
| `toc.import` | `toc_import_row_invalid` |
| `toc.import` | `toc_import_order_invalid` |
| `toc.import` | `toc_import_invariant` |
| `consumer.ingest` | `consumer_ingest_config_invalid` |
| `consumer.ingest` | `consumer_ingest_input_invalid` |
| `consumer.ingest` | `consumer_ingest_provider_changed` |
| `consumer.ingest` | `consumer_ingest_publication_invalid` (new) |
| `consumer.attach_plans` | `plan_attach_config_invalid` |
| `consumer.attach_plans` | `plan_attach_input_invalid` |
| `consumer.attach_plans` | `plan_attach_invariant` |

Keep the current immediate codes listed in Context.

Workers already return these codes from their mappers. Do not change sibling
parser/consumer mappings except:

- HTTP 410 → `artifact.ErrHTTPNotFound` (then existing download mappers);
- attach snapshot inspect → `plan_attach_input_invalid`;
- ingest existing-final validation → `consumer_ingest_publication_invalid`.

### Still retryable (do not add to `isImmediateFail`)

```text
discovery_listing_failed
discovery_result_invalid
discovery_database_failed
toc_download_failed
mrf_download_failed
toc_parse_resource_failed
toc_parse_output_failed
toc_parse_cleanup_failed
mrf_parse_execution_failed
mrf_parse_cleanup_failed
mrf_parse_database_failed
toc_import_database_failed
consumer_ingest_output_failed
consumer_ingest_output_invalid
consumer_ingest_database_failed
plan_attach_output_failed
plan_attach_output_invalid
plan_attach_database_failed
reconciliation_database_failed
artifact_reconciliation_failed
```

Cancellation/deadline remain the context error. Recreate/SIGTERM remains an
interruption (`jobs.Run` returns nil, no `MarkFailed`, no Terminal). Busy
execution locks still snooze 5s and do not consume attempts.

Success-transaction / confirm failures stay retryable (they are usually
database). This story only changes work-error classification and control
schedule.

### Occupancy and cleanup

| Kind | Immediate work fail after claim | Cleanup |
|---|---|---|
| `mrf.parse` | `parse_status=failed` with the work code | Story 23 Terminal: reset unpublished parsed output, remove parser temp and raw, confirm absence, set download `blocked`, `ReleaseSlotTx`, `WakeTx`. Valid completed Story 10 output still stops occupancy cleanup. Cleanup that cannot delete files keeps the slot. |
| `mrf.download` | `download_status=failed` with `mrf_download_not_found` (404/410) or, on last attempt only, `mrf_download_failed` | Story 26 Terminal: `RemoveUnpublishedDownload`, then release when empty. |
| `toc.download` | `download_status=failed` with `toc_download_not_found` (404/410) | Story 26 Terminal: `RemoveUnpublishedDownload`. No resident slot. |
| `toc.parse` / `toc.import` | stage `failed` with the work code | No Terminal. Leave download / parsed parquet. Import must not cancel already committed batches or downstream MRF jobs. |
| `consumer.ingest` / `consumer.attach_plans` | stage `failed` with the work code | No Terminal. Leave parsed MRF output, warehouse snapshots, frozen plan batches, and published parts. |
| `discovery.run` | unchanged | Listing retry still allowed. `ErrInvalidConfig` remains `domain_invariant` (already immediate). |

A waiting selected source may occupy a slot freed by immediate parse or
download cleanup. The failed source stays selected. `retry` remains the
only reopen, including rematerialize when Story 23 deleted raw bytes.

`month status` already counts `mrf_parse_failed` without requiring a held
slot. Do not treat immediate-fail rows as waiting capacity.

### `control.schedule`

This worker does not call `jobs.Run`. `WakeTx` inserts a new
`control_schedule_events` id each time, and uniqueness is `ByArgs` on that
id, so cancelling one wake does not block later wakes. `RestoreWakes`
republishes leftover events after cancel or lost delivery.

Wrap only permanent wiring errors with `river.JobCancel`:

- `invalid_job_arguments` (nil pool/client/tx on this worker)

Retry `reconciliation_database_failed` and
`artifact_reconciliation_failed`. Also retry `domain_invariant` from
`jobs.Schedule` / `Unblock` (row-state races against other workers).

Do not treat `capacity_below_held_slots` or
`runtime_artifact_root_mismatch` as this job's work errors: those come from
control **startup** `ConfigureRuntime` / `ValidateRuntimeRoot`, not from
`Worker.Work`. Recreate/SIGTERM of control remains an interruption.

Do not route this worker through `jobs.Run` solely to reuse
`isImmediateFail` if that would invent a fake domain stage; wrapping the
returned error is enough.

### Operator retry

`retry --stage <kind> --id <id>` is unchanged. Immediate-fail rows are
ordinary terminal failures: same identity, same frozen artifacts, same
Story 23 rematerialize rules when parse deleted raw bytes.

After deploy, do not require the operator to cancel in-flight retryable
jobs whose code is now immediate. The next worker attempt converts them.

## Documentation

Update current operator text only (README local-runbook attempts/404
paragraph, DESIGN.md active architecture and Failure/retry section,
`retry --help` if it still says only 404 is non-retryable):

- Immediate failure is the listed work codes, not only HTTP 404.
- HTTP 410 uses the 404 codes and the same download Terminal cleanup.
- Immediate `mrf.parse` failure runs the same slot-release cleanup as
  parse exhaustion.
- Other HTTP 4xx/5xx, listing, parser execution, database, and cleanup
  errors still retry until the existing attempt budget.

Bump the DESIGN story index to include Story 34. Keep every README
packaging-test command string. Do not rewrite Stories 03, 07–12, 23, or 26
as if they originally specified this taxonomy.

## Required tests

Do not run formatters, linters, or the full suite while implementing. Land
tests that prove the contract.

### Unit / policy

- `isImmediateFail` is true for every code in the immediate table, 404/410
  download codes, and the previous invariant/sealed/missing-record codes.
- `isImmediateFail` is false for every code in the still-retryable list,
  including `mrf_parse_execution_failed`, `toc_download_failed`,
  `consumer_ingest_output_invalid`, and `plan_attach_output_invalid`.
- Download mapper: HTTP 410 is `*_download_not_found`; HTTP 403/500 remain
  `*_download_failed`.
- Attach snapshot inspect maps to `plan_attach_input_invalid`.
- Ingest: existing final leaf + failed recognition is
  `consumer_ingest_publication_invalid`; absent leaf + `Ingest` report
  mismatch remains `consumer_ingest_output_invalid`.

### Lifecycle

- Attempt 1 of `toc_parse_output_invalid` / `mrf_parse_output_invalid` /
  `toc_import_row_invalid` / `consumer_ingest_input_invalid` /
  `plan_attach_invariant` calls `MarkFailed` with that code, runs Terminal
  if the kind has one, and returns `JobCancel`. `MarkRetryable` is not
  called.
- Attempt 1 of `mrf_parse_execution_failed` / `toc_download_failed` /
  `discovery_listing_failed` still `MarkRetryable` and returns the error.
- Plain context cancel of `mrf.parse` still returns nil and does not run
  Terminal. Remote UI cancel is unchanged from Stories 25/26.

### PostgreSQL integration

- `mrf.parse` that returns `mrf_parse_output_invalid` on attempt 1 becomes
  domain-failed, download blocked, raw and parser temp absent, slot free,
  and a second selected waiting source can be admitted. Do not wait for
  attempt 4.
- `mrf.parse` that returns `mrf_parse_execution_failed` on attempts 1–3
  keeps the slot and raw file.
- `mrf.download` HTTP 410 is immediate `mrf_download_not_found`, deletes
  unpublished bytes, and releases an empty slot. HTTP 403 still retries.
- `toc.download` HTTP 410 is immediate `toc_download_not_found` and deletes
  unpublished TOC bytes. No slot.
- `toc.parse` immediate `toc_parse_output_invalid` fails the parse stage,
  leaves import blocked, and does not delete the TOC download.
- `toc.import` immediate `toc_import_row_invalid` does not delete already
  committed batches or their MRF jobs.
- `consumer.ingest` immediate `consumer_ingest_input_invalid` leaves source
  parse succeeded and does not rewrite warehouse files.
- `control.schedule` `invalid_job_arguments` ends as River `cancelled`
  without consuming eight attempts.

### Documentation tests

Extend help/packaging assertions only if README or retry help gains a new
required phrase. Do not drop existing required command strings.

## Acceptance criteria

1. The immediate table is `isImmediateFail`; attempt 1 is terminal for
   those work errors.
2. HTTP 410 is the existing download-not-found path, including Terminal
   file delete and empty MRF slot release.
3. Immediate `mrf.parse` failure releases the resident slot through the
   Story 23 Terminal hook; non-terminal `mrf_parse_execution_failed`
   retries still keep the slot and raw file.
4. TOC parse/import immediate failure does not invent occupancy cleanup
   and does not cancel downstream committed MRF work.
5. Ingest/attach immediate failure does not delete warehouse publications.
6. `control.schedule` `JobCancel`s `invalid_job_arguments` and still retries
   database / artifact-reconciliation errors and `Schedule`/`Unblock` races.
7. Recreate/SIGTERM, claim-time stale/missing cancel, and busy-lock snooze
   are unchanged.
8. README/DESIGN describe the fail-fast list without breaking packaging
   tests. Historical Stories 03 and 07–12 are not rewritten.

## Implementation notes

Current code to change, not a second design:

- `internal/jobs/jobs.go` `isImmediateFail` and `allowedFailureCode`.
- `internal/jobs/failure_test.go` (several tests currently require the
  parse/import/ingest “invalid” codes to retry).
- `internal/artifact/download.go` status handling (410 next to 404).
- `internal/planattach/worker.go` snapshot inspect mapping.
- `internal/consumeringest/worker.go` existing-final recognition path.
- `internal/admission/worker.go` `JobCancel` for permanent schedule errors.
- Docs listed above.

Do not add per-worker retry policy or a second application retry loop.
Do not release a slot while raw, download staging, or parser temp still
exists. Tests must not delete `mrf_materialization_slots` rows except
through `admission.ReleaseSlotTx` / `ReleaseSlotAndWake`.
