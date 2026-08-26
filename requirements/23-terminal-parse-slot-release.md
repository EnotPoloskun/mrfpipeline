# Story 23: Terminal MRF parse slot release

## Status

Implemented.

## User story

As an operator running a bounded local sample, I want a selected MRF source
whose parse has reached a terminal failure to stop occupying a resident slot
and its downloaded raw file, so that other selected sources can materialize
while the failed source remains selected for an explicit later retry.

## Context

Story 21 keeps a resident slot through download and parse, including automatic
parse retries, so a retry does not redownload. A successful parse deletes the
raw file and then releases the slot. A terminal download may release the slot
only after proving that no raw or staging file remains. A terminal parse does
the opposite: it keeps the source selected, the raw file, and the slot until
the operator retries that parse.

That contract pins capacity when a downloaded object cannot be parsed. The
current parser rejects some real payer files immediately (for example an
in-network object that inlines `provider_groups` on each rate group instead of
CMS 2.x `provider_references`). The pipeline maps the parser failure to
`mrf_parse_execution_failed` without logging parser text. River then retries
until the uniform eight-attempt budget is exhausted, stores the job as
`cancelled` via `JobCancel`, and leaves `parse_status=failed` holding a slot.
With resident capacity 2, one unparsable file reduces productive parse
concurrency to one.

This story changes only that terminal-parse residency and the `mrf.parse`
attempt budget. It does not teach the parser a second in-network shape.

## Dependencies

- Implemented Stories 14–22, especially Story 10 parse cleanup, Story 13
  retry/reconcile, and Story 21 admission, slots, and execution locks.
- The pinned `mrfparser` package. This story does not change it.

## Goal

1. Keep automatic in-flight parse retries on the existing raw file and slot.
2. When `mrf.parse` becomes terminal, delete the raw download and parser
   staging, reset unpublished parse output, release the slot, and wake control
   so a waiting selected source can acquire it.
3. Limit newly inserted `mrf.parse` jobs to four attempts including the first
   (first attempt plus three retries). Every other production kind stays at
   eight.
4. Keep the failed source selected. Do not replace it in the numeric target
   and do not auto-retry parse after terminal cleanup.
5. Make `retry --stage mrf.parse` rematerialize when the raw file is gone:
   wait for a slot if needed, redownload, then parse.
6. Make reconcile finish terminal-parse cleanup after a crash, including
   sources that already failed under the Story 21 contract.

## Non-goals

- Accepting inlined `provider_groups`, `V2.0.0`, or any other parser schema
  change. That belongs in `mrfparser`, not this pipeline story.
- Unselecting a failed source, decreasing `set-total`, or dropping it from
  `mrf_sources_selected`.
- Changing download, TOC, discovery, consumer, or plan-attachment attempt
  counts.
- River UI retry, cancel, or delete. Pause/resume remains the only supported
  UI mutation.
- Auto-retry of a terminal parse without `cli retry`.
- Byte quotas, deleting parsed success output, or deleting warehouse data.
- Releasing a slot on a non-terminal parse attempt.
- Releasing a slot when raw or parser staging still exists after a failed
  cleanup.
- A new operator “drop source” or “force release slot” command.
- Mutating an already-running live sample as part of landing the story.
  After deploy, `reconcile` is the supported repair for already-terminal
  parses that still hold slots.

## Product decisions

- Resident capacity bounds **in-progress raw materialization**, not the count
  of selected or failed sources. A terminal parse is no longer in-progress.
- A selected failed parse still blocks activation (`source_incomplete`).
  Freeing the slot must not look like success and must not admit a substitute
  URL.
- Four is a **total** attempt budget on `mrf.parse` only, matching Story 03’s
  wording of “attempts including the first.” It is not four retries after a
  first attempt.
- Already-inserted River rows keep their stored `max_attempts`. The new budget
  applies to jobs inserted after this story, including jobs created by
  `retry`.
- Parser error text remains unlogged. Terminal cleanup uses the existing
  sanitized parse failure code already stored on the domain row.
- Story 10’s “before attempt eight” parse-retry paragraph and Story 21’s
  “terminal parse keeps raw file and slot” rows are superseded by this story
  for MRF parse only. Do not rewrite those historical stories as if the old
  contract never existed. Current DESIGN.md, README, and retry help must
  describe the new behavior.

## Required behavior

### Parse attempt budget

`mrf.parse` insert options must set `MaxAttempts` to 4. The shared River
client default `jobs.MaxAttempts` remains 8 and continues to apply to every
other production kind.

Automatic retries before the parse budget is exhausted keep the current
behavior:

- `parse_status` returns to pending;
- the raw download and slot remain;
- parser temp and manifest-absent partial output are reset as Story 10
  already requires;
- snapshots stay blocked;
- no redownload.

Immediate invariant failures (`invalid_job_arguments`, `missing_domain_record`,
`domain_invariant`, sealed-release codes) remain non-retryable. When
`jobs.Run` marks parse failed after a successful claim (work-time immediate
fail or exhausted attempts), they use the same `Terminal` cleanup as an
exhausted budget. Claim-time `JobCancel` without `MarkFailed` (stale job,
unadmitted source, missing record) must not run that cleanup and must not
change occupancy.

### Schema

`mrf_sources_parse_blocked_check` currently requires
`download_status = 'succeeded' OR parse_status = 'blocked'`. Add migration
`0006` that replaces it so a terminal parse may store download `blocked`
while parse stays `failed`:

```text
download_status = 'succeeded'
OR parse_status = 'blocked'
OR (parse_status = 'failed' AND download_status = 'blocked')
```

Keep the crash window `parse_status=failed AND download_status=succeeded`
legal until cleanup finishes. Do not allow parse `pending`/`running`/
`succeeded` with download not succeeded. `failure_code` stays the single
shared column: it is required iff download or parse is `failed`. Successful
terminal cleanup leaves the parse failure code, clears `download_river_job_id`,
and does not invent a download failure code.

### Terminal parse cleanup

Terminal parse means the domain row has been marked `parse_status=failed`
because attempts were exhausted or the failure was immediate. Keep
`MarkFailed` in its own transaction as today. Do not fold occupancy cleanup
into that commit: parse must stay durably `failed` if cleanup crashes.

After that durable mark, still under the source MRF execution lock, the parse
worker `Terminal` hook must:

1. Reset unpublished parse output. Manifest-absent, empty, or incomplete
   `parsed/` trees are removed entirely. `manifest_present` that fails
   Story 10 `ValidateCompletedOutput` is also removed so a later
   rematerialize cannot reuse it as `ParsedManifestPresent`. If
   `manifest_present` validates as completed output, stop occupancy cleanup
   (do not delete raw, do not block download, do not release the slot) and
   leave that source to the existing success-repair path. If inspect or
   validation cannot decide, keep the slot and remaining files and return
   a cleanup error for reconcile; do not delete conservatively.
2. Remove the MRF parser temp directory for that source.
3. Remove the exact download leaf when present.
4. Confirm download data, download staging, and parser temp are absent.
5. In one transaction after those files are confirmed absent: set
   `download_status` to `blocked`, clear `download_river_job_id`, leave the
   parse failure code on `failure_code`, `ReleaseSlotTx`, and `WakeTx`.
   Do not mark download `failed` merely because parse could not ingest a
   complete object.

If step 3 or 4 fails, keep the slot, remaining files, and
`download_status=succeeded`. Use `mrf_parse_cleanup_failed` only when that
is already the durable parse failure code; otherwise leave the original
parse code and let reconcile retry cleanup. Never overwrite
`mrf_parse_execution_failed` (or another parse work code) with cleanup
failed just because `Terminal` could not delete files. Never release a
slot while raw, download staging, or parser temp still exists.

Do not enqueue a replacement source. Do not change monthly-release membership.
Do not insert a new parse or download job from the terminal hook.

River still finalizes the exhausted job with `JobCancel` (UI `cancelled`).
That is unchanged.

### Operator retry

`retry --stage mrf.parse --id <source-id>` remains the only way to reopen a
terminal parse. It may run while workers are up and still takes the release
and source execution locks.

| Durable state at retry | Required action |
| --- | --- |
| Slot held and a valid raw download present | Today’s parse-only reopen: insert `mrf.parse` with MaxAttempts 4, reopen parse to pending. Do not redownload. |
| No valid raw download, slot held | Rematerialize on the existing slot: reopen download to pending and insert `mrf.download`; reopen parse to `blocked` with no parse job. Download success continues to insert parse as successor. |
| No valid raw download, no slot | Same durable waiting path as a terminal-download retry without a slot: reopen download and parse to `blocked`, clear job identities and parse failure code, insert no River job, wake control. Control acquires a slot, then inserts `mrf.download`. |
| Source not selected, or release not building | Existing invariant / `sealed_release_retry_forbidden`. |

Retry must inspect artifact state under the execution lock, not trust
`download_status=succeeded` as proof that bytes exist. `retry` for
`mrf.parse` therefore opens the artifact workspace. A valid raw download
is a Story 04 complete leaf (`InspectDownload` success). Absent,
incomplete, or invalid raw is “no valid raw.” An incomplete or invalid
leaf is removed under the lock during rematerialize retry so the later
download starts clean; parser temp is removed then too.

`cli retry` must pass that workspace into `reconcile.Retry`. Other stage
retries may keep ignoring artifacts.

A busy execution lock still returns `stage_execution_busy` without mutation.

`retry --stage mrf.download` is unchanged. Retrying download on a source whose
parse already succeeded remains an invariant.

### Admission and status

Control may admit a waiting selected source into a slot freed by terminal
parse cleanup. Failed parses are not waiting work until `retry` reopens them.

`month status` must remain honest:

- `mrf_parse_failed` counts selected terminal parses even when they hold no
  slot;
- `mrf_sources_waiting_capacity` does not include those failed rows until
  retry reopens them;
- `mrf_resident_held` falls when the slot is released;
- activation blockers still include `source_incomplete` while any selected
  source is not fully parsed and consumed.

The Story 21 row “if all slots are held by failed sources, new materialization
stops” remains true for terminal **downloads** that still have files. It is
no longer true for terminal **parses** that have completed cleanup.

### Reconciliation

Extend control reconcile so a source selected for a **building** release
with `parse_status=failed` is converged to the new terminal-parse occupancy
(analog of `releaseEmptyTerminalDownloads`, under the source execution lock):

- If raw, download staging, or parser temp still exists, finish the same
  terminal cleanup (unpublished parse output, parser temp, and raw; then
  blocked download; then slot release and wake).
- If those materialization files are already absent and a slot is still
  held, still converge `download_status` to `blocked` and clear
  `download_river_job_id` when download is still `succeeded`, then
  `ReleaseSlotTx` and wake. Do not leave `download_status=succeeded` with
  no bytes: operator retry would then have no table row that matches.
- If files are absent and no slot is held, do not auto-retry and do not
  change parse/download status. Invalid unpublished `parsed/` (not valid
  completed Story 10 output) may still be removed under the source lock so
  a later rematerialize cannot treat it as `ParsedManifestPresent`.
- Sealed-release members are not occupancy-repaired here.

Do not restore a parse job for `parse_status=failed`. The Story 21 rule
“valid downloaded raw file and incomplete parse: retain the slot and restore
only the parse job” continues to apply only to **non-terminal** parse
(`pending` / `running` / retrying).

If durable state says the slot is free but an unfinished raw file exists for
a non-succeeded parse, keep the existing fail-closed inconsistency.

`reconcile` still requires control to be stopped. MRF and consumer workers
may remain running; the source lock skips a busy domain.

### Success path

Unchanged: validate completed output, delete raw and parser temp, confirm
absence, then `confirmParseSuccess` releases the slot and wakes control.
Cleanup failure after a valid parse still keeps the slot and retries
deletion without reparsing.

## Documentation

Update current operator text (README local runbook slot paragraph, DESIGN.md
active architecture covering Stories 14–23, and `retry --help` if it implies
that a parse retry always reuses bytes on disk):

- Automatic parse retries reuse the raw file and slot.
- Terminal parse cleanup deletes raw and parser staging, then releases the
  slot; the source stays selected and failed.
- `retry --stage mrf.parse` rematerializes when the raw file is gone.
- `mrf.parse` has four attempts including the first; other kinds remain at
  eight.
- After deploy, already-terminal parses that still hold slots are repaired
  by `reconcile`, not by deleting slot rows from SQL or River UI.

Keep every README packaging contract string required by existing tests.
Do not rewrite Story 10 or Story 21 as if they originally specified this
behavior.

## Required tests

Do not run formatters, linters, or the full suite while implementing. Land
tests that prove the contract; the operator can run `go test -p 1 ./...`
once.

### Unit / policy

- `MRFParseArgs.InsertOpts` sets MaxAttempts 4.
- Production policy default MaxAttempts remains 8.
- Other production kinds’ insert options do not set 4.

### PostgreSQL integration

Run against a disposable `mrfpipeline_test_` database, serial as required by
the DB-suite contract.

- Migration 0006 accepts `parse_status=failed` with `download_status=blocked`
  and still accepts the crash window `failed` plus `succeeded`.
- A parse that fails on every attempt with `mrf_parse_execution_failed`
  becomes terminal on attempt 4, not 8. Domain parse is failed, download is
  blocked, raw and parser temp are absent, the slot is free, and a second
  selected waiting source is admitted into that slot.
- Attempts 1–3 keep the slot and a present raw file and do not insert a
  download job.
- Terminal cleanup that cannot delete the raw file keeps the slot and does
  not admit another source.
- `retry --stage mrf.parse` with slot and valid raw inserts only `mrf.parse`
  with MaxAttempts 4.
- `retry --stage mrf.parse` with no raw and no slot reopens download and
  parse to blocked, inserts no job, and after a control schedule pass the
  source holds a slot and has a pending download job.
- `retry --stage mrf.parse` with no raw but a held slot inserts download, not
  parse.
- Reconcile of `parse_status=failed` plus held slot plus present raw performs
  terminal cleanup and wakes admission.
- Reconcile of `parse_status=failed` plus held slot plus absent files only
  converges download to blocked if needed, releases the slot, and wakes
  admission.
- Reconcile does not auto-reopen a cleaned terminal parse.
- A successful parse still deletes raw and releases the slot.
- Terminal download occupancy is unchanged.

### Documentation tests

Extend help/packaging assertions only if retry help or README gains a new
required phrase. Do not drop existing required command strings.

## Acceptance criteria

1. Newly inserted `mrf.parse` jobs have MaxAttempts 4; other kinds stay at 8.
2. Non-terminal parse retries reuse the raw file and keep the slot.
3. Terminal parse deletes raw and parser staging, resets unpublished parse
   output, sets download to blocked, releases the slot, and wakes control.
4. The failed source remains selected; no other URL takes its target slot in
   the monthly membership sense.
5. A waiting selected source can occupy the freed resident slot.
6. `retry --stage mrf.parse` rematerializes when bytes are gone and still
   reuses bytes when they remain.
7. Reconcile converges pre-existing and crash-window terminal parses to
   “failed, selected, no files, no slot,” with download blocked.
8. Status shows `mrf_parse_failed` without those rows holding capacity.
9. README/DESIGN/retry help describe the new occupancy rules without breaking
   packaging tests.
10. Parser behavior is unchanged.

## Implementation notes

Prefer a parse-worker `Terminal` hook plus a reconcile repair analogous to
`releaseEmptyTerminalDownloads`, rather than a new CLI. Admission already
inserts download when `download_status != succeeded` and parse when download
succeeded; retry should feed that scheduler instead of inventing a second
admission path.

Keep slot release and file absence in one locked critical section. Tests must
not delete `mrf_materialization_slots` rows except through
`admission.ReleaseSlotTx` / `ReleaseSlotAndWake`.
