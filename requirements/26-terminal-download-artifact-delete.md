# Story 26: Delete leftover download bytes on cancel and exhaustion

## Status

Implemented.

## User story

As an operator, I want a terminal `mrf.download` or `toc.download` — River UI
cancel or retry exhaustion — to delete unpublished download bytes and staging,
so leftover gzip does not keep an MRF slot or sit on disk until I remember to
retry.

## Context

Story 25 made River UI cancel of `mrf.download` / `mrf.parse` a terminal
occupancy failure. Cancelled MRF download already deletes unpublished
download/staging, then releases the slot. Exhausted `mrf.download` retries
still keep leftover files so `releaseEmptyDownloadSlot` will not release a
non-empty slot.

`toc.download` has no Terminal hook. River UI cancel of it is still an
interruption (`TestRunRemoteCancelLeavesTOCAsInterruption`). Control's live
cancel repair and `river_control` watch cover only `mrf_download` /
`mrf_parse`.

Successful MRF parse still deletes raw gzip and keeps shared parsed parquet.
That is unchanged: several TOC files can point at the same MRF URL.

Worker recreate / SIGTERM still uses `jobs.Shutdown` → `ErrStop`, not
`rivertype.ErrJobCancelledRemotely`. That path must stay an interruption and
must not delete.

## Dependencies

Implemented Stories 14–25, especially Story 04 download leaves, Story 13
cancelled-job repair, Story 21 slots, Story 23 terminal parse cleanup, and
Story 25 UI-cancel occupancy.

## Goal

1. Terminal `mrf.download` (last attempt, HTTP 404 immediate fail, or River UI
   cancel) deletes unpublished download bytes and matching staging, then
   releases the resident slot when those files are gone. `cli retry --stage
   mrf.download` redownloads from scratch.
2. Terminal `toc.download` (last attempt, HTTP 404 immediate fail, or River UI
   cancel) fails that TOC download stage and deletes unpublished TOC download
   bytes and matching staging. Reopen with `cli retry --stage toc.download`.
   TOC has no resident slot.
3. UI cancel of a **queued** `toc.download` is noticed by control without
   `cli reconcile`: fail the stage, delete unpublished TOC download files.
   Still-`running` River jobs are not replaced.
4. Worker recreate / SIGTERM of a running download remains an interruption:
   no `MarkFailed`, no Terminal delete, leftover bytes kept for resume.
5. `mrf.parse` occupancy, parsed parquet, attempt budgets, and HTTP 404
   mapping stay as Stories 23–25 left them.

## Non-goals

- Deleting successful shared MRF `parsed` output, warehouse files, or TOC
  parsed output after import (Story 13 already owns succeeded-TOC cleanup).
- UI cancel of `toc.parse`, `toc.import`, `discovery.run`, or consumer kinds.
- River UI retry or delete.
- Unselecting sources or changing numeric targets.
- Changing MaxAttempts or 404-is-terminal mapping.
- Adding a TOC advisory execution namespace unless a real race requires it.
  TOC cancel repair must not take `LockNamespaceMRF` on a `toc_file_id`.
- Rewriting historical Stories 03, 06, 09, 13, 23, or 25 as if they originally
  deleted leftover download bytes on exhaustion.
- Mutating the live Compose sample or applying migration 0007.

## Product decisions

Locked with the operator before implementation:

- **toc_cancel_meaning:** River UI cancel of `toc.download` fails the TOC
  download stage, deletes unpublished bytes/staging, and is reopened with
  `cli retry`. Recreate/SIGTERM stays an interruption.
- **exhaustion_scope:** Retry exhaustion and 404 immediate fail delete leftover
  download/staging for both `mrf.download` and `toc.download`. After that
  delete, an MRF slot is released when the leaf and staging are gone.
- **recreate_untouched:** Recreate/SIGTERM must not delete and must not fail
  the stage.

Further:

- Remote-cancel terminal kinds are `mrf.download`, `mrf.parse`, and
  `toc.download`. Other kinds stay interruptions.
- Download Terminal delete uses existing `RemoveUnpublishedDownload` (published
  leaf + matching staging; absent is success). Parsed output is not touched.
- For `mrf.download`, Terminal and UI-cancel cleanup are the same delete-then-
  empty-slot-release path. `CancelTerminal` may be removed if Terminal already
  deletes.
- Control `LISTEN` on `mrfpipeline_river.river_control` also wakes for
  `queue=toc_download` cancel payloads. Live `BeforeSchedule` repair includes
  `toc.download` using the unlocked Story 13 TOC path, not the MRF lock.
- Reconcile crash-window repair deletes leftover files for **any** selected
  failed `mrf.download` that still holds a slot (not only
  `river_terminal_without_domain_result`), then releases when empty. Failed
  `toc.download` rows get the same unpublished-file delete.
- Claim-time `JobCancel` without `MarkFailed` still must not run Terminal.

## Required tests

- `jobs.Run`: plain context cancel of `mrf.download` / `toc.download` returns
  nil and does not run Terminal. Remote cancel of those kinds fails the stage
  and runs Terminal. Remote cancel of `toc.parse` stays an interruption.
- `jobs.Run`: last attempt (or immediate-fail) of a download kind runs
  Terminal. A download Terminal hook that deletes is invoked for exhaustion,
  not only for remote cancel.
- Repair of a cancelled `toc.download` with leftover bytes or staging fails
  the stage and deletes those files. A still-running TOC download job is not
  replaced.
- Repair of a failed `mrf.download` with leftover bytes (exhaustion code or
  UI-cancel code) deletes files and releases the slot.
- Control notify classification: `cancel` on `mrf_download`, `mrf_parse`, and
  `toc_download` wakes; `toc_parse` and `pause` do not.
- Existing Story 25 recreate tests remain green.

## Documentation

Bump Stories 01–25 to 01–26 in README and DESIGN. River UI / DOCKER.md /
`retry` help: cancelling `mrf.download`, `mrf.parse`, or `toc.download` fails
that stage and deletes unpublished download bytes; download retry exhaustion
does the same; recreate stays an interruption. Keep packaging-test command
strings. Do not put `postgres://` in DOCKER.md.

## Implementation notes

Current code to change, not a second design:

- `internal/jobs/lifecycle.go`: remote-cancel gate must include
  `KindTOCDownload`. Do not treat every TOC kind as terminal occupancy.
- `internal/mrfdownload/worker.go`: Terminal must delete then
  `releaseEmptyDownloadSlot`.
- `internal/tocdownload/worker.go`: set Terminal to
  `RemoveUnpublishedDownload(artifact.KindTOC, id)` after durable fail.
- `internal/reconcile/cancel.go` (+ tests): TOC in live repair and notify
  classification; generalize leftover failed-download file removal.
- Docs listed above. Replace `TestRunRemoteCancelLeavesTOCAsInterruption`.
