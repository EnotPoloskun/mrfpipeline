# Story 23 implementer handoff

Authoritative spec: `requirements/23-terminal-parse-slot-release.md` (Specified).
Repo: `/Users/enotpoloskun/go/mrfpipeline` on `master`.
HEAD: `8fe12bb Address Story 22 operational follow-ups`.

## What this story is
Terminal `mrf.parse` must stop occupying a resident slot and must delete the raw download + parser temp after the attempt budget is exhausted (or on immediate invariant failure that marks parse failed). Automatic in-flight retries still keep the raw file and slot. Newly inserted `mrf.parse` jobs have MaxAttempts 4 (first + 3 retries). Other kinds stay at 8. Parser schema is out of scope.

## Locked product decisions
- 4 = total attempts including the first, parse-only.
- Failed source stays selected; still counts toward numeric target; still blocks activation (`source_incomplete`).
- Do not unselect, do not decrease `set-total`, no “drop source” CLI.
- `cli retry --stage mrf.parse` rematerializes when bytes are gone (wait for slot → redownload → parse); parse-only when raw is still present.
- Reconcile repairs already-terminal parses that still hold slots (live sample after deploy).
- River UI: pause/resume only. Do not cancel/retry/delete jobs from UI.
- Do not log parser error text.
- Do not mutate the live UHC Compose sample (no restart MRF, no SQL slot deletes, no `down -v`).
- Do not implement leftover Story 22 nits.
- Uncommitted `README.md` already has Story 22 local-runbook edits. Story 23 requires a slot-paragraph update; keep packaging-test command strings intact. Do not add catalog, `.env`, or `services.csv`.
- Serial `go test -p 1` is the DB-suite contract. Subagent must skip formatters, linters, and project-wide test suites; parent runs those once at the end.

## Implementation hints from parent review of current code
- `jobs.MaxAttempts` remains 8 (client default). Set MaxAttempts 4 only on `MRFParseArgs.InsertOpts`.
- Parse worker currently has no `Terminal` hook; download worker’s `releaseEmptyDownloadSlot` is the analog.
- `confirmParseSuccess` already releases slot on success after files are gone.
- Reconcile `releaseEmptyTerminalDownloads` only covers `download_status=failed` with no files. Need parse analog.
- Today `retry` of `mrf.parse` errors if no slot (`domain_invariant`). Must follow the rematerialize table in the story.
- Admission `ScheduleWaiting` already skips `parse_status=failed` / `download_status=failed`. After terminal parse, download should be `blocked` so a later retry can wait for a slot and redownload.
- Immediate fails (`invalid_job_arguments`, `missing_domain_record`, `domain_invariant`, sealed-release codes) that mark parse failed use the same terminal cleanup.

## Communication
Ask the parent (Main) questions before writing code. Do not start implementation until Main says there are no more questions.
