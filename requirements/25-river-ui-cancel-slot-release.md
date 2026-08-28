# Story 25: River UI cancel of MRF occupancy

## Status

Implemented.

## User story

As an operator watching River UI, I want cancelling `mrf.download` or
`mrf.parse` to fail that stage and free its resident slot, so a cancelled
Drive 404 or unwanted in-flight parse does not pin capacity until I remember
to run `reconcile`.

## Context

A slot is a row in `mrf_materialization_slots`. River UI cancel only changes
the job row. Occupancy is released only after the domain stage is terminal and
Story 23 cleanup proves the raw/staging files are gone.

Today that repair happens in `cli reconcile` (and control startup). Cancelling
a retryable download from the UI leaves the source pending/running and the slot
held. Recreating an MRF worker also cancels job contexts; that path must stay
an interruption so in-flight work retries.

River distinguishes the two. UI cancel sets `rivertype.ErrJobCancelledRemotely`
as the job context cause and, for a queued job, stores `cancelled` immediately.
Graceful/canceling stop uses `ErrStop`.

## Dependencies

Implemented Stories 14–24, especially Story 13 cancelled-job repair, Story 21
slots, and Story 23 terminal occupancy cleanup.

## Goal

1. UI cancel of a **running** `mrf.download` or `mrf.parse` fails that stage
   with `river_terminal_without_domain_result`, then frees the slot.
   Cancelled download deletes unpublished download bytes and staging first.
   Cancelled parse uses the existing Terminal hook (unpublished parsed
   output, parser temp, and raw bytes). Exhausted download retries still
   keep leftover files for resume.
2. UI cancel of a **queued** `mrf.download` or `mrf.parse` is noticed by
   control without `cli reconcile`: control wakes, fails the cancelled stage,
   and runs the same occupancy cleanup.
3. Worker recreate / SIGTERM remains an interruption: no `MarkFailed`, no
   Terminal occupancy cleanup, job retries.
4. Other production kinds keep today's interrupt-or-reconcile behavior.
5. The source stays selected and counts toward a numeric target. `cli retry`
   is how it is reopened.

## Non-goals

- River UI retry or delete.
- Unselecting the source or substituting another URL.
- Changing attempt budgets or HTTP 404 mapping.
- TOC/discovery/consumer occupancy (they do not hold resident slots).
- Rewriting historical Stories 03, 13, 21, or 23 as if they originally
  specified UI-cancel occupancy.
- Mutating the live sample as part of landing this story.

## Product decisions

- UI cancel of MRF occupancy is the same terminal path as exhausting retries,
  with failure code `river_terminal_without_domain_result`.
- A cancelled parse deletes raw/temp so the slot can actually free. A later
  parse retry rematerializes when bytes are gone.
- A cancelled download deletes unpublished download bytes and staging so the
  slot can actually free. Exhausted download retries still keep leftover
  files for resume.
- Claim-time `JobCancel` without `MarkFailed` still must not run Terminal
  cleanup.
- Live control must not treat a still-`running` River job as abandoned and
  must not insert a replacement. Startup `reconcile` keeps that replacement
  for dead workers.
- Control listens for River cancel notifications on `mrf_download` /
  `mrf_parse` and wakes the existing `control.schedule` worker. That worker
  repairs cancelled occupancy before refill.

## Required tests

- `jobs.Run`: plain context cancel of `mrf.parse` returns nil to River and
  does not run Terminal. Remote cancel cause of `mrf.parse` is not treated as
  interruption. Remote cancel of `toc.download` stays an interruption.
- Repair of a cancelled `mrf.download` with a held slot and no bytes fails the
  stage and releases the slot. A cancelled download with leftover bytes or
  staging deletes those files and then releases the slot. A still-running
  download job is not replaced.
- Control notify classification: only `cancel` on `mrf_download` / `mrf_parse`.
- `jobs.Run`: remote cancel of `mrf.parse` runs Terminal after a durable fail.
  Remote cancel of `mrf.download` uses CancelTerminal, not the empty-only
  Terminal hook. Plain `WithCancel` still preserves `ErrJobCancelledRemotely`.
