# Story 16: Feed-free TOC import and snapshot scheduling

## Status

Implemented.

## User story

As a pipeline operator, I want imported TOC associations to converge on one
monthly source capture and source/payer/month snapshot without creating a
synthetic feed so that same-month shared MRFs are parsed once and monthly
consumer outputs remain independently plan-aware.

## Product decision

TOC import maps every valid association through these identities:

```text
exact mrf_location + TOC collection month  -> mrf_source
mrf_source + toc payer + collection month -> mrf_snapshot
mrf_snapshot + five-field plan tuple      -> mrf_plan
```

There is no feed lookup, feed insert, feed formatter, or cross-month current
semantics.

## Dependencies and supersession

This story depends on Stories 14–15 and the implemented Story 08 import and
Story 12 additive plan scheduling contracts.

It supersedes Story 08's `feed_id=mrf-source-<source-id>` policy and every
import/scheduling step that reads or writes `mrf_feeds`.

It preserves:

- strict TOC parser-output validation;
- exact HTTPS MRF location and filename validation;
- same-month exact-URL `mrf_sources` reuse;
- one plan-independent MRF download/parse lifecycle per source;
- source TOC provenance;
- sponsor-independent plan identity;
- transactionally inserted River jobs;
- frozen additive plan batches; and
- bounded, redacted, restart-safe import.

## Import mapping

For each validated association row, in deterministic parser order:

1. Select or insert `mrf_sources` by exact `mrf_location` plus the claimed
   TOC's collection month.
2. Preserve the existing first nonempty MRF filename behavior.
3. Select or insert `mrf_snapshots` using:

   ```text
   mrf_source_id
   toc_file.payer_id
   toc_file.collection_month
   ```

4. Insert exact TOC/source/snapshot plan provenance into
   `toc_mrf_plan_associations`.
5. Project and select/insert the sponsor-independent five-field `mrf_plans`
   row for that snapshot.
6. Schedule a new source download only when the source lifecycle needs one.
7. Schedule consumer ingest only when the snapshot is eligible and the source
   parse is already complete.

The snapshot insert uses the named unique constraint from Story 14 and re-reads
the existing row after a conflict. Story 14's composite foreign key enforces
that source and snapshot collection months match. Import never chooses a
snapshot by source alone or by output path.

## Required convergence behavior

### Overlapping TOCs in one month

If two TOCs for the same payer/month reference the same exact MRF URL:

- one source exists;
- one MRF download/parse lifecycle exists;
- one snapshot/output ID exists;
- both TOC provenance sets remain; and
- their distinct plans accumulate on the snapshot and are attached through
  later frozen batches.

No TOC-wide wait is required. Importing A/B may schedule the source and
snapshot; importing B/C later creates only plan C as new snapshot plan state.

### Same URL in another month

If September references the same exact MRF URL captured in August:

- create a distinct September source capture, download, and parse;
- create a distinct September snapshot;
- run consumer ingestion again for September under its own output ID;
- attach September plans to that output only; and
- leave August warehouse data unchanged.

The new parse is required because an exact URL may serve different bytes in a
later month. Identity remains exact URL + collection month; the pipeline does
not use hashes, ETags, content comparison, or URL normalization.

### Same source for another payer

A domain fixture may associate one monthly source capture with two valid
payers. Reuse the same-month parse but create one snapshot per payer/month.
Plans and consumer outputs remain isolated by snapshot ID. The production
discovery adapter is still UHC only.

### Different source URLs

Different exact URLs always create distinct sources and snapshots even when
their filenames, reporting entities, plans, or bytes might match. Import does
not infer duplicate content or logical replacement.

## Transaction and lock order

Retain short transactions and deterministic ascending-ID work. When a row
requires creation or plan scheduling, lock in this order:

```text
toc_file
mrf_source
mrf_snapshot
mrf_plan rows / attachment batches as required
```

There is no feed row in the order. Do not hold locks while reading Parquet,
downloading, parsing, consuming, or attaching.

Source/snapshot creation, relevant River job insertion, and stored River job ID
commit together under existing Story 08 rules. Import remains retryable after
partial committed pages and finalizes only after every association is durable.

## Scheduling rules

Source scheduling remains independent from snapshots:

- a new monthly source capture begins pending download;
- an already parsed source receives no new parse job merely because another
  TOC or payer in the same month references it;
- the same exact URL in a later month creates a new source lifecycle; and
- a failed source remains terminal until explicit retry.

Snapshot scheduling uses only source parse state plus snapshot lifecycle:

- source parsed + new/blocked snapshot -> pending `consumer.ingest`;
- source not parsed -> snapshot remains blocked;
- snapshot pending/running/succeeded/failed -> do not duplicate its current
  stage job; and
- source parse success schedules every blocked snapshot referencing it in
  deterministic snapshot-ID order.

Story 12's common plan-batch scheduler remains snapshot-scoped. Ingest success,
later TOC import, batch completion, and deployment reconciliation all invoke it
under the snapshot lock exactly as before.

## Plan and provenance rules

The canonical plan identity remains:

```text
(mrf_snapshot_id,
 plan_name,
 issuer_name,
 plan_id_type,
 plan_id,
 plan_market_type)
```

Sponsor is validated/stored for provenance and EIN JSON projection but is not
canonical identity. Existing HIOS sponsor-null and duplicate-collapse rules
remain.

`toc_mrf_plan_associations` continues recording one TOC capture and one
snapshot. Because TOC captures are month-specific, provenance cannot attach an
August TOC row to a September snapshot: import requires association payer/month
to equal both the claimed TOC and snapshot.

## Import completion and cleanup

Successful finalization still marks only the claimed TOC import succeeded and
then allows safe cleanup of that TOC's parsed leaf. Shared MRF parsed output is
never deleted by import.

Re-importing a completed TOC is an idempotent no-op for existing sources,
snapshots, provenance, plans, and jobs. A crash after some association pages
commit resumes without creating feed-like duplicates.

## Errors and redaction

Remove feed invariant failures and feed formatting errors. Preserve the
existing safe categories for malformed TOC output, invalid MRF location,
database invariant, River scheduling, and artifact problems.

River arguments remain numeric source/snapshot/batch IDs. Logs and errors do
not expose URLs, filenames, plans, paths, provider/service values, credentials,
response content, or raw errors. Validated payer/month, derived output IDs,
numeric IDs, counts, statuses, and blocker codes may appear as structured
operational context.

## Required tests

Add or revise tests proving:

- import creates no feed row or value;
- one TOC association creates the exact source/payer/month snapshot;
- overlapping same-month TOCs converge to one source and one snapshot while
  preserving both provenance sets;
- A/B then B/C produces canonical A/B/C snapshot plans and later batches only
  newly unassigned plans;
- the same exact URL in another month creates a new source, parse, and
  snapshot/output;
- the same source/month for another payer creates a separate snapshot in
  domain fixtures;
- different URLs in one payer/month remain different snapshots;
- source parse success schedules every eligible monthly snapshot once;
- concurrent imports converge through the new unique constraint;
- retry after page/finalization interruption creates no duplicate plans,
  provenance, snapshots, or jobs;
- TOC payer/month must equal snapshot payer/month, and source/snapshot month
  mismatch is rejected;
- status/reconciliation queries no longer join `mrf_feeds`; and
- redaction and exact URL behavior follow the Stories 14–15 boundary.

## Acceptance criteria

- Import contains no synthetic feed policy.
- One exact URL is parsed once per collection month and can back several
  same-month payer outputs.
- Same-month overlapping TOCs add plans without re-consuming rates.
- Cross-month snapshots remain separate immutable warehouse publications.
- Every source, snapshot, plan, provenance row, and job converges under retry
  and concurrency.

## Out of scope

- Consumer `2.0.0` invocation and manifest recognition (Story 17).
- Active-month state or release sealing (Story 18).
- Detecting repeated mutable bytes behind an unchanged MRF URL inside one
  collection month.
- Deduplicating distinct URLs or comparing file contents.
- Plan removal/correction or stable plan lineage.
- Non-UHC discovery.
