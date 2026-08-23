# Story 15: Month-specific TOC captures

## Status

Planned.

## User story

As a pipeline operator, I want the same payer TOC URL admitted independently
in different collection months so that a reused listing URL cannot pin every
future run to the month in which it was first seen.

## Product decision

One `toc_files` row represents one monthly capture, not one eternal URL.

The exact identity becomes:

```text
(payer_id, collection_month, source_url)
```

Within one payer/month, exact URL equality remains the deduplication rule. The
same URL in another month is new monthly work and receives a new TOC ID,
download, parser output, and import lifecycle.

This is required even when the URL text is unchanged because a payer listing
endpoint may serve updated content at a stable location. The design uses no
content hash, ETag identity, timestamp inference, or URL normalization.

## Dependencies and supersession

This story depends on Story 14 and the implemented discovery/download/parse
contracts in Stories 05–07.

It supersedes the sticky first-admission behavior in Stories 02, 05, 07, 08,
and 13:

- `toc_files` is no longer unique by `(payer_id, source_url)`;
- rediscovery in a later month no longer reuses the older TOC row;
- collection month is immutable for one monthly capture, not permanently
  frozen for that URL across all months; and
- status/retention docs no longer describe a single sticky TOC month.

Bounded discovery, exact listing order, per-run observation joins, River job
arguments, artifact IDs, download safety, TOC parser configuration, import
validation, and redaction otherwise remain unchanged.

## Migration

Add:

```text
internal/database/migrations/0003_monthly_toc_captures.sql
```

Migration `0003` replaces the old TOC unique constraint with one named unique
constraint on `(payer_id, collection_month, source_url)`. Because Story 14
already requires a clean feed-free deployment, no duplicate-collapse or row
rewrite is performed.

The migration is forward-only, transactional, and idempotent through the
existing ledger. It does not alter TOC numeric IDs or artifact paths.

## Discovery admission semantics

For one discovery run, deduplicate repeated listing occurrences by exact URL
while preserving first-occurrence ordinal, exactly as before.

For each distinct listing URL, lookup uses the run's exact payer and month:

```text
payer_id = run.payer_id
collection_month = run.collection_month
source_url = exact discovered URL
```

Behavior:

- Existing in the same payer/month: reuse that capture, update its
  `last_seen_at`, insert the run/capture observation, and count it as existing.
- Existing only in another month: treat it as new for this run; it competes for
  the current run's admission limit.
- New and within limit: insert one pending monthly capture plus its TOC
  download job transactionally.
- New and beyond limit: count overflow and insert no capture or job.

`existing_count`, `admitted_count`, and `overflow_count` are therefore
month-relative. A URL previously processed in August is not an existing
September capture.

Concurrent discovery runs for the same payer/month rely on the new unique
constraint and existing retry/re-read pattern. At most one capture/download
job is admitted for the exact monthly identity.

## Monthly capture lifecycle

The stored collection month remains required and immutable on one `toc_files`
row. A wrong month on that capture is never edited in place; remove/rebuild the
affected nonproduction database/warehouse or admit the correct month as a
separate capture under the supported rebuild procedure.

Every monthly capture retains independent:

- first discovery run and observation joins;
- download/parse/import statuses and River job IDs;
- failure code and explicit retry;
- `toc-<id>` output identity;
- downloaded file and parsed output paths; and
- exact parser manifest payer/month validation.

The same URL in August and September therefore produces `toc-41` and `toc-92`
or equivalent distinct numeric identities. Their files never share a mutable
directory.

## Parser and import boundary

Continue passing the capture's stored payer and exact `YYYY-MM` month to
`mrftocparser`. Completed-output recognition requires those values in the
manifest and Parquet rows.

Import uses only the claimed monthly capture. It must not search an older
capture with the same URL, copy its parsed output, or reuse its TOC-plan
provenance. Story 16 may reuse exact MRF sources found inside the new output;
TOC capture identity remains independent.

After successful import, Story 13 cleanup may remove that capture's parsed TOC
leaf and downloaded file according to existing validation gates. Cleanup of
one month must never target the other month's capture.

## Exact URL and mutability boundary

The TOC URL is stored and requested exactly. HTTP redirect, credential,
private-address, timeout, staging, and filename rules remain unchanged.

The pipeline deliberately downloads a reused TOC URL once per admitted month.
It does not compare bytes between months or try to prove content changed.

Story 14 makes `mrf_sources` monthly captures too. An exact MRF URL found in
several TOCs in one collection month is downloaded and parsed once. The same
URL found in another month creates another source capture, download, and parse.
TOC and MRF captures therefore follow the same monthly mutability boundary
without hashes, ETags, byte comparison, or URL normalization.

If an exact TOC or MRF URL changes more than once inside one collection month
after its capture succeeds, the pipeline does not detect or automatically
recapture it. That narrower limitation is accepted for the manual monthly
release model.

## Release integration boundary

Until Story 18, any valid month can receive discovery runs. Story 18 later
creates/seals monthly releases and rejects new discovery for active/inactive
months. Story 15's database identity already supports that gate without
another TOC rewrite.

## Reconciliation and retry

Reconciliation and retry continue addressing one numeric `toc_file_id`.
Queries that locate a TOC by URL must include payer and month or use the
numeric ID. Never choose an arbitrary row when the same URL exists in several
months.

Missing-job repair, predecessor restoration, safe artifact reset, and terminal
failure rules remain per capture. River arguments remain:

```json
{"toc_file_id":41}
```

No URL, payer, or month enters River job arguments. Validated payer/month and
numeric IDs may appear as structured operational context; URLs remain redacted.

## Required tests

Add or revise tests proving:

- migration `0003` installs the exact new unique constraint;
- same URL twice in one run produces one first-occurrence observation;
- same URL in later runs for the same month reuses one capture;
- same payer/URL in a different month creates a new capture and download job;
- same month/URL for a different valid payer is a distinct capture in domain
  fixtures, while production discovery remains UHC-only;
- admission limits count prior-month URLs as new monthly candidates;
- concurrent same-month admission converges to one row/job;
- each monthly capture uses its own `toc-<id>` paths and exact manifest month;
- retry/reconcile/cleanup by one ID cannot affect another month's capture;
- import provenance remains attached to the correct monthly capture;
- URL values remain exact and redacted; and
- identity and scheduling behavior require no digest, ETag identity, content
  comparison, or URL normalization.

Tests assert observable keys, constraints, scheduling, paths, and redaction.
They do not scan source text for forbidden words.

## Acceptance criteria

- Reusing a TOC URL across months cannot keep new work assigned to the old
  month.
- Deduplication remains exact and idempotent inside one payer/month.
- Monthly captures have independent artifacts and stage lifecycles.
- MRF URL/source deduplication remains month-scoped and plan-independent.
- Bounded discovery counters and concurrency are deterministic under the new
  identity.

## Out of scope

- Recurring or automatically scheduled discovery.
- Detecting whether monthly TOC bytes are identical.
- Detecting repeated mutation behind one exact URL inside a collection month.
- Non-UHC discovery implementation.
- Monthly release activation or query serving.
- Editing a capture's payer or month in place.
