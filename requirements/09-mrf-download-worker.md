# Story 09: Shared MRF download worker

## Status

Implemented.

## User story

As a pipeline operator, I want each exact in-network MRF source downloaded once
in a bounded background queue so that several TOCs and snapshots can reuse one
large local source without duplicate network transfer.

## Goal

Implement `mrf.download` using the Story 04 streaming downloader, register the
`mrf_download` queue, and atomically schedule one plan-independent `mrf.parse`
job after the shared source download succeeds.

This story does not parse MRF JSON, use plans, create snapshots, ingest the
warehouse, or attach plan associations.

## Dependencies and scope

This story depends on Stories 01 through 08.

Story 08 inserts new `mrf_sources` and their first download jobs. Story 04 owns
the HTTP client, public-network policy, local publication, byte-count manifest,
and targeted cleanup. Story 03 owns job attempts and transitions.

The implementation should share the same small download coordinator used by
Story 06 where that improves clarity, parameterized by the fixed domain kind
and row operations. Do not create a generic workflow framework or change TOC
download behavior.

## Product decisions

- One job owns one `mrf_sources.id`, not one TOC association or snapshot.
- Exact URL uniqueness already decided whether the source is shared.
- The job carries only `mrf_source_id`; it reloads the exact URL from
  PostgreSQL.
- Plans, payer, feed, collection month, TOC ID, and snapshot ID do not affect
  download or parse scheduling.
- At most two large MRF downloads run in the one worker process.
- A valid completed download is reused after retry/crash without another HTTP
  request.
- Range resume, alternate mirrors, conditional requests, and worker-internal
  retry remain unsupported. A retry starts from byte zero.
- Download success unblocks and enqueues exactly one `mrf.parse` job.
- Completed bytes remain until Story 10 validates parser publication.
- A terminal source download failure leaves every dependent snapshot blocked;
  it does not fail or delete those snapshot/plan records automatically.
- No disk reservation or free-space preflight is added. Version 1 does not
  guess required capacity from HTTP headers.
- All download failures use the shared eight-attempt policy, including HTTP
  404 and other 4xx responses.

## Worker registration

The worker process now consumes:

| Queue | Maximum workers | Registered kinds |
|---|---:|---|
| `discovery` | 1 | `discovery.run` |
| `toc_download` | 4 | `toc.download` |
| `toc_parse` | 2 | `toc.parse` |
| `toc_import` | 2 | `toc.import` |
| `mrf_download` | 2 | `mrf.download` |

`mrf.parse` and `consumer` jobs remain pending until later stories.

## Job argument and row ownership

Exact argument:

```json
{"mrf_source_id":151}
```

The worker owns these `mrf_sources` values:

- `download_status`
- `download_river_job_id`
- shared `failure_code` on terminal download failure
- `updated_at`
- successor `parse_status`
- successor `parse_river_job_id`

It never changes source URL, first filename, creation time, snapshots,
associations, plans, or consumer state.

## Claim transaction

Lock the source row and apply Story 03:

- Missing row: `missing_domain_record`.
- Different stored download job ID: stale no-op.
- Download succeeded: completed no-op.
- Download failed: terminal no-op.
- Assigned pending/same-job running: require parse blocked, set/retain download
  running, leave failure code null, and update `updated_at`.
- Any other combination: `domain_invariant`.

Read the exact `source_url` before claim commit. Do not lock dependent
snapshots or association rows; they do not participate in the transfer.

## External download

Call the Story 04 downloader with:

- the River context;
- artifact kind `mrf`;
- the positive source ID; and
- exact stored source URL.

It derives:

```text
<artifact-root>/mrf/mrf-source-<id>/download/data
```

The source may be raw JSON or gzip; the downloader stores response bytes
without decoding. `mrfparser` later detects encoding from bytes.

Large body size does not change buffering: use the fixed 256 KiB streaming
buffer and transport limits from Story 04. Do not preload content, inspect
JSON, calculate a digest, or duplicate bytes for each snapshot.

The returned path and byte count are not persisted in PostgreSQL. The
download's `manifest.json` remains their recoverable local record.

## Completion validation

Before database success, re-open the final download through Story 04 and
require exact schema `1.0.0`, exact two-file layout, regular non-symlink files,
and manifest/file-size equality.

Do not reject a download because its URL filename lacks `.json.gz`, its body is
empty, or its bytes do not begin with gzip. Those are parser input decisions in
Story 10.

An unsupported manifest is preserved and fails closed. An incomplete final
leaf is eligible for the shared downloader's targeted reset on retry.

## Success and parse scheduling

Use one short transaction:

1. Lock and re-read `mrf_sources`.
2. Verify this River job remains assigned.
3. Return no-op if download already succeeded.
4. Require download running and parse blocked.
5. Set download succeeded and parse pending.
6. Insert one `mrf.parse` job with the same source ID through `InsertTx`.
7. Store `parse_river_job_id`, clear any download failure code, update
   `updated_at`, and commit.

Do not schedule consumer jobs here. Dependent snapshots remain blocked until
the parser output succeeds. Story 10 unblocks all eligible snapshots after one
shared parse.

If the transaction fails after artifact publication, retry reuses the final
download. If it commits, parse eligibility and its job cannot be separated.

## Retry and terminal behavior

Before the final attempt, any URL-policy, network, status, streaming,
content-length, filesystem, or manifest failure:

- preserves a valid completed artifact;
- cleans only the current incomplete staging operation where possible;
- changes the assigned download state from running back to pending;
- leaves parse and every snapshot blocked; and
- returns the redacted error to River.

On attempt eight, mark only the source download failed and persist:

```text
mrf_download_failed
```

Do not propagate the source failure into every snapshot row. Their blocked
state plus the source relationship explains the prerequisite failure and lets
Story 13 repair/retry the one shared source rather than many copies.

There is no special retry count for HTTP `4xx`, including `404`, private-address
rejection, or disk errors in version 1.

## Crash behavior

| Crash point | Recovery |
|---|---|
| During body stream | No final artifact; matching staging is removed and the full request restarts. |
| After final rename, before DB commit | Validate/reuse the completed bytes; do not issue another request. |
| After DB commit | One parse job already exists with parse pending. |
| Stale old job | Return no-op before artifact inspection or deletion. |

Several TOCs may continue inserting associations/plans for this source while
its download runs. They do not alter the assigned job or artifacts.

## Cancellation and redaction

Pass the job context unchanged. Forced shutdown cancels the request/body copy
and performs targeted staging cleanup. Cancellation cannot mark success without
a valid final artifact.

Never expose:

- Source URL, query, redirect, DNS, IP, or HTTP body/header.
- First MRF filename.
- Generated local path.
- Source, TOC, snapshot, feed, or plan IDs in ordinary logs.
- Raw network, TLS, filesystem, or PostgreSQL diagnostics.

Safe logs follow Story 03 and may contain the fixed job kind/queue, River job
ID, attempt, duration, terminal classification, and throttled download
`progress`.

## Required tests

### Unit tests

- Claim state mapping and allowed field ownership match this story.
- Downloader receives exact stored URL plus only the generated MRF source
  address.
- No association/snapshot value enters args, artifacts, or HTTP configuration.
- Success inserts exactly one parse job atomically.
- Final failure does not update dependent snapshot rows.
- Errors remain classified/redacted with hostile long URLs and filenames.
- A live body copy emits throttled `progress`; a reused completed download
  does not.

### PostgreSQL/River/filesystem/HTTP integration tests

- A new Story 08 source job streams a large controlled body and publishes the
  exact completed artifact.
- Two TOCs referencing the same exact URL still produce one request and one
  parse job.
- Different exact URLs produce independent downloads even when body/filename
  is equal.
- Crash after rename reuses the artifact without a second request.
- Midstream failure/cancellation leaves no complete final artifact and restarts
  from byte zero.
- Incomplete final content is reset only for the claimed source.
- Unsupported/corrupt completion metadata is preserved and fails closed.
- Transaction rollback creates no visible parse job; retry finalizes from the
  existing download.
- Eighth failure marks the source failed while snapshots remain blocked.
- Queue execution never exceeds two concurrent MRF downloads.

Run:

```text
go test ./...
go test -race ./...
go vet ./...
```

## Acceptance criteria

- Newly imported exact MRF sources download through a two-worker bounded queue.
- Shared source identity causes one transfer regardless of TOC/plan overlap.
- Downloads stream with fixed memory and atomically publish reusable completion
  metadata.
- Success and one plan-independent parser job commit together.
- Failure/cancellation leaves snapshots blocked and preserves retryable shared
  state without duplicating work.
- No MRF JSON, plan, Parquet, or warehouse data is processed in this story.

## Non-goals

- MRF parsing, service filtering, provider resolution, or manifest validation.
- Consumer snapshot ingestion or plan attachment.
- Per-snapshot source copies or jobs.
- Resume/range downloads, alternate URLs, internal retry loops, S3 artifacts,
  URL normalization, filename identity, content hashes, or free-space
  preflight.
