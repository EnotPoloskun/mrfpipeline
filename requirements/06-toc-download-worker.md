# Story 06: TOC download worker

## Status

Implemented.

## User story

As a pipeline operator, I want each newly admitted TOC downloaded in a
retry-safe background job so that payer/network failures do not block
discovery or create ambiguous partial input files.

## Goal

Implement the production `toc.download` River worker using the Story 04 shared
HTTP downloader, register queue `toc_download`, and atomically publish the
`toc.parse` successor when one TOC download succeeds.

This story downloads response bytes only. It does not inspect TOC JSON, invoke
`mrftocparser`, read Parquet, create an MRF source, or change the consumer
warehouse.

## Dependencies and scope

This story depends on Stories 01 through 05.

Story 02 owns `toc_files` and its stage constraints. Story 03 owns job
arguments, claim/retry/finalize behavior, and transactional `InsertTx`. Story
04 owns URL/network validation, streaming, download artifacts, recovery, and
cleanup. Story 05 owns initial `toc.download` insertion.

Do not implement another HTTP client or filesystem layout in this worker. The
worker coordinates one domain row around the existing downloader.

## Product decisions

- One `toc.download` job owns one `toc_files.id`.
- The exact URL is loaded from PostgreSQL after claim and is never included in
  job arguments or logs.
- A valid completed Story 04 download is success evidence and is reused without
  another HTTP request.
- An incomplete download leaf is removed and restarted by the shared
  downloader; the worker does not delete a valid completed download first.
- Download success unblocks TOC parse and inserts exactly one `toc.parse` job
  in the same transaction.
- Download bytes remain present until Story 07 validates parser publication.
- All download and publication failures use River's shared eight-attempt
  policy, including HTTP 4xx and URL-policy failures. Version 1 does not
  introduce special per-status attempt counts or a fail-fast taxonomy.
- HTTP failures never cause the TOC URL row to be deleted.
- Byte count remains in the download manifest, not PostgreSQL.
- A zero-byte completed download still schedules TOC parsing, which then fails
  input validation.
- Inconsistent lifecycle combinations fail with `domain_invariant`. Only
  Story 13 reconciliation repairs that shape.

## Worker registration

Extend the production `work` client to register:

| Queue | Maximum workers | Registered kinds |
|---|---:|---|
| `discovery` | 1 | `discovery.run` |
| `toc_download` | 4 | `toc.download` |

No other production queue is consumed yet. `toc.parse` jobs created by this
story remain pending until Story 07.

The fixed maximum is per the one supported worker process. Do not add a
download-concurrency flag or environment variable. `MaxConnsPerHost=4` on the
shared HTTP client is sufficient; do not add another per-host semaphore.

## Job argument and row ownership

The durable argument is exactly:

```json
{"toc_file_id":72}
```

The worker owns only these `toc_files` fields during its stage:

- `download_status`
- `download_river_job_id`
- `failure_code` when download reaches terminal failure
- `updated_at`
- successor `parse_status`
- successor `parse_river_job_id`

It does not change payer, collection month, source URL, first discovery,
last-seen time, parse completion, or import state.

## Claim transaction

Apply the common Story 03 claim protocol while locking the TOC row:

- Missing row: fixed permanent `missing_domain_record` classification.
- `download_status=succeeded`: successful idempotent no-op.
- Stored download job ID differs: stale successful no-op.
- `download_status=failed`: terminal/stale no-op.
- Assigned `pending` or same-job `running`: set/retain `running`, clear no
  artifact, leave `failure_code` null, update `updated_at`, and commit.
- Any other lifecycle combination: `domain_invariant`.

`parse_status` must still be `blocked` when claiming an ordinary unfinished
download. The worker does not repair a prematurely pending/running parse.

Read the exact `source_url` into memory before committing the claim. Do not
hold the row lock during HTTP or filesystem work.

## External download operation

After claim commit, call the shared downloader with:

- the River job context;
- artifact kind `toc`;
- the positive `toc_file_id`; and
- the exact stored `source_url`.

The final data path is derived internally as:

```text
<artifact-root>/toc/toc-<id>/download/data
```

The worker never supplies a basename, extension, relative path, or download
manifest. Story 04 owns those choices.

The downloader result contains the final local data path and nonnegative byte
count. The worker does not persist the path or byte count in PostgreSQL.
Successful zero-byte transfer still completes the download stage; Story 07's
parser will classify invalid/empty TOC content.

## Completion validation

Before the success transaction, inspect the final download through the Story
04 reader and require:

- real `download` directory;
- exact regular `data` and `manifest.json` entries;
- supported download manifest schema `1.0.0`; and
- manifest byte count equal to the current regular-file size.

Do not open, decompress, or parse `data`. Do not trust only the downloader's
in-memory return value; the final artifact is the recoverable boundary.

If completion inspection fails, treat the attempt as an artifact/download
failure. A later retry may remove the incomplete leaf using Story 04. A
complete manifest with unsupported schema is not automatically deleted.

## Success and successor transaction

In one short transaction:

1. Lock and re-read the TOC row.
2. Verify the executing job ID is still assigned.
3. If download already succeeded, return a no-op and do not insert another
   successor.
4. Require `download_status=running` and `parse_status=blocked`.
5. Set `download_status=succeeded`.
6. Set `parse_status=pending`.
7. Insert one `toc.parse` job with the same `toc_file_id` through `InsertTx`.
8. Store the returned job ID in `parse_river_job_id`.
9. Clear any download terminal failure code, update `updated_at`, and commit.

The artifact is complete before this transaction begins. If the transaction
rolls back or acknowledgement is lost, River retry reuses the same completed
download and repeats only finalization.

The transaction never deletes the download. Story 07 owns deletion only after
it validates the parser's final manifest.

## Retry behavior

For attempts before the eighth, a failure from URL policy, DNS, dial, TLS,
redirect, HTTP status, copy, close, content length, artifact inspection, or
filesystem publication:

1. Leaves any valid completed final download intact.
2. Lets Story 04 remove only this invocation's incomplete staging work.
3. Changes the still-assigned `download_status` from `running` to `pending` in
   a short transaction.
4. Leaves `failure_code` null and `parse_status=blocked`.
5. Returns the redacted error to River.

There is no worker-level loop and no alternate URL. River owns delay and
attempt count.

On the final attempt, mark the assigned download `failed`, leave parse and
import blocked, set `updated_at`, and use one fixed safe code:

```text
toc_download_failed
```

The code does not distinguish a URL policy failure from HTTP, TLS, disk, or
publication details. River logs retain safe execution context; source details
remain only in the authorized database URL column.

## Crash and stale-job behavior

| Crash point | Required retry result |
|---|---|
| Before HTTP request | Claim can be repeated by the same assigned job. |
| While streaming | Targeted staging entry is cleaned; no final download exists. |
| After final rename, before database success | Completed download is reused; no second request. |
| After success transaction commit | Download is succeeded and one parse job already exists. |
| Old job after replacement | Job-ID mismatch returns a stale no-op and does not inspect/remove artifacts. |

A generic startup scan must not delete partial work for every TOC. Cleanup is
targeted to the claimed row so an active or future job's artifacts are not
removed accidentally.

## Cancellation and shutdown

The River context is passed unchanged through the downloader. Cancellation:

- aborts DNS/dial/request/body copy;
- closes the body and local file;
- removes only the current operation's staging directory when possible;
- never marks download succeeded without a valid final artifact; and
- returns the context error for River/shutdown handling.

During graceful worker shutdown, no new TOC download is fetched. Active
downloads may finish during the shared grace interval; forced shutdown cancels
their contexts.

## Redaction

In addition to Stories 01, 03, and 04, this worker must not expose:

- TOC URL or any substring of it.
- Redirect target, DNS name, or resolved address.
- Generated absolute artifact paths.
- Response body, headers, or raw network error.
- TOC ID in ordinary worker logs.

Safe logs may include the fixed job kind/queue, River job ID, attempt, duration,
`toc_download_failed` classification, and Story 04 throttled download
`progress`.

## Required tests

### Unit tests

- Job argument validation and worker registration are exact.
- Claim state combinations map to work, stale no-op, completed no-op, terminal
  no-op, or invariant failure.
- The downloader receives only the stored URL and generated TOC address.
- Success finalization inserts `toc.parse` and stores its ID atomically.
- Retry and final failure update only the allowed TOC fields.
- Hostile URL/path/network errors are redacted.

### PostgreSQL, River, filesystem, and HTTP integration tests

Using a disposable database, Story 04 temporary workspace, and controlled HTTP
server/dialer, prove:

- A pending `toc.download` job streams exact bytes and transitions to
  succeeded with one pending `toc.parse` job.
- The final download manifest byte count matches the file.
- A second execution after success performs no HTTP request and creates no
  second parse job.
- A crash/failure injected after download rename but before database commit
  causes retry to reuse the final artifact without another request.
- A mid-stream failure leaves no complete final download and a later attempt
  restarts from byte zero.
- Existing incomplete download content is removed only at that TOC leaf.
- Unsupported or malformed completed metadata fails closed.
- HTTP/status, redirect-policy, content-length, filesystem, and cancellation
  failures leave parse blocked until success.
- A completed zero-byte download still schedules TOC parsing.
- Eighth failure marks only download failed with the fixed safe code.
- A stale job cannot remove or replace another job's completed artifact.
- Queue concurrency never exceeds four within the one worker process.

Run:

```text
go test ./...
go test -race ./...
go vet ./...
```

## Acceptance criteria

- Story 05 `toc.download` jobs are executed by a four-worker bounded queue.
- Each job loads its exact URL from PostgreSQL and streams it through the one
  shared downloader.
- Completed downloads are atomically published, structurally validated, and
  reused across retries.
- Download success and one `toc.parse` successor commit together.
- Partial work is removed only at exact owned leaves; valid completed downloads
  are preserved until parse success.
- Retry, terminal failure, cancellation, stale delivery, and diagnostics
  follow the common contracts.
- No TOC JSON or Parquet is read in this story.

## Non-goals

- TOC parsing or manifest-schema validation beyond the download manifest.
- MRF source, feed, snapshot, or plan creation.
- Alternate URLs, range resume, conditional requests, or internal retries.
- Persisting response metadata or downloaded byte count in PostgreSQL.
- Deleting a successful download before parser success.
- Dynamic queue configuration, several worker hosts, or content hashes.
