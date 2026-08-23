# Story 04: Local artifact workspace and HTTP download

## Status

Implemented.

## User story

As a pipeline operator, I want one safe local workspace and one streaming HTTP
download implementation so that later TOC and MRF workers can recover partial
work without guessing whether a file or parser output is complete.

## Goal

Implement the local artifact-root contract, deterministic per-record paths,
completion inspection and cleanup primitives, and the shared HTTP downloader
used later by TOC and MRF download workers.

This story tests those components directly. It does not register a production
River worker, change a domain stage, enqueue a job, contact UHC discovery,
invoke a sibling parser or consumer, read Parquet, or create a warehouse.

## Dependencies and scope

This story depends on Stories 01 through 03.

Story 01 remains authoritative for environment parsing, absolute cleaned
paths, redaction, and command behavior. Story 02 remains authoritative for
numeric domain IDs and exact stored URL identity. Story 03 remains
authoritative for cancellation, retry ownership, and the rule that external
work does not run inside a database transaction.

This story:

- Validates and initializes `MRFPIPELINE_ARTIFACT_ROOT` for a worker.
- Defines exact artifact paths derived only from positive database IDs.
- Defines small manifests for completed HTTP downloads without content hashes.
- Defines safe inspection and cleanup for parser-owned output directories.
- Streams HTTP or HTTPS response bytes to local storage with bounded memory.
- Makes a completed download reusable after a process crash.
- Provides exact error and redaction behavior to later workers.

`mrfpipeline work` remains the Story 03 placeholder because there are still no
production workers. Story 05 starts the runtime.

## Product decisions

- Version 1 artifacts and downloads are local-only. There is no S3 artifact
  root, network filesystem protocol, or remote cache abstraction.
- The configured artifact root is owned exclusively by `mrfpipeline`.
- Database numeric IDs determine artifact directories. URLs, filenames,
  payer values, plan values, timestamps, UUIDs, and hashes do not.
- A small download manifest and the byte length of its sibling data file are
  the download completion boundary. No SHA, digest, ETag, MD5, checksum, or
  content-derived identity is calculated or stored.
- The downloader preserves received representation bytes. It does not decode
  gzip or JSON and does not infer encoding from a filename or HTTP header.
- A completed download is never overwritten merely because a River job runs
again. A completed artifact for an exact URL is reused permanently in version
1 even if the remote response later changes. An incomplete download is removed
from its exact record directory and restarted from byte zero.
- Version 1 does not implement HTTP range resume. Atomic publication and full
  retry are simpler and safe for the bounded initial pipeline.
- `manifest.json` published by `mrftocparser` or `mrfparser` remains their
  completion boundary. The orchestrator never creates a fake parser manifest.
- Parser outputs with no manifest are disposable partial work. Parser outputs
  with a manifest are never deleted by generic cleanup code.
- The artifact root is not the warehouse and must not overlap any configured
  consumer or selector path.

## Workspace layout

The exact version 1 layout is:

```text
<artifact-root>/
  workspace.json
  .staging/
  toc/
    toc-<toc-file-id>/
      download/
        data
        manifest.json
      parsed/
        toc_files/
        mrf_plan_associations/
        manifest.json
  mrf/
    mrf-source-<mrf-source-id>/
      download/
        data
        manifest.json
      parsed/
        <eight parser dataset directories>
        processing_stats.json
        manifest.json
  plan-batches/
    plan-batch-<plan-attachment-batch-id>/
      plans.json
```

Only paths needed by the current stage must exist. The example parser and
plan-batch contents show their eventual locations; this story does not create
them.

Formatting is exact:

- TOC record directory: `toc-` plus the base-10 `toc_files.id`.
- MRF source directory: `mrf-source-` plus the base-10 `mrf_sources.id`.
- Plan-batch directory: `plan-batch-` plus the base-10
  `plan_attachment_batches.id`.
- IDs are positive signed 64-bit integers formatted without a sign, padding,
  separators, or leading zeroes.

These directory names are workspace addresses, not payer, feed, source, or
content identity. The consumer snapshot output ID remains `mrf-<snapshot-id>`
as defined by Story 02 and is not used as the shared MRF parser directory.

Do not preserve a source URL basename or extension in the path. Both sibling
parsers detect JSON versus gzip from bytes and accept an extensionless local
input path.

## Workspace marker

The root marker is exact UTF-8 JSON:

```json
{"application":"mrfpipeline","schema_version":"1.0.0"}
```

The production writer emits the compact object above plus one newline. The
reader accepts insignificant JSON whitespace but requires exactly those two
string properties and values; duplicate or unknown properties are invalid.

The marker identifies a directory that the pipeline may manage. It does not
contain a path, hostname, process ID, database ID, timestamp, credential, or
hash.

## Workspace initialization

Worker workspace initialization occurs after Story 01 configuration
validation and before a River client starts. It is idempotent and follows
this sequence:

1. Resolve the nearest existing ancestor of the configured absolute root and
   reject an inaccessible or non-directory ancestor.
2. Create missing parent components without following a symlink introduced at
   a component being created. When the final root is absent, build the complete
   initial workspace in an exclusive sibling temporary directory and rename
   that directory atomically to the configured root.
3. Require an existing final artifact root to be a real directory rather than a
   symlink, device, socket, FIFO, or regular file.
4. Resolve its physical path for overlap comparison and retain that path for
   the process lifetime.
5. If the root already exists and is empty, publish `workspace.json`
   atomically through an exclusive temporary marker in that root.
6. If the root is nonempty, require a valid existing `workspace.json` before
   creating or removing anything.
7. Create or verify the real directories `.staging`, `toc`, `mrf`, and
   `plan-batches`.

Root initialization requests mode `0700` for directories and `0600` for the
marker, subject to a more restrictive process umask. Existing entries are not
made more permissive.

An existing recognized workspace may be repaired only by creating a missing
fixed top-level directory. A missing or invalid marker in a nonempty root, an
unsupported marker version, a symlink, or a wrong-type fixed entry returns an
artifact error. Initialization does not adopt, rename, empty, quarantine, or
delete an unrecognized root.

Extra root entries are rejected, including `.DS_Store`, `Thumbs.db`, and
similar desktop noise. These are managed roots, not general user directories.
The sole pre-marker exception is an exact temporary-marker filename created by
an interrupted empty-root initialization; initialization removes that regular
file and retries marker publication. A symlink, directory, several marker
temporaries, or any other pre-marker entry remains an error.

Two processes must not initialize or mutate one artifact root concurrently.
Story 03 already limits version 1 to one worker process. The filesystem checks
are safety validation, not a distributed lock.

## Path separation

For `work`, compare the artifact root with each of these configured paths:

- `MRFPIPELINE_WAREHOUSE_PATH`
- `MRFPIPELINE_PROVIDER_CATALOG_PATH`
- `MRFPIPELINE_SERVICES_PATH`

Reject equality or containment in either direction, first using Story 01's
absolute cleaned paths and then using physical paths for entries that exist.
For a missing path, resolve its nearest existing ancestor, append the cleaned
unresolved suffix, and compare the resulting anticipated physical path. Merely
sharing an existing ancestor does not constitute overlap.

This prevents parser cleanup from reaching the warehouse, provider catalog,
or service selector and prevents those inputs from appearing inside a
pipeline-owned record directory. This story does not otherwise validate,
create, open, or mutate those three paths. Their detailed contracts belong to
Stories 10 and 11.

Provider-catalog and warehouse relationships with each other remain governed
by `mrfconsumer`; this story only separates each from the artifact root.

## Filesystem safety rules

All workspace operations follow these rules:

- Accept only the fixed artifact kind plus a validated positive database ID;
  do not accept a caller-supplied relative path.
- Join only fixed path segments generated by this package.
- Verify every managed ancestor from the retained root to the target with
  `Lstat` before reading, creating, renaming, or removing an entry.
- Reject a symlink or non-directory ancestor and never follow it.
- Require downloaded data, download manifests, parser manifests, and
  `plans.json` to be regular non-symlink files when inspected.
- Use exclusive creation for temporary files and directories.
- Keep temporary and final download directories on the same filesystem so
  publication uses one atomic rename.
- Never recursively remove the artifact root, a fixed top-level directory,
  an ID directory, a path supplied directly by a user, or a path selected by
  globbing.
- Recursive removal is permitted only for one exact generated `download` or
  `parsed` leaf beneath an already verified positive-ID directory, or for one
  exact operation-owned temporary directory under `.staging` whose fixed name
  parser yields the requested artifact kind and positive ID.
- Re-verify the target immediately before recursive removal and refuse if its
  identity or type changed.

Version 1 assumes a trusted local operating-system account. These checks
prevent ordinary path mistakes and symlink traversal but are not a security
boundary against another process running as the same account and racing every
filesystem operation.

## Download artifact contract

A published `download/` directory contains exactly two entries:

```text
download/
  data
  manifest.json
```

`data` is a regular file containing the received HTTP response body bytes.
It may be empty after a successful empty response; TOC and MRF parsers will
later reject content that is not valid input.

The manifest is exact JSON:

```json
{"schema_version":"1.0.0","byte_count":123}
```

It is compact JSON plus one newline. `byte_count` is the nonnegative signed
64-bit number of bytes written to `data`. The reader permits insignificant
JSON whitespace but rejects unknown, duplicate, missing, null, fractional,
negative, or overflowing values and requires the exact schema version.

The manifest intentionally does not repeat the source URL, redirect URL,
HTTP headers, URL filename, ETag, Last-Modified value, content type, content
encoding, timestamp, job ID, or a digest. The database already owns the URL
and the directory already owns the stage identity.

A download is reusable only when:

- `download` is a real directory with exactly `data` and `manifest.json`.
- Both entries are regular non-symlink files.
- The manifest is valid and its `byte_count` exactly equals `data` file size.

The byte count detects truncation and inconsistent publication metadata. It
does not prove remote content identity and is not presented as a checksum.

## Download publication protocol

The downloader receives a caller context, a validated HTTP or HTTPS source
URL loaded from PostgreSQL, and one generated TOC or MRF source artifact
address.

It performs:

1. Verify and create the positive-ID directory as needed.
2. Inspect the final `download` directory.
3. If it is complete by the contract above, return its `data` path and byte
   count without issuing an HTTP request.
4. If `download` exists but is incomplete, remove only that exact leaf after
   the filesystem safety checks.
5. Create an exclusive operation directory under root `.staging` whose name
   begins with the exact prefix `toc-download-<id>-` or `mrf-download-<id>-`
   and is followed by an OS-generated random collision suffix. The suffix is
   temporary, not identity. Story 13 and targeted cleanup parse only these
   prefixes.
6. Create `data` inside it with requested mode `0600`.
7. Issue one HTTP request and stream its body directly to `data` through a
   fixed-size copy buffer. During that copy, emit Story 03 throttled
   `progress` logs with phase `download`. When `Content-Length` is known and
   matches a nonnegative total, include integer `percent` and
   `copied_bytes`/`total_bytes`. When length is unknown or chunked, omit
   `percent` and log `copied_bytes` only. A completed download that is reused
   without a new request emits no progress.
8. Close the response body and data file and validate any advertised content
   length.
9. Write and close `manifest.json` with the observed byte count.
10. Re-check that final `download` is absent, then atomically rename the
    complete operation directory to the final path.
11. Return the final `data` path and byte count.

The final directory does not exist during body transfer. A crash before the
rename can leave only a `.staging` entry. A crash after rename but before the
domain success transaction leaves a reusable complete download. The later
worker must inspect it before performing another request.

The implementation does not promise `fsync` durability. It closes data and
metadata before rename and relies on ordinary local filesystem semantics,
matching the sibling parsers' manifest-last durability boundary.

If another final directory appears before rename, validate it. Return the
existing completed artifact and remove this operation's staging directory, or
return an artifact conflict without overwriting an incomplete final. This is
defensive behavior; concurrent worker processes remain unsupported.

## Staging cleanup and recovery

On an ordinary download error or cancellation, close the response and file,
then remove that invocation's exact staging directory. Failure to clean it is
joined with a safe artifact classification; it does not trigger broader
cleanup.

Before starting a download for one record, inspect `.staging` for entries that
exactly match that artifact kind and ID according to the implementation's
fixed name parser. Remove only matching real directories. Reject matching
symlinks or wrong-type entries. Leave staging entries for other IDs alone
because their owners may be active.

General age-based staging collection, disk quotas, and cleanup of artifacts
for deleted domain rows are deferred to Story 13. Version 1 domain rows are
not automatically deleted.

## HTTP request policy

Use one shared `net/http.Client` and transport per worker process. The client:

- Accepts only parsed absolute `http` or `https` URLs with a nonempty host.
- Rejects URL user information, control characters, unsupported schemes, and
  malformed authority or port syntax.
- Allows query strings and fragments in the exact stored URL. The HTTP client
  does not send the fragment, per HTTP semantics.
- Sends `GET`, `User-Agent: mrfpipeline`, and
  `Accept-Encoding: identity` with no other application header.
- Disables Go's transparent response decompression so stored bytes match the
  received representation.
- Uses normal system certificate and hostname verification for HTTPS.
- Does not accept an insecure-TLS option or custom certificate bypass.
- Does not use proxy environment variables in version 1. Direct connections
  are the contract. A deployment that requires a proxy needs an explicit
  networking story before the live run.
- Follows at most five redirects.
- Resolves a relative `Location` against the preceding request URL, then
  revalidates the complete resolved URL under this same policy.
- Permits HTTP to HTTPS and same-scheme redirects, including cross-host CDN
  redirects.
- Rejects an HTTPS-to-HTTP redirect.
- Revalidates the complete redirect target under this same URL and network
  policy before sending it.
- Never forwards credentials or source-derived headers across a redirect;
  version 1 sends none.

Every connection, including redirects, must reject the complete DNS result
when **any** resolved address is loopback, unspecified, link-local, multicast,
or private-use. Revalidate the chosen address again at dial selection. This is
the version 1 server-side request-forgery boundary for payer-provided URLs.
IPv4 and IPv6 are both covered. Mixed public-and-private answers fail without
connecting.

The client makes direct network connections. Corporate HTTP proxies, private
mirrors, authenticated URLs, custom headers, client certificates, and private
address destinations require a later explicit deployment story.

## HTTP timeouts and streaming

There is no whole-request client timeout because an MRF may be very large.
The caller's River context is the overall cancellation mechanism.

Use fixed transport bounds:

| Setting | Value |
|---|---:|
| TCP dial timeout | 30 seconds |
| TCP keepalive | 30 seconds |
| TLS handshake timeout | 10 seconds |
| Response-header timeout | 2 minutes |
| Expect-continue timeout | 1 second |
| Idle connection timeout | 90 seconds |
| Maximum response-header bytes | 1 MiB |
| Maximum idle connections | 8 |
| Maximum idle connections per host | 4 |
| Maximum connections per host | 4 |
| Streaming copy buffer | 256 KiB |

The downloader does not buffer the response body in memory, inspect JSON,
decompress gzip, or calculate a digest while copying. It does not impose a
fixed body-size limit because production MRF size is not yet bounded. Disk
capacity planning and quotas belong to Story 13.

Only HTTP status `200 OK` is accepted, including an empty body. Redirects are
handled by the client before this check. Empty `200` is a completed download;
TOC and MRF parsers later reject invalid or empty content. `206`, other `2xx`,
`3xx` after the redirect limit, `4xx`, and `5xx` are failures. The downloader
closes the body without reading or logging it.

When a valid nonnegative `Content-Length` is present, the observed body byte
count must match it exactly. Chunked or otherwise unknown-length bodies succeed
on a clean EOF. An early EOF, copy failure, close failure, length mismatch,
context cancellation, or manifest/publication failure never creates a completed
final download.

The downloader makes one request sequence per invocation. It has no internal
status-code retry, connection retry loop, or backoff. River owns whole-stage
retries so retry budgets do not multiply invisibly.

## Parser-output inspection and reset

Provide a narrow helper for a generated TOC or MRF source `parsed` directory.
It reports one of four structural states:

```text
absent
empty
incomplete
manifest_present
```

- `absent`: the path does not exist.
- `empty`: it is a real directory with no entries.
- `incomplete`: it is a nonempty real directory and has no `manifest.json`.
- `manifest_present`: `manifest.json` is a regular non-symlink file.

A symlink, wrong-type directory, or nonregular `manifest.json` is an artifact
error, not a state. This story does not validate a sibling tool's manifest
schema or output datasets; Stories 07 and 10 do that before treating
`manifest_present` as a successful parse.

The reset helper:

- Creates an absent `parsed` path as an empty real directory.
- Reuses an already empty directory.
- Removes and recreates an `incomplete` directory.
- Refuses to remove or modify a `manifest_present` directory.

Removal targets only the exact generated `parsed` leaf and uses the
filesystem safety rules above. This implements the operator's desired
"clean partial output, preserve completed output" behavior without asking a
parser to append, resume, overwrite, or repair.

The helper does not inspect or clean the consumer warehouse. Consumer
publication and seed-recovery behavior remain entirely owned by
`mrfconsumer` and Stories 11 and 12.

## Source cleanup primitive

Provide an idempotent removal operation for one exact completed or incomplete
`download` leaf. It refuses symlinks and malformed record paths and succeeds
when the leaf is already absent.

Later parse workers call it only after the sibling parser's final manifest has
been validated and the parse success transaction can safely proceed. A parse
failure or cancellation keeps the completed download for retry.

This story does not automatically remove anything after a test download.
Later retention decisions are:

- TOC and MRF download bytes may be deleted after validated parse success.
- Parsed MRF output must remain available because a later TOC can add another
  feed/month snapshot or more plans for the same shared source.
- TOC parser output and plan-batch JSON retention are decided by their owning
  worker and Story 13 recovery requirements.

## Error classification and redaction

Introduce:

```go
var ErrArtifact = errors.New("artifact operation failed")
var ErrDownload = errors.New("download failed")
```

Path validation, workspace recognition, filesystem inspection, creation,
close, rename, cleanup, and manifest failures wrap `ErrArtifact`.

URL-policy, DNS, dial, TLS, redirect, timeout, HTTP-status, response-copy,
content-length, and body-close failures wrap `ErrDownload`. A failure may
match both when a download fails and its artifact cleanup also fails.
Context cancellation remains discoverable as the context error.

Production error strings and logs never contain:

- The source or redirect URL, scheme authority, host, path, query, fragment,
  or basename.
- The configured artifact root or any derived absolute path.
- DNS names or resolved IP addresses.
- HTTP request or response headers.
- Response-body bytes or snippets.
- Operating-system paths returned by filesystem errors.
- TLS peer details or raw network diagnostics.

Safe diagnostics may contain only a fixed operation name and classification.
HTTP status may be mapped to a bounded class such as `http_4xx` or
`http_5xx`; do not persist the status text or response body. Numeric byte
counts may appear in the Story 03 throttled `progress` logs and in successful
internal results. They still must not appear in error strings.

No story-specific CLI success output or exit status is added because no
production command exposes this component yet.

## Required tests

### Unit tests

- Positive database IDs format to the exact TOC, MRF source, and plan-batch
  directories; invalid IDs cannot produce a path.
- Paths contain no URL, payer, filename, timestamp, UUID, or derived digest.
- Workspace and download manifests accept only their exact schema and reject
  duplicate, unknown, missing, wrong-type, and out-of-range values.
- Complete-download inspection checks exact entries, regular-file types, and
  byte-count equality.
- Parser-output inspection and reset implement all four states and never
  remove a manifest-present output.
- Errors remain classifiable and redact hostile URL, header, IP, path, TLS,
  and response-body fixtures.
- Context cancellation remains distinguishable from artifact and download
  classifications.
- Body-copy progress is throttled, omits percent when length is unknown, and
  never logs URLs, hosts, or paths.

### Filesystem integration tests

Use a fresh test temporary directory and prove:

- An absent or empty root initializes with the exact marker and fixed
  directories, and repeated initialization is a no-op.
- Nonempty unmarked, unsupported-version, extra-entry including `.DS_Store`,
  symlinked, and wrong-type roots fail without deletion.
- Missing fixed directories in a recognized root are repaired, while
  wrong-type replacements are rejected.
- Lexical and resolved overlap with warehouse, provider catalog, or services
  is rejected in both containment directions.
- Managed-path symlinks and symlink swaps are rejected before read, rename,
  or recursive cleanup.
- Download publication leaves no final directory until its complete atomic
  rename.
- A complete final download is reused without invoking the test HTTP server.
- A truncated or malformed final download is removed only from its generated
  leaf and downloaded again.
- Injected write, close, manifest, rename, cancellation, and cleanup failures
  never publish a complete final artifact.
- Reset removes incomplete parser output, preserves manifest-present output,
  and never touches sibling record directories.

### HTTP integration tests

Use `httptest` plus controlled dial hooks; do not access the public internet.
Prove:

- Raw JSON, gzip-looking bytes, an empty body, chunked transfer, and a large
  streamed body are stored byte-for-byte with the correct byte count.
- The implementation's peak buffer use does not grow with response size.
- Transparent compression is disabled and the requested headers are exact.
- Five valid redirects succeed and a sixth fails.
- HTTPS downgrade, unsupported scheme, user information, malformed target,
  and a redirect to a rejected network address fail before the unsafe request.
- Loopback, unspecified, link-local, multicast, private IPv4, private IPv6,
  and mixed public-and-private DNS answers are rejected for the complete
  resolution.
- Relative redirects are resolved against the preceding URL and then
  revalidated.
- Only status 200 succeeds; failure bodies are neither copied to the final
  artifact nor included in errors.
- Known content length must match; unknown length succeeds; an early EOF or
  other observable length mismatch fails.
- Header timeout and caller cancellation close active work and leave no final
  artifact.
- One downloader invocation does not perform an implicit retry after a server
  or transport failure.

Run:

```text
go test ./...
go test -race ./...
go vet ./...
```

## Acceptance criteria

- A recognized local artifact root has one exact versioned layout based only
  on positive application database IDs.
- Workspace initialization fails closed on unrecognized content, overlap,
  wrong types, and symlinks.
- HTTP response bodies stream to disk with fixed memory and no transparent
  decoding or content hashing.
- A final download appears atomically only with valid byte-count metadata and
  is reusable after a crash before database success.
- Partial downloads and parser outputs can be cleaned only at exact generated
  leaves; completed parser outputs are preserved.
- URL and network policy prevents local/private destination access and unsafe
  redirects.
- Cancellation, timeout, network, HTTP, disk, and publication failures are
  classifiable and redacted.
- No production job, domain transition, parser execution, Parquet read, or
  warehouse write occurs in this story.

## Non-goals

- UHC discovery or production River workers.
- TOC/MRF domain-stage scheduling or transitions.
- Range requests, resumable transfer, mirrors, conditional GET, or cache
  revalidation.
- Source authentication, custom headers, proxies, private endpoints, or TLS
  customization.
- S3, object storage, distributed filesystems, or multiple worker hosts.
- Decompressing or validating TOC/MRF JSON.
- Invoking or embedding `mrftocparser`, `mrfparser`, `mrfconsumer`, or
  `mrfenricher`.
- Validating parser manifests or Parquet datasets.
- Creating plan JSON or touching the consumer warehouse.
- Disk quotas, free-space prediction, artifact age retention, or general
  garbage collection.
- Content hashes, checksums, ETags, URL normalization, or filename-based
  identity.
