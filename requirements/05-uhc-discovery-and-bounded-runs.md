# Story 05: UHC discovery and bounded runs

## Status

Planned.

## User story

As a pipeline operator, I want to enqueue a UHC discovery and optionally admit
only a few new TOCs so that I can start the background pipeline with a bounded
real-data test instead of processing the full payer listing at once.

## Goal

Replace the `discover` placeholder with durable discovery-run creation and
River insertion, implement the `discovery.run` worker using `mrfdiscoverer`,
and replace the `work` placeholder with the first production River process.

The worker stores exact TOC URLs, records run membership and counters, and
schedules one `toc.download` job for each newly admitted TOC. Story 06 later
executes those queued download jobs.

This story does not download a TOC, parse any file, infer an MRF feed, read
Parquet, or call `mrfparser` or `mrfconsumer`.

## Dependencies and scope

This story depends on Stories 01 through 04.

Those stories remain authoritative for:

- CLI syntax and discovery flag validation.
- PostgreSQL schema, URL identity, counters, and lifecycle constraints.
- River schemas, typed jobs, transactional insertion, attempts, and shutdown.
- Worker local-path validation and workspace initialization.

This story adds one sibling module dependency:

```text
github.com/EnotPoloskun/mrfdiscoverer
```

Pin the dependency in `go.mod`; do not commit a workspace-relative `replace`
directive. The root package API is the integration boundary:

```go
mrfdiscoverer.Discover(ctx, mrfdiscoverer.Config{Payer: "uhc"})
```

Do not execute the `mrfdiscoverer` CLI, parse its line output, copy its UHC
listing logic, or add a second payer HTTP implementation to this repository.

## Product decisions

- `discover` enqueues work and returns immediately. It never performs the live
  UHC request in the CLI process.
- One discovery run represents one payer, one caller-selected collection
  month, and one optional admission limit.
- The optional limit applies only to exact URLs that would create new
  `toc_files` rows. Known TOCs never consume the limit.
- Newly encountered URLs beyond the limit are not inserted. A later bounded
  discovery can admit them.
- Returned URLs are compared exactly. No normalization, filename comparison,
  or hash is used.
- `mrfdiscoverer` listing order decides which new URLs are admitted first.
- Duplicate exact URLs returned within one listing are collapsed for domain
  processing at their first ordinal. The raw occurrence count remains visible
  in `discovered_count`.
- A successfully admitted TOC receives its first download job in the same
  transaction as its row.
- Existing TOCs are observed again but do not receive duplicate download jobs.
- Discovery is manual or externally scheduled. There is no periodic River job.

## `discover` command behavior

For:

```text
mrfpipeline discover \
  --payer uhc \
  --collection-month 2026-08 \
  --limit 5
```

after Story 01 argument/configuration validation, the command:

1. Opens a small bounded pgx pool.
2. Connects and validates current application and River schemas without
   migrating.
3. Constructs an insertion-only River client for schema
   `mrfpipeline_river`; it has no queue consumers and is not started.
4. Begins one PostgreSQL transaction.
5. Inserts a `discovery_runs` row with exact payer, first-of-month date,
   optional limit, and `pending` status.
6. Inserts one `discovery.run` job with only `discovery_run_id` through
   `InsertTx`.
7. Stores the returned numeric River job ID on the run.
8. Commits and reports the durable enqueue result.

No live payer request, artifact-root initialization, filesystem access, or
other domain insertion occurs in this command.

If job insertion or commit fails, no successful run is reported. A rollback
removes both the new domain row and River job. The command does not search
River arguments to infer whether an insert happened.

Every valid invocation creates a new discovery run, even when payer, month,
and limit equal an earlier invocation. Runs are operator requests and are not
deduplicated.

## Discovery enqueue result

On success, standard output is one compact JSON object plus newline:

```json
{"discovery_run_id":41,"river_job_id":9001,"payer_id":"uhc","collection_month":"2026-08","toc_limit":5}
```

When `--limit` is omitted, `toc_limit` is JSON `null`. The field order is
exactly:

1. `discovery_run_id`
2. `river_job_id`
3. `payer_id`
4. `collection_month`
5. `toc_limit`

The report means only that the run and job committed. It does not include
discovery counts, URLs, artifact paths, queue position, attempts, or an
estimated completion time.

If result writing fails after commit, exit `1`; do not cancel the committed
job or create another run.

## First production `work` behavior

Story 05 replaces the `work` placeholder. After full Story 01 configuration
validation, `work`:

1. Opens the production pgx pool.
2. Validates current application and River migrations without applying them.
3. Initializes and validates the Story 04 workspace and path separation.
4. Registers the `discovery.run` worker.
5. Starts a River client consuming only queue `discovery` with maximum one
   worker.
6. Waits until context cancellation or a fatal River runtime failure.
7. Performs Story 03 graceful then canceling shutdown.
8. Closes the pool and returns the classified outcome.

The remaining production job kinds are already durable contracts, and the
discovery worker may insert `toc.download` jobs. This Story 05 worker client
does not consume `toc_download` yet. Those jobs remain pending in River until
Story 06 registers that worker and queue.

Do not register no-op workers for unimplemented stages. A no-op could complete
a durable job without completing its domain stage.

## Discovery worker claim

`discovery.run` uses the common Story 03 wrapper. Its argument is exact JSON:

```json
{"discovery_run_id":41}
```

In its claim transaction, it locks `mrfpipeline.discovery_runs` and:

- returns a successful no-op for `succeeded`;
- returns a stale no-op when `river_job_id` differs from the executing job;
- returns a terminal no-op for `failed`;
- rejects a missing row or invalid state through the shared safe failure
  classification; or
- changes assigned `pending`/same-job `running` to `running`, sets
  `started_at` only when still null, clears no counters, and updates
  `updated_at`.

A retry of the same job preserves the original `started_at`. Counts remain at
their prior committed values; ordinary execution writes final counts only in
the success transaction.

## Calling `mrfdiscoverer`

After claim commit, call:

```go
mrfdiscoverer.Discover(ctx, mrfdiscoverer.Config{Payer: run.PayerID})
```

The call uses the River job context. A canceled/deadline context remains the
context error. `mrfdiscoverer.ErrListing` is retryable until the last attempt.
An unexpected `mrfdiscoverer.ErrInvalidConfig` for a stored supported payer is
a domain invariant failure.

The package returns `[]mrfdiscoverer.TOCFile` in payer listing order. Before
opening the success transaction, validate each returned URL again against the
Story 02 storage boundary:

- nonempty valid UTF-8;
- no NUL, carriage return, or newline; and
- representable as PostgreSQL text.

Do not apply Story 04 HTTP destination policy here; discovery stores exact
URLs, while the download worker validates whether each may be requested.

One invalid returned TOC URL fails the complete discovery attempt. Do not
publish a partial listing or skip the invalid occurrence.

## Listing occurrence and first-ordinal rules

Let the package return `N` entries. Set `discovered_count = N`, including
duplicate exact URL occurrences.

For domain processing, create an in-memory first-occurrence sequence by exact
Go string equality:

1. Walk package results in order with zero-based ordinal.
2. Keep the first occurrence of each exact URL and its original ordinal.
3. Ignore later occurrences of that same exact string for database membership,
   existing count, admission, and last-seen update.

This sequence is only per-run deduplication. It is not durable identity and is
not calculated with a digest.

`existing_count` is the number of distinct first-occurrence URLs that resolve
to an already existing `(payer_id, source_url)` row at the point this run
processes them. `admitted_count` is the number of new `toc_files` rows actually
inserted by this run.

Consequently the three counters need not sum:

```text
discovered_count != existing_count + admitted_count
```

Duplicate occurrences and new URLs beyond the admission limit appear only in
`discovered_count`. This is intentional and compatible with the Story 02
constraints.

## Atomic admission transaction

After a complete successful listing call and validation, use one database
transaction for run finalization, TOC admission, membership, and job insertion.

1. Lock the discovery run and verify the same River job remains assigned and
   the run is `running`.
2. Iterate the distinct first-occurrence sequence in listing order.
3. For each URL, attempt to select or insert exact `(payer_id, source_url)`
   using uniqueness as the race arbiter.
4. When the row already exists:
   - preserve its original `collection_month` and
     `first_discovery_run_id`;
   - set `last_seen_at` and `updated_at` to
     `transaction_timestamp()`;
   - insert this run's membership at the first ordinal with `was_new=false`;
   - increment `existing_count`; and
   - do not change any stage or River job ID.
5. When the row does not exist and the optional limit is already exhausted:
   do not insert a TOC, membership, or job and continue scanning.
6. When the row does not exist and admission remains available:
   - insert it with this run's payer, month, first-run reference, pending
     download, and blocked successor stages;
   - insert membership at the first ordinal with `was_new=true`;
   - insert `toc.download` through River `InsertTx`;
   - store its job ID in `download_river_job_id`; and
   - increment `admitted_count` only after the row insertion wins.
7. Set the run counts, `status=succeeded`, `completed_at` and `updated_at` to
   transaction time, leave `failure_code` null, and commit.

A concurrent uniqueness conflict means the URL is existing for this run. It
does not consume an admission slot, and the iteration continues so a later new
URL can be admitted. Although version 1 normally has one discovery worker,
this rule makes transaction retries and defensive concurrency deterministic.

Known URLs are always recorded regardless of the admission limit. A limit of
five means "insert at most five new TOCs," not "process only the first five
listing entries."

## Bounded-run progression example

Suppose the distinct listing order is:

```text
known-1, new-A, new-B, new-C, new-D
```

and the run has `--limit 2`.

The result is:

- `known-1`: membership, `was_new=false`.
- `new-A`: inserted, membership, download job.
- `new-B`: inserted, membership, download job.
- `new-C`, `new-D`: not inserted and no membership.

A later run with the same limit observes `known-1`, `new-A`, and `new-B`, then
admits `new-C` and `new-D`. The limit therefore supports deterministic staged
test expansion without permanently suppressing overflow candidates.

If the listing contains `new-A` twice, only its first ordinal becomes
membership. `discovered_count` still counts both occurrences.

## Retry and failure behavior

No TOC rows or counts commit until the complete listing succeeds. If the payer
call or URL validation fails, apply the Story 03 retry transition to the run
and return the safe error to River.

If the admission transaction fails, roll it back completely. A retry may call
the live current listing again; the pipeline does not cache listing responses
as artifacts in version 1.

Fixed terminal failure codes are:

```text
discovery_listing_failed
discovery_result_invalid
discovery_database_failed
```

Use the most specific fixed code on the eighth/final attempt. Raw
`mrfdiscoverer` errors, URLs, response bodies, and database diagnostics are not
stored.

A final failed run has its terminal count values left at the last committed
values, normally zero, sets `completed_at`, and does not insert any TOC from
the failed attempt.

If a crash occurs after the admission transaction commits, the run and all
new TOC download jobs already succeeded atomically. River retry sees the run
as `succeeded` and becomes a no-op.

## CLI exit behavior

`discover` extends the existing mapping:

| Outcome | Exit status | Usage hint |
|---|---:|:---:|
| Durable enqueue success | `0` | No |
| Invalid CLI/configuration | `2` | Yes |
| Database/schema/transaction failure | `3` | No |
| River construction/insertion failure | `4` | No |
| Cancellation or result-write failure | `1` | No |

`work` returns `0` only after an orderly requested shutdown. Startup database
failure exits `3`; River runtime/start/stop failure exits `4`; invalid
configuration exits `2`; cancellation during startup or an unexpected local
runtime failure exits `1` unless it matches the more specific safe class.

## Logging and redaction

The CLI enqueue report is the only place this story emits application domain
IDs or collection month. Worker logs follow Story 03 and do not emit run IDs,
TOC IDs, counts labeled by payer data, or URLs.

Do not log:

- Any URL returned by `mrfdiscoverer`.
- The UHC listing body or response headers.
- A database URL or local configured path.
- Raw sibling-package or PostgreSQL errors.

Safe discovery lifecycle events may say that a listing started, admission
committed, an attempt will retry, or the run reached terminal failure, with
only fixed event/job fields allowed by Story 03.

## Required tests

### Unit tests

- `discover` produces the exact insertion-only transaction inputs and compact
  report, including `toc_limit:null` when absent.
- Output failure after commit does not insert a second run or cancel the job.
- First-occurrence processing preserves ordinal and exact URL equality.
- Raw discovered count includes duplicates while existing/admitted counts use
  distinct first occurrences.
- Limit selection skips known URLs and admits the first actual new rows.
- Invalid returned URLs fail the complete attempt without exposure.
- `mrfdiscoverer` error and cancellation mapping is exact and redacted.
- Production `work` registers only the implemented discovery worker/queue.

### PostgreSQL and River integration tests

Using the disposable database safeguards from Story 02, prove:

- Enqueue commits one run and one River job together and stores the returned
  job ID.
- Injected insert/commit failure rolls back both sides.
- Every invocation creates a distinct run even for identical arguments.
- A private discovery adapter result creates exact TOC rows, membership,
  counters, and pending `toc.download` jobs in one commit.
- A second identical run creates no new TOC or download jobs, updates
  last-seen values, and records `was_new=false` membership.
- A limit counts only inserts, known rows do not consume it, overflow rows are
  absent, and a later run admits the next new rows.
- Duplicate listing occurrences use the first ordinal and create one
  membership/job.
- Two defensive concurrent admissions resolve uniqueness without duplicate
  TOCs/jobs and without wasting the winner's limit slot.
- Retry after pre-commit failure publishes no partial counters, membership, or
  jobs.
- Retry after committed success is a no-op.
- Missing, stale, succeeded, failed, and same-job-running claims follow Story
  03.
- Final attempt maps to the fixed safe failure code.

The discovery function used in integration tests is a narrow injected
function with the same input/output shape as `mrfdiscoverer.Discover`. Do not
introduce a public payer plugin or generic adapter framework for testing.

### Worker integration test

Start the real River runtime against a controlled discovery function and prove
that:

- `discovery.run` is consumed.
- Generated `toc.download` jobs remain pending because Story 05 does not
  consume their queue.
- Graceful shutdown stops new discovery fetches and honors cancellation.
- No artifact or warehouse path is modified beyond Story 04 workspace
  initialization.

Run:

```text
go test ./...
go test -race ./...
go vet ./...
```

## Acceptance criteria

- `discover` atomically creates a durable run and `discovery.run` job and
  returns its safe enqueue report.
- `work` runs the first production River worker with one discovery executor.
- UHC discovery uses the `mrfdiscoverer` package rather than copied listing
  logic.
- Exact URLs and first listing ordinals are stored without normalization.
- `--limit` admits only the first requested number of actually new TOCs and a
  later run can admit prior overflow.
- Each newly admitted TOC receives exactly one pending download job.
- Repeated, duplicate, failed, canceled, and stale executions are idempotent
  and redacted.
- No TOC content is downloaded or parsed in this story.

## Non-goals

- A payer other than UHC.
- Internal recurring scheduling or cron configuration.
- Caching or storing payer listing JSON.
- TOC HTTP download or artifact creation.
- TOC parsing, Parquet import, MRF creation, or feed assignment.
- Automatically retrying a terminally failed discovery through a CLI command.
- Dynamic queue concurrency or multiple worker processes.
- URL normalization, filename identity, River uniqueness, UUIDs, or hashes.
