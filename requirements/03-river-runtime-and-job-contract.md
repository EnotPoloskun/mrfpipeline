# Story 03: River runtime and job contract

## Status

Planned.

## User story

As a pipeline operator, I want durable background jobs with explicit queue,
retry, and transaction rules so that every later pipeline stage can recover
from process crashes without creating duplicate domain work.

## Goal

Add River to the PostgreSQL-backed application, apply River's schema through
the explicit `mrfpipeline migrate` command, and establish the common job
contract used by discovery, TOC, MRF, and consumer workers.

This story creates the runtime foundation and tests it with private test
workers. It does not implement a production pipeline worker, enqueue a real
discovery, contact a payer, download a file, invoke a parser or consumer, or
write an artifact.

## Dependencies and scope

This story depends on Stories 01 and 02.

Story 01 remains authoritative for the command syntax, configuration,
cancellation, streams, and lexical redaction rules. Story 02 remains
authoritative for application migrations, domain identities, statuses,
uniqueness, and database error classification.

This story:

- Pins River and its pgx v5 driver.
- Adds and validates River's own PostgreSQL schema.
- Extends `mrfpipeline migrate` to apply both application and River
  migrations under the existing pipeline migration lock.
- Defines all version 1 production job kinds, arguments, queues, and default
  runtime policy before their workers are added.
- Defines the transaction and idempotency protocol later workers must follow.
- Provides reusable River client construction and safe observability.

`mrfpipeline work` and `mrfpipeline discover` still end with their private
not-implemented errors after their existing validation. A production worker
process is not started until Story 05 provides the first production worker.

## Product decisions

- River is the only version 1 background-job system.
- PostgreSQL remains the shared durable store for both River and domain
  state. There is no Redis, message broker, filesystem queue, or in-memory
  fallback.
- River provides at-least-once execution. Every later worker must therefore
  be safe when invoked more than once.
- Domain rows, not retained River rows, remain the source of truth for
  pipeline progress and future UI state.
- Job arguments contain only one numeric application database identity.
  They never contain source URLs, local paths, plan documents, taxonomy or
  service selections, database credentials, or serialized domain records.
- Application uniqueness constraints decide whether domain work is new.
  Do not use River unique-job options, uniqueness periods, or encoded-argument
  hashes as application identity or deduplication.
- A stage state change and insertion of its River job occur in the same
  PostgreSQL transaction with `InsertTx`.
- Version 1 runs exactly one `mrfpipeline work` process per database and
  warehouse. This preserves single-writer consumer behavior. Horizontal
  worker scaling and distributed warehouse locks are deferred.
- Queue concurrency bounds resource use; they are not correctness locks.
- No recurring discovery schedule is added. Discovery remains an explicit
  CLI operation or an external scheduler responsibility.

## Dependency baseline

Add these exact module dependencies:

```text
github.com/riverqueue/river v0.39.0
github.com/riverqueue/river/riverdriver/riverpgxv5 v0.39.0
```

Continue using:

```text
github.com/jackc/pgx/v5 v5.9.2
```

Use River's `rivermigrate` package and its `riverdriver/riverpgxv5`
subpackage. Do not shell out to the River CLI, vendor River SQL into the
application migration directory, or manage the River tables with a second
migration library.

## Database schemas and migration ownership

Application tables remain in:

```text
mrfpipeline
```

River-managed tables live in the separate fixed schema:

```text
mrfpipeline_river
```

Every migrator, client, insertion, fetch, maintenance service, and test uses
that exact River schema. River tables are not created in `public` or mixed
with the application migration ledger.

River owns the contents and migration history of `mrfpipeline_river`.
Application SQL must not create, alter, index, truncate, delete from, or add a
foreign key to a River table. The Story 02 `*_river_job_id` columns remain
plain nullable `bigint` audit references with no foreign key because River is
allowed to retain and eventually delete job rows independently.

## Extended migration command

`mrfpipeline migrate` performs these steps while holding the single fixed
pipeline advisory lock introduced by Story 02:

1. Complete Story 02 configuration, connection, PostgreSQL-version, and
   application-ledger validation.
2. Apply pending application migrations in ascending order.
3. Construct River's migrator with the pgx v5 driver and schema
   `mrfpipeline_river`.
4. Apply all pending River `main` migration lines in the forward direction.
5. Query and validate the resulting current River version.
6. Release the application lock, close the pool, and report success.

The lock covers both migration systems. Two `mrfpipeline migrate` processes
therefore cannot interleave application and River setup. River may use its own
internal locking as well; that does not replace the pipeline lock.

Application migrations commit according to Story 02 before River migrations
begin. If River migration fails, the already committed application schema is
not rolled back or declared dirty. A later `migrate` invocation validates the
application ledger and resumes River migration. The command does not attempt
a cross-system rollback.

Do not run down migrations. Do not adopt an existing River schema whose
migration state the selected River version does not recognize. Missing,
newer-than-supported, partially applied, or otherwise invalid River migration
state matches `ErrDatabase` and is never repaired destructively.

On success, standard output remains one compact JSON object and newline. The
Story 02 fields remain stable and two River fields are added:

```json
{"application_version":1,"applied_migration_count":0,"river_version":6,"applied_river_migration_count":0}
```

The pgx v5 River driver at 0.39.0 has `main` migration line version `6`.
`applied_river_migration_count` is the number applied by this invocation.
Repeating the current migration reports zero for both applied counts. A later
River dependency upgrade may change `river_version` only as part of its
explicit dependency and migration story.

The output does not include migration SQL, schema table names, database
coordinates, advisory-lock values, or River diagnostic details.

## Startup schema validation

Reusable runtime construction accepts the already validated application
configuration and a `pgxpool.Pool`. Before a command may insert or execute a
River job, it verifies:

- Application migration state is exactly current for the binary.
- River migration state is exactly current for the pinned River binary.
- Both fixed schemas are accessible through the configured database role.

Commands do not automatically migrate. A missing or stale schema returns a
redacted `ErrDatabase` diagnostic instructing the operator to run
`mrfpipeline migrate`; it must not try to repair schema state while starting
workers or enqueueing discovery.

This validation is read-only and uses the caller context. It does not lock
all job traffic or hold the migration advisory lock for the lifetime of a
worker.

## Production job catalog

Define one typed `river.JobArgs` value for each version 1 job kind. `Kind()`
returns the exact value in the table.

| Job kind | Queue | Exact argument | Domain stage owned |
|---|---|---|---|
| `discovery.run` | `discovery` | `discovery_run_id` | One discovery run. |
| `toc.download` | `toc_download` | `toc_file_id` | One TOC download. |
| `toc.parse` | `toc_parse` | `toc_file_id` | One TOC parse. |
| `toc.import` | `toc_import` | `toc_file_id` | One TOC association import. |
| `mrf.download` | `mrf_download` | `mrf_source_id` | One shared MRF download. |
| `mrf.parse` | `mrf_parse` | `mrf_source_id` | One shared plan-independent MRF parse. |
| `consumer.ingest` | `consumer` | `mrf_snapshot_id` | One consumer snapshot ingest. |
| `consumer.attach_plans` | `consumer` | `plan_attachment_batch_id` | One additive plan batch. |

Each JSON argument object has exactly one snake-case field whose value is a
positive signed 64-bit integer. Unknown fields are rejected when decoding.
Zero, negative, fractional, string, null, overflowing, missing, and duplicate
fields are invalid job arguments.

Job argument structs do not expose optional fields for convenience. A worker
loads every mutable value and relationship from PostgreSQL using the supplied
ID. It does not trust a payer, month, URL, output ID, path, or plan list copied
into a job payload.

The exact job kinds and argument field names are durable data contracts.
Changing their meaning requires an explicit compatibility story for already
queued jobs.

## Queue topology and concurrency

The version 1 worker client declares these exact queues and maximum workers:

| Queue | Maximum workers | Reason |
|---|---:|---|
| `discovery` | 1 | Avoid concurrent payer listing pressure. |
| `toc_download` | 4 | TOCs are relatively small network transfers. |
| `toc_parse` | 2 | Bound parser invocation and local-I/O load. |
| `toc_import` | 2 | Bound Parquet reads and database writes. |
| `mrf_download` | 2 | Bound bandwidth and disk growth for large files. |
| `mrf_parse` | 1 | Bound the heaviest CPU, memory, and disk stage. |
| `consumer` | 1 | Serialize all writes to the single configured warehouse. |

These values are fixed for version 1 and are not environment variables or
CLI flags. A later performance story may make them configurable after
measuring real workloads.

The `consumer` queue deliberately contains both ingest and plan attachment.
No two consumer operations may execute concurrently in the one supported
worker process.

Running a second worker process would multiply every bound and could allow
simultaneous warehouse writes. Version 1 deployment instructions must call
that unsupported. Detecting or leasing the only allowed worker process is an
operational/recovery concern for Story 13; queue bounds alone do not provide a
cluster-wide singleton.

## River client policy

Create the production River client with these fixed policies:

- The shared pgx v5 pool and schema `mrfpipeline_river`.
- The exact queue map above once the corresponding production workers are
  registered.
- Eight maximum attempts per job, including the first attempt.
- River's exponential retry policy with jitter; do not busy-loop or add a
  second application retry loop around a worker.
- No default execution timeout for long downloads, parsers, or consumers.
  Workers must honor the River context and terminate child processes on
  cancellation.
- Rescue running jobs considered stuck after 24 hours. At-least-once recovery
  may then overlap a process that is alive but no longer updating River; the
  domain claim and artifact publication rules remain the correctness guard.
- River's default fetch cooldown and polling behavior.
- No periodic jobs, scheduled discovery, custom leader election, job snoozing,
  job tags, or job metadata in version 1.

Keep completed River jobs for River's default completed-job retention and
discarded jobs for River's default discarded-job retention. No feature may
depend on a River row remaining present after its domain stage becomes
terminal.

The 24-hour stuck threshold is an initial operational choice, not a maximum
file-processing duration. A healthy job may run longer because its worker has
no timeout. Story 13 must test and document reconciliation of the rare stale
claim or overlapping rescue case.

## Transactional enqueue contract

All production job insertion goes through a small application-owned enqueue
adapter around River `InsertTx`. The adapter requires a caller-owned
`pgx.Tx`; it never opens and commits an independent transaction for a domain
transition.

For an initial or successor stage, the caller transaction must:

1. Lock the relevant application row.
2. Re-read the stage state and existing `*_river_job_id`.
3. Return the existing job ID without inserting when that stage is already
   pending or running with a job ID.
4. Return a completed no-op when the stage already succeeded.
5. Reject a terminally failed stage; automatic replacement is not part of
   normal scheduling.
6. Insert the typed River job through `InsertTx` when the stage is eligible
   and has no job ID.
7. Store the returned River job ID and set the stage to `pending` in the same
   transaction.
8. Commit the complete domain mutation and River insertion together.

If insertion or commit fails, neither the new job ID nor the domain
transition is considered published. Retry the complete transaction. Do not
query River by serialized arguments and infer success.

A successfully inserted River job may become visible only when its enclosing
transaction commits. This rule applies to:

- `discover` creating a discovery run and its first job in Story 05.
- A worker publishing its successor stage in Stories 05 through 12.
- A later reconciliation operation replacing a genuinely missing job.

Do not hold this transaction open during discovery HTTP calls, downloads,
filesystem work, Parquet reads, parser or consumer execution, or child-process
shutdown.

## Deduplication and job IDs

The database uniqueness and row-lock rules above are the only application
deduplication mechanism. In particular:

- Do not set `UniqueOpts` on any River job.
- Do not calculate a digest of job arguments, URLs, files, plans, or paths.
- Do not derive a River job ID from a domain ID.
- Do not treat a retained River job with equivalent arguments as proof that
  the domain row was scheduled.
- Do not create a second pending job merely because a completed River row was
  pruned.

River's generated numeric job ID is stored in the matching application row
only to identify the currently assigned attempt series and reject stale
deliveries. It is not the domain identity.

## Common worker lifecycle protocol

Stories 05 through 12 must use one common worker wrapper or equivalent shared
logic that implements this lifecycle.

### Claim

At job start, in a short database transaction:

1. Validate the positive domain ID from the typed argument.
2. Lock the named domain row.
3. If the row does not exist, return a permanent safe job error; do not infer
   or create it.
4. If the stage is `succeeded`, complete the River job as an idempotent no-op.
5. If the stored `*_river_job_id` differs from the executing River job ID,
   complete it as a stale no-op without changing the row.
6. If the stage is `failed`, complete it as a stale/terminal no-op; ordinary
   job execution does not reopen terminal domain state.
7. If the stage is `pending` or is `running` for this same River job, set or
   retain `running`, set `started_at` when that table has it, update
   `updated_at`, and commit the claim.

`blocked` is never claimable. Receiving a job for a blocked stage is an
application invariant failure and is not worked around by changing its
predecessor state.

### External work

After the claim commits, perform the stage-specific network, filesystem,
parser, consumer, or database batch work without retaining the claim
transaction. The worker uses the River context for cancellation.

Stage-specific artifact rules make repeated external execution safe. This
story does not define those artifacts, but later workers may not assume the
claim alone proves that an earlier process did no work before crashing.

### Success and successor publication

After external work is durably complete, use a new short transaction to:

1. Lock and re-read the row.
2. Verify the executing River job ID is still assigned and the stage is
   `running` or already `succeeded`.
3. Persist stage-specific result metadata and set the stage to `succeeded`.
4. Clear its safe `failure_code` when relevant, set terminal timestamps, and
   update `updated_at` as defined by Story 02.
5. Unblock the immediate successor, insert its River job with `InsertTx`, and
   store that job ID when a successor is now known.
6. Commit the stage success and successor publication atomically.

If the row already records this stage's success, return an idempotent no-op
and ensure only the separately specified reconciliation path may fill a
missing successor job. A normal retry must not create an unbounded series of
successor jobs.

### Retryable failure

Before returning a retryable error to River, use a short transaction to put a
still-assigned `running` stage back to `pending` and update `updated_at`.
Leave `failure_code` null because Story 02 reserves it for terminal safe
classification. If this bookkeeping update fails, return the original safe
worker classification joined or wrapped with `ErrDatabase`; the next retry
must tolerate the remaining `running` claim.

Never persist raw error text, a URL, path, plan value, response body, SQL
diagnostic, or tool output in a domain table.

### Final failure and panic

On the eighth attempt, a worker that cannot complete marks its still-assigned
domain stage `failed`, records a fixed stage-specific `failure_code`, sets the
applicable completion timestamp, and returns its redacted error so River
discards the job.

A shared River error/panic handler must attempt the same terminal transition
when River is discarding an unexpected error or panic. Failure to update the
domain row is logged safely and left for Story 13 reconciliation; it does not
delete or mutate the River job manually.

The handler uses the registered job kind and its one numeric argument to
select the exact table and stage column. Unknown kinds or undecodable
arguments cannot trigger a broad database update.

## Error classification

Introduce:

```go
var ErrJob = errors.New("background job operation failed")
```

River client construction, insert, start, stop, argument decoding, and
runtime failures wrap `ErrJob` after any lower-level error has been redacted.
Database connection, schema-validation, transaction, or query failures also
remain discoverable through `errors.Is(err, ErrDatabase)` where applicable.
Context cancellation remains discoverable as the context error.

Worker stories add stage-specific safe failure codes. This story reserves
these shared codes:

```text
invalid_job_arguments
missing_domain_record
domain_invariant
job_attempts_exhausted
```

They are stable database values, not prose. No code contains a dynamic suffix
or embedded value.

## Logging and metrics boundary

Use the Go standard library `log/slog` JSON handler on standard error as the
River logger adapter. Do not add a second logging framework.

Normal lifecycle logs may include only:

- A fixed event name.
- The fixed River job kind and queue.
- River's numeric job ID and attempt number.
- A fixed safe failure classification.
- A duration and count with no source-derived label.

Do not log encoded job arguments, domain IDs, URLs, file paths, plans,
database coordinates, SQL, HTTP headers, response bodies, child-process
output, or raw errors. River library errors must pass through an adapter that
emits a safe classification rather than forwarding arbitrary error text.

This story does not introduce Prometheus, OpenTelemetry, an HTTP metrics
endpoint, or a UI. Structured logs and durable database state are sufficient
for the initial runtime.

## Shutdown behavior

The reusable production runtime follows Story 01 signal handling:

1. Start the River client only after configuration and both schema versions
   validate.
2. On first cancellation, stop fetching new work and request graceful River
   shutdown.
3. Give active jobs 30 seconds to stop gracefully.
4. If that interval expires, call River's canceling stop path so job contexts
   and their subprocesses are canceled.
5. Close the database pool only after River shutdown returns.

Shutdown errors are redacted `ErrJob` failures. Cancellation is not reported
as a successful stage unless the stage-specific external work and success
transaction actually completed.

Story 03 exposes and tests this runtime logic without starting it from the
production `work` command because no production worker is registered yet.

## Required tests

### Unit tests

- Every production job kind, queue, and one-field JSON contract matches the
  table in this story.
- Invalid, unknown, missing, duplicate, noninteger, nonpositive, and
  overflowing argument values are rejected safely.
- Queue concurrency, attempt count, River schema, retry, timeout, stuck-job,
  and retention settings match the fixed production policy.
- No production insertion uses River `UniqueOpts`, tags, or metadata.
- Runtime, migration, insertion, decoding, logging, and shutdown errors never
  expose supplied database URLs, source URLs, paths, or plan-like fixtures.
- The logger adapter includes only its allowlisted fields.
- Story 01 command parsing and configuration tests continue to pass.

### PostgreSQL and River integration tests

Use the disposable test-database safeguards from Story 02. When
`MRFPIPELINE_TEST_DATABASE_URL` is set, prove:

- A fresh `migrate` creates current application and River schemas and reports
  both applied counts.
- Repeated and concurrent migrations are idempotent and serialized.
- Application migration success followed by injected River migration failure
  is recoverable by a later migration without rewriting application history.
- Newer, missing, or malformed River migration state is rejected without
  automatic repair.
- Runtime construction rejects stale application or River schemas and never
  auto-migrates.
- A private test job inserted with `InsertTx` is invisible before commit,
  visible after commit, and absent after rollback.
- A domain stage update and test-job insertion commit or roll back together.
- Concurrent attempts to schedule one locked domain stage store exactly one
  River job ID.
- The private test worker executes, retries, reaches its configured final
  attempt, and exercises safe panic/error handling.
- Graceful shutdown stops new fetches, permits a cooperative test job to
  finish, and cancels a noncooperative test job after the fixed interval.
- Pruning a completed private River job does not change the application
  domain record.

Private test jobs and their tables or fixtures are test-only. They do not add
a production job kind or application migration.

Run:

```text
go test ./...
go test -race ./...
go vet ./...
```

## Acceptance criteria

- `mrfpipeline migrate` applies and reports both current application and River
  migration state using the fixed schemas.
- Runtime commands reject missing or stale schemas instead of migrating
  implicitly.
- All version 1 job kinds carry exactly one positive numeric database ID.
- Queue names and single-process concurrency limits are fixed and tested.
- Domain changes and River job insertion are atomic through `InsertTx`.
- Application database constraints and row locks, not River uniqueness or
  hashes, prevent duplicate scheduling.
- The shared claim, retry, success, stale-job, terminal-failure, and shutdown
  rules are precise enough for later worker stories to implement uniformly.
- River failures and logs remain redacted, while cancellation and database
  classification remain machine-detectable.
- No production worker, external I/O, or artifact is introduced yet.

## Non-goals

- Implementing any production worker body.
- Enqueueing a real discovery run.
- Payer discovery or automatic scheduling.
- HTTP download behavior or local artifact layout.
- TOC or MRF parsing, association import, consumer ingest, or plan attachment.
- Running `mrfenricher`.
- Multiple worker processes, horizontal scaling, distributed filesystem
  locking, or dynamic queue configuration.
- A manual retry, cancel, repair, or reconciliation command.
- An HTTP API, metrics endpoint, dashboard, or UI.
- River schema customization beyond its fixed PostgreSQL schema.
- River unique jobs, argument hashes, content hashes, UUIDs, or custom job
  tables.
