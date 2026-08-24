# Story 20: Production hardening and test readiness

## Status

Planned.

## User story

As an operator, I want sealed releases protected at worker execution time and
failures exposed through small, redacted operational signals so that a stale
job, repeated parser failure, stalled stage, or missing active publication
cannot change or quietly invalidate the release being served.

## Goal

Close the production-readiness gaps found after Stories 14–19 by:

1. enforcing the monthly-release state gate in every worker claim transaction;
2. validating the frozen plan-publication inventory before activation and
   rollback;
3. reporting every failed attempt with useful, sanitized context;
4. exposing a redacted query for old pending/running work;
5. detecting missing or invalid active consumer publications during
   reconciliation; and
6. running the PostgreSQL, DuckDB consumer, race, vet, and bounded live
   acceptance checks that are normally environment gated.

This is a focused hardening story. It does not add content identity, content
hashes, a metrics service, automatic repair of externally deleted successful
artifacts, or a universal production job timeout.

## Dependencies and scope

This story depends on Stories 14–19 and the exact pinned feed-free consumer
`2.0.0` contract. It tightens Story 19's sealed-release behavior; it does not
change release membership, output IDs, retry identities, consumer formats, or
the single-worker topology.

No database migration is expected. Add one only if implementation proves it is
strictly required for a durable invariant; do not add retry-history, heartbeat,
artifact-checksum, or duplicate release-membership tables.

## Execution-time release gate

Reconciliation is not the enforcement boundary. Every production job must
verify release mutability when its delivery is claimed, before the domain stage
is changed to `running` and before any network, parser, filesystem, consumer,
or warehouse work begins.

The gate applies to every production job kind:

```text
discovery.run
toc.download
toc.parse
toc.import
mrf.download
mrf.parse
consumer.ingest
consumer.attach_plans
```

It applies equally to the generic lifecycle claim and every stage-specific
claim. A later success/pre-publication check may remain as defense against an
unsupported invariant violation, but it does not replace the claim gate.

For release-owned work, the claim transaction must:

1. identify the exact dependent payer/month release row or rows from durable
   domain relationships, never from River arguments alone;
2. lock those release rows in deterministic payer/month order before locking
   and changing the stage row;
3. require every dependent release to be `building`;
4. verify the current River job identity and existing stage invariants; and
5. only then transition the stage to `running` and commit the claim.

MRF source download and parse can be shared by several payer snapshots in the
same collection month. They may execute only when every dependent release is
still building. A sealed dependent release blocks mutation of that shared
prerequisite.

The lock order must serialize safely with discovery and activation. Activation
cannot seal a release between a successful gate and the stage transition. A
stage already running blocks readiness, so activation cannot seal underneath
supported in-flight work.

An implementation may first read the stage relationship without a lock to
discover the dependent release identities. It must then lock the release rows,
lock the stage row, and re-read/revalidate the complete relationship and River
identity before mutation. The first read authorizes nothing. This preserves
release-before-stage lock order while keeping stale deliveries as no-ops.

If any dependent release is `active` or `inactive`:

- leave every domain status, timestamp, failure code, job ID, and successor
  relationship unchanged;
- do not invoke external work or create/remove/acknowledge an artifact;
- do not enqueue a successor;
- cancel the stale River delivery through the existing `river.JobCancel`
  mechanism so it reaches River's terminal `cancelled` state and does not retry
  forever;
- emit one sanitized warning with fixed event/failure code, job kind, numeric
  domain/job IDs, and attempt; and
- leave reconciliation to report the durable nonterminal sealed inconsistency.

A missing release or contradictory domain relationship remains a safe fixed
invariant failure. A stale delivery whose domain job ID no longer matches
remains an ordinary no-op.

## Activation and rollback publication gate

`month activate` already recognizes each base consumer snapshot. Extend its
read-only filesystem preflight to validate the exact expected plan-association
part inventory for every snapshot in the target release.

Expected plan publications come only from succeeded frozen attachment batches
with a positive added-plan count. Validation must detect:

- a missing expected plan part;
- an unexpected plan part for the target output;
- a symlink or non-regular entry where a part is expected;
- plan/batch/item rows created after the release was first sealed; and
- an invalid or missing base consumer publication.

The created-after-seal checks apply only when `sealed_at` is already present.
First activation still validates the complete current inventory and base
publications, but has no earlier seal timestamp to compare.

Reuse the existing consumer publication recognizer and sealed plan-set audit
rules. Do not duplicate consumer parsing logic, scan full fact rows, run
DuckDB, or add content hashes.

The preflight runs under the exclusive worker lease and before the short
activation transaction. On any publication mismatch:

- fail with the existing safe release readiness/inconsistency classification;
- keep the previously active month unchanged;
- leave the target release and warehouse unchanged; and
- expose only sanitized blocker/failure context.

An unreadable filesystem entry or permission/I/O failure is an
`artifact_reconciliation_failed` infrastructure error, not proof of a
publication mismatch. It still fails activation without mutation.

The same validation applies to first activation, idempotent activation, and
reactivation of an inactive release. Rollback must never make a known-damaged
historical publication active.

## Active-release reconciliation audit

Extend reconciliation's sealed-release audit to recognize every base consumer
publication belonging to an `active` release, in addition to the existing
frozen plan-part inventory checks.

Use the pinned consumer recognizer and the same payer/month/output/catalog
identity required by activation. Missing or invalid active publication state:

- increments the existing `sealed_release_inconsistency_count` using the same
  per-pass counting contract;
- emits sanitized stage context and numeric domain identity;
- does not change release, snapshot, batch, plan, or River state;
- does not attempt automatic warehouse repair or fallback activation; and
- does not prevent independent safe repairs for building releases.

An expected publication that is validly readable but missing or structurally
invalid is a sealed inconsistency. A permission error, I/O error, or other
condition that prevents the audit from deciding is the existing
`artifact_reconciliation_failed` infrastructure failure and aborts the pass;
it must not be counted as a proven sealed inconsistency.

Inactive base publications are revalidated before reactivation rather than
fully inspected on every startup. This keeps normal startup work proportional
to data being served while preserving rollback safety.

## Failed-attempt visibility

After a successful claim, the shared worker lifecycle must emit one
application-owned structured record for every error returned by the stage's
external `Work` function after its durable bookkeeping outcome is known. Do
not rely on River's generic error record as the primary operator signal.
Caller cancellation and explicit worker-lease loss are interruptions, not
failed stage attempts.

Claim/invariant failures, sealed-delivery suppression, and infrastructure
failures in the claim/success transaction use their own sanitized lifecycle
records; they are not mislabeled as a failed external-work attempt. A success
bookkeeping error does not claim that external work failed.

Lifecycle records omit `outcome` and instead include the existing allowlisted
`phase`, using a fixed value such as `claim`, `success`,
`retry_bookkeeping`, or `terminal_bookkeeping`. They otherwise use the same
safe job/domain identity fields when those identities are available.

Each record contains only allowlisted fields:

```text
kind
queue
job_id
domain_id
attempt
failure
outcome
```

`failure` is a fixed sanitized application failure code. It is never the raw
Go error, URL, path, filename, response body, plan, provider/service value,
credential, SQL, or River diagnostic. `outcome` is exactly one of:

```text
retrying
terminal
```

Behavior:

- before the maximum attempt, persist the existing retryable domain state and
  log `outcome=retrying`;
- at the maximum attempt or an immediate domain failure, persist the existing
  terminal failure code and log `outcome=terminal`;
- if retry/failure bookkeeping itself cannot commit, emit a sanitized
  bookkeeping failure record and return the infrastructure error.

Add one generic fixed `job_bookkeeping_failed` code for a failure to persist
retry or terminal lifecycle bookkeeping. It is a log classification only and
must not overwrite the stage's domain failure code when the bookkeeping
transaction did not commit. Do not misclassify it as a parser, consumer, or
reconciliation failure.

Worker lease loss is owned by the work runtime rather than individual stage
attempts. When the lease watcher marks the runtime lost, emit exactly one
sanitized `worker_lease_lost` record before shutdown. Ordinary operator/context
cancellation is not logged as lease loss and does not mark a domain stage
failed. The runtime record uses `kind=runtime` and
`failure=worker_lease_lost`; it omits queue, job, domain, and attempt fields
because the process lease is not owned by one River delivery.

Do not store one database row per attempt. Existing terminal `failure_code`
columns remain the durable terminal history.

The safe logger must retain the application fields above even when the
underlying River logger uses a different attribute spelling such as
`job_kind`. Application-owned records normalize this to canonical `kind`;
arbitrary River attributes are not broadly allowlisted. Raw `error`/`err`
attributes remain dropped.

This story explicitly authorizes `domain_id` only in failed-attempt,
sealed-suppression, and lifecycle-failure records. It supersedes Story 13's
routine-log restriction for those records only. Routine success/progress logs
continue omitting domain IDs.

## Stalled-work diagnostics

Keep the infinite production job timeout: valid MRF downloads and parses can
take hours, and an arbitrary deadline could convert slow healthy work into a
retry storm.

Add one exported/redacted operational SQL query and matching README example
that accepts one positive PostgreSQL `interval` bind parameter and returns
nonterminal domain stages at least that old. The result contains only:

```text
stage
domain_id
status
river_job_id
attempt
age_seconds
```

For `running`, age starts at the stage's `started_at` when that column exists
and is nonnull; otherwise it starts at `updated_at`. For `pending`, age starts
at `updated_at`. `age_seconds` is a nonnegative whole number.

It covers all eight production job kinds, includes building and sealed
releases, and orders oldest first with deterministic stage/domain-ID ties. A
River row supplies `attempt` only when its ID, exact expected kind, and
one-field numeric domain argument all match the current domain job identity.
When the current River row is absent or mismatched, `attempt` is null. It must
distinguish at least:

- old `pending` work, which may indicate queue starvation or repeated retry;
- old `running` work, which may indicate a slow or stalled operation; and
- a missing current River job, which reconciliation can repair for a building
  release.

The query is diagnostic, not a mutation or health verdict. Operators choose
thresholds based on observed payer file sizes. Documentation must say that
progress-record cessation plus increasing running age warrants investigation;
the pipeline does not automatically cancel, retry, or delete the job.

External alert routing, dashboards, paging, and metrics storage remain the
deployment operator's responsibility. The pipeline provides stable structured
logs and SQL inputs for those systems.

## Durability and recovery boundary

Retain the existing staging, atomic publication, manifest/layout validation,
crash-window acknowledgement, exact retry, and artifact-owner cleanup rules.
Normal parser or consumer errors must converge to retrying logs and finally a
durable terminal `failure_code`; they must not disappear as a successful job.

Externally deleting or altering an artifact after its owning stage succeeded
is unsupported operator damage:

- sealed releases are reported and never repaired in place;
- active publication damage blocks idempotent activation and is reported by
  reconciliation;
- inactive publication damage blocks rollback/reactivation;
- building work may use only the safe repairs already defined by Story 13; and
- recovery of an externally lost successful artifact is restore-from-backup or
  deliberate release rebuild, not automatic status rewinding.

Update the recovery documentation with a short decision table for terminal
stage failure, nonterminal stale job, invalid incomplete generated output,
missing building input, and missing sealed publication. Do not imply that
retry repairs externally deleted succeeded output.

## Tests

Add permanent focused tests for the new contracts.

### Sealed delivery suppression

Use a table-driven shared-gate test covering each of the eight production job
kinds in both `active` and `inactive` state with a current pending/running River
delivery. Require:

- the external work callback is not invoked;
- domain rows and timestamps are unchanged;
- no artifact or successor is created;
- the River delivery stops retrying; and
- the sanitized suppression record is emitted.

Include a shared MRF source with one building and one sealed dependent release.
It must not download or parse.

Include a claim-versus-activation concurrency test. The only valid outcomes
are work claimed while activation remains blocked, or activation committed
while the delivery becomes a sealed no-op. No execution may begin after seal.

The shared table plus focused PostgreSQL integration coverage of the generic
claim, one stage-specific claim, the shared-source gate, and activation race is
sufficient. Do not duplicate a bulky sixteen-case database worker harness when
the common gate is already exercised directly.

### Activation and reconciliation integrity

- Remove one expected plan part before activation and require no pointer
  change.
- Add one unexpected plan part and require no pointer change.
- Damage an inactive release publication and require rollback to fail without
  changing the active release.
- Damage an active base publication, reconcile alongside repairable building
  work, and require a sealed inconsistency plus successful independent repair.
- Restore each fixture and require activation/reconciliation to converge.

### Failure diagnostics

- A retryable parser failure before the final attempt emits `retrying` and
  leaves the domain pending with the same identity.
- The final failed attempt emits `terminal` and persists its fixed failure
  code.
- A forced retry-bookkeeping failure emits `job_bookkeeping_failed`, does not
  fabricate a committed domain failure, and returns the infrastructure error.
- Runtime lease loss emits exactly one runtime-owned record; ordinary shutdown
  emits none.
- Logger tests prove raw errors, URLs, paths, and response text are absent.
- The stale-stage SQL covers every job kind, current job identity, ordering,
  and missing-job behavior.

## Required verification

Before changing this story to `Implemented`, run and record successful results
for:

```text
go test ./...
go test -race ./...
go vet ./...
```

Also run the environment-gated suites with:

```text
MRFPIPELINE_TEST_DATABASE_URL
MRFCONSUMER_DUCKDB_BIN
```

The PostgreSQL suite must execute migrations and release concurrency tests; it
must not report them as skipped. The DuckDB consumer-selection smoke must use
the exact pinned consumer revision and must not report it as skipped.

This is an explicit implementer/operator verification requirement. Ordinary
`go test ./...` continues to skip these integration tests when their guarded
environment variables are absent; do not make normal tests depend on local
PostgreSQL or DuckDB.

After hermetic checks pass, run the opt-in bounded real-UHC acceptance with:

- a fresh disposable database;
- dedicated artifact and warehouse roots;
- one explicit TOC limit;
- one explicit collection month;
- the pinned provider catalog and consumer; and
- a caller-controlled long test timeout.

The live run must complete discovery through attachment, reach readiness,
activate, survive restart/reconciliation, and repeat activation idempotently.
It emits only sanitized counts and timings. It never runs in ordinary tests.

## Documentation cleanup

- Mark Stories 14–17 `Implemented`.
- Add the failed-attempt log schema and stale-work SQL to `README.md`.
- Add the activation plan-inventory and active-publication checks to the
  operator cutover/reconciliation procedure.
- Clarify restore-versus-retry behavior for externally lost successful
  artifacts.
- Keep capacity monitoring and backup/restore prerequisites prominent.
- Keep `requirements/DESIGN.md` aligned with the final enforcement boundary.

## Acceptance criteria

- No production job begins external work for an active or inactive release.
- Claim and activation races preserve the release seal without deadlock.
- A shared source is mutated only while all dependent releases are building.
- Activation and rollback reject missing, unexpected, or invalid publication
  inventory without changing the active pointer.
- Every failed attempt has one useful redacted application log; terminal
  failure remains durable in the domain row.
- Operators can query old pending/running stages without seeing sensitive data.
- Reconciliation detects invalid active publications without repairing or
  deactivating them and continues safe unrelated building work.
- PostgreSQL and DuckDB gated integration suites actually execute and pass.
- The bounded live acceptance completes on disposable state.
- No content hash, retry-history table, heartbeat table, metrics platform,
  automatic sealed repair, or arbitrary production job timeout is added.

## Out of scope

- Automatic recurring discovery, activation, rollback, or reconciliation.
- Automatic repair or deletion of sealed warehouse data.
- Content-based identity, checksums, signatures, or duplicate detection.
- A query server, UI, metrics backend, alert delivery, or on-call policy.
- Full Parquet row scans or DuckDB analytical validation during activation.
- Warehouse compaction, migration, deletion, or plan correction.
- Multi-worker fencing beyond the existing exclusive lease topology.
