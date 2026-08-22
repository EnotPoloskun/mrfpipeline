# Story 13: Reconciliation, operational acceptance, and documentation

## Status

Planned.

## User story

As a pipeline operator, I want the completed pipeline to detect interrupted
coordination, retry an exact failed stage deliberately, clean safe obsolete
artifacts, and prove a bounded real-data run so that I can operate it before a
UI exists.

## Goal

Complete version 1 with a database-backed single-worker lease, automatic safe
startup reconciliation, explicit `reconcile` and `retry` commands, final
artifact retention rules, bounded UHC operational acceptance, and current
README/design documentation.

This story does not add another transformation, recurring discovery, automatic
provider enrichment, destructive warehouse repair, or a web UI.

## Dependencies and scope

This story depends on Stories 01 through 12 and supersedes their deferred
reconciliation/retention placeholders. Every normal worker remains governed by
the existing exact identities, stage states, River arguments, retry limit,
artifact boundaries, parser/consumer versions, and redaction rules.

Story 13 adds no application table and no content identity. If implementation
requires a forward SQL migration for an operational index, it must be a new
contiguous migration whose only semantic effect is that index; do not rewrite
Story 02 migration `0001`.

## Product decisions

- Version 1 permits exactly one active `mrfpipeline work` process per database
  and warehouse.
- A fixed PostgreSQL advisory lease enforces that supported topology. River
  queue concurrency alone is not sufficient.
- Safe reconciliation runs automatically after the lease is acquired and
  before River starts fetching jobs.
- Reconciliation may restore missing eligible work and normalize orphaned
  nonterminal claims. It never reopens a terminal failed stage automatically.
- An operator retries one exact failed domain stage through an explicit
  command. Retry preserves the domain record, output ID, source identity,
  frozen attachment items, and plan batch ID.
- Reconciliation never edits River-owned rows. It may read River state and
  insert a replacement job through the public client transaction API.
- Published parser output and consumer warehouse data are never deleted by
  general cleanup.
- TOC parsed output is removed after successful import; plan JSON is
  removable after successful attachment; shared parsed MRF output is retained
  with no supported automatic deletion path. Manual deletion of shared parsed
  MRF output while stopped is possible but outside the supported contract and
  may break later snapshots.
- Manifest-present invalid parser output is never removed automatically. The
  operator must stop the worker, inspect and remove or quarantine that exact
  generated directory, then issue `retry`.
- Parser temporary files younger than 24 hours remain after a crash. Immediate
  reconciliation is not parse-temp cleanup.
- The first live run admits one newly discovered TOC. Then run two, then five.
  Ten remains the explicit maximum after sizing is known. The acceptance
  harness defaults to one and retains a maximum of ten.
- Default status SQL is redacted. A separate authorized debug query accepts a
  numeric TOC or MRF ID and returns its URL.
- Collection month is sticky. A wrong first admission requires rebuilding
  affected pipeline/warehouse state.
- README must prominently document possible current-view double counting after
  URL rotation and require explicit-month or single-month serving until curated
  feeds exist.
- Operational acceptance is opt-in and never runs against live UHC or a
  nondisposable database in ordinary `go test ./...`.
- Keep advisory lease `(7319, 1)` frozen. PostgreSQL advisory locks are
  isolated per database.

Before implementation lands, Story 02 migration `0001` can still be finalized.
Once applied or released, it is immutable; all later schema changes use
`0002+`.

## Final command surface

The complete executable exposes:

```text
mrfpipeline migrate
mrfpipeline work
mrfpipeline discover --payer uhc --collection-month <YYYY-MM> --limit <count>
mrfpipeline reconcile
mrfpipeline retry --stage <job-kind> --id <domain-id>
```

Story 01 parsing, flag spelling, repeated-scalar final-value behavior, help,
streams, signal handling, exit classifications, and redaction conventions
apply to the two new commands.

### `reconcile`

`reconcile` accepts no flags or positional arguments. It requires the complete
`work` environment because it validates and cleans the local artifact root and
recognizes the configured warehouse/provider/selector boundaries. It acquires
the same exclusive worker lease, runs the safe database/artifact reconciliation
defined below, releases the lease, and exits.

On success it writes one compact JSON object and newline:

```json
{"repaired_job_count":3,"unblocked_stage_count":2,"scheduled_plan_batch_count":1,"cleaned_artifact_count":4}
```

Counts are nonnegative. They count changes committed by this invocation, not
rows inspected. A repeated converged invocation reports zeroes. The report
contains no domain IDs, paths, URLs, plans, provider/service values, or River
diagnostics.

### `retry`

Exact syntax:

```text
mrfpipeline retry --stage consumer.attach_plans --id 230
```

Both flags are required. `--stage` accepts exactly one production job kind:

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

`--id` is a positive signed 64-bit base-10 application row ID without a sign,
padding requirement, separators, whitespace, exponent, or decimal point.
Leading zeroes are accepted and converted to their ordinary numeric value.

`retry` requires only `MRFPIPELINE_DATABASE_URL`. It validates current
application/River schemas, acquires the worker lease, reopens the exact failed
stage as defined below, inserts its replacement River job transactionally,
releases the lease, and exits. The worker must be stopped while this command
runs; lease contention fails without mutation. A remote operator may therefore
enqueue retry using only the database URL. Actual work waits for the correctly
configured worker host.

On success it writes:

```json
{"stage":"consumer.attach_plans","domain_id":230,"river_job_id":9012}
```

These numeric operational identities are safe. The command does not execute
the job synchronously and does not wait for completion.

Help states that `reconcile` performs only safe nonterminal repairs, `retry`
is required for terminal failure, the exact same stage identity is retained,
and an attachment retry reuses the frozen plan batch rather than creating a
new batch for its items.

## Exclusive worker lease

Use PostgreSQL's two-integer session advisory lock with this fixed project
constant:

```text
pg_try_advisory_lock(7319, 1)
```

The values are literal application constants and remain frozen at `(7319, 1)`.
They are not calculated from a database URL, warehouse path, hostname, process
ID, time, source, or content. They are distinct from the migration lock and are
never printed. PostgreSQL advisory locks are isolated per database.

For `work`, `reconcile`, and `retry`:

1. Complete command-specific configuration and schema validation.
2. Acquire one dedicated pgx pool connection.
3. Call the nonwaiting two-key advisory-lock function.
4. If it returns false, release the connection and fail with the fixed safe
   classification `worker_lease_unavailable` without starting River or
   changing domain/artifact state.
5. Keep that exact connection open until the command is done.
6. Explicitly unlock during orderly shutdown and then release the connection.

The `work` process watches the dedicated connection. Connection closure or a
failed lease-health query cancels the worker context and begins the existing
graceful/canceling River shutdown. It must not acquire a new connection and
silently continue under a different session.

Check lease health before every consumer operation as well as periodically
while work is running. A failed check prevents beginning another warehouse
write and cancels the process. An operation already inside the external
consumer call follows context cancellation and the consumer's atomic
publication contract.

This is a supported-deployment guard, not a general distributed fencing
protocol. Operators must not copy the warehouse, bypass the pipeline, or run
consumer writers outside the leased deployment.

## Safe startup reconciliation

`work` runs the same safe reconciliation as `mrfpipeline reconcile` while
holding the lease and before starting River. Perform bounded ascending-ID
pages and one short transaction per domain record or tightly related snapshot
group. Re-read and lock every target before mutation.

The pass is idempotent and may commit earlier repairs before a later error. A
retry starts again from durable state; there is no global cross-table
transaction.

### Missing predecessor-to-successor transitions

Before scheduling a parse successor, reconcile the Story 07/10 prerequisite
loss case. When download status is succeeded, parse is blocked/pending/running
rather than failed, no valid completed parser output exists, and the generated
download is absent or safely classified as incomplete:

1. Lock the TOC/source row and recheck every condition.
2. Change parse back to blocked and clear its assigned parse job ID; any old
   parse delivery is thereafter stale.
3. Change download back to pending, insert a new exact TOC/MRF download job,
   replace its stored job ID, and commit together.

This is safe prerequisite restoration, not a new source identity or terminal
retry. A manifest-present invalid parser output, manifest-present invalid
download, or failed parse is preserved and not reset automatically. After an
operator explicitly retries such a failed parse, the next leased startup may
apply this prerequisite restoration if its download is also missing, then the
new download success schedules the parse normally.

Repair only these exact lifecycle gaps when the predecessor is succeeded and
the successor is blocked with a null job ID:

| Record | Predecessor | Repaired successor |
|---|---|---|
| `toc_files` | download succeeded | set parse pending; insert `toc.parse`. |
| `toc_files` | parse succeeded | set import pending; insert `toc.import`. |
| `mrf_sources` | download succeeded | set parse pending; insert `mrf.parse`. |
| `mrf_snapshots` | source parse succeeded | set consume pending; insert `consumer.ingest`. |

Insert each job with `InsertTx`, store its generated River ID, clear no
terminal state, and commit the transition and job together. A failed successor
or a blocked successor whose prerequisites are not succeeded is not changed.

Discovery has no predecessor transition. Newly created TOC/source initial jobs
are covered by the missing-current-job rules below.

### Missing or orphaned current jobs

For every nonterminal stage that is pending or running, compare its stored job
ID with read-only River state:

- Null job ID: insert a replacement job for the same domain ID, store it, and
  normalize the stage to pending.
- Stored ID has no River row, including retention pruning: insert/store a
  replacement and normalize to pending.
- River state is `available`, `pending`, `scheduled`, or `retryable`: retain
  the same job ID and normalize an orphaned domain `running` state to pending.
- River state is `running`: because the exclusive lease proves no other
  supported worker process is active during startup/reconcile, insert/store a
  replacement and normalize to pending. The old River delivery becomes stale
  by job-ID mismatch if River later rescues it.
- River state is `completed` while domain state is nonterminal: insert/store a
  replacement. Its idempotent worker recognizes any completed external
  publication and repairs acknowledgement.
- River state is `cancelled` or `discarded`: do not grant another automatic
  attempt series. Mark the still-assigned domain stage failed with fixed
  `failure_code=river_terminal_without_domain_result` and its applicable
  completion timestamp, even if publication might exist. Explicit retry lets
  the normal worker recognizer prove completion.

Never update, delete, retry, cancel, or rescue the old River row directly.
Never search River by serialized job arguments. The stored generated job ID is
the only River-row correlation.

For a nonterminal stage whose current River job exists but has an unknown
state, fail reconciliation with `domain_invariant` and make no mutation to
that record.

### Succeeded predecessors with missing normal work

After stage/job repair, run the existing exact schedulers:

- for every successfully parsed MRF source, scan its snapshots and schedule a
  blocked eligible consumer ingest;
- for every consumed snapshot with unassigned plans and no unresolved batch,
  run Story 12's common plan-batch scheduler; and
- for every succeeded attachment batch whose snapshot has later unassigned
  plans, run the same scheduler.

The scheduler remains the only normal creator of attachment batches. This pass
does not create a new batch when a pending/running/failed batch exists.

### States reconciliation does not guess about

Fail the affected record closed and report a safe invariant without changing
it when, for example:

- a successor advanced while its predecessor is not succeeded;
- a stored job ID belongs to a different kind or domain argument;
- a batch count/items/snapshot relationship is inconsistent;
- a plan is assigned across snapshots;
- a succeeded domain row's immutable external output conflicts with its
  expected identity; or
- a failed stage has an invalid lifecycle shape.

Do not infer correctness from a URL basename, directory name alone, file
bytes, counts alone, or approximate month/feed similarity.

## Explicit retry of one terminal stage

`retry` uses the supplied job kind to select exactly one domain table/stage;
unknown kinds cannot cause a broad update. In one transaction:

1. Lock the exact domain row.
2. Require its selected stage to be `failed`, its stored failure code to be a
   fixed recognized code, and its predecessor to remain succeeded where one
   exists.
3. Require dependent state to be compatible with retry. In particular, a plan
   batch must retain its exact snapshot, requested count, and frozen items.
4. Do not delete, rename, reset, or pre-judge any external artifact. The normal
   worker will recognize completed output, reset only its permitted partial
   leaf, or fail closed under its owning story.
5. Set the same stage to pending, clear `failure_code`, clear its terminal
   `completed_at` where that table has one, and preserve original
   `started_at`/creation timestamps.
6. Insert the same job kind with the same numeric domain ID through `InsertTx`
   and replace the stored River job ID.
7. Commit together.

The command refuses blocked, pending, running, or succeeded stages. It also
refuses a failed successor whose predecessor is no longer valid, an attachment
batch with changed/missing items, or a domain row that does not exist.

Retry never creates a new TOC, source, snapshot, plan, or attachment batch. For
`consumer.attach_plans`, the same `plan-batch-<batch-id>` is passed again. Newer
unassigned plans stay unassigned until that batch succeeds.

Each explicit invocation grants a fresh River eight-attempt series. This is a
deliberate operator action and is recorded by the new stored River job ID and
existing timestamps; version 1 adds no separate revision or retry-history
table.

## Artifact reconciliation and retention

All removal uses Story 04's retained/validated roots, non-following traversal,
exact generated leaves, and safe deletion primitive. The worker lease must be
held, so no supported pipeline job is active during the scan.

### Automatic safe removals

`work` startup and `reconcile` remove only:

- a TOC `download` leaf when that TOC parse is succeeded and the strict TOC
  parser output remains valid;
- an MRF `download` leaf when source parse is succeeded and the strict shared
  parser output remains valid;
- a TOC `parsed` leaf when import is succeeded;
- a `plan-batches/plan-batch-<id>` leaf when that exact attachment batch is
  succeeded; and
- real regular files or real directories immediately beneath the pipeline
  artifact root `.staging` whose modification time is at least 24 hours old.

Entries younger than 24 hours remain. Immediate reconciliation is not
parse-temp cleanup. Parser temporary files created under `TMPDIR` can survive a
crash inside that window until a later leased reconcile.

Shared successful MRF `parsed` output has no supported automatic deletion
path. Manual deletion while the worker is stopped is possible but outside the
supported contract and may break later snapshots.

For staging cleanup, lstat each immediate child, reject symlinks and special
files, recheck its metadata immediately before removal, and never descend
through a changed entry. There is no current job under the exclusive lease;
the age threshold protects a recently interrupted command/reconciliation run.

Before deleting a download, revalidate its successor publication. Before
deleting a plan JSON directory, re-read the succeeded batch and exact ID under
lock. TOC parsed output needs no later consumer work because all accepted
provenance/canonical plans are durable in PostgreSQL after import success.

Cleanup is idempotent: an absent eligible leaf is success with no cleaned
count. If path safety, type, validation, or removal fails, stop the cleanup
pass with a fixed `artifact_reconciliation_failed` error. Preserve the target
and do not broaden deletion.

### Retained data

Retain indefinitely in version 1:

- the artifact `workspace.json` marker and fixed top-level directories;
- every shared successful MRF `parsed` output, because a later TOC/month may
  create a new consumer snapshot from it;
- every failed-stage artifact, including manifest-present invalid output, for
  explicit inspection/retry;
- PostgreSQL domain/provenance rows;
- consumer `warehouse.json`, pinned provider catalog, completed snapshots,
  plan schema seed, and immutable plan-association parts; and
- the manually configured provider catalog and service selector.

The pipeline never cleans `<warehouse>/.staging` or any other consumer-owned
warehouse entry. Consumer staging is outside pipeline artifact ownership;
operators may remove it only while the worker is stopped and according to the
consumer's own documented recovery procedure.

There is no automatic age deletion of parsed MRF output, completed warehouse
snapshots, plan parts, database rows, or River domain history. River retains or
prunes only its own rows under Story 03 policy.

### Disk-capacity guidance

README guidance must state that `discover --limit` bounds newly admitted TOCs,
not MRF count or bytes. Before a bounded run, record free space on the artifact
and warehouse filesystems. During the run, monitor:

- bytes in MRF downloads and parsed outputs;
- bytes in the warehouse;
- free space on both filesystems;
- database size; and
- the count of pending/running MRF downloads and parses.

Version 1 does not guess required disk from HTTP headers, impose a destructive
quota, or delete a shared parsed MRF merely to recover space. Stop the worker
gracefully if available capacity approaches the operator's safety threshold.

## Operational state and readiness queries

Final documentation provides copyable, read-only PostgreSQL queries for:

- discovery counts and status;
- TOC stage counts;
- MRF source download/parse counts;
- snapshot consume counts;
- plan attachment batch counts and requested/added totals;
- terminal failures with fixed codes and numeric IDs;
- consumed snapshots with unassigned plans;
- plan-ready snapshots using Story 12's derived rule; and
- exact sources referenced by more than one TOC, reported only as numeric
  source ID and reference count.

Queries must not print source URLs, plan values, database credentials, or
configured paths by default. The future UI may reuse equivalent read models.

### Authorized URL-debug query

Document a separate, clearly labeled authorized debug query for operators who
already have database access and a numeric TOC or MRF ID. It accepts that ID
and returns the stored URL. Default status queries remain redacted and must
not be rewritten to include URLs. Worker logs never print URLs.

The debug query is documentation, not a CLI flag. It is intended for
incident response on an authorized connection, not for routine monitoring.

## Bounded real-data acceptance

Add an opt-in operational harness or documented script that uses the actual
commands and worker binary. It is disabled unless all of these are supplied:

```text
MRFPIPELINE_REAL_ACCEPTANCE=1
MRFPIPELINE_TEST_DATABASE_URL=<disposable database>
MRFPIPELINE_ARTIFACT_ROOT=<dedicated empty acceptance root>
MRFPIPELINE_WAREHOUSE_PATH=<dedicated empty acceptance warehouse>
MRFPIPELINE_PROVIDER_CATALOG_PATH=<accepted manual catalog>
MRFPIPELINE_SERVICES_PATH=<small intended CPT selector>
```

The test database must satisfy Story 02's disposable-name guard. Both local
output roots must be dedicated, empty/initializable, non-symlink paths whose
normalized values are outside normal production roots. The harness refuses to
run otherwise and never deletes a path it did not initialize and mark for the
acceptance run.

The harness takes an optional `MRFPIPELINE_REAL_TOC_LIMIT` whose accepted range
is `1..10` and whose default is `1`. The documented live progression is one
TOC, then two, then five; ten remains the explicit maximum after measuring
fan-out and disk use.

Procedure:

1. Build the exact binary and run current migrations.
2. Start one leased worker.
3. Run one UHC discovery for the operator-supplied collection month with the
   bounded limit.
4. Wait by polling redacted aggregate database state, with a documented
   operator timeout rather than a hardcoded production stage timeout.
5. Confirm every admitted TOC reaches import success or an explicit fixed
   terminal failure.
6. Allow the resulting MRF sources/snapshots/batches to converge, without
   assuming their count is at most the TOC limit.
7. Stop the worker gracefully, run `reconcile`, restart it, and prove no
   duplicate domain work or warehouse publication is created.
8. Record aggregate duration, peak artifact/warehouse bytes, row counts from
   manifests, and stage/failure counts in an operator-local report excluded
   from source control when it may reveal sensitive/high-cardinality data.

Live UHC results are not required to contain overlapping MRF URLs or the exact
A/B then B/C order. Deterministic local integration fixtures separately prove:

- two TOCs share one exact MRF URL and create one download/parse;
- the same source participates in required snapshots without plan-dependent
  parsing; and
- A/B followed by B/C attaches A/B/C while only C is new in the second batch.

The real run proves current external compatibility and representative sizing;
the deterministic suite proves identities and edge cases.

## Final documentation

Update root `README.md` and `requirements/DESIGN.md` so they describe the
implemented version 1 contract rather than future placeholders. README must
include:

- component/version prerequisites and PostgreSQL 15+;
- build, environment, migration, worker, discovery, reconcile, and retry
  examples;
- manual `mrfenricher`/taxonomy procedure and catalog pinning warning;
- the initial `--limit 1` first live run, then two, then five, with ten only
  after measuring fan-out and disk, plus a disk-monitoring warning;
- exact stage flow and one-worker deployment rule;
- exact URL deduplication and why shared MRFs parse once;
- parser plan independence and additive consumer plan attachment;
- A/B then B/C example;
- snapshot plan-readiness rule, and that serving must wait for that
  PostgreSQL-derived state when planless rates should not be visible;
- crash/retry/reconciliation behavior;
- retention and backup guidance for PostgreSQL, the shared parsed MRFs, and
  the append-only warehouse;
- redacted status-query examples plus a clearly labeled authorized URL-debug
  query;
- optional external River UI: `RIVER_SCHEMA=mrfpipeline_river`, pause/resume
  only, do not cancel/retry/delete jobs, pause does not stop in-flight work;
- throttled stderr `progress` logs for downloads, MRF parse, consumer ingest,
  and TOC parse stages;
- a prominent warning that source-based feeds are not logical cross-month
  networks, so `current_*` views can double-count after URL rotation, and
  serving must use an explicit collection month or a single-month warehouse
  until a curated feed map exists; and
- explicit limitations: UHC only, local storage, no automatic enrichment, no
  plan removal/correction, no recurring discovery, sticky collection month,
  required `--limit`, and no UI in this binary.

Documentation must not embed a real database URL, local developer path, payer
source URL, signed token, provider NPI, taxonomy selection, or plan value.

The design's story-status wording must make clear that Stories 01–13 specify
the full version 1 implementation sequence. Remove the deferred-decisions
section once this story is incorporated.

## Errors and redaction

Add fixed safe classifications:

```text
worker_lease_unavailable
worker_lease_lost
river_terminal_without_domain_result
reconciliation_database_failed
artifact_reconciliation_failed
retry_stage_not_failed
retry_stage_invariant
```

Configuration and syntax remain Story 01 errors; database and River failures
remain discoverable through the established sentinels; context cancellation
remains the context error.

Reconciliation/retry diagnostics and logs may include fixed command/event,
job kind, safe code, numeric River job ID/attempt, duration, and aggregate
counts. They must not include domain IDs in routine worker logs, URLs, paths,
plans, provider/service values, manifest contents, SQL, database coordinates,
raw River state rows, external errors, or response bodies.

## Required tests

### Unit tests

- Final CLI syntax/help/configuration for `reconcile` and `retry` is exact.
- Lease uses the literal two-key constant, fails without mutation on
  contention, and never logs its values.
- Every stage maps to exactly one table/status/job-ID/argument field.
- Reconciliation transition table is exhaustive and rejects unknown River or
  lifecycle states.
- Missing successor/current jobs insert transactionally and are idempotent.
- Discarded/cancelled jobs become terminal rather than receiving silent extra
  attempts.
- Explicit retry accepts only one compatible failed stage, clears only its
  terminal state, retains identities/artifacts, and inserts one new job.
- Attachment retry retains the frozen batch ID/items and blocks newer plans.
- Cleanup eligibility and non-following path checks are exact for every
  removable/retained artifact class.
- Aggregate reports and logs contain only allowlisted fields.

### PostgreSQL/River/filesystem integration tests

- Two work processes compete and exactly one acquires the lease; the loser
  starts no worker or reconciliation mutation.
- Lease connection loss cancels River and prevents a subsequent consumer call.
- Repeated startup reconciliation converges and the second pass changes zero
  rows.
- Every missing successor and pruned/missing current job is recreated once.
- Missing download input for a nonterminal parse safely restores the exact
  download predecessor; invalid manifest-present artifacts and terminal parses
  remain untouched.
- Orphaned running/completed River states converge through a replacement while
  the old job becomes a stale no-op.
- Cancelled/discarded River state becomes a failed domain stage and waits for
  explicit retry.
- Retrying each production stage preserves its numeric identity and executes
  through the normal idempotent worker.
- Failed attachment retry reuses one batch and then schedules plans that
  arrived behind it.
- Crash injection at every download, parser manifest, consumer snapshot
  rename, plan-part rename, and database-success boundary converges without
  duplicate domain rows or unsafe deletion.
- Cleanup removes only proven successful TOC/MRF downloads, imported TOC
  output, succeeded plan JSON, and old safe staging entries.
- Cleanup retains parsed MRF output, failed artifacts, all warehouse data, and
  unsafe/symlinked targets. Staging entries younger than 24 hours remain.
- Redacted readiness/status queries match the A/B then B/C warehouse outcome.
- The authorized URL-debug query is documented separately and is not used by
  default status examples.

### Optional real acceptance

- Harness remains skipped without the explicit opt-in environment.
- Safety guards reject non-disposable databases, overlapping roots, symlinks,
  nonempty unmarked roots, and limits above ten before network or deletion.
  Omitted `MRFPIPELINE_REAL_TOC_LIMIT` defaults to one.
- An authorized bounded run exercises discovery through attachment and emits
  only aggregate local results.

Run the deterministic suite:

```text
go test ./...
go test -race ./...
go vet ./...
```

Run real acceptance only with the documented explicit environment and operator
supervision.

## Acceptance criteria

- One database/warehouse has at most one supported active worker process.
- Startup automatically repairs safely inferable nonterminal gaps without
  bypassing terminal failure or changing domain identity.
- Operators can reconcile idempotently and retry one exact failed stage with a
  fresh River attempt series.
- Interrupted stages converge through their existing artifact/publication
  boundaries without duplicate TOC, source, snapshot, plan, batch, or
  warehouse rows.
- Retention frees completed TOC/download/plan-input intermediates while keeping
  shared parsed MRFs and the append-only warehouse.
- A guarded live run defaults to one TOC, then progresses through two and
  five, with ten as the measured maximum; deterministic fixtures prove
  shared-MRF and additive-plan semantics.
- README and design describe the complete operable version 1 system and its
  limitations before UI work begins.
- No recovery, retry, cleanup, or reporting identity depends on content
  hashing.

## Non-goals

- Automatic retry of terminal failure without an explicit operator command.
- Direct modification of River-owned rows or destructive database repair.
- Recurring discovery or a payer-wide completion barrier.
- Automatic provider enrichment or taxonomy changes.
- Plan removal/correction or warehouse migration.
- Deleting shared parsed MRF output or consumer warehouse parts.
- Distributed multi-host fencing, several warehouses, S3 artifacts, or a UI.
