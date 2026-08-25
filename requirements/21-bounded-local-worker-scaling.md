# Story 21: Bounded MRF materialization and local worker scaling

## Status

Implemented.

## User story

As an operator, I want to admit a small cumulative number of MRF sources, bound
their shared local-disk residency, and scale parser and consumer workers as
ordinary River processes so that I can run a controlled local test and later
increase throughput without duplicating or losing work.

## Context

Stories 14-20 provide the end-to-end pipeline, durable retries, reconciliation,
and release safety. The first live run needs two additional controls:

1. Admit only an explicit number of MRF sources so an operator can process a
   small sample, inspect it, and later admit more without rediscovering or
   resetting the month.
2. Bound the number of downloaded or in-progress MRF files occupying local
   disk, while allowing parsing and consumption to scale across several
   long-lived local Docker containers.

The pipeline must retain the existing staged jobs and direct Go package calls.
This story must not introduce parser subprocesses, per-job containers, content
hashes, or a distributed filesystem design.

The existing discovery `--limit` remains a TOC-URL admission limit. It does
not become an MRF limit and it may be used together with the new
`--mrf-source-limit`. MRF jobs that TOC import currently inserts immediately
must instead be deferred until source admission and resident capacity permit
them.

## Dependencies

- Stories 14-20.
- The pinned `mrfparser` package and its current process-wide memory-limit
  contract.
- A pinned `mrfconsumer` revision that explicitly supports concurrent writers
  for different output IDs after serialized warehouse initialization. The
  current consumer contract, which requires all writers to one warehouse to be
  serialized, is not sufficient for concurrent consumer workers.

## Goal

- Keep discovery complete while admitting only an operator-selected number of
  physical MRF sources for download and parse.
- Allow the operator to increase that cumulative source total or change it to
  `all` without duplicating work.
- Enforce one durable, shared capacity limit across MRF download and parse.
- Scale parsing by running multiple long-lived MRF worker processes or
  containers, with at most one parse executing in each process.
- Scale consumption safely by output ID after the consumer dependency supports
  that concurrency contract.
- Prevent concurrent or rescued River deliveries from processing the same
  mutable domain at the same time.
- Preserve clear retry, reconciliation, and failure visibility.
- Document a simple local Docker topology using PostgreSQL and a shared local
  volume.

## Non-goals

- Running pipeline stages on different physical computers.
- Cloud, object, S3-compatible, or network-shared artifact storage.
- Merging independently produced warehouses or consumer outputs.
- Invoking the parser through a CLI or child subprocess.
- Starting a container for every River job.
- Making `mrfparser.Parse` safe for concurrent calls inside one process.
- Exact byte-based disk quotas or filesystem free-space prediction.
- Bounding retained parsed output or consumer warehouse size.
- Automatic horizontal scaling or container orchestration.
- Recurring discovery or a query-serving API.
- Adding checksums or SHA-256 validation where the upstream artifacts do not
  already provide and require them.

## Terminology

- **Source admission target**: the cumulative number of distinct MRF source
  records associated with a release that the operator has selected for
  materialization, or `all`.
- **Selected source**: a source admitted by that target. Selection is durable
  and never silently transferred to another source after failure.
- **Resident capacity**: the maximum number of selected physical MRF sources
  that may simultaneously hold a download/parse slot.
- **Materialization slot**: a durable PostgreSQL record or equivalent durable
  ownership state acquired before download. Successful materialization releases
  it only after parse is complete and the raw file has been deleted. A terminal
  download with no remaining raw or staging artifact is the explicit exception
  because it no longer occupies disk.
- **Output ID**: the immutable parsed-output identity used by the consumer
  pipeline and its writer lock.

The source admission target and resident capacity are independent. For
example, a target of `3` and resident capacity of `2` selects three sources but
allows only two raw/in-progress materializations at once.

## Worker roles

### Command

`mrfpipeline work` must accept an explicit worker role:

```text
mrfpipeline work --role control
mrfpipeline work --role mrf
mrfpipeline work --role consumer
```

Each process registers only the queues belonging to its role. A process must
reject an unknown role before starting River.

### Control worker

The control role owns:

- discovery;
- TOC download, parse, and import;
- source admission scheduling;
- materialization-slot scheduling and recovery;
- reconciliation; and
- release activation coordination.

Exactly one control worker may execute against one pipeline database. It must
hold a PostgreSQL advisory lease for its lifetime. A second control worker must
exit with the existing sanitized `worker_busy` behavior and must not mutate
pipeline or attempt state.

Control must acquire this lease before any mutating artifact-root
initialization or cleanup. A losing control process may perform configuration
and read-only validation needed to connect, but it must not call mutating
artifact initialization before returning `worker_busy`.

Only the control role may perform global initialization, cleanup, and
reconciliation. MRF and consumer workers must never initialize or clean the
shared artifact root. They perform read-only validation that the root was
initialized by control before accepting jobs.

### MRF worker

The MRF role owns only:

- `mrf.download`; and
- `mrf.parse`.

The role must call the pinned parser Go package directly. It must not invoke a
parser CLI or create a parser subprocess.

Each MRF worker process must allow at most one `mrf.parse` job to execute at a
time because the parser currently changes a process-wide Go memory limit.
Parsing concurrency is obtained by running multiple MRF worker processes or
containers. Download work may overlap a parse when the process and configured
queues permit it, but a process must never run two parser calls concurrently.

Each MRF worker may mutate only the artifact directory belonging to the source
whose job it owns and uniquely named `.staging` entries for that source. It may
not clean unrelated source or staging entries.

The parse worker must give `mrfparser` a source-owned temporary parent beneath
the shared `.staging` root, using the existing numeric source identity in the
safe internal name. Anonymous `mrfparser-*` invocation directories created by
the package must therefore be nested under that owned parent. Control can then
map cleanup to the source execution lock; it must never infer ownership from
age alone for an anonymous directory directly under the shared staging root.

### Consumer worker

The consumer role owns only:

- `consumer.ingest`; and
- `consumer.attach_plans`.

Each consumer process executes at most one consumer mutation at a time.
Concurrency is obtained by running multiple consumer processes or containers.
Concurrent consumer workers must remain disabled until the pinned consumer
dependency satisfies the contract in **Consumer concurrency** below.

### Long-lived processes

All three roles are ordinary long-lived River workers. Docker Compose starts
and supervises the processes; River claims many jobs over each process's
lifetime. The pipeline must not create a new process or container for each job.

## Source admission

### Complete discovery, bounded execution

TOC import must continue to record all discovered MRF sources, snapshots, and
plan associations in PostgreSQL. A release may therefore contain thousands of
known sources while only a small explicit subset is eligible for MRF download
and parse.

An unselected source must:

- remain durably visible as blocked by the source limit;
- have no MRF River job;
- consume no materialization slot; and
- accumulate no retry attempts.

`pending` means that the source is selected and has an actual current River job
for its current stage. Waiting for source admission or resident capacity is not
a failed or stale attempt.

MRF download state must support a non-executable `blocked` state. Selection
state distinguishes `source_limit` from `resident_capacity` and an explicit
retry waiting for capacity. Waiting conditions may be derived from the durable
selection/slot relation, but an MRF stage without a current executable River
job must not remain `pending`.

### Initial target

Discovery that creates a new monthly release must require an explicit MRF
source target:

```text
mrfpipeline discover ... --mrf-source-limit <N|all>
```

`N` must be a positive integer. Omitting the option for a new release must fail
before discovery starts; the pipeline must not accidentally default a first
live run to unlimited MRF processing. `all` selects all sources known for the
release and also selects sources added by later TOC imports.

For an existing release with a numeric or `all` target, omitting the option
reuses its durable target. Supplying a different value fails without mutation;
`set-total` is the only way to increase an existing target. An existing
migrated release whose target is `unset` must receive either this option or a
prior `set-total`; omission while still `unset` fails without discovery.

The configured policy is durable PostgreSQL state and remains authoritative
after process restarts or configuration changes.

### Existing-release migration

The Story 21 migration must be applied with the old single worker stopped.
Existing releases must not silently inherit an unlimited scheduling policy.

- Active and inactive releases are sealed and may be recorded as `all` without
  creating new work.
- A building release initially has target `unset`. No newly discovered source
  is selected until the operator runs `set-total`.
- A source whose MRF external work has already begun, including a running,
  retrying, succeeded, or failed domain attempt or owned artifact, is
  backfilled as selected so the migration does not discard work or artifacts.
- An old unstarted MRF River job for an unselected source becomes an admission
  no-op/cancelled delivery and must not begin external work. Migration or its
  first safe claim normalizes the domain to non-executable `blocked` and clears
  the old current-job identity. It must not leave `download_status=pending`
  pointing at a cancelled or stale job. The control scheduler later creates
  the correct job if that source is selected.
- The same normalization applies to every unselected, non-started source found
  with `download_status=pending` and no current River job, even when no old
  River row can be found.
- Existing raw files, owned download staging, running MRF work, and failed
  parses with retained raw input are backfilled as durable slot holders before
  new scheduling begins. A terminal download with no remaining raw/staging
  file does not hold a slot.
- `set-total N` for an `unset` release must be at least its backfilled selected
  count. `set-total all` is always allowed.

Status must show `unset` until the operator chooses a target. Selected
pre-migration work may finish, but no additional source is admitted and the
release cannot activate while its target is `unset`.

The first Story 21 deployment starts control before MRF or consumer replicas.
If backfilled slot holders exceed the requested resident capacity, control
must not schedule new materialization and must fail startup with the held and
requested safe counts. The operator must restart it with capacity at least
equal to the backfilled held count; migration must not delete existing
artifacts to force them under a newly chosen limit.

### Increasing the cumulative target

The operator must be able to increase a release's cumulative target:

```text
mrfpipeline month sources set-total \
  --payer <payer> \
  --collection-month <YYYY-MM> \
  --total <N|all>
```

Examples:

- changing `3` to `8` selects exactly five additional sources when at least
  five remain;
- repeating `8` is a no-op;
- changing `3` to `all` selects every remaining source and any source imported
  later.

A numeric target may only increase. Lowering it must fail without changing
selection, jobs, slots, attempts, or release state. `all` cannot be changed
back to a number.

The command must be transactionally safe when invoked concurrently. It returns
the old target, new target, number newly selected, and safe aggregate counts.
It must not print source URLs or local paths.

### Stable selection

New selection must be deterministic: order eligible source records by exact
source URL ascending and use numeric source ID as the tie-breaker. Selection is
durable; restarts, reconciliation, and repeated commands must not choose a
different subset.

The target counts distinct `mrf_sources` associated with the release, not plan
records, snapshot records, TOCs, or reporting-plan associations. A source
already selected or completed by another release still counts as one of the
release's selected source records but must reuse the same physical work and
must not create a duplicate download.

Sources that already have valid parsed output may immediately advance their
dependent consumer stages without acquiring a new materialization slot. Status
must distinguish newly materialized selected sources from reused selected
sources.

### Failure behavior

A selected source that fails remains selected. The scheduler must retry or wait
for operator retry on that exact source; it must not silently select a
replacement to keep the number of successes equal to the target.

For a numeric target smaller than the number of known sources, the release is
intentionally partial and cannot activate. Unselected known sources are an
explicit readiness blocker even when every selected source and consumer job has
succeeded.

With a numeric target whose selected count is below `N`, later TOC imports must
select newly available sources up to `N` in stable order. Existing selections
are never reshuffled. With target `all`, later TOC imports must select all new
sources. The scheduler must eventually materialize newly selected work subject
to resident capacity.

Increasing a target, importing additional eligible sources, releasing a slot,
or accepting a download retry must durably signal the control scheduler. The
control worker must refill eligible work without restart, using one coalesced
control job or an equivalently small durable wake-up mechanism. It must not use
one waiting River job per source.

## Resident materialization capacity

### Durable configuration

The control worker must require one explicit positive
`MRFPIPELINE_MRF_RESIDENT_CAPACITY` value for the artifact root it manages. MRF
and consumer worker processes do not configure this value.

While holding the singleton control lease, control stores the requested value
as authoritative PostgreSQL state before scheduling. To change it, the
operator stops only the control worker, changes the value, and restarts
control; MRF and consumer workers may remain running. Increasing it may
immediately fill new slots. Decreasing it below the number of currently held
slots must fail before changing the stored value or scheduling. A safe decrease
to at least the held count may take effect on restart.

### Slot lifetime

The control scheduler acquires a durable materialization slot for a selected
source before inserting its `mrf.download` job. The same source retains the
same slot across download and parse stages.

For a successful materialization, the slot is released only when all of the
following are true:

1. parsed output has passed the existing validation required to mark parse
   successful;
2. the final raw MRF file is absent; and
3. download staging files for that source are absent.

The parse worker must delete the raw download after successful parsed-output
publication and validation. It then records deletion and completion in the
same durable completion protocol used to let the control scheduler release the
slot. A new source may not begin downloading before that release.

The only pre-parse release is a terminal download failure after cleanup proves
that no final raw or owned staging file exists. That source remains a selected
failure and is not replaced beyond the already selected target.

The number of simultaneously held slots must never exceed resident capacity,
including with multiple control clients, retries, rescued River jobs, and
process crashes.

### Waiting work and River job count

Selected sources that do not yet have a slot remain visibly waiting for
capacity. They must not have placeholder MRF River jobs. When a slot is
released, the control scheduler selects the next waiting source in the same
stable order, acquires its slot, and inserts only the job for its current stage.

The pipeline must not enqueue thousands of blocked MRF jobs merely to have
workers snooze them. River jobs represent executable work, while PostgreSQL
pipeline state represents admitted work waiting for capacity.

### Retries and partial files

Download and parse remain separate River job kinds. Together with their slot,
they form one logical materialization unit.

- A failed or interrupted download keeps its source's slot across automatic
  retries. Existing staging cleanup and resumability rules continue to apply.
- A completed download followed by a failed parse keeps the raw file and slot
  so parse can retry without downloading again.
- A terminal download failure keeps the source selected. If cleanup confirms
  that no final raw or staging file remains, it releases its slot; retry later
  waits for and reacquires a slot before inserting a new download job. If any
  raw or staging file remains, it keeps the slot.
- A terminal parse failure keeps the source selected, raw file, and slot until
  the operator retries that parse. It is never silently replaced by another
  source.
- A successful parse whose raw deletion fails must fail or remain incomplete
  with a fixed sanitized failure code. It keeps its slot and retries deletion;
  it must not reparse valid output merely to retry cleanup.
- If all slots are held by failed sources, new downloads stop. Status must make
  that condition obvious rather than bypassing the capacity limit.

Parsed outputs and successfully consumed warehouse data are not deleted when a
slot is released. Resident capacity bounds raw and in-progress MRF
materialization, not all disk usage.

## Cross-process execution locks

River uniqueness does not by itself prevent a rescued or manually retried job
from overlapping an older delivery. Before calling the existing `jobs.Run`
claim/work/confirm lifecycle for a mutable MRF or consumer stage, the delivery
wrapper must acquire a PostgreSQL advisory execution lock on a dedicated
connection. It holds that connection and lock through claim, external work,
and confirmation, then releases both.

### MRF lock domain

MRF download and parse use the same lock domain keyed by physical source ID. It
must prevent two processes from downloading, parsing, publishing, deleting, or
confirming the same source concurrently while still allowing different sources
to execute concurrently.

### Consumer lock domain

Consumer ingest and plan attachment use a lock keyed by output ID. All mutation
stages for one output ID serialize with each other across processes. Different
output IDs may execute concurrently after warehouse initialization and after
the pinned consumer contract permits it.

Parsed source output is immutable and may be read concurrently by different
consumer output IDs. The writer lock protects the consumer output/warehouse
mutation boundary, not every read of the parsed source.

### Lock behavior

- An unavailable execution lock causes the delivery to defer or snooze without
  incrementing pipeline domain-attempt state and without recording a stage
  failure. River may increment its own delivery attempt when it redelivers.
- Loss of the PostgreSQL connection holding the lock must cancel the work and
  classify it as an interruption. The delivery must not confirm success after
  lease loss.
- Retry and reconciliation must obey the same locks.
- Advisory lock namespace values and derived lock keys must never appear in
  logs or CLI output.
- Existing release gates and sealed-release protections continue to apply in
  addition to these execution locks.

The current process-global worker advisory lease must be narrowed or replaced:
one singleton lease protects the control role, while replicated MRF and
consumer workers are allowed and depend on their per-domain execution locks.

## Live operator-command coordination

- `reconcile` is a temporary control operation. It acquires the singleton
  control lease, so a long-lived control worker must be stopped first. MRF and
  consumer workers may remain running; reconciliation must obtain each domain
  execution lock before repairing or cleaning that domain and must skip a busy
  domain without changing its state.
- `retry` may run while all worker roles are running. It obtains the release
  and exact domain execution locks before changing domain or River state. A
  busy domain returns a fixed sanitized busy result without mutation. Retrying
  a terminal download with no slot moves it to selected/waiting-for-capacity;
  retrying a terminal parse retains its existing slot and raw file. When no
  slot is available, retry records a durable retry request, sets the stage to
  non-executable `blocked`, clears its current job identity, and creates no
  placeholder River job. Control changes it to `pending` and inserts the exact
  job only after acquiring a slot.
- `month activate` may run while workers are running. It coordinates through
  the existing deterministic release-row locking and Story 20 execution-time
  release gates. Either activation seals a ready release before a new claim,
  or an in-progress claim makes readiness fail; external work must not cross a
  successful seal.

Unlike Story 20's single-worker topology, `retry` and `month activate` must stop
acquiring the singleton control lease. Their release-row and domain locks are
their coordination mechanism. `reconcile` remains the command that acquires
the control lease.

Global artifact cleanup performed by control or `reconcile` must map an owned
staging entry to its domain and obtain that domain lock before removal. Existing
rules for truly orphaned, safely identifiable staging entries remain in force.

## Consumer concurrency prerequisite

Before enabling more than one consumer worker, the consumer repository must
define and test this pinned contract:

1. Warehouse initialization is serialized and idempotent. It establishes the
   warehouse metadata, pinned provider catalog, fixed directories, and plan
   schema seed once.
2. After initialization, ingest and plan attachment for different output IDs
   may overlap safely in one warehouse.
3. Ingest and plan attachment for the same output ID must not overlap.
4. Existing uniqueness scans or final rechecks are not treated as the
   concurrency protocol; the consumer must provide an actual safe transaction
   or writer protocol for distinct outputs.
5. Failure of one output must not expose partial committed state as another
   output's success.

The pipeline must pin the first consumer revision satisfying this contract and
must keep consumer concurrency at one until then. No pipeline-side lock may
claim to make an unsupported consumer implementation safe for distinct-output
concurrency.

Artifact-root initialization belongs to control. Consumer warehouse
initialization is separate and belongs to the consumer's serialized,
idempotent initialization protocol; control must not impersonate a consumer
writer to initialize the warehouse.

## Reconciliation and recovery

Only the control worker reconciles. Reconciliation must preserve source
selection, resident capacity, and per-domain locking.

For each selected source with a materialization slot:

- valid parsed output and no raw/staging files: repair stage success if needed,
  release the slot, and schedule the next selected source;
- valid downloaded raw file and incomplete parse: retain the slot and restore
  only the parse job;
- download staging without a valid final raw file: apply the existing staging
  cleanup/resume rules, retain the slot, and restore only the download job;
- valid parsed output plus a raw file left after cleanup failure: retain the
  slot and restore cleanup/completion without reparsing;
- terminal stage failure: retain the slot and report it without silently
  selecting or scheduling a replacement, except that a terminal download may
  release the slot after proving that no raw or staging artifact remains.

Reconciliation must never:

- select beyond a numeric source target;
- exceed resident capacity;
- enqueue an MRF job for an unselected source;
- initialize or clean the artifact root from an MRF or consumer worker; or
- bypass the building-release gate or sealed-release suppression.

If durable state claims a slot is free while artifacts prove that an unfinished
raw materialization exists, reconciliation must fail closed, report a fixed
safe inconsistency code, and avoid scheduling another source into that slot
until the inconsistency is repaired.

## Release readiness and activation

Release readiness must include the new bounded-work states:

- any known unselected MRF source blocks activation;
- any selected source waiting for a slot blocks activation;
- any held materialization slot blocks activation until its source succeeds and
  raw cleanup completes;
- all existing TOC, MRF, consumer, plan-attachment, validation, and failure
  checks remain required.

A deliberately bounded numeric sample is therefore runnable and consumable but
not activatable. Changing its target to `all` and successfully draining all
work can make it activatable.

## Operator commands and observability

### Status

Extend the existing authoritative monthly status command:

```text
mrfpipeline month status \
  --payer <payer> \
  --collection-month <YYYY-MM>
```

It must report at least:

- source target (`unset`, `N`, or `all`);
- known distinct sources;
- selected sources;
- selected sources reusing valid physical output;
- blocked-by-limit sources;
- selected sources waiting for resident capacity;
- resident capacity, held slots, and free slots;
- download pending, running, retrying, and terminal failure counts;
- parse pending, running, retrying, and terminal failure counts;
- raw-cleanup failure count;
- parsed-success count;
- consumer pending, running, retrying, failed, and successful counts; and
- whether activation is blocked specifically because the release is partial.

Routine status output and logs must not include source URLs, query strings,
local artifact paths, advisory keys, or upstream payload fragments.

### Structured logs

Worker logs must include safe fields for role, queue, River job ID, domain ID,
attempt number, result, and fixed failure/interruption code. They must make it
possible to distinguish:

- blocked by source target;
- selected but waiting for resident capacity;
- deferred because another delivery holds the execution lock;
- download or parse retry;
- terminal failure holding a slot;
- raw cleanup preventing slot release; and
- successful slot release and refill.

These are state distinctions, not reasons to log sensitive URLs or paths.

## Docker and local deployment documentation

Add a documented local Compose topology with:

- one control worker container;
- one or more identical MRF worker containers;
- one or more consumer worker containers after the consumer concurrency
  prerequisite is satisfied;
- one PostgreSQL database reachable by every container; and
- one shared local Docker named volume mounted at the same artifact-root path
  in every pipeline container.

The control worker initializes and reconciles the shared volume. MRF workers
write only their owned source directories. Consumer workers read immutable
parsed outputs and mutate the shared warehouse under the consumer protocol.

Documentation must explain:

- four MRF worker containers yield up to four concurrent parser calls;
- each container may set its own `GOMAXPROCS` or Docker CPU allowance;
- Docker CPU limits bound scheduling time but do not make concurrent parser
  calls inside one process safe;
- the parser's memory limit remains per process, not a shared limit across
  containers;
- an optional Docker memory cap may terminate a container at the cap and is not
  a soft backpressure mechanism;
- resident MRF capacity bounds raw/in-progress files but not retained parsed
  output, temporary peak expansion, PostgreSQL, or the consumer warehouse; and
- on Windows Docker Desktop, a Linux Docker named volume is preferred over a
  cloud-synchronized host directory for this first local run.

The example must not require Kubernetes, a job-per-container launcher, or an
external queue beyond River/PostgreSQL.

## Failure semantics

| Failure | Required behavior |
| --- | --- |
| Control lease unavailable | Second control exits `worker_busy`; no mutation. |
| MRF/consumer execution lock busy | Delivery defers without pipeline domain-attempt or failure mutation. |
| Execution-lock connection lost | Cancel work and classify as interruption; never confirm success. |
| Download fails partway | Existing staging cleanup/resume applies; selection and slot remain. |
| Parse fails | Raw file and slot remain for retry; no redownload. |
| Raw deletion fails after parse | Parsed output remains; retry cleanup; slot remains; do not reparse. |
| Worker/container crashes | River redelivers; control reconciles artifacts and durable slot ownership. |
| Download reaches terminal failure | Keep selection; release the slot only after all raw/staging files are absent; no replacement. |
| Parse reaches terminal failure | Keep selection, raw file, and slot until retry; no replacement. |
| All slots are held by failures | New materialization stops and status reports the cause. |
| Consumer output fails | Only that output retries; no other output is reported successful because of it. |
| Numeric target leaves sources unselected | Selected work may drain and be inspected, but activation remains blocked. |
| Capacity is below held slots at control startup | Control leaves the stored value unchanged and exits with a fixed sanitized configuration error. |

## Acceptance criteria

### Source admission

1. Discovery records all sources but selects only the explicit numeric target.
2. A numeric target of `3` creates executable MRF work for no more than three
   deterministically selected distinct source records.
3. Moving the target from `3` to `8` selects five more; repeating `8` selects
   none; concurrent invocations do not over-select.
4. A target cannot decrease and `all` cannot revert to numeric.
5. Target `all` selects sources imported later without another operator command.
6. Failed selected sources are not replaced by other sources.
7. Valid already-materialized sources reuse their physical output and do not
   download again.
8. Unselected sources have no MRF River jobs and no attempt records.
9. A numeric target below the known-source count and a numeric target above the
   currently known-source count both behave correctly when later TOC imports
   add sources; selection fills only the target remainder and never reshuffles.

### Resident capacity

1. With resident capacity `N`, concurrent schedulers and workers never produce
   more than `N` held materialization slots.
2. A slot is acquired before download and remains held through download and
   parse retries.
3. Successful parse does not release the slot until the raw and staging files
   are confirmed absent.
4. Releasing a slot schedules at most one next waiting source per newly free
   slot.
5. A parse retry reuses the downloaded raw file.
6. A cleanup-only retry does not rerun a successful parse.
7. Crash reconciliation restores the correct stage without exceeding capacity.
8. A terminal download with no remaining files releases capacity but remains a
   selected failure; retry waits for a new slot.
9. Increasing capacity fills new slots; an unsafe decrease is rejected.
10. Backfilled pre-Story-21 materializations occupy slots before new work, and
    startup fails safely when requested capacity is below that count.

### Multi-worker safety

1. A second control worker is refused without mutation.
2. Multiple MRF workers can start and different sources can parse concurrently.
3. No MRF worker process executes two parser calls concurrently.
4. Duplicate/rescued deliveries for the same source cannot overlap mutable
   work, while different sources can overlap.
5. Losing an execution-lock connection interrupts work and prevents success
   confirmation.
6. Each role registers only its allowed queues.
7. MRF workers do not perform global artifact initialization, cleanup, or
   reconciliation.

### Consumer safety

1. The pinned consumer dependency documents and tests safe concurrent writes
   for different output IDs after one-time initialization.
2. Pipeline tests prove different output IDs can overlap while all ingest and
   plan-attachment work for one output ID serializes across processes.
3. Warehouse initialization occurs exactly once under concurrent startup.
4. The pipeline refuses or documents single-worker operation when pinned to a
   consumer revision without the distinct-output concurrency contract.

### Readiness and observability

1. A numeric partial sample can finish all selected parse and consumer work but
   cannot activate while known sources remain unselected.
2. Changing the target to `all`, draining all work, and satisfying existing
   validation makes the release eligible for activation.
3. Status exposes selected, blocked, waiting, slot, stage, failure, cleanup,
   consumer, and partial-release counts without sensitive data.
4. Logs distinguish capacity waiting, lock deferral, stage failure, cleanup
   failure, and slot release using fixed safe fields.

## Required tests

### Unit and repository tests

- Numeric and `all` source-target parsing and validation.
- Deterministic selection and ID tie-breaking.
- Idempotent cumulative target increases and rejected decreases.
- Existing-release migration, `unset` behavior, and admission no-op for old
  unstarted jobs.
- Existing pending-row normalization and slot backfill from running/retained
  artifacts.
- Source-owned parser temporary parents and lock-aware cleanup of their nested
  invocation directories.
- Failed selected source not being replaced.
- Later TOC import filling an `all` target.
- Later TOC import filling the remainder of a numeric target without
  reshuffling existing selections.
- Durable slot acquire/release and resident-capacity invariants.
- Raw cleanup gating slot release.
- Role-to-queue registration and one-parse-per-process configuration.
- Sanitized status and structured logging.

### PostgreSQL integration tests

- Concurrent `set-total` calls cannot over-select.
- Concurrent control schedulers cannot exceed capacity or assign one slot
  twice.
- A second control lease is refused.
- Different MRF source locks can coexist; the same source lock cannot.
- Busy locks defer without attempt mutation.
- Running-worker coordination for `retry`, `month activate`, and control-lease
  exclusion for manual `reconcile`.
- A capacity-blocked download retry records a durable request but creates no
  River job until control acquires a slot.
- Lock connection loss cancels a running worker path.
- Crash/reconciliation cases listed in **Reconciliation and recovery**.
- Output-ID consumer lock serializes ingest and plan attachment for the same
  output while allowing different outputs to overlap.
- Release activation remains blocked for a numeric partial target.

### Dependency and concurrency tests

- Upstream consumer tests demonstrating safe distinct-output warehouse writes.
- Pipeline tests against the pinned consumer revision, including concurrent
  ingest, concurrent plan attachment, and a mixed ingest/attach workload.
- Race-enabled tests for pipeline-owned concurrent state.
- Existing `go test`, `go vet`, PostgreSQL, and DuckDB integration suites.

### Live acceptance

Add a bounded first-run acceptance procedure that:

1. discovers a real monthly release with numeric source target `N`;
2. starts one control worker and multiple MRF workers;
3. proves no more than resident capacity raw/in-progress MRF sources exist;
4. drains parse and consumer work for the selected sources;
5. verifies raw files were removed after successful parse;
6. verifies parsed outputs and consumer results remain available;
7. verifies the partial release is not activatable; and
8. records safe counts and timing without URLs, paths, payloads, or advisory
   keys.

This bounded procedure supplements, but does not replace, the full Story 20
live acceptance run. The full run uses target `all`, drains every source, and
proves activation and serving behavior.

## Implementation guidance

- Prefer small PostgreSQL state additions for durable source selection and slot
  ownership over encoding waiting work as River jobs.
- Preserve the existing `mrf.download` and `mrf.parse` workers and their
  observable retry boundaries.
- Use River as the durable delivery mechanism and PostgreSQL as the authority
  for admission, capacity, ownership, and coordination.
- Keep configuration explicit and fail closed on conflicting control settings.
- Scale parsing and consumption by process count; do not add an internal
  process supervisor or parser subprocess protocol.
- Reuse existing artifact validation and atomic publication. Do not add
  speculative checksums, duplicate manifests, or filesystem protocols beyond
  what these correctness rules require.
