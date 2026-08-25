# Story 22: Runnable local acceptance and test convergence

## Status

Planned.

## User story

As an operator, I want a checkout that builds, starts, and exercises the
Story 21 topology using documented local inputs, and I want its PostgreSQL
integration suite to pass before a live payer run, so that the first bounded
test is reproducible and failures represent pipeline behavior rather than
missing packaging, stale documentation, or broken fixtures.

## Context

Story 21 implemented role-specific River workers, cumulative source admission,
shared materialization slots, and per-domain execution locks. Review of the
result found that the core commands exist but the repository is not yet a
complete first-run package:

- `docker-compose.story21.yml` references an image and configuration tree that
  the checkout cannot build or supply;
- operator help and historical design text still describe the old topology;
- a permanent control capacity error would be restarted indefinitely by the
  example Compose policy;
- the Compose health check recognizes a persistent file rather than a healthy
  control process;
- the PostgreSQL integration packages reset the same schemas concurrently;
  and
- when run serially, the database-enabled suite exposes Story 21 fixture and
  behavior regressions in admission, CLI activation and worker startup, parse
  completion, reconciliation, release readiness, TOC import, and role runtime.

The first observed test database was
`mrfpipeline_test_1`. The repository safety guard correctly requires the
`mrfpipeline_test_` prefix. The failures discovered there are requirements for
this story; they must not be hidden by skipping, weakening, or deleting the
tests.

## Dependencies

- Implemented Stories 14–21.
- A disposable PostgreSQL database whose name starts with
  `mrfpipeline_test_`.
- Existing private-module access for the pinned parser and consumer modules.
- An operator-supplied accepted provider catalog for a real consumer run.
- An operator-supplied service selector. Its machine-specific source path is
  supplied out of band and must not be committed or hard-coded.

## Goal

1. Make the documented local Docker topology buildable and runnable from this
   checkout after the operator supplies the explicitly external inputs.
2. Provide one exact, safe command path for migration, role startup, bounded
   discovery, status, target increase, retry, reconciliation, and shutdown.
3. Converge the complete PostgreSQL-enabled test suite on the Story 21 domain
   contract.
4. Make current CLI help and operator documentation authoritative without
   requiring the reader to reconcile it with historical single-worker text.
5. Make permanent startup configuration failures stable and visible rather
   than automatic restart loops.
6. State the real first-run limits: numeric samples are not activatable,
   consumer writes remain singleton, resident capacity is not a byte quota,
   retained outputs are not automatically deleted, and no query service is
   provided.

## Non-goals

- A query API or serving application.
- Activating a deliberately partial numeric MRF sample.
- Concurrent consumer writers before the pinned consumer contract supports
  them.
- Byte-accurate disk quotas, filesystem prediction, or a resource governor.
- Automatic deletion of parsed MRF output, warehouse output, or River history.
- A distributed filesystem, cloud artifact store, or multi-computer pipeline.
- Parser subprocesses, per-job containers, or a new orchestration system.
- Checksums, SHA-256 validation, or content-addressed storage.
- A universal River job timeout or automatic cancellation of slow but healthy
  downloads and parses.
- Bundling a fabricated provider catalog merely to make Compose appear
  self-contained.

## Runnable local topology

### Image build

The repository must contain the Docker build definition used by Compose.
`docker compose build` from the repository root must either build the exact
runtime image or fail before compilation with a clear instruction for
providing private-module credentials.

The build must:

- pin or derive the Go toolchain and runtime dependencies needed by the current
  modules;
- produce the `mrfpipeline` binary at the path used by every service command;
- avoid embedding Git credentials, database URLs, provider-catalog contents,
  or host filesystem paths in image layers;
- use an explicit BuildKit secret or SSH mechanism if private-module access is
  required inside the build; and
- document a host-build fallback only if the produced Linux binary is actually
  compatible with the runtime image and current native dependencies.

Do not add a custom image builder, release service, or registry workflow for
this local story.

### External configuration

The checkout must include a small `config/` contract or equivalent documented
mount layout. It must contain:

- a README describing the required accepted provider-catalog directory;
- a service-selector example with the real CSV header and at least one clearly
  labeled example row; and
- placeholders or ignored mount points that do not imply that an example
  provider catalog is valid for production ingestion.

Compose must accept operator paths through documented environment variables or
an ignored local `.env` file. It must not require editing the Compose YAML or
copying credentials into the repository. Machine-specific absolute paths must
not be committed.

Startup must fail with the existing sanitized configuration classifications
when the provider catalog or services file is missing or incompatible.

### Database and operator command access

Every pipeline container uses the Compose PostgreSQL service name internally.
The local recipe must also provide at least one exact supported way to run
operator commands against that network:

- execute the installed binary in an existing pipeline container;
- run a documented one-shot CLI service/container; or
- publish PostgreSQL to a loopback-only host port and document the distinct
  host URL.

The recipe may support more than one, but it must identify one preferred path.
It must include copy-paste commands for:

```text
migrate
work --role control
work --role mrf
work --role consumer
discover --payer uhc --collection-month <YYYY-MM> --limit <N> --mrf-source-limit <N|all>
month status --payer uhc --collection-month <YYYY-MM>
month sources set-total --payer uhc --collection-month <YYYY-MM> --total <N|all>
retry --stage <kind> --id <domain-id>
reconcile
```

The documentation must explain that `reconcile` requires the control process
to be stopped, while `retry` may coordinate with running roles.

### Service startup and failure policy

The Compose topology must preserve:

- one control process;
- one or more MRF processes, each with one parser call at a time;
- exactly one consumer process with the currently pinned consumer; and
- one shared named artifact/warehouse volume.

A permanent control startup error, including
`capacity_below_held`, must reach a stable exited state with its sanitized
error visible. The local example must not restart that error forever. The
simplest acceptable policy is no automatic restart for control; a bounded
restart wrapper is allowed only if it reliably distinguishes or stops after
permanent configuration failures.

A health check must not report control healthy solely because
`workspace.json` remains from an earlier process. Either implement a check of
the current control runtime/lease readiness or omit the misleading health
check and use startup/retry behavior that remains correct. Do not add a
heartbeat table only for Compose.

MRF and consumer workers must continue to validate, without globally
initializing, the artifact root established by control. Their restart policy
may recover ordinary process crashes, but configuration errors must remain
diagnosable from sanitized logs.

## Current operator contract

### Help and requirement status

Update executable help and generated/static documentation together:

- `retry --help` must say that retry may run while worker roles are active and
  coordinates through release and exact execution locks;
- `reconcile --help` must name the singleton control lease, state that control
  must be stopped, and state that MRF and consumer roles may remain running;
- Story 20 must be marked implemented; and
- examples must use `work --role ...`, never the obsolete all-in-one `work`.

Help tests must assert the current wording or its durable behavioral meaning so
the old instructions cannot silently return.

### Current design versus historical design

An operator must be able to find one concise current architecture description
covering Stories 14–22. Historical feed, schema 1.5.0, and single-worker design
material may remain for provenance, but it must be moved to a clearly named
historical document or separated so prominently that it cannot be mistaken for
the active contract.

The current document must directly describe:

- feed-free monthly releases;
- control/MRF/consumer roles and queue ownership;
- source admission and shared resident slots;
- per-source and per-output execution locks;
- singleton consumer behavior; and
- the active-output relation as a handoff rather than a query service.

Do not rewrite historical stories to pretend their original contracts never
existed.

### Obsolete queue API

Remove the unused all-in-one `work.Queues()` map, or rename and restrict it so
production code and tests cannot mistake it for the active topology.
`QueuesForRole` or a simpler equivalent remains the only runtime queue source.

## First-run expectations and resource limits

The first bounded live run validates discovery, selected download, parse,
consume, cleanup, and partial status. With a numeric source target below the
known count it must finish selected work and remain non-activatable with the
specific partial-source blocker. This is success for the bounded test, not a
failed activation test.

Activation is tested only after target `all` and every known source and
existing release gate has drained. The documentation must warn that this may
process the complete payer/month and should not be done merely to validate a
small sample.

The active PostgreSQL `(payer, collection_month, output_id)` relation is a
handoff contract. No HTTP, SQL query facade, or end-user serving layer is
created by activation.

Resident capacity counts raw/in-progress sources; it does not bound bytes.
The local recipe must tell the operator to use a small initial target and
capacity, inspect free space and peak artifact/warehouse growth, and stop
before crossing an operator-selected disk threshold. It must explicitly list
parsed output, warehouse output, PostgreSQL, temporary peak expansion, and
River history as outside the slot bound.

No automatic retention is added. Documentation must identify which parsed,
warehouse, and River data remains after success and require the operator to
use a separate, deliberate backup/retention procedure rather than deleting it
as part of normal slot release.

Infinite or unbounded job duration remains intentional. The existing redacted
stalled-work query must be exposed as a copy-paste operator command or a small
existing-CLI extension. It must report age, stage, fixed status, and numeric
identity without URLs, paths, payloads, or raw errors. Do not add a monitoring
service in this story.

## Connection and execution-lock bounds

Each role's PostgreSQL pool size must provide explicit headroom for:

- its configured concurrent River work;
- one dedicated advisory-lock connection per executing MRF or consumer job;
- River's own client/runtime connections; and
- lease health checks and completion bookkeeping.

The implementation may keep fixed conservative role-specific pool sizes. It
must not introduce adaptive pool sizing or resource scheduling. Integration
tests must start the supported per-process concurrency and prove work can claim,
check, and release execution locks without pool starvation.

Domain tables use positive `bigint` identities. Execution locking must not
reject otherwise valid identities merely because they exceed `MaxInt32`.
Replace the current limit with a deterministic, non-hashed PostgreSQL advisory
key mapping that distinguishes MRF source and consumer output domains and
supports the positive `bigint` identity range. The mapping must not use a
cryptographic hash or expose advisory keys in logs.

## PostgreSQL test convergence

### Canonical local commands

The repository must document and, preferably, expose through one small script
or Make target the exact verification commands:

```text
go test ./...
go vet ./...
go test -race ./internal/jobs ./internal/mrfparse ./internal/work ./internal/reconcile
MRFPIPELINE_TEST_DATABASE_URL=<disposable-url> go test -p 1 ./...
```

The database-enabled command uses `-p 1` because packages destructively reset
the same dedicated schemas. This is the chosen simple contract for this story;
do not build a schema allocator merely to recover package-level parallelism.
Individual tests may still exercise concurrency within a package.

Documentation must explain that the database name must start with
`mrfpipeline_test_` and that the suite drops its application, River, and test
schemas. A production, development, or ambiguously named database must be
rejected before destructive DDL.

### Existing failures to close

Fix the failures observed with the serial PostgreSQL suite, including:

- admission integration fixtures that send multiple SQL commands through one
  prepared statement;
- admission backfill/capacity reconciliation failures;
- activation fixtures missing Story 21 source targets, selections, slots, or
  other readiness state;
- role-worker startup tests that still assume the old all-in-one worker;
- parse-success tests that expect consumers to become executable without
  Story 21 selection/admission state;
- reconciliation execution-lock tests that unexpectedly classify healthy lock
  ownership as `stage_execution_interrupted`;
- release fixtures with invalid `selected_at` values, argument counts, or
  incomplete current readiness state;
- TOC-import tests that expect immediate MRF jobs rather than selected/slot
  scheduling; and
- the end-to-end role runtime test that times out or reads missing state.

Each fix must preserve or strengthen the asserted product behavior. Do not
change a correctness assertion to a row-exists smoke test, add arbitrary
sleeps, broadly accept multiple outcomes, or skip a failing package.

### CI and private modules

Add a repository CI workflow that can authenticate and run the private pinned
modules after its explicitly documented repository secret/deploy credential is
configured. The workflow must:

- use an explicitly named repository secret/deploy credential;
- set `GOPRIVATE` without printing credentials;
- run the ordinary tests and vet;
- start a disposable PostgreSQL service whose database name has the required
  prefix;
- run the database suite serially; and
- never run live UHC acceptance by default.

Configuring that credential is an external repository prerequisite. Until it
is configured, CI must report the missing prerequisite rather than appear
green. Keep the local verification command authoritative and do not add a
workflow that silently skips compilation or integration tests.

## Failure semantics

| Failure | Required behavior |
| --- | --- |
| Missing private-module build credential | Build fails before or during dependency resolution with setup guidance; no credential is logged or baked into an image. |
| Missing provider catalog/services input | Affected role exits with the existing sanitized configuration failure; no work starts. |
| Unsafe test database name | Integration suite fails before destructive DDL. |
| Concurrent package execution against one test database | Canonical command prevents it with `-p 1`; documentation does not claim plain database-enabled `go test ./...` is supported. |
| Control capacity below held slots | Control exits stably with `capacity_below_held`; Compose does not loop forever or report stale health. |
| MRF/consumer starts before control initialization | It fails or retries safely without initializing or cleaning global artifact state. |
| Numeric bounded sample drains | Status remains partial and activation is rejected without mutation. |
| Disk approaches operator threshold | Operator stops roles; durable jobs, selections, slots, and artifacts converge on restart. No automatic byte quota is claimed. |
| Stalled long-running job | Redacted stalled-work output makes it visible; no universal timeout is inferred. |

## Acceptance criteria

1. A clean checkout plus documented external inputs can build the exact image
   referenced by Compose.
2. The local recipe starts PostgreSQL, migrates once, initializes control, and
   starts one MRF and one consumer worker without manual network discovery.
3. Scaling the MRF service starts independent one-parser River processes; the
   consumer service remains fixed at one.
4. Every documented operator command has an exact container/network execution
   path.
5. A permanent control capacity/configuration error stops visibly and does not
   enter an unbounded restart loop.
6. No health check reports readiness solely from a stale workspace file.
7. CLI help accurately distinguishes retry coordination from reconcile's
   singleton control lease.
8. Current architecture is readable without following obsolete feed or
   single-worker instructions.
9. The obsolete all-in-one queue map is absent from the production/test API.
10. The canonical non-database tests, vet, focused race tests, and serial
    PostgreSQL suite all pass.
11. Story 21 integration fixtures exercise source target, selection, slot, and
    role semantics rather than bypassing them.
12. Supported role concurrency does not starve its PostgreSQL connection pool.
13. Execution locks support valid positive domain IDs greater than
    `MaxInt32` without hashing.
14. A numeric bounded acceptance run drains selected parse and consumer work,
    releases raw slots, records its report, and remains correctly
    non-activatable.
15. Documentation clearly states the singleton consumer, no-query-service,
    count-not-byte, no-retention, private-module, and manual stalled-work
    constraints.
16. Repository CI runs ordinary, vet, and serial PostgreSQL verification after
    its documented private-module credential is configured, and never reports
    success by skipping unavailable private dependencies.

## Required tests

### Repository and documentation tests

- Docker/Compose configuration resolves with the documented environment and
  contains a buildable image source.
- Every service command names a valid role and the consumer cannot be scaled
  accidentally by the provided recipe.
- Help tests cover retry and reconcile coordination wording.
- No production caller uses an all-in-one queue map.
- Example configuration contains no credential or machine-specific path.

### PostgreSQL integration tests

- The complete serial suite passes against a disposable prefixed database.
- Unsafe database names remain rejected before schema reset.
- Admission scheduling is cumulative, capacity-safe, and valid under concurrent
  schedulers.
- Current activation fixtures prove healthy readiness and damaged-publication
  rejection with Story 21 membership state present.
- Parse success schedules only selected consumer work.
- Reconciliation distinguishes a held lock from a lost lock and cleans only
  unlocked domain-owned staging.
- TOC import records all sources while creating executable MRF work only for
  selected sources with slots.
- Role runtime starts control/MRF/consumer, executes at least one bounded
  materialization, and shuts down without missing-domain state.
- Advisory execution locks serialize identities above `MaxInt32` in separate
  MRF and consumer domains.
- Supported per-role concurrency completes without connection-pool starvation.

### Local acceptance

With a real accepted provider catalog and the intended service selector:

1. build the local image from the checkout;
2. start PostgreSQL and migrate;
3. start one control, two MRF, and one consumer process;
4. discover one TOC with a small numeric MRF target and resident capacity;
5. wait for selected work to drain;
6. verify raw downloads and held slots are absent while parsed and warehouse
   output remain;
7. verify status reports only the expected partial-source activation blocker;
8. verify activation is rejected without mutation;
9. stop and restart all roles and verify no duplicate domain or warehouse
   output; and
10. save the existing bounded acceptance report.

The real live acceptance remains explicit opt-in and must never run as part of
ordinary tests or CI.

## Implementation guidance

- Prefer a Dockerfile, one Compose file, a small configuration README/example,
  and one canonical test command over new tooling layers.
- Update fixtures to the current contract instead of adding compatibility
  behavior for obsolete immediate scheduling.
- Reuse existing status, stalled-work SQL, consumer recognizers, execution
  locks, and acceptance report code.
- Keep credentials and accepted provider data external.
- Do not turn packaging work into a query service, retention subsystem,
  resource scheduler, or artifact-integrity redesign.
