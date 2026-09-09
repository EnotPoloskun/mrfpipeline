# Story 35: Concurrent consumer workers for distinct output IDs

## Status

Implemented.

## User story

As an operator, I want more than one `consumer.ingest` / `consumer.attach_plans`
job to run at once when they target different snapshots so that consume
keeps up with parallel MRF parses instead of draining one warehouse write
at a time.

## Context

Story 21 split roles, scaled MRF processes, and added per-domain execution
locks. It left consumer replicas **disabled** until a pinned `mrfconsumer`
revision implemented a real distinct-output writer protocol. That pin does
not exist yet. Current production still serializes consume three ways:

1. Singleton consumer role lease (`AcquireConsumerLease`, PG advisory
   class 7319 object 3). A second replica exits `worker_busy`.
2. River queue `consumer` `MaxWorkers: 1` in `QueuesForRole` and
   `productionQueues`.
3. Per-snapshot execution lock (`LockNamespaceConsumer`, key
   `-mrf_snapshot_id`) shared by `consumer.ingest` and
   `consumer.attach_plans`.

(1) and (2) are the global serialisation. (3) is the correct same-output
overlap guard and already allows different snapshot IDs to hold locks at
the same time. It is almost unused for distinct files because (1) and (2)
never run two consumer mutations together.

Parsed MRF directories are immutable and may be read concurrently. One
parse can enqueue several ingest jobs (`mrf_snapshots` unique on
`(mrf_source_id, payer_id, collection_month)`). Every ingest still writes
the one configured warehouse (`MRFPIPELINE_WAREHOUSE_PATH`). Distinct
parser outputs and distinct snapshots are distinct consumer `output_id`s
(`mrf-<snapshot_id>`). That is the unit of allowed overlap, not “two
processes read two folders.”

Consumer Story 26 replaces “one writer per warehouse” with init
serialisation plus per-output `flock`. This pipeline story pins that
revision and turns on the Story 21 consumer topology.

## Dependencies and compatibility

Depends on implemented pipeline Stories 11, 12, 17, 21, and 34, and on
**merged, test-proven** consumer Story 26.

This story and consumer Story 26 are one delivery batch. Do not merge or
deploy pipeline enablement against the current pin
(`v0.0.0-20260827032738-489ea6d60cd7`). No feature flag, dual pin, or
“run concurrent workers against the old library” mode.

Pin the first `github.com/enotpoloskun/mrfconsumer` pseudo-version that
implements Story 26. `go.mod` must not grow a `replace` directive.

Warehouse / snapshot / catalog versions stay exact `2.0.0` / parser
`1.1.0` / catalog schema `1`. No pipeline migration.

## Supersession

This story supersedes Story 21 wherever it:

- keeps consumer concurrency at one until a future pin;
- forbids `--scale consumer=2` because the extra replica exits
  `worker_busy`;
- describes the consumer role as remaining singleton while the pinned
  writer requires serialized writes.

It preserves Story 21’s:

- one control lease;
- one `mrf.parse` per process;
- consumer **queue** `MaxWorkers: 1` (one mutation per consumer process);
- per-output execution lock for ingest+attach of one snapshot;
- “no pipeline-side lock may claim to make an unsupported consumer
  implementation safe” — that sentence stays true for any older pin;
- control-owned artifact-root init; consumer-owned warehouse init.

Do not rewrite Story 11, 12, or 21 files as if they originally enabled
replicas. Do not raise in-process `consumer` `MaxWorkers` in this story.

## Goal

1. Pin consumer Story 26.
2. Stop taking the consumer role lease so identical consumer processes
   can run against one database and one warehouse volume.
3. Keep `QueueConsumer` at `MaxWorkers: 1` per process. Obtain consume
   concurrency with `--scale consumer=N` (same shape as MRF parse).
4. Keep the per-snapshot execution lock so ingest and attach of one
   snapshot never overlap, including across replicas.
5. Document and test that distinct snapshot IDs overlap in time.
6. Default Compose still starts **one** consumer replica. Scaling is
   explicit.

## Non-goals

- In-process `MaxWorkers > 1` on the `consumer` queue. Ingest RSS is
  large (pinned catalog plus one output’s relationships). Isolation
  stays process-level, matching Story 21. A later story may raise
  in-process workers after measurement.
- Changing control or MRF leases, MRF parse mutex, or resident capacity.
- A pipeline flock, lock file, or warehouse mutex that duplicates
  consumer Story 26.
- Control impersonating a consumer writer to pre-init the warehouse.
- Overlapping ingest and attach of the **same** snapshot.
- Filter-catalog overlap rules. `filters build` already has its own
  payer/month lock and reads published snapshots; that race exists with
  one consumer today.
- Auto-scaling, Kubernetes, or a default replica count other than 1.
- Windows non-Docker writer topology.
- Changing retry taxonomy (Story 34) or adding a “warehouse lock busy”
  failure code. Same-output busy remains execution-lock snooze.
- Mixed old/new consumer binaries in one warehouse. Stop consumer
  replicas, deploy the new image, start N.

## Product decisions

Locked for this story:

- **Pin first, then enable.** Code that omits the consumer lease must
  not ship against a pin that still documents serialized callers.
- **Replicas, not in-process workers.** `QueuesForRole("consumer")`
  stays `{MaxWorkers: 1}`. `productionQueues` stays 1. Tests that freeze
  those numbers stay. Concurrency is extra `work --role consumer`
  processes.
- **Drop `AcquireConsumerLease`.** `Runtime.Run` for role `consumer`
  starts River without a role lease. `ConsumerLeaseObject` remains
  reserved and unused so the key is not recycled. Control still uses
  `AcquireControlLease`. Mixed-version: an old consumer that still
  takes the lease can block nothing new and can overlap writes with a
  new replica — unsupported; deploy by stopping consumers first.
- **Nil consumer lease is not lease-lost.** Today `work.go` always
  builds `health := reconcile.Health(lease, …)` and passes it to
  `consumeringest` / `planattach`. `Health` returns
  `worker_lease_lost` when `lease == nil`, which would cancel every
  consume write after this story. Change `Health` so a nil lease
  succeeds (no-op). `WatchLease` already returns immediately on nil.
  Execution-lock watch remains the job-level guard. Do not stop
  calling `Health` without making the nil case succeed.
- **Keep execution lock.** `consumeringest.Worker` passes
  `LockNamespaceConsumer` + `MRFSnapshotID`. `planattach.Worker` still
  locks the **snapshot id**, not the batch id. Busy → `JobSnooze(5s)`
  without consuming a pipeline attempt.
- **Default one replica.** `docker-compose.story21.yml` `consumer`
  service count stays 1. Docs add `--scale consumer=2` (or more) next
  to `--scale mrf=2`. Remove “Do not `--scale consumer=2`” /
  `worker_busy` for consumer.
- **Pool size.** `ConsumerMaxConns` stays 6 per process (one executing
  job plus lock/bookkeeping). Update the comment that says “singleton
  warehouse writer.” N replicas means N pools; that is intended.
- **Same parsed directory.** Distinct snapshots may ingest one source’s
  parsed output concurrently. Pipeline must not add a parser-dir lock.
- **Startup inspect stays shallow.** `InspectWarehouse` still accepts
  absent, empty, or recognized `2.0.0`. It does not create warehouse
  files. Two replicas may start against an empty warehouse; first
  overlapping ingest runs consumer Story 26 init. Do not treat
  `.staging/locks/` as a startup root entry — those files exist only
  after `warehouse.json` and `.staging/` (consumer Story 26).
- **No new failure codes** for lock wait inside `mrfconsumer`. Context
  cancel remains interruption. Consumer `ErrOutput` mapping unchanged
  except that uniqueness after waiting is the existing already-exists
  path.

## Required behavior

### Worker process

`mrfpipeline work --role consumer`:

- still registers only `consumer.ingest` and `consumer.attach_plans`;
- still uses `QueueConsumer` `MaxWorkers: 1`;
- does **not** call `AcquireConsumerLease`;
- still requires `MRFPIPELINE_WAREHOUSE_PATH` and
  `MRFPIPELINE_PROVIDER_CATALOG_PATH`;
- still fails startup on a nonempty unrecognized warehouse;
- still calls `mrfconsumer.Ingest` / `AttachPlans` in-process;
- still invokes the consumer `Health` hook before warehouse writes;
  with no role lease that hook must return nil.

A second consumer process against the same database must start and log
`worker_started`, not `worker_busy`.

Control remains singleton. `reconcile` still takes the control lease
(stop control first). `retry` / `month activate` still use release and
domain execution locks only.

### Job overlap

| Pair | Pipeline execution lock | Consumer Story 26 | Allowed to overlap? |
|---|---|---|---|
| Ingest snapshot 45 and ingest 46 | different keys | different output locks | yes |
| Attach 45 and attach 46 | different keys | different output locks | yes |
| Ingest 45 and attach 46 | different keys | different output locks | yes |
| Ingest 45 and attach 45 | same key; second snoozes | same output lock | no |
| Two deliveries of ingest 45 | same key; second snoozes | same output lock | no |

Claim, confirm, building-release gate, provider-catalog path-metadata
guard, and existing-final-leaf reuse stay as Stories 11/12/17/34.

### Docker and operator docs

Update README, DOCKER.md, `work --help`, and DESIGN.md current
architecture:

- Consumer role is no longer a cluster singleton.
- One mutation per consumer **process**; scale with `--scale consumer=N`.
- Example: `--scale mrf=3 --scale consumer=2`.
- Two consumer containers share the artifact volume and warehouse path.
- RSS/CPU: each replica can hold one full ingest; size memory for N
  concurrent ingests plus parser replicas. No new env var.
- Keep packaging-test command strings that still appear. In particular
  **keep this exact command as its own line/fragment**:

  `docker compose -f docker-compose.story21.yml --profile workers up -d --scale mrf=2 mrf consumer`

  Do not replace or merge it with `--scale consumer=2`;
  `TestLocalPackagingContract` requires that substring. Add a
  **separate** documented command that contains `--scale consumer=2`.

### DESIGN.md

When implementing, bump the implemented-story index through 35. Replace
the active-architecture bullet “serialized consumer writers until the
pinned consumer contract permits more” with the Story 26/35 contract:
init once, then distinct output IDs may overlap; same output ID stays
serial; consumer processes are replicable.

## Required tests

Do not run formatters, linters, or the full suite while implementing.
Land tests that prove the contract.

### Pin and policy

- `go.mod` / module-path test (existing `cmd/mrfpipeline` pin assertion)
  names the Story 26 revision, not `20260827032738-489ea6d60cd7`.
- `QueuesForRole("consumer")` remains `MaxWorkers: 1`.
- `productionQueues()[QueueConsumer].MaxWorkers` remains 1.
- Consumer `Runtime.Run` does not acquire a role lease. A second
  `AcquireConsumerLease` in a test must not be required for a second
  worker to start.
- `work --help` no longer says the consumer role is serialized because
  the warehouse writer is serialized.
- `reconcile.Health(nil, …)` returns nil. A consumer worker with a nil
  role lease still runs ingest/attach (unit test; do not rely on a
  fake `Health` that skips the production function).

### Execution lock (existing guarantees, keep)

- Ingest and attach of one snapshot cannot run together (existing
  `TestIntegrationIngestAndAttachSerialized` or equivalent).
- Distinct snapshot IDs can both hold consumer execution locks
  (Story 21 already asked for this; enable a test that actually runs two
  consumer workers).

### Overlap against the pinned consumer

Use the **pinned** `mrfconsumer.Ingest` / `AttachPlans`, not fakes that
skip the warehouse, for at least one integration test each:

- Two consumer River processes (two clients, each `MaxWorkers: 1`), two
  `consumer.ingest` jobs, different `mrf_snapshot_id`, two parsed
  fixtures. Both succeed. An in-flight counter or overlapping
  `running` statuses proves they were not fully serial.
- Mixed: ingest of snapshot A overlapping attach of already-consumed
  snapshot B. Both succeed.
- Same snapshot: enqueue ingest while attach is running (or the reverse
  after consume succeeded). The second delivery snoozes or no-ops per
  claim rules; warehouse output is not duplicated.

Fakes remain allowed for claim/lifecycle tests that do not assert
warehouse bytes.

### Lease / Compose docs

- Starting two consumer roles no longer expects `worker_busy`.
- Packaging tests: drop assertions (if any) that freeze
  “Do not `--scale consumer=2`” / extra replica `worker_busy`. **Keep
  the exact** `--scale mrf=2 mrf consumer` fragment unchanged. Add a
  **separate** fragment that contains `--scale consumer=2`.
- `ConsumerMaxConns` comment/test still describes one executing job per
  process, not a cluster singleton.

### Race

- Race-enabled tests covering two consumer clients overlapping ingest
  remain in the Story 21/22 race job.

## Documentation

Update current operator text only:

- README worker-role paragraph and Compose “Do not `--scale consumer=2`”.
- DOCKER.md “Do not” list and “Leave consumer at one replica.”
- `internal/cli/help.go` `workHelp`.
- DESIGN.md active architecture and story index.

Do not claim in-process consumer `MaxWorkers > 1`. Do not document NFS
warehouses.

## Acceptance criteria

1. A second `work --role consumer` process starts.
2. Two `consumer.ingest` jobs for different snapshots run at the same
   time and both publish `mrf-<id>` into one warehouse.
3. Ingest and attach of one snapshot still cannot overlap.
4. `--scale consumer=2` is documented and does not exit `worker_busy`.
5. Default Compose topology is still one consumer replica.
6. The pinned consumer is a Story 26 revision. The previous serialized
   pin is gone from `go.mod`.
7. `reconcile.Health(nil, …)` does not fail consume writes.

## Implementation guidance

- Land consumer Story 26 and its tests first; pin; then delete the
  lease acquire in `internal/work/work.go` **and** make
  `reconcile.Health` succeed on a nil lease in the same change.
- Do not raise `MaxWorkers` “temporarily” to simulate overlap in
  production policy tests; use two processes/clients.
- Keep redaction: no lock keys, paths, or output IDs in new log fields
  beyond existing `domain_id`.
- Size local acceptance RAM for two ingests if you scale consumer during
  Story 22-style bounded runs; do not make that scale mandatory for the
  default first-run procedure.
