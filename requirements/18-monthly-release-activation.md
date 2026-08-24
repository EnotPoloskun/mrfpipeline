# Story 18: Monthly release activation

## Status

Implemented.

## User story

As a pipeline operator, I want to build one month while another remains served
and then manually activate the completed month per payer so that incomplete
future data never becomes visible merely because it was ingested.

## Product decision

The pipeline owns a small PostgreSQL control plane for monthly releases.
Consumer warehouse files remain immutable and contain all history.

For each payer/month, lifecycle is:

```text
building -> active -> inactive
             ^           |
             +-----------+  manual rollback/reactivation
```

- `building` is open for new discovery and downstream work.
- `active` is sealed and is the month selected for serving that payer.
- `inactive` is sealed historical state that may be reactivated for rollback.

At most one month is active per payer. Different payers may have different
active months. A payer may have no active month before its first activation.

The exact outputs served for an active release are the pipeline's immutable
`mrf_snapshots` rows for that payer/month, expressed as
`output_id = mrf-<snapshot-id>`. There is no release revision, feed, global
latest-month calculation, automatic activation, or warehouse mutation.

## Dependencies and supersession

This story depends on Stories 14–17 and the exact consumer Story 23
explicit-release query contract.

It supersedes all guidance that treats collection month merely as a sticky
label with no control-plane state or tells serving callers to use consumer
`current_*` views.

It preserves bounded/manual discovery, one worker lease, immutable snapshots,
additive plans, explicit retry, and redaction.

## Migration and tables

Add:

```text
internal/database/migrations/0004_monthly_releases.sql
```

Create `mrfpipeline.monthly_releases`:

| Column | Type | Contract |
|---|---|---|
| `payer_id` | `text` | Required generic payer identifier. |
| `collection_month` | `date` | Required first day of month. |
| `status` | `text` | Exact `building`, `active`, or `inactive`. |
| `sealed_at` | `timestamptz` nullable | First successful activation; immutable once set. |
| `last_activated_at` | `timestamptz` nullable | Most recent successful activation. |
| `created_at` | `timestamptz` | Required creation time. |
| `updated_at` | `timestamptz` | Required mutable-state time. |

Primary key:

```text
(payer_id, collection_month)
```

A partial unique index enforces one active row per payer:

```text
UNIQUE (payer_id) WHERE status = 'active'
```

Constraints require:

- `building` has null `sealed_at` and null `last_activated_at`;
- `active` has both timestamps;
- `inactive` has both timestamps;
- `sealed_at <= last_activated_at`; and
- valid payer/month formats.

Reactivation updates only status, `last_activated_at`, and `updated_at`;
`sealed_at` records the first seal. The table stores no duplicated output
inventory, counts, paths, feed, hashes, or user identity. Exact output
membership is derived from the release's constrained `mrf_snapshots` rows.

Migration `0004` backfills one `building` release for every distinct
`(payer_id, collection_month)` already present in feed-free discovery runs,
TOC captures, or snapshots. It never infers an active month from status,
timestamps, filenames, or greatest month. Conflicting or invalid domain pairs
fail the migration transaction.

After backfill, add named composite `ON DELETE RESTRICT` foreign keys from:

```text
discovery_runs(payer_id, collection_month)
toc_files(payer_id, collection_month)
mrf_snapshots(payer_id, collection_month)
```

to `monthly_releases(payer_id, collection_month)`. Monthly release rows have no
supported delete path. These constraints guarantee that every admitted or
published monthly domain row belongs to durable release state.

## Release creation and discovery gate

`discover --payer uhc --collection-month <month>` creates the monthly release
as `building` in the same transaction that creates the discovery run when no
row exists.

Concurrent first discovery for the same payer/month converges through the
release primary key: insert-or-select, lock the one row, require `building`,
then create each independently requested bounded discovery run/job. A unique
conflict is retried from a fresh transaction and never loses the status gate.

Before creating any discovery run, lock the release row and require
`status=building`. If it is `active` or `inactive`, reject the command as a
safe invalid release-state error without creating a run or job.

This seals the intended monthly population at first activation. Existing jobs,
reconciliation, and explicit retry for work already admitted to a building
release continue normally. Once sealed, no later discovery can silently add
TOCs, MRF snapshots, or plans to that month.

## Exact release output membership

For serving, one release expands to the exact relation:

```text
(payer_id, collection_month, output_id)
```

Select `mrf_snapshots` belonging to the active release and derive each output
ID as `mrf-<mrf_snapshots.id>`. Do not discover release membership by scanning
consumer `all_ingestions` for payer/month. A consumer output created outside
the pipeline is warehouse history but is not part of the active release.

No separate membership table is needed. Before first activation every admitted
TOC has completed import, every snapshot already exists, and the release lock
prevents concurrent discovery from extending the population. After activation
the release is sealed and snapshot payer/month/ID values are immutable. The
derived relation is therefore stable and rollback recreates exactly the same
output set.

Read the complete active relation in one PostgreSQL statement equivalent to:

```sql
SELECT snapshot.payer_id,
       snapshot.collection_month,
       'mrf-' || snapshot.id::text AS output_id
FROM mrfpipeline.monthly_releases AS release
JOIN mrfpipeline.mrf_snapshots AS snapshot
  ON snapshot.payer_id = release.payer_id
 AND snapshot.collection_month = release.collection_month
WHERE release.status = 'active'
ORDER BY snapshot.payer_id, snapshot.collection_month, snapshot.id;
```

One statement provides one MVCC-consistent handoff even when different payers
are activated concurrently.

The snapshot unique key `(mrf_source_id, payer_id, collection_month)` prevents
the same monthly MRF capture from producing two release outputs for one payer.
Two different exact MRF URLs remain distinct admitted sources, and the same URL
in another collection month is a different source capture. The pipeline does
not compare content or infer semantic duplication. If an unintended distinct
source was admitted, the operator must not activate that release under this
simple additive model.

The production CLI still supports discovery only for UHC. Generic release rows
for other payers are created only by future supported adapters or controlled
test fixtures; do not add a manual arbitrary snapshot-import feature here.

## Command surface

Add one command group:

```text
mrfpipeline month status
mrfpipeline month status --payer <payer> --collection-month <YYYY-MM>
mrfpipeline month activate --payer <payer> --collection-month <YYYY-MM>
```

For `month status`, accept either no flags or both payer/month flags. Supplying
only one, unknown flags, or positional arguments is a usage error.

`month activate` requires both flags and no positional arguments. Identifier
and month syntax match `discover`; however activation may address any valid
payer row already present in the database, not only UHC.

`month status` is read-only, requires only `MRFPIPELINE_DATABASE_URL`, and does
not acquire the worker lease. It may run while `work` is active; its result is
a point-in-time database report and may become stale immediately. The targeted
form reports database readiness counts only and does not claim that warehouse
files were revalidated.

`month activate` requires the complete worker environment and the worker to be
stopped. It validates database/application/River schemas and all configured
path boundaries, then acquires the same exclusive worker lease as `work` and
`reconcile`. Lease contention fails without mutation.

No month command starts River, downloads, parses, consumes, attaches, repairs,
or retries work.

## Status output

With no flags, return the complete active output relation ordered by payer,
month, and numeric snapshot ID:

```json
{"active_outputs":[{"payer_id":"aetna","collection_month":"2026-08","output_id":"mrf-41"},{"payer_id":"uhc","collection_month":"2026-09","output_id":"mrf-72"},{"payer_id":"uhc","collection_month":"2026-09","output_id":"mrf-73"}]}
```

A payer with no active release is absent. An empty relation is successful:

```json
{"active_outputs":[]}
```

With payer/month, return one database-readiness report:

```json
{"payer_id":"uhc","collection_month":"2026-09","status":"building","database_ready":false,"blockers":["attachment_incomplete","toc_incomplete"],"discovery_run_count":1,"toc_count":5,"snapshot_count":8}
```

Counts are nonnegative and intentionally coarse. `blockers` is a sorted unique
array containing zero or more of these stable database-readiness codes:

```text
discovery_missing
discovery_incomplete
toc_missing
toc_incomplete
snapshot_missing
source_incomplete
consume_incomplete
attachment_missing
attachment_incomplete
plan_unassigned
job_inconsistent
```

`database_ready` is true exactly when `blockers` is empty. It is necessary but
not sufficient for activation; `month activate` additionally recognizes exact
warehouse publications under the lease. The no-flag form intentionally returns
validated output IDs because they are the query handoff contract. Neither form
contains URLs, paths, plans, failure codes, provider/service values, raw errors,
or warehouse details. Validated payer/month, derived output IDs, numeric IDs,
counts, statuses, and blocker codes are authorized structured control-plane
values.

Unknown release lookup is a safe not-found/invalid-state failure rather than a
fabricated empty release.

## Readiness definition

A release is ready only when all checks hold under the exclusive lease:

1. The release exists and is `building`, `inactive`, or already `active` for
   the idempotent activation case.
2. At least one discovery run exists and every run for that payer/month is
   `succeeded`.
3. At least one admitted monthly TOC exists and every such TOC has download,
   parse, and import `succeeded`.
4. At least one snapshot exists for the payer/month.
5. Every referenced MRF source has download and parse `succeeded`.
6. Every snapshot has consume `succeeded`.
7. Every snapshot has at least one succeeded plan-attachment batch.
8. No snapshot has pending, running, or failed attachment batch.
9. Every canonical `mrf_plan` belongs to one succeeded frozen batch; there is
   no unassigned plan.
10. Every exact consumer target is recognized as a valid feed-free `2.0.0`
    publication matching database payer/month/output/catalog identity.
11. No relevant nonterminal stage lacks its current River job and no relevant
    domain stage is failed or inconsistent.

Bounded discovery overflow does not block readiness: overflow was not admitted
to the release. A valid zero-rate consumer snapshot is allowed if provider
filtering produced it, but it still needs its initial plan attachment.

Readiness does not scan full rate/provider Parquet rows, call DuckDB, contact
the payer, refresh the provider catalog, or infer completeness beyond admitted
domain records.

## Sealed plan-set invariant

Activation freezes the complete plan-association set for every output in the
release. The exclusive worker lease and readiness gate ensure that all admitted
TOCs are imported, every canonical plan is assigned to a succeeded attachment
batch, and no attachment is pending or running before the status changes.

After a release becomes `active` or `inactive`, the pipeline must never:

- create another canonical plan, attachment batch, or batch item for it;
- schedule or execute `consumer.attach_plans` for one of its snapshots;
- retry or reconcile an attachment into that sealed release; or
- invoke the standalone consumer attachment CLI/API out of band.

Any such database or warehouse state is `sealed_release_inconsistent`; it is
reported without mutation. Adding or correcting plans requires a newly built
release. Rollback therefore restores the exact same output and plan sets, and
plan discovery cannot change between release cutovers.

## Activation algorithm

Activation has a read-only filesystem preflight followed by one short database
transaction.

1. Acquire the exclusive worker lease and validate configured environment.
2. Read the target release and perform the complete readiness check, including
   exact consumer publication recognition.
3. Begin a transaction and lock all release rows for the payer in ascending
   month order. Do not lock every domain row; a release may contain many.
4. Repeat every database readiness condition with aggregate/select queries. The
   worker lease prevents worker/reconcile/retry mutations, and every concurrent
   discovery must first lock this same release row, so admitted state cannot
   change past the release lock. If any condition changed, roll back.
5. Mark the previously active row, if any, `inactive`.
6. Mark the target `active`; on first activation set `sealed_at`, and always set
   `last_activated_at` and `updated_at` to `transaction_timestamp()`.
7. Commit both changes atomically and release the lease.

Filesystem preflight occurs while no supported worker can mutate/publish.
Warehouse files are immutable, so the short transaction need not hold open
while scanning them.

If any validation, lock, uniqueness, or commit step fails, the former active
month remains unchanged. Do not deactivate first in a separate transaction.

Successful output is:

```json
{"payer_id":"uhc","collection_month":"2026-09","output_count":8,"previous_collection_month":"2026-08"}
```

`previous_collection_month` is JSON null on first activation or when the
target was already the active month. Activating an already active, still-ready
target is an idempotent success that does not change timestamps.

## Rollback

Rollback uses the same `month activate` command targeting an `inactive`
release. It repeats readiness and exact publication recognition before the
atomic pointer change. It does not rewrite, copy, or reconsume warehouse data.

An inactive release is sealed: rollback does not reopen discovery. Correcting
or adding data requires a newly built month under this version's simple model.

## Multi-payer active mapping

The active state is a relation, not one global month. Its compact operator view
is:

```text
uhc   -> 2026-09
aetna -> 2026-08
```

A future query service reads all active release rows joined to their snapshots
in one PostgreSQL statement at startup or control-plane refresh, validates the
complete `(payer_id, collection_month, output_id)` relation against the
warehouse, and atomically publishes verified serving state. Each request
captures that state without rescanning warehouse metadata. A query with a payer
filter uses that payer's output rows. A query spanning payers uses every active
output row. Payers without an active row contribute no served data. Membership
is never expanded by scanning every warehouse ingestion for payer/month.

Per-payer activations are atomic but are not a synchronized global release. A
cross-payer request sees one committed mapping snapshot, which may legitimately
contain different months. A future global release-generation feature would be
required to switch several payers as one unit.

## Required tests

Add or revise tests proving:

- migration `0004` constraints and one-active-per-payer index;
- migration backfills distinct existing feed-free payer/months as `building`,
  adds all three composite foreign keys, and never infers active state;
- first discovery creates a building release transactionally;
- repeat discovery in building succeeds, while active/inactive rejects without
  a run/job;
- readiness reports sorted unique blocker codes and becomes database-ready only
  when all database conditions are complete;
- overflow does not block readiness;
- activation atomically changes old active to inactive and target to active;
- failed activation preserves the former active month;
- activating already active is idempotent;
- rollback reactivates an inactive ready release without reopening it;
- two payers may have different active months;
- no payer can have two active months, including concurrent activation;
- no-flag status returns the deterministic complete active-output relation;
- activation `output_count` equals the number of derived snapshots and remains
  stable on idempotent activation and rollback;
- activation freezes plan batches and association parts; active/inactive work,
  retry, and reconciliation cannot add another plan;
- an extra consumer output with an active payer/month but no pipeline snapshot
  is absent from status and the query handoff;
- consumer warehouse bytes and manifests are unchanged by activation;
- month commands cannot run while `work` holds the lease; and
- reports/errors/logs expose only the authorized structured control-plane
  values and preserve the redaction boundary above.

## Acceptance criteria

- Building a future month or adding an unrelated consumer output cannot affect
  the active output relation.
- Only a completely admitted, consumed, plan-ready, recognized release can be
  activated.
- One transaction changes a payer's active pointer and supports rollback.
- Sealed releases reject new discovery.
- Different payers can serve different active months, and the complete exact
  output relation is queryable without using feeds.
- Activation never mutates consumer warehouse data.

## Out of scope

- Query-service or UI implementation.
- A global synchronized multi-payer release generation.
- Adding data to an active/inactive release.
- Automatically choosing or activating the greatest month.
- Automatic recurring discovery or provider enrichment.
- Snapshot correction, plan removal, or warehouse rewriting.
