# mrfpipeline

Operator executable for CMS Transparency in Coverage discovery, TOC and MRF
processing, warehouse ingestion, and additive plan attachment.

Version 1 is specified by Stories 01–13 in [`requirements/`](requirements/).
[`requirements/DESIGN.md`](requirements/DESIGN.md) records the product
decisions that stay consistent across those stories.

## Planned feed-free monthly-release contract

Stories [14](requirements/14-feed-free-domain-schema.md) through
[19](requirements/19-release-aware-reconciliation-acceptance-and-documentation.md)
define the approved next rebuild-only contract. They are requirements, not the
currently implemented command/schema behavior documented below.

The target removes `mrf_feeds` and `feed_id`, identifies an MRF source capture
by exact URL + collection month, identifies a consumer snapshot by source +
payer + collection month, admits stable TOC and MRF URLs as new captures in a
later month, and integrates exact feed-free `mrfconsumer 2.0.0`.

Stories 14–17 form one atomic breaking delivery batch. They may be implemented
as separate commits but are not independently mergeable, releasable, or
deployable. There is no compatibility adapter, temporary feed state, feature
flag, or supported intermediate runtime.

The pipeline will keep one monthly release per payer in `building`, `active`,
or `inactive` state. An operator completes and validates a building month, then
atomically activates it without rewriting warehouse data. Activation seals the
month against new discovery; rollback reactivates an already sealed historical
month. Different payers may have different active months.

A future query service will capture the complete active
`(payer_id, collection_month, output_id)` relation derived from the pipeline's
sealed snapshots once per request. Queries with no payer filter must apply
every active output row, not one global month and not every warehouse output
that happens to share an active payer/month. Query planning, partition pruning,
and performance acceptance belong to that query service and the consumer.

Until Stories 14–19 are implemented, use the Story 01–13 commands, schema,
consumer `1.5.0`, and operational guidance in the remaining README.

## Prerequisites

- Go 1.26
- PostgreSQL 15 or newer
- Local filesystem paths for the artifact root, warehouse, provider catalog,
  and service-selector CSV
- Outbound HTTPS to the payer listing and CDN endpoints
- A manually prepared `mrfenricher` provider catalog. The pipeline never runs
  enrichment. Pin the catalog path; changing it while a warehouse exists fails
  closed.

## Build

The four sibling modules in `go.mod` (`mrfdiscoverer`, `mrftocparser`,
`mrfparser`, and `mrfconsumer`) are private pseudo-versions. `go.mod` must
not contain a `replace` directive. Grant git access and tell the toolchain
not to use a public proxy for those paths:

```text
export GOPRIVATE=github.com/EnotPoloskun/*,github.com/enotpoloskun/mrfconsumer
go build -o mrfpipeline ./cmd/mrfpipeline
```

A clean checkout needs that `GOPRIVATE` value (and credentials that can
read those repositories) before `go build` or `go test`. An existing
`GOMODCACHE` populated from those revisions is also sufficient.

## Commands

```text
mrfpipeline migrate
mrfpipeline work
mrfpipeline discover --payer uhc --collection-month <YYYY-MM> --limit <count>
mrfpipeline reconcile
mrfpipeline retry --stage <job-kind> --id <domain-id>
```

`migrate` applies application and River schemas. No other command migrates.

`work` validates the complete worker environment, acquires the exclusive
worker lease, runs safe reconciliation, then consumes discovery, TOC
download, TOC parse, TOC import, MRF download, MRF parse, and consumer
ingest and attach queues until canceled.

`discover` enqueues one bounded UHC discovery run and reports identifiers
without waiting for downloads.

`reconcile` performs only safe nonterminal repairs. Terminal failed stages
require `retry`.

`retry` reopens one exact failed stage and inserts a replacement River job.
It does not run the job. The worker must be stopped. An attachment retry
reuses the frozen plan batch.

## Environment

| Variable | Used by |
|---|---|
| `MRFPIPELINE_DATABASE_URL` | all commands |
| `MRFPIPELINE_ARTIFACT_ROOT` | `work`, `reconcile` |
| `MRFPIPELINE_WAREHOUSE_PATH` | `work`, `reconcile` |
| `MRFPIPELINE_PROVIDER_CATALOG_PATH` | `work`, `reconcile` |
| `MRFPIPELINE_SERVICES_PATH` | `work`, `reconcile` |

`retry` needs only the database URL so a remote operator can enqueue work for
the correctly configured worker host.

## One-worker deployment

One database and warehouse has at most one supported `work` process. The
process holds a PostgreSQL session advisory lease on a dedicated connection.
A second `work`, `reconcile`, or `retry` fails without mutation.

Lease loss cancels River and prevents beginning another warehouse write.

## Stage flow

1. `discover` admits at most `--limit` new TOC URLs for one sticky collection
   month.
2. Each TOC downloads, parses, and imports.
3. Exact MRF URLs download and parse once, even when several TOCs reference
   them. Parsing is plan-independent.
4. Each required source/feed/month snapshot is ingested once.
5. Plans attach additively in frozen batches. Later plans do not rewrite
   rate or provider Parquet.

Example: plans A and B attach first; later B and C yield warehouse
associations A, B, and C, and the second batch writes only C.

## Collection month and current views

Collection month is a caller-supplied label. The first admitting discovery
freezes it on each TOC. A wrong first admission requires rebuilding affected
pipeline and warehouse state.

**Warning:** source-based feeds are not logical cross-month networks. After
URL rotation, `current_*` warehouse views can double-count the same logical
product. Serving must use an explicit collection month or a single-month
warehouse until a curated feed map exists.

A snapshot is plan-ready only when consume succeeded, at least one attachment
batch succeeded, every plan is assigned, and no pending/running/failed batch
exists. A planless snapshot is not plan-ready. Do not serve planless rates
when that PostgreSQL-derived state is required.

## First live run

`--limit` bounds newly admitted TOCs, not MRF count or bytes. Start with
`--limit 1`, then 2, then 5. Use 10 only after measuring fan-out and disk.

Before a bounded run, record free space on the artifact and warehouse
filesystems. During the run, monitor download and parsed bytes, warehouse
bytes, free space, database size, and pending/running MRF download/parse
counts. Version 1 does not guess required disk from HTTP headers. Stop the
worker if capacity approaches the operator safety threshold.

## Crash, retry, and reconciliation

Interrupted nonterminal work converges through artifact publication and
startup reconciliation. `reconcile` restores missing eligible jobs and
normalizes orphaned claims. It never reopens a terminal failed stage.

Use `retry --stage <kind> --id <id>` for one failed record. Retry preserves
numeric identity, output IDs, frozen attachment items, and artifacts. The
normal worker recognizes completed publication.

Manifest-present invalid parser output is never auto-deleted. Stop the
worker, inspect that exact generated directory, then `retry`.

## Retention

Automatic cleanup, while the lease is held, removes only:

- a TOC download after parse succeeded and the TOC parser output is still valid
- an MRF download after parse succeeded and the shared parser output is still valid
- a TOC parsed leaf after import succeeded
- a `plan-batches/plan-batch-<id>` leaf after that batch succeeded
- real `.staging` children whose mtime is at least 24 hours old

Shared successful MRF parsed output has no automatic deletion path. The
pipeline never cleans the consumer warehouse or `<warehouse>/.staging`.

Back up PostgreSQL, shared parsed MRFs, and the append-only warehouse.

## Status queries

Default queries are redacted. They return counts and numeric IDs only.

```sql
SELECT status, count(*) AS n
FROM mrfpipeline.discovery_runs
GROUP BY status
ORDER BY status;

SELECT download_status, parse_status, import_status, count(*) AS n
FROM mrfpipeline.toc_files
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

SELECT download_status, parse_status, count(*) AS n
FROM mrfpipeline.mrf_sources
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT consume_status, count(*) AS n
FROM mrfpipeline.mrf_snapshots
GROUP BY consume_status
ORDER BY consume_status;

SELECT status, count(*) AS n,
       coalesce(sum(requested_plan_count), 0) AS requested_total,
       coalesce(sum(added_plan_count), 0) AS added_total
FROM mrfpipeline.plan_attachment_batches
GROUP BY status
ORDER BY status;

SELECT 'discovery.run' AS stage, id, failure_code
FROM mrfpipeline.discovery_runs WHERE status = 'failed'
UNION ALL
SELECT 'toc.download', id, failure_code FROM mrfpipeline.toc_files WHERE download_status = 'failed'
UNION ALL
SELECT 'toc.parse', id, failure_code FROM mrfpipeline.toc_files WHERE parse_status = 'failed'
UNION ALL
SELECT 'toc.import', id, failure_code FROM mrfpipeline.toc_files WHERE import_status = 'failed'
UNION ALL
SELECT 'mrf.download', id, failure_code FROM mrfpipeline.mrf_sources WHERE download_status = 'failed'
UNION ALL
SELECT 'mrf.parse', id, failure_code FROM mrfpipeline.mrf_sources WHERE parse_status = 'failed'
UNION ALL
SELECT 'consumer.ingest', id, failure_code FROM mrfpipeline.mrf_snapshots WHERE consume_status = 'failed'
UNION ALL
SELECT 'consumer.attach_plans', id, failure_code FROM mrfpipeline.plan_attachment_batches WHERE status = 'failed'
ORDER BY 1, 2;

SELECT n.id AS snapshot_id, count(*) AS unassigned_plan_count
FROM mrfpipeline.mrf_snapshots n
JOIN mrfpipeline.mrf_plans p ON p.mrf_snapshot_id = n.id
WHERE n.consume_status = 'succeeded'
  AND NOT EXISTS (
      SELECT 1 FROM mrfpipeline.plan_attachment_batch_items i WHERE i.mrf_plan_id = p.id
  )
GROUP BY n.id
ORDER BY n.id;

SELECT n.id
FROM mrfpipeline.mrf_snapshots n
WHERE n.consume_status = 'succeeded'
  AND EXISTS (
      SELECT 1 FROM mrfpipeline.plan_attachment_batches b
      WHERE b.mrf_snapshot_id = n.id AND b.status = 'succeeded'
  )
  AND NOT EXISTS (
      SELECT 1 FROM mrfpipeline.mrf_plans p
      WHERE p.mrf_snapshot_id = n.id
        AND NOT EXISTS (
            SELECT 1 FROM mrfpipeline.plan_attachment_batch_items i
            WHERE i.mrf_plan_id = p.id
        )
  )
  AND NOT EXISTS (
      SELECT 1 FROM mrfpipeline.plan_attachment_batches b
      WHERE b.mrf_snapshot_id = n.id AND b.status IN ('pending', 'running', 'failed')
  )
ORDER BY n.id;

SELECT mrf_source_id, count(DISTINCT toc_file_id) AS toc_count
FROM mrfpipeline.toc_mrf_plan_associations a
JOIN mrfpipeline.mrf_snapshots n ON n.id = a.mrf_snapshot_id
GROUP BY mrf_source_id
HAVING count(DISTINCT toc_file_id) > 1
ORDER BY mrf_source_id;
```

### Authorized URL-debug query

For incident response on an authorized connection only. This is not a CLI
flag and must not replace the redacted status queries. Worker logs never
print URLs.

```sql
SELECT 'toc' AS kind, id, source_url
FROM mrfpipeline.toc_files
WHERE id = :toc_id
UNION ALL
SELECT 'mrf', id, source_url
FROM mrfpipeline.mrf_sources
WHERE id = :source_id;
```

## Progress logs

`work` writes throttled stderr `progress` records for downloads, TOC parse,
MRF parse, and consumer ingest. Fields are allowlisted. They never include
URLs, paths, plans, or provider values.

## River UI (optional, not part of this binary)

```text
export DATABASE_URL=<same URL as the worker>
export RIVER_SCHEMA=mrfpipeline_river
riverui
```

Pause and resume only. Pause stops fetching new jobs and does not cancel
in-flight work. Do not cancel, retry, or delete jobs from the UI.

## Limitations

UHC only. Local storage only. No automatic enrichment. No plan
removal/correction. No recurring discovery. Sticky collection month.
Required `--limit`. No UI in this binary. TOC import rejects HTTPS MRF
URLs that contain user information; the shared downloader would refuse
those locations.

Configured artifact, warehouse, catalog, and services paths must not
overlap, including the service selector sitting inside the warehouse.

## Tests

There is no CI configuration. Tests, including PostgreSQL integration,
DuckDB consumer paths, and `go test -race`, run on an operator machine
that can resolve the private sibling modules.

```text
export GOPRIVATE=github.com/EnotPoloskun/*,github.com/enotpoloskun/mrfconsumer
go test ./...
go vet ./...
```

Live UHC acceptance is `TestRealUHCAcceptance` in `internal/reconcile`.
It is skipped unless every opt-in variable is set, the database name
starts with `mrfpipeline_test_`, both output roots are dedicated,
non-symlink, and initializable, and a collection month is supplied.
Ordinary `go test ./...` never contacts live UHC.

```text
export MRFPIPELINE_REAL_ACCEPTANCE=1
export MRFPIPELINE_TEST_DATABASE_URL=<disposable database>
export MRFPIPELINE_ARTIFACT_ROOT=<dedicated empty acceptance root>
export MRFPIPELINE_WAREHOUSE_PATH=<dedicated empty acceptance warehouse>
export MRFPIPELINE_PROVIDER_CATALOG_PATH=<accepted manual catalog>
export MRFPIPELINE_SERVICES_PATH=<small intended CPT selector>
export MRFPIPELINE_REAL_COLLECTION_MONTH=<YYYY-MM>
```

Optional: `MRFPIPELINE_REAL_TOC_LIMIT` (`1`–`10`, default `1`),
`MRFPIPELINE_REAL_ACCEPTANCE_TIMEOUT` (Go duration, default `2h`), and
`MRFPIPELINE_REAL_ACCEPTANCE_REPORT` (JSON path outside the artifact and
warehouse roots). Raise the test timeout to cover the wait, for example
`go test -timeout 3h ./internal/reconcile -run TestRealUHCAcceptance`.

The harness builds `./cmd/mrfpipeline`, runs `migrate`, starts one
`work` process, runs `discover`, waits until domain stages are idle,
stops the worker, runs `reconcile`, restarts `work`, and asserts that
domain and warehouse counts do not increase.
