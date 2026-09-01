# mrfpipeline

Operator executable for CMS Transparency in Coverage discovery, TOC and MRF
processing, warehouse ingestion, and additive plan attachment.

Version 1 is implemented through Stories 01–27 in
[`requirements/`](requirements/). Stories 28–31 implement the versioned
`mrfweb` schema, typed DuckDB extraction, explicit filter catalog
build/status, and catalog-aware release publication. Proposed Story 32
specifies final operational acceptance for a future public query service.
[`requirements/DESIGN.md`](requirements/DESIGN.md) records the product
decisions that stay consistent across those stories.

## Feed-free monthly-release contract

Stories [14](requirements/14-feed-free-domain-schema.md) through
[22](requirements/22-runnable-local-acceptance-and-test-convergence.md) and
[23](requirements/23-terminal-parse-slot-release.md) through
[27](requirements/27-incremental-release-publication.md) define the current
rebuild-only contract. Story 21 adds bounded MRF admission and resident slots;
Story 22 makes the local topology runnable; Stories 23–26 converge terminal
cleanup and cancellation. Story 27 persists additive active-output membership,
allows numeric partial checkpoints and terminal file failures, and keeps known
MRF work running after first activation. These stories do not upgrade an old
populated warehouse in place.

The target removes `mrf_feeds` and `feed_id`, identifies an MRF source capture
by exact URL + collection month, identifies a consumer snapshot by source +
payer + collection month, admits stable TOC and MRF URLs as new captures in a
later month, and integrates exact feed-free `mrfconsumer 2.0.0`.

There is no compatibility adapter, temporary feed state, feature flag, or
supported intermediate runtime. The pipeline integrates exact feed-free
`mrfconsumer 2.0.0`.

The pipeline keeps one monthly release per payer in `building`, `active`, or
`inactive` state. The first activation freezes discovery and TOC-derived source
and plan inventory, atomically switches that payer's active month, and publishes
only the fully consumed, plan-ready outputs validated in the warehouse. MRF
download, parse, consume, and attachment continue for the known selected
sources while the release is active.

Repeating activation appends newly complete outputs to the same active month;
previously published outputs remain members. Terminal TOC/MRF file failures are
reported and omitted rather than blocking publication. Pending MRF work is
omitted until a later activation. Consumer-ingest and plan-attachment failures
still block the checkpoint because they indicate publication defects. An
inactive release is frozen and may be reactivated as an exact rollback.

A future query service will load the complete active
`(payer_id, collection_month, output_id)` relation from the pipeline's durable
published-output membership at startup or control-plane refresh, validate it
against the warehouse, and atomically publish verified serving state. Each
request captures that relation without rescanning warehouse metadata. Queries
with no payer filter must apply every active output row, not one global month
and not every warehouse output that happens to share an active payer/month.
Query planning, partition pruning, and performance acceptance belong to that
query service and the consumer.

## Release filter catalogs: Stories 28–31 implemented; Story 32 planned

Stories 28–31 implement the versioned `mrfweb` schema, typed read-only
DuckDB extraction, explicit filter catalog build/status commands, and
catalog-aware release publication. Story 32 remains the planned operational
acceptance story.

The catalog is keyed by `(payer_id, collection_month, publication_generation)`.
It stores exact output membership and fingerprint, source-provided billing
labels and negotiated observation counts, code-scoped filter values,
sponsor-independent canonical plans and plan/output relationships,
output/code/network relationships, and release-reachable provider taxonomy,
state, and state/city values. It stores no rates, ZIP/NPI directory,
expiration filter, external code dictionary, or aggregate cube.

The required operator flow is:

```text
mrfpipeline filters status --payer <payer> --collection-month <YYYY-MM>
mrfpipeline filters build --payer <payer> --collection-month <YYYY-MM>
mrfpipeline month activate --payer <payer> --collection-month <YYYY-MM>
# future, outside this repository:
mrfweb switch
```

`filters status` is database-only. `filters build` selects the exact current
or prospective Story 27 checkpoint, snapshots plans/readiness, extracts
deterministic values through pinned DuckDB, and atomically marks one catalog
`ready`. Activation requires that exact ready catalog, compares complete
output/plan sets under release locks, and commits membership, generation,
release pointer, and catalog publication together. A stale race leaves all
state unchanged; rerun `filters build` explicitly. Published generations are
immutable and retained for rollback/in-flight query state. Inactive rollback
reuses its exact current-generation catalog.

Existing releases use the explicit quiesced cutover: stop workers, perform a
final Story 27 checkpoint, build the now-current catalog, deploy catalog-aware
activation, promote it, verify fail-closed status/views, then restart workers.
No automatic builder, worker, queue, heartbeat, repair daemon, or force mode
exists. Active catalog views and no-selector status fail closed globally when
any active payer lacks an exact published current-generation catalog.
Reconciliation reports `filter_catalog_backfill_required_count` and
`sealed_release_inconsistency_count` without building, promoting, repairing,
deleting, or running DuckDB. The future `mrfweb switch` operation remains
outside this repository.
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

The preferred local container build forwards an SSH agent only while private
modules are downloaded. It does not copy the key into the image:

```text
eval "$(ssh-agent -s)"
ssh-add <path-to-private-module-key>
export GOPRIVATE=github.com/EnotPoloskun/*,github.com/enotpoloskun/mrfconsumer
DOCKER_BUILDKIT=1 docker compose -f docker-compose.story21.yml build
```

Compose forwards the agent through its `build.ssh: [default]` configuration
and builds the image used by every service. A direct build is also supported:
`DOCKER_BUILDKIT=1 docker build --ssh default --tag mrfpipeline:local .`.

The key must be able to read every private sibling module named by `go.mod`.
If the SSH agent or private-module access is missing, the build fails during
dependency resolution. The key is forwarded through the SSH agent only; it is
never a build argument or image layer. No database URL, accepted catalog, or
host path is a build argument or image layer. The Dockerfile's `CGO_ENABLED=0`
setting can be compile-checked on the host with `CGO_ENABLED=0 go build ./cmd/mrfpipeline`;
a host-built
macOS/Windows binary is not a substitute for the Linux container image.

## External local inputs

The Compose file mounts the accepted provider catalog and service selector
from paths outside the image. Prepare ignored local paths and copy the real
catalog into the catalog directory; do not copy the placeholder catalog from
this checkout:

```text
cp .env.example .env
mkdir -p local/provider-catalog
cp config/services.csv.example local/services.csv
```

Copy those files **before** any Compose command that mounts them. A missing
`local/services.csv` is created as a directory, and control then fails as a
runtime configuration error.

Edit `.env` so `MRFPIPELINE_PROVIDER_CATALOG_DIR` points to the operator's
accepted catalog and `MRFPIPELINE_SERVICES_FILE` points to the compatible
selector.

The selector is the CPT (and other billing-code) list. It is not the provider
catalog. It must keep the exact header `billing_code_type,billing_code`. An
externally supplied selector is not modified or reinterpreted; make a
compatible copy with that header before using it:

```csv
billing_code_type,billing_code
CPT,99213
```

The catalog is the `mrfenricher` Parquet export (NPIs and taxonomies), not a
CPT list. The pipeline never starts enricher. Produce it against a **separate**
enricher database, then export into the mounted directory:

```text
export MRFENRICHER_DATABASE_URL=<enricher postgres URL>
mrfenricher migrate
mrfenricher refresh --nppes <NPPES_Data_Dissemination_<Month>_<Year>_V2.zip> --taxonomies <taxonomy.csv>
mrfenricher export --output ./local/provider-catalog
```

The taxonomy CSV is one column with header `taxonomy_code`. The NPPES ZIP must
keep the monthly Version 2 basename; weekly, V1, and renamed files are
rejected. The catalog must contain a real `manifest.json` plus `providers/` and
`provider_taxonomies/` files accepted by the pinned consumer. Confirm:

```text
test -f local/provider-catalog/manifest.json && echo catalog_ok
test -f local/services.csv && echo services_ok
```

Missing or incompatible inputs produce the existing sanitized runtime
configuration failure. The first successful ingest pins the catalog under the
warehouse. Changing taxonomies, NPPES month, or catalog files while that
warehouse exists fails closed (`consumer_ingest_provider_changed`); start a
new warehouse instead of refreshing in place.

## Commands

```text
mrfpipeline migrate
mrfpipeline work --role <control|mrf|consumer>
mrfpipeline discover --payer uhc --collection-month <YYYY-MM> --limit <count> [--mrf-source-limit <N|all>]
mrfpipeline month sources set-total --payer <payer> --collection-month <YYYY-MM> --total <N|all>
mrfpipeline month status --payer <payer> --collection-month <YYYY-MM>
mrfpipeline month activate --payer <payer> --collection-month <YYYY-MM>
mrfpipeline month status
mrfpipeline reconcile
mrfpipeline retry --stage <job-kind> --id <domain-id>
```

Compose wrappers for those verbs (build, start, scale, `cli` one-shots) are
in [`DOCKER.md`](DOCKER.md).

`migrate` applies application and River schemas. No other command migrates.

`work` requires an explicit role. The control role validates the complete
environment, holds the singleton control lease, initializes the artifact root,
reconciles, and consumes discovery/TOC/admission queues. The `mrf` role owns
only MRF download and parse. The `consumer` role owns consumer ingest and plan
attachment and remains singleton while the pinned warehouse writer requires
serialized writes.

`discover` enqueues one bounded UHC discovery run for a building monthly
release and reports identifiers without waiting for downloads. `--limit` is a
TOC limit. `--mrf-source-limit` is the cumulative MRF target; it is required
when a release is still `unset`, and can be increased later with `set-total`.

`month status` reports strict completion blockers, or the complete active
`(payer_id, collection_month, output_id)` handoff relation with no selector.
`month activate` atomically publishes the currently consumed, plan-ready,
warehouse-valid outputs. The first activation freezes discovery/TOC inventory;
repeating it appends newly ready outputs while known MRF work continues.

`reconcile` performs only safe nonterminal repairs. Terminal failed stages
require `retry`; stop control first before running reconcile because it holds
the control lease. Reconciliation skips busy MRF/consumer domains.

`retry` reopens one exact failed stage and inserts a replacement River job.
It does not run the job. It coordinates through the release and exact
source/output execution locks, so workers may remain running. An attachment
retry reuses the frozen plan batch; a terminal download without a resident
slot is returned to blocked waiting state until control can refill it.

## Environment

| Variable | Used by |
|---|---|
| `MRFPIPELINE_DATABASE_URL` | all commands |
| `MRFPIPELINE_ARTIFACT_ROOT` | `work`, `reconcile` |
| `MRFPIPELINE_WAREHOUSE_PATH` | `work`, `reconcile` |
| `MRFPIPELINE_PROVIDER_CATALOG_PATH` | `work`, `reconcile` |
| `MRFPIPELINE_SERVICES_PATH` | `work`, `reconcile` |
| `MRFPIPELINE_MRF_RESIDENT_CAPACITY` | control `work` |

`retry` needs only the database URL so a remote operator can enqueue work for
the correctly configured worker host.

## Local Docker topology and operator commands

[`DOCKER.md`](DOCKER.md) is the copy-paste Compose command sheet. This
section is the topology contract those commands run against.

[`docker-compose.story21.yml`](docker-compose.story21.yml) is the runnable
local topology. It uses PostgreSQL's Compose service name on the private
Compose network; PostgreSQL is intentionally not published to the host. The
`cli` service is the preferred one-shot path for database commands.
The MRF and consumer services use the `workers` profile, so an unqualified
`docker compose up -d` starts only PostgreSQL, migration, and control
prerequisites; use the explicit worker-profile command below to start
processing.

All operator commands below run from the repository root. Repeat
`-f docker-compose.story21.yml`, or export `COMPOSE_FILE=docker-compose.story21.yml`.
Compose does not take a host pipeline database URL; workers use the unpublished
Compose PostgreSQL. Do not `--scale consumer=2`: the extra replica exits
`worker_busy`.

| Goal | Command |
|---|---|
| Build image | `DOCKER_BUILDKIT=1 docker compose -f docker-compose.story21.yml build` |
| Direct image build | `DOCKER_BUILDKIT=1 docker build --ssh default --tag mrfpipeline:local .` |
| Migrate | `docker compose -f docker-compose.story21.yml --profile operator run --rm cli migrate` |
| Start control | `docker compose -f docker-compose.story21.yml up -d control` |
| Follow control | `docker compose -f docker-compose.story21.yml logs -f --tail=50 control` |
| Start MRF + consumer | `docker compose -f docker-compose.story21.yml --profile workers up -d --scale mrf=2 mrf consumer` |
| Bounded discover | `docker compose -f docker-compose.story21.yml --profile operator run --rm cli discover --payer uhc --collection-month <YYYY-MM> --limit 1 --mrf-source-limit 3` |
| Month status | `docker compose -f docker-compose.story21.yml --profile operator run --rm cli month status --payer uhc --collection-month <YYYY-MM>` |
| Raise MRF target | `docker compose -f docker-compose.story21.yml --profile operator run --rm cli month sources set-total --payer uhc --collection-month <YYYY-MM> --total <N\|all>` |
| Retry one stage | `docker compose -f docker-compose.story21.yml --profile operator run --rm cli retry --stage <kind> --id <domain-id>` |
| Reconcile | stop control, then `docker compose -f docker-compose.story21.yml --profile operator run --rm cli reconcile` |
| Stalled-work query | `docker compose -f docker-compose.story21.yml exec -T postgres psql -U mrfpipeline -d mrfpipeline -v ON_ERROR_STOP=1 < scripts/stalled-work.sql` |
| Worker logs | `docker compose -f docker-compose.story21.yml logs -f --tail=50 control mrf consumer` |
| Stop roles | `docker compose -f docker-compose.story21.yml --profile workers stop consumer mrf control` |

If Compose build exits immediately with a Buildx context error (common when the
Docker context is Colima rather than Docker Desktop), build the same tag with
the direct command above, or `docker buildx build --ssh default --progress=plain --load --tag mrfpipeline:local .`. Continue at migrate once `docker images mrfpipeline:local` shows a real image.

Build and start in stages. Staging makes control initialization visible and
avoids treating Compose's `service_started` dependency as a readiness signal:

```text
docker compose -f docker-compose.story21.yml --profile operator run --rm cli migrate
docker compose -f docker-compose.story21.yml up -d control
docker compose -f docker-compose.story21.yml logs -f --tail=50 control
```

Control also waits for Compose's repeatable `migrate` service, so a direct
control start cannot race schema application; after the preferred one-shot
command above this dependency is a no-op migration check.

After the control log shows the application-owned structured event
with `msg=worker_started`, `kind=runtime`, and `role=control` fields, stop
following logs with Ctrl-C (this does not stop the container), then start the
MRF and consumer roles:

```text
docker compose -f docker-compose.story21.yml --profile workers up -d --scale mrf=2 mrf consumer
```

The control service has no automatic restart. A permanent capacity or
configuration error therefore leaves it exited with a sanitized log instead
of entering a restart loop. The MRF and consumer services use bounded
`on-failure` restarts for ordinary process crashes. They do not initialize the
artifact root; if they are started before control, they fail safely and may
retry, so the staged startup above is the supported path. If control exits,
stop those roles and inspect `docker compose logs control mrf consumer` before
repairing the input or capacity.

There is deliberately no control health check: a persistent workspace file is
not proof that a current process owns the control lease. Readiness is the
current control process log plus successful role startup. No heartbeat table
or monitoring service is added.

Run the following commands through the one-shot `cli` service. They use the
same internal database URL and mounts as the workers:

```text
docker compose -f docker-compose.story21.yml --profile operator run --rm cli discover --payer uhc --collection-month <YYYY-MM> --limit 1 --mrf-source-limit 3
docker compose -f docker-compose.story21.yml --profile operator run --rm cli month status --payer uhc --collection-month <YYYY-MM>
docker compose -f docker-compose.story21.yml --profile operator run --rm cli month sources set-total --payer uhc --collection-month <YYYY-MM> --total <N|all>
docker compose -f docker-compose.story21.yml --profile operator run --rm cli retry --stage <kind> --id <domain-id>
docker compose -f docker-compose.story21.yml --profile operator run --rm cli reconcile
```

`reconcile` requires only the control service to be stopped; leave MRF and
consumer running. It coordinates through release and exact execution locks,
while `retry` may also run with roles active:

```text
docker compose -f docker-compose.story21.yml stop control
docker compose -f docker-compose.story21.yml --profile operator run --rm cli reconcile
docker compose -f docker-compose.story21.yml up -d control
```

For a controlled retry, leave the MRF/consumer roles running and execute only
the `retry` command. `month activate` is also a one-shot CLI command. It does
not require strict `database_ready:true`: numeric targets, pending MRF work, and
terminal TOC/MRF file failures may remain. Discovery and TOC work must be
terminal, at least one output must be plan-ready, and consumer/attachment
failures still reject the checkpoint:

```text
docker compose -f docker-compose.story21.yml --profile operator run --rm cli month activate --payer uhc --collection-month <YYYY-MM>
```

Stop roles in reverse ownership order after a run:

```text
docker compose -f docker-compose.story21.yml --profile workers stop consumer mrf control
```

`docker compose down` removes containers and the network but keeps named
volumes. Do not use `docker compose down -v` unless deleting the disposable
PostgreSQL database and artifact volume is intentional.

Each MRF container has one parser worker, so two MRF containers provide up to
two concurrent parser calls; scale the service for more. The consumer remains
exactly one process because the pinned warehouse writer is not concurrent-safe.
The shared PostgreSQL resident capacity bounds selected raw/in-progress MRF
sources across all MRF containers. It is a count, not a byte quota, and does
not bound parsed output, temporary peak expansion, PostgreSQL, River history,
or the consumer warehouse. Docker CPU and memory limits are hard termination
limits, not soft backpressure; set them only after measuring the local run.

On Windows Docker Desktop, prefer the Linux named artifact volume in Compose
over a cloud-synchronized host directory for the first run. Control lease loss
cancels control work; per-source and per-output execution locks defer rescued
or duplicate deliveries without mutating domain attempts.

## Stage flow

1. `discover` admits at most `--limit` new TOC URLs for one payer/month
   building release. A TOC capture is identified by payer + month + exact URL.
2. Each TOC downloads, parses, and imports.
3. Exact MRF URL + collection-month captures download and parse once, even
   when several TOCs reference them. Parsing is plan-independent.
4. Each required source + payer + month snapshot is ingested once.
5. Plans attach additively in frozen batches. Later plans do not rewrite
   rate or provider Parquet.

Example: plans A and B attach first; later B and C yield warehouse
associations A, B, and C, and the second batch writes only C.

## Monthly releases and serving relation

Collection month is part of domain identity and release state, not a label.
Discovery creates a `building` release transactionally. First activation
changes it to `active`, freezes discovery and TOC-derived inventory, and writes
durable membership for only the currently consumed, plan-ready outputs. An
`inactive` release remains available for exact rollback/reactivation.

The serving relation is read from `monthly_release_outputs` as
`(payer_id, collection_month, 'mrf-' || snapshot.id)` for the active release.
A payer-less query uses the complete persisted relation; it does not infer
membership from every warehouse output or a global latest month. No automatic
latest-month or `current_*` serving view is supported.

An active release remains open only for already-known MRF download, parse,
consume, and attachment work. Repeating activation appends newly plan-ready
outputs. Discovery, TOC processing, source-target changes, and new plan
inventory are closed after first activation. Inactive releases are fully
frozen. Reconciliation audits only published output membership.

A snapshot is plan-ready only when consume succeeded, at least one attachment
batch succeeded, every plan is assigned, and no pending/running/failed batch
exists. A planless snapshot is not plan-ready. Do not serve planless rates.

## Operator procedures

Inspect the release before a checkpoint. `database_ready` remains the strict
all-work completion signal; incremental activation may succeed while its
blockers include numeric partial coverage, pending MRF work, or terminal
TOC/MRF file failures:

```text
mrfpipeline month status --payer uhc --collection-month 2026-09
```

Activate the currently ready subset explicitly. The first call switches the
payer and freezes TOC inventory; later calls append newly ready outputs.
Reactivating an inactive historical month restores its frozen membership:

```text
mrfpipeline month activate --payer uhc --collection-month 2026-09
mrfpipeline month activate --payer uhc --collection-month 2026-08  # rollback
```

Back up PostgreSQL together with the append-only warehouse, shared parsed MRF
directories, and provider catalog before a cutover or rebuild. For example:

```text
pg_dump --format=custom --file=mrfpipeline-2026-09.dump "$MRFPIPELINE_DATABASE_URL"
tar -C "$MRFPIPELINE_WAREHOUSE_PATH" -czf warehouse-2026-09.tgz .
tar -C "$MRFPIPELINE_ARTIFACT_ROOT/mrf" -czf parsed-mrf-2026-09.tgz .
tar -C "$MRFPIPELINE_PROVIDER_CATALOG_PATH" -czf provider-catalog-2026-09.tgz .
```

Restore the database, warehouse, parsed MRF tree, and catalog as one set before
starting workers; PostgreSQL's active-release mapping is required for serving.
For a fresh restore, the corresponding commands are:

```text
pg_restore --dbname="$MRFPIPELINE_DATABASE_URL" mrfpipeline-2026-09.dump
tar -C "$MRFPIPELINE_WAREHOUSE_PATH" -xzf warehouse-2026-09.tgz
tar -C "$MRFPIPELINE_ARTIFACT_ROOT/mrf" -xzf parsed-mrf-2026-09.tgz
tar -C "$MRFPIPELINE_PROVIDER_CATALOG_PATH" -xzf provider-catalog-2026-09.tgz
```

Populated pre-Story-14 databases and consumer `1.5.0` warehouses are
rebuild-only: preserve the backup, create fresh database/filesystem roots, run
`migrate`, and rebuild through the current feed-free pipeline.

A query-service handoff uses the complete no-selector relation and refreshes it
atomically after validating every selected `mrf-<snapshot-id>` against the
consumer 2.0.0 warehouse:

```text
mrfpipeline month status > active-output-relation.json
```

The service refreshes on startup/control-plane change, then serves the captured
relation without inferring membership from other warehouse outputs.

## Local runbook

Compose is the preferred local path. Host `mrfpipeline` uses the same verbs
with `MRFPIPELINE_*` set. Replace `<YYYY-MM>` with the collection month you
intend (CMS listing month, exactly `YYYY-MM`). The Compose-only command list
is [`DOCKER.md`](DOCKER.md).

`--limit` is newly admitted **TOC files**, required, and always a positive
integer. Unlimited TOC discovery is out of version 1 scope. `--mrf-source-limit`
is the cumulative **MRF source** target (`N` or `all`). Resident capacity
(`MRFPIPELINE_MRF_RESIDENT_CAPACITY`, default `2`) is how many selected raw
sources may occupy disk at once. It is not a byte quota and not a cap on how
many sources you can eventually process.

You do not drop MRF files into the repo. UHC listing/CDN supplies them. Raising
the target, or admitting more TOCs, is how a test processes more MRF sources.

### First run (bounded sample)

1. Prepare `.env`, a real catalog with `manifest.json`, and a selector with the
   `billing_code_type,billing_code` header. See [External local inputs](#external-local-inputs).
2. Load an SSH agent that can read the private sibling modules, then build
   `mrfpipeline:local` (Compose build or the direct `docker build --ssh default`
   command).
3. Migrate, start control, and wait for `worker_started`:

```text
docker compose -f docker-compose.story21.yml --profile operator run --rm cli migrate
docker compose -f docker-compose.story21.yml up -d control
docker compose -f docker-compose.story21.yml logs -f --tail=50 control
```

4. After a JSON line with `msg=worker_started`, `kind=runtime`, and
   `role=control`, stop following logs (Ctrl-C does not stop the container) and
   start workers:

```text
docker compose -f docker-compose.story21.yml --profile workers up -d --scale mrf=2 mrf consumer
```

5. Enqueue one TOC and three MRF sources. Discover returns identifiers and does
   not wait for downloads:

```text
docker compose -f docker-compose.story21.yml --profile operator run --rm cli discover --payer uhc --collection-month <YYYY-MM> --limit 1 --mrf-source-limit 3
```

6. Watch until selected work drains:

```text
docker compose -f docker-compose.story21.yml --profile operator run --rm cli month status --payer uhc --collection-month <YYYY-MM>
docker compose -f docker-compose.story21.yml logs -f --tail=50 control mrf consumer
```

Success for a numeric sample: completed outputs may be activated after
discovery/TOC work is terminal. `mrf_source_target_partial` remains a strict
completion blocker and `partial:true` remains visible, but neither prevents an
incremental publication checkpoint.

Before a bounded run, record free space on the artifact and warehouse
filesystems. During the run, monitor download and parsed bytes, warehouse
bytes, free space, database size, and pending/running MRF download/parse
counts. Version 1 does not guess required disk from HTTP headers. Stop the
worker if capacity approaches the operator safety threshold.

An interrupted or retryable download keeps its selected source and slot; the
existing staging directory is used for cleanup/resume. Automatic parse retries
keep the raw file and slot, so they do not redownload. When a download reaches
terminal failure (retry exhaustion, HTTP 404, or River UI cancel), the worker
removes unpublished download bytes and matching staging; an MRF slot is then
released. Recreate/SIGTERM remains an interruption and keeps bytes for resume.
When parse reaches a terminal failure, the worker removes unpublished parsed
output, parser staging, and raw bytes, marks download blocked, then releases the
slot and wakes control. The source remains selected and failed, and
`retry --stage mrf.download` redownloads from scratch; `retry --stage
mrf.parse` reuses valid raw bytes when present and rematerializes the download
when they are gone. `mrf.parse`, `mrf.download`, and `toc.download` have four
attempts including the first. HTTP 404 is not retried. Other stages retain
eight. After deploy, reconcile repairs already terminal parses that still hold
slots; do not delete slot rows from SQL.
Cancelling `mrf.download`, `mrf.parse`, or `toc.download` in River UI fails
that stage and deletes unpublished download bytes; download retry exhaustion
does the same. Recreate stays an interruption.

### Subsequent runs (same Compose volumes)

Named volumes keep PostgreSQL, artifacts, and the warehouse across container
restarts. Do not `docker compose down -v` unless you intend to delete that
state.

If roles were stopped:

```text
docker compose -f docker-compose.story21.yml up -d control
# wait for worker_started, then:
docker compose -f docker-compose.story21.yml --profile workers up -d --scale mrf=2 mrf consumer
docker compose -f docker-compose.story21.yml --profile operator run --rm cli month status --payer uhc --collection-month <YYYY-MM>
```

`migrate` is repeatable and a no-op when schemas are current. Do not enqueue
another `discover` just to resume: control already wakes eligible work.

Same payer/month, still `building`: use status, `retry`, or `set-total` as
below. A later `discover` with `--mrf-source-limit` must match the durable
target or it fails `source_target_conflict`; omit `--mrf-source-limit` to reuse
it.

New collection month: keep the same catalog/selector/warehouse only if the
pinned catalog is unchanged. Enqueue a new `discover` with that month and an
explicit `--mrf-source-limit` (required while the new release is unset).

### Admit more MRF sources (same test month)

The numeric target is cumulative even when PostgreSQL already contains many
pending sources: only the stable selected prefix up to the target can acquire
resident slots and executable MRF jobs. `partial:true` and
`mrf_source_target_partial` continue to describe incomplete full-month
coverage, while activation may publish the plan-ready subset.

To process more **already imported** MRF URLs, raise the target. It cannot
decrease, and `all` cannot be changed back to a number:

```text
docker compose -f docker-compose.story21.yml --profile operator run --rm cli month sources set-total --payer uhc --collection-month 2026-09 --total 6
docker compose -f docker-compose.story21.yml --profile operator run --rm cli month status --payer uhc --collection-month 2026-09
```

`--total` must be greater than the current numeric target. Control then selects
the next stable prefix and fills free resident slots. Leave workers running.

If `known_sources` is still below the new target, import more TOC files so more
MRF URLs exist, then raise the target if needed:

```text
docker compose -f docker-compose.story21.yml --profile operator run --rm cli discover --payer uhc --collection-month <YYYY-MM> --limit 2
```

`--limit 2` admits two **additional** TOCs. Omit `--mrf-source-limit` so the
existing target is reused.

### Run without an MRF cap

There is no unlimited TOC flag; pick a `--limit` large enough for the listing
you want. To select every known MRF source for the month, use `all`.

On a **new** unset release:

```text
docker compose -f docker-compose.story21.yml --profile operator run --rm cli discover --payer uhc --collection-month <YYYY-MM> --limit 1 --mrf-source-limit all
```

On an existing numeric building release:

```text
docker compose -f docker-compose.story21.yml --profile operator run --rm cli month sources set-total --payer uhc --collection-month <YYYY-MM> --total all
```

`all` still processes only sources discovered from admitted TOCs. Raise
`--limit` (new TOCs) if the listing has more TOC files you have not admitted.
Disk, warehouse, and River history remain operator-bounded; raise
`MRFPIPELINE_MRF_RESIDENT_CAPACITY` only if you want more concurrent raw
sources, not as a substitute for `all`.

Activation is an explicit publication checkpoint, not a claim that the full
month completed. Numeric targets and terminal TOC/MRF file failures are
allowed; pending MRF work continues and can be appended by running activation
again. Discovery/TOC work must be terminal, and consumer/attachment failures
must be resolved:

```text
docker compose -f docker-compose.story21.yml --profile operator run --rm cli month activate --payer uhc --collection-month <YYYY-MM>
```

The active `(payer, collection_month, output_id)` relation is a handoff for a
future serving process; this repository creates no query HTTP service or SQL
query facade.

## Crash, retry, and reconciliation

Interrupted nonterminal work converges through artifact publication and
startup reconciliation. `reconcile` restores missing eligible jobs and
normalizes orphaned claims for building releases. It never reopens a terminal
failed stage, and a sealed inconsistency is reported without mutation while
unrelated building repairs continue.

Use `retry --stage <kind> --id <id>` for one failed record in a building
release. Retry preserves numeric identity, output IDs, frozen attachment
items, and artifacts. A failed stage in an active/inactive release returns
`sealed_release_retry_forbidden` and does not mutate it. The normal worker
recognizes completed publication.

Every external work failure emits one structured, redacted application log
with a fixed failure code, kind, River identity, attempt, and `retrying` or
`terminal` outcome. Claim and bookkeeping failures use fixed lifecycle phases;
raw errors, URLs, paths, SQL, and response text are never logged. A runtime
`worker_lease_lost` record means the process stopped after its lease health
check failed.

For stalled work in the private Compose PostgreSQL, run the repository's
redacted query through the database container:

```text
docker compose -f docker-compose.story21.yml exec -T postgres psql -U mrfpipeline -d mrfpipeline -v ON_ERROR_STOP=1 < scripts/stalled-work.sql
```

For an externally reachable PostgreSQL connection, use:

```text
psql "$MRFPIPELINE_DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/stalled-work.sql
```

It returns only `stage`, `domain_id`, `status`, `river_job_id`, `attempt`, and
`age_seconds`; change `stale_after` in that script to another positive
interval when needed. Investigate when progress stops and running age keeps
increasing. The pipeline does not automatically cancel, retry, or delete
stalled jobs.

Manifest-present invalid parser output is never auto-deleted. Stop the
worker, inspect that exact generated directory, then `retry`.

### Recovery decision table

| Observed state | Operator action |
|---|---|
| Terminal failed stage | Confirm the release is building, then run `retry --stage <kind> --id <id>`. A sealed release returns `sealed_release_retry_forbidden`. |
| Stale pending/running stage | Run `reconcile.SQLStaleStages` with a positive interval and inspect the exact River identity; reconcile may repair building work, but never cancels or retries it automatically. |
| Invalid or incomplete generated output | Stop workers, preserve the directory for inspection, and retry only after the stage input/output is corrected. Successful publication loss is restored from the matching database/warehouse backup, not repaired in a sealed release. |
| Missing building input | Run `reconcile`, then retry the failed prerequisite or rerun the bounded building workflow; do not create domain rows by hand. |
| Missing or invalid sealed publication | Do not mutate or deactivate the release. Restore PostgreSQL and warehouse state together, or build and activate a new release after investigation. |

## Retention

Automatic cleanup, while the lease is held, removes only:

- a TOC download after parse succeeded and the TOC parser output is still valid
- an MRF download after parse succeeded and the shared parser output is still valid
- a TOC parsed leaf after import succeeded
- a `plan-batches/plan-batch-<id>` leaf after that batch succeeded
- real `.staging` children whose mtime is at least 24 hours old

Shared successful MRF parsed output has no automatic deletion path. The
pipeline never cleans the consumer warehouse or `<warehouse>/.staging`.

After success, parsed MRF output, consumer warehouse output, PostgreSQL data,
River job history, and any deliberate backups remain until an operator uses a
separate retention or backup procedure. Normal slot release deletes only the
raw/staging material that is safe to remove after successful publication; it
does not imply disk reclamation for the whole run. Keep a measured free-space
threshold and stop roles before crossing it.

Back up PostgreSQL, shared parsed MRFs, the append-only warehouse, and the
pinned provider-catalog identity. Restoring only warehouse files is
insufficient because active release mapping lives in PostgreSQL.

### Recovery procedures

- **Cutover:** run `month status --payer <payer> --collection-month <month>`,
  then `month activate` when discovery/TOC inventory is terminal, the source
  target is selected, at least one output is plan-ready, consumer/attachment
  publication has no failure, and warehouse preflight succeeds.
  `database_ready` is the stricter all-work signal and is not required for a
  Story 27 incremental checkpoint; numeric partial coverage, pending known MRF
  work, and terminal TOC/MRF file failures may remain.
- **Rollback/reactivation:** run `month activate` for the sealed historical
  month. A damaged publication is rejected and the current active pointer is
  left unchanged.
- **Backup and restore:** back up PostgreSQL together with shared parsed MRFs,
  the warehouse, and the pinned catalog. Restore all of them, run `month
  status` and `reconcile`, then refresh the query service from the verified
  active-output relation.
- **Rebuild-only old state:** stop workers, create a fresh database and
  warehouse, run `migrate`, prepare catalog/services, and rerun discovery and
  processing. Stories 01–13 databases and consumer 1.5.0 warehouses are not
  upgraded in place.
- **Query-service handoff:** load the complete active relation from `month
  status`, validate each exact consumer publication, and atomically refresh
  the service snapshot. Requests use that captured relation until the next
  explicit refresh and never infer membership from warehouse directories.

## Status queries

Default queries are redacted. They return validated payer/month values,
derived output IDs, blocker codes, counts, statuses, and numeric IDs only.
The targeted `month status --payer ... --collection-month ...` command is the
authoritative readiness check, including exact current River-job identity.
The aggregate query below is an operational overview, not a replacement for
that targeted check.

```sql
SELECT status, count(*) AS n
FROM mrfpipeline.monthly_releases
GROUP BY status
ORDER BY status;

SELECT r.payer_id, r.collection_month, 'mrf-' || s.id AS output_id
FROM mrfpipeline.monthly_releases r
JOIN mrfpipeline.monthly_release_outputs o
  ON o.payer_id = r.payer_id AND o.collection_month = r.collection_month
JOIN mrfpipeline.mrf_snapshots s ON s.id = o.mrf_snapshot_id
WHERE r.status = 'active'
ORDER BY r.payer_id, r.collection_month, s.id;

SELECT status, count(*) AS n
FROM mrfpipeline.discovery_runs
GROUP BY status
ORDER BY status;

SELECT payer_id, collection_month, download_status, parse_status,
       import_status, count(*) AS n
FROM mrfpipeline.toc_files
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5;

SELECT collection_month, download_status, parse_status, count(*) AS n
FROM mrfpipeline.mrf_sources
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

SELECT payer_id, collection_month, consume_status, count(*) AS n
FROM mrfpipeline.mrf_snapshots
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

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

`SQLBuildingReleaseBlockers` provides the stable stage/readiness blocker codes
and current River-job identity checks for all building releases. The targeted
monthly status command remains authoritative because it also reports release
counts and performs the full readiness/publication checks.

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

Against unpublished Compose PostgreSQL, run the UI on the Compose network
instead (`<project>` is usually `mrfpipeline`):

```text
docker run --rm -p 8080:8080 --network <project>_default \
  -e DATABASE_URL=<same URL as the worker> \
  -e RIVER_SCHEMA=mrfpipeline_river \
  ghcr.io/riverqueue/riverui:latest
```

Then open http://localhost:8080. The UI shows River queues and jobs, not domain
readiness; keep using `month status` for that.

Pause and resume queues. Pause stops fetching new jobs and does not cancel
in-flight work. Cancelling `mrf.download`, `mrf.parse`, or `toc.download` fails
that stage and deletes unpublished download bytes. The source stays selected;
reopen it with `retry`. Do not retry or delete jobs from the UI, and do not
cancel other kinds.

## Limitations

Discovery is UHC-only; month control and import honor the generic payer
contract. Local storage only. No automatic enrichment. No plan
removal/correction. No recurring discovery. Required `--limit`. No UI in this
binary. TOC import rejects HTTPS MRF
URLs that contain user information; the shared downloader would refuse
those locations.

Configured artifact, warehouse, catalog, and services paths must not
overlap, including the service selector sitting inside the warehouse.

## Tests

Tests, including PostgreSQL integration, DuckDB consumer paths, and focused
race tests, run on an operator machine that can resolve the private sibling
modules. Repository CI uses the explicitly named
`MRFPIPELINE_PRIVATE_MODULES_SSH_KEY` secret; until that secret is configured,
CI fails with a missing-prerequisite error rather than skipping private
modules or reporting a false green build.

```text
export GOPRIVATE=github.com/EnotPoloskun/*,github.com/enotpoloskun/mrfconsumer
go test ./...
go vet ./...
go test -race ./internal/jobs ./internal/mrfparse ./internal/work ./internal/reconcile
MRFPIPELINE_TEST_DATABASE_URL=<disposable-test-database> go test -p 1 ./...
```

The database-enabled command must use a disposable database whose name starts
with `mrfpipeline_test_`. Each package drops and recreates the application,
River, and `mrfpipeline_test` schemas, so `-p 1` is required when running the
complete suite against one database. The safety guard rejects production,
development, and ambiguously named databases before destructive DDL. Plain
`go test ./...` is the non-database command; do not use a parallel database
suite against the same URL.

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

The harness builds `./cmd/mrfpipeline`, runs `migrate`, starts control, MRF,
and consumer role processes, runs bounded UHC `discover`, and waits until
domain stages are idle. The default full path checks readiness, activates the
month, stops and restarts all roles, then reconciles twice. Set
`MRFPIPELINE_REAL_BOUNDED=1` with a numeric
`MRFPIPELINE_REAL_MRF_SOURCE_LIMIT` to run the partial-sample path instead;
it verifies selected work drains, resident slots and raw files are released,
and activation remains blocked. The harness records safe counts and verifies
restart/reconciliation do not duplicate domain or warehouse results.

The ordinary commands above never run live UHC acceptance. The real accepted
provider catalog and compatible selector remain explicit operator inputs.
