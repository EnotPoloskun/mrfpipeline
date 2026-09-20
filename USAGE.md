# Pipeline usage

How to set up `mrfpipeline` with Docker Compose and the commands you actually
run. Product behavior, flags, and blockers stay in [`README.md`](README.md).
The copy-paste Compose sheet is [`DOCKER.md`](DOCKER.md). Topology:
[`docker-compose.yml`](docker-compose.yml).

Run every command from the repository root. `-f docker-compose.yml` is
optional (it is the default Compose file). Compose workers never take a host
database URL; they use unpublished Compose PostgreSQL.

Operator CLI is the one-shot `cli` service:

```text
docker compose --profile operator run --rm cli <command>
```

`--payer` is `uhc`, `optum`, or `cigna`. `--collection-month` is `YYYY-MM`.

## What this stack is

| Piece | Role |
|---|---|
| `postgres` | Durable pipeline + River state (unpublished) |
| `migrate` | Applies schema, then exits |
| `control` | Singleton scheduler (discovery, TOC, admission) |
| `mrf` | One MRF parse per replica; scale with parser slots |
| `consumer` | One ingest/attach job per replica; scale independently |
| `cli` | One-shot operator commands (`operator` profile) |
| `riverui` | Optional River job UI on `127.0.0.1:8080` (`ui` profile) |

The image is `mrfpipeline:local`. It contains the pipeline binary and DuckDB
`v1.5.5`. It does **not** contain enricher, NPPES, or the provider catalog.
Produce the catalog with `mrfenricher` (separate program, separate Postgres),
then copy the export directory onto this host.

## What you need

- Docker Compose
- An SSH key that can read the private sibling modules (`mrfparser`,
  `mrfdiscoverer`, `mrfconsumer`) **only to build** the image
- A real `mrfenricher` catalog directory with `manifest.json`
- A services CSV with header `billing_code_type,billing_code`

The pipeline host does not need Go, River, DuckDB, enricher, or enricher
Postgres.

## Setup

### 1. Inputs

```text
cp .env.example .env
mkdir -p local/provider-catalog
cp config/services.csv.example local/services.csv
```

Copy a real enricher export under `local/provider-catalog/` (must contain
`manifest.json`). Edit `.env`:

| Variable | Meaning |
|---|---|
| `MRFPIPELINE_PROVIDER_CATALOG_DIR` | Enricher export directory |
| `MRFPIPELINE_SERVICES_FILE` | Selector CSV (`billing_code_type,billing_code`) |
| `MRFPIPELINE_MRF_RESIDENT_CAPACITY` | Concurrent raw MRF sources; must match `mrf` replica count |

Confirm:

```text
test -f local/provider-catalog/manifest.json && echo catalog_ok
test -f local/services.csv && echo services_ok
```

Create these files **before** any Compose command that mounts them. A missing
`local/services.csv` is created as a directory, and control then fails.

### 2. Build the image

```text
eval "$(ssh-agent -s)"
ssh-add <path-to-private-module-key>
export GOPRIVATE=github.com/EnotPoloskun/*,github.com/enotpoloskun/mrfconsumer
DOCKER_BUILDKIT=1 docker compose -f docker-compose.yml build
```

Direct equivalent:

```text
DOCKER_BUILDKIT=1 docker build --ssh default --tag mrfpipeline:local .
```

If Compose build fails with a Buildx/Colima context error:

```text
docker buildx build --ssh default --progress=plain --load --tag mrfpipeline:local .
```

Confirm `docker images mrfpipeline:local` before migrate.

Pushing to a private registry is optional:

```text
docker tag mrfpipeline:local ghcr.io/<namespace>/mrfpipeline:<git-sha>
docker push ghcr.io/<namespace>/mrfpipeline:<git-sha>
```

Keep that package private. The image includes compiled private modules.

### 3. First start

```text
docker compose --profile operator run --rm cli migrate
docker compose up -d control
docker compose logs -f --tail=50 control
```

Wait for a JSON line with `msg=worker_started`, `kind=runtime`, and
`role=control`. Ctrl-C stops following logs; it does not stop control.

Then start workers (2 parsers + 1 consumer is the documented first start):

```text
docker compose --profile workers up -d --scale mrf=2 mrf consumer
```

Match capacity to parser count. For 2 parsers, `.env` should have
`MRFPIPELINE_MRF_RESIDENT_CAPACITY=2` (the default). For 6 parsers:

```text
scripts/scale-mrf.sh 6
```

That writes capacity, recreates **control only**, and adds/removes `mrf`
replicas without recreating in-flight parser containers.

Plain `docker compose up -d` starts only PostgreSQL, migrate, and control.
Workers are the `workers` profile. River UI is the `ui` profile.

## Typical month

Replace the payer and month.

```text
# 1. Enqueue discovery (required --limit; --mrf-source-limit on a new month)
docker compose --profile operator run --rm cli discover \
  --payer optum --collection-month 2026-09 --limit all --mrf-source-limit all

# 2. Watch
docker compose --profile operator run --rm cli stats --payer optum --collection-month 2026-09
docker compose --profile operator run --rm cli month status --payer optum --collection-month 2026-09
docker compose logs -f --tail=50 control mrf consumer

# 3. If parsers are saturated, add slots (example: 6)
scripts/scale-mrf.sh 6

# 4. If ingest is the bottleneck, add consumers (example: 2). Does not
#    recreate parsers or change resident capacity.
scripts/scale-consumer.sh 2

# 5. Retry one failed stage (domain id, not a River job id)
docker compose --profile operator run --rm cli retry --stage mrf.parse --id <domain-id>

# 6. After discovery/TOC is terminal, build filters then activate
docker compose --profile operator run --rm --no-deps cli filters status --payer optum --collection-month 2026-09
docker compose --profile operator run --rm --no-deps cli filters build --payer optum --collection-month 2026-09
docker compose --profile operator run --rm --no-deps cli month activate --payer optum --collection-month 2026-09
```

`--limit` is additional TOC files for **this** discover run (`N` or `all`).
`--mrf-source-limit` / `set-total --total` is the cumulative MRF source
target. Required while the release target is still `unset`. On a later
discover for the same building month, omit `--mrf-source-limit` to reuse the
durable target. A different value fails `source_target_conflict`.

First `month activate` freezes discovery/TOC inventory. MRF
download/parse/consume/attach continue. Repeat activate to append newly ready
outputs. Do not activate before discovery/TOC work is terminal.

## Command list

### Stack

| Goal | Command |
|---|---|
| Build image | `DOCKER_BUILDKIT=1 docker compose -f docker-compose.yml build` |
| Direct image build | `DOCKER_BUILDKIT=1 docker build --ssh default --tag mrfpipeline:local .` |
| Migrate | `docker compose --profile operator run --rm cli migrate` |
| Start control | `docker compose up -d control` |
| Follow control | `docker compose logs -f --tail=50 control` |
| Start workers | `docker compose --profile workers up -d --scale mrf=2 mrf consumer` |
| List containers | `docker compose ps` |
| Worker logs | `docker compose logs -f --tail=50 control mrf consumer` |
| Stop workers + control | `docker compose --profile workers stop consumer mrf control` |
| Stop control only | `docker compose stop control` |
| Remove containers, keep volumes | `docker compose down` |

### Scale

Parser slots are two knobs that must match: `MRFPIPELINE_MRF_RESIDENT_CAPACITY`
and `--scale mrf=N`. Consumer replicas are independent.

| Goal | Command |
|---|---|
| Parser slots (preferred) | `scripts/scale-mrf.sh <N>` |
| Consumer replicas (preferred) | `scripts/scale-consumer.sh <N>` |
| Parsers by hand | `docker compose --profile workers up -d --no-recreate --scale mrf=<N> --scale consumer=<C> mrf consumer` |
| Consumers by hand | `docker compose --profile workers up -d --no-recreate --scale mrf=<N> --scale consumer=<C> mrf consumer` |

When scaling by hand, pass **both** `--scale` values. Scaling only `consumer`
can drop extra `mrf` replicas back to 1. `--no-recreate` keeps in-flight
parser containers. Do not `--force-recreate` `mrf` while a parse is running
unless you accept interrupting it.

`scripts/scale-mrf.sh` recreates control so it rereads capacity. Consumer
scale does not recreate control or parsers.

Each `mrf` replica runs one parse at a time. Each `consumer` replica runs one
ingest/attach job at a time. Distinct warehouse `output_id`s may overlap; the
same `output_id` stays serial.

### Observe

| Goal | Command |
|---|---|
| Stage counts | `docker compose --profile operator run --rm cli stats` |
| Stage counts for one month | `docker compose --profile operator run --rm cli stats --payer optum --collection-month 2026-09` |
| JSON stats | `docker compose --profile operator run --rm cli stats --payer optum --collection-month 2026-09 --json` |
| Month readiness | `docker compose --profile operator run --rm cli month status` |
| One month readiness | `docker compose --profile operator run --rm cli month status --payer optum --collection-month 2026-09` |
| Filter catalog state | `docker compose --profile operator run --rm cli filters status --payer optum --collection-month 2026-09` |
| Stalled work | `docker compose exec -T postgres psql -U mrfpipeline -d mrfpipeline -v ON_ERROR_STOP=1 < scripts/stalled-work.sql` |

`stats` is stage counts. `month status` is slots, blockers, and whether the
month can be activated. Worker logs never print URLs.

### Discover, admit, retry, reconcile

| Goal | Command |
|---|---|
| Bounded discover | `docker compose --profile operator run --rm cli discover --payer uhc --collection-month <YYYY-MM> --limit 1 --mrf-source-limit 3` |
| Full-month discover | `docker compose --profile operator run --rm cli discover --payer optum --collection-month 2026-09 --limit all --mrf-source-limit all` |
| Later discover, reuse target | `docker compose --profile operator run --rm cli discover --payer optum --collection-month 2026-09 --limit 11` |
| Raise MRF target | `docker compose --profile operator run --rm cli month sources set-total --payer optum --collection-month 2026-09 --total <N\|all>` |
| Retry one stage | `docker compose --profile operator run --rm cli retry --stage <kind> --id <domain-id>` |
| Reconcile | stop control, then `docker compose --profile operator run --rm cli reconcile`, then `docker compose up -d control` |

`--total` must be greater than the current numeric target. `all` cannot be
changed back to a number. Leave workers running; control selects the next
prefix and fills free slots.

If `mrf_sources_known` is still below the new target, admit more TOC files
(`--limit` is additional TOCs, not a reset).

`retry --stage` is one of:

- `discovery.run`
- `toc.download`
- `toc.parse`
- `toc.import`
- `mrf.download`
- `mrf.parse`
- `consumer.ingest`
- `consumer.attach_plans`

`--id` is the domain row, not a River job id. The command enqueues a
replacement job; workers pick it up. It may run while workers are active.

Reconcile (stop only control; leave `mrf` and `consumer` running):

```text
docker compose stop control
docker compose --profile operator run --rm cli reconcile
docker compose up -d control
```

Use this after deploying terminal-parse slot release so already-failed parses
that still hold slots are repaired. Do not delete slot rows by SQL.

### Activate and filter catalogs

Build filters before first activation. Rerun the build after a stale race:

```text
docker compose --profile operator run --rm --no-deps cli filters status --payer uhc --collection-month <YYYY-MM>
docker compose --profile operator run --rm --no-deps cli filters build --payer uhc --collection-month <YYYY-MM>
docker compose --profile operator run --rm --no-deps cli month activate --payer uhc --collection-month <YYYY-MM>
```

Published catalogs are immutable. Reactivating an inactive month rolls back to
its exact published generation. Activating a new month makes the previous
month inactive. Back up PostgreSQL and the warehouse together.

Filter help smoke checks (same as CI):

```text
docker compose --profile operator run --rm --no-deps cli filters status --help
docker compose --profile operator run --rm --no-deps cli filters build --help
docker compose --profile operator run --rm --no-deps cli filters measure --help
```

Private measurement wrappers (not Compose; they need a host
`MRFPIPELINE_DATABASE_URL` pointing at a disposable environment):

```text
scripts/measure-filter-build.sh <payer> <YYYY-MM>
scripts/lookup-filter-measure.sh <payer> <YYYY-MM> <CPT|HCPCS> <code> <plan-id> <file>
```

`filters measure` refuses an existing ready/published target and never
activates.

### River UI

Optional. Off until you start it. Binds to loopback only
(`RIVER_SCHEMA=mrfpipeline_river`):

```text
docker compose --profile ui up -d riverui
```

Open http://localhost:8080. It shows River queues and jobs, not domain
readiness; keep using `month status` for that.

Pause and resume queues. Cancelling `mrf.download`, `mrf.parse`, or
`toc.download` fails that stage and deletes unpublished download bytes. Do not
retry or delete jobs from the UI, and do not cancel other kinds.

```text
docker compose --profile ui stop riverui
```

### Reload a rebuilt image

Rebuild `mrfpipeline:local`, then recreate workers. Do not recreate `mrf`
while a large parse is still running unless you accept interrupting it.

```text
DOCKER_BUILDKIT=1 docker compose -f docker-compose.yml build
docker compose --profile workers up -d --no-build --force-recreate --no-deps --scale mrf=3 mrf consumer
docker compose stop control
docker compose --profile operator run --rm cli migrate
docker compose --profile operator run --rm cli reconcile
docker compose up -d --no-build --force-recreate --no-deps control
```

Confirm every pipeline container is on the new image:

```text
docker compose ps
docker inspect --format '{{.Name}} {{.Image}}' \
  mrfpipeline-control-1 mrfpipeline-mrf-1 mrfpipeline-consumer-1
```

### Host binary (optional)

Compose is the supported runtime. A host binary is for tests and help only:

```text
CGO_ENABLED=0 go build ./cmd/mrfpipeline
./mrfpipeline --help
./mrfpipeline --version
```

A macOS/Windows host binary is not a substitute for the Linux image.

## Do not

- `docker compose down -v` unless deleting the disposable PostgreSQL and
  artifact volumes is intentional
- run first `month activate` before discovery/TOC work is terminal
- retry or delete River jobs from River UI; do not cancel kinds other than
  `mrf.download` / `mrf.parse` / `toc.download`
- `DELETE` from `mrf_materialization_slots`
- change the catalog or selector while a warehouse already exists
- put enricher or an enricher database URL into this stack
- `--force-recreate` in-flight `mrf` containers unless you accept interrupting
  those parses
