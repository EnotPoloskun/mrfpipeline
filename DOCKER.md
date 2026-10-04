# Local pipeline setup (Docker Compose)

This is the copy-paste Compose command sheet for running `mrfpipeline` on a
machine that already has Docker Compose. The setup walkthrough and command
list are in [`USAGE.md`](USAGE.md). Product behavior, flags, and blockers
stay in [`README.md`](README.md). The topology file is
[`docker-compose.yml`](docker-compose.yml).

The pipeline image is `mrfpipeline:local`. It contains the `mrfpipeline`
binary and pinned DuckDB `v1.5.5`. It does **not** contain enricher, NPPES
data, or the provider catalog. Enricher is a separate program with its own
Postgres; copy its export directory onto this host.

Run every command from the repository root. `-f docker-compose.yml` is
optional (it is the default Compose file). Compose does not take a host
pipeline database URL; workers and the `cli` service use unpublished
Compose PostgreSQL. The optional River UI profile publishes only
`127.0.0.1:8080`.

## Do not

- `docker compose down -v` unless deleting the disposable PostgreSQL and
  artifact volumes is intentional
- run first `month activate` before discovery/TOC work is terminal; activation
  freezes that inventory, while known MRF work continues
- retry or delete River jobs from River UI; do not cancel kinds other than
  `mrf.download` / `mrf.parse` / `toc.download` (those fail the stage; MRF
  download cleanup frees the slot)
- `DELETE` from `mrf_materialization_slots`
- change the catalog or selector while a warehouse already exists
- put enricher or an enricher database URL into this stack

## What you need

- Docker Compose
- An SSH key that can read the private sibling modules (`mrfparser`,
  `mrfdiscoverer`, `mrfconsumer`) **only to build** the image
- A real `mrfenricher` catalog directory with `manifest.json`
- A services CSV with header `billing_code_type,billing_code`
- Disk: resident slots bound in-flight raw MRFs, not warehouse/parsed output

The other computer does not need Go, River, DuckDB, enricher, or enricher
Postgres.

## Inputs

```text
cp .env.example .env
mkdir -p local/provider-catalog
cp config/services.csv.example local/services.csv
```

Copy those files **before** any Compose command that mounts them. Put a real
`mrfenricher` catalog under `local/provider-catalog/` (must contain
`manifest.json`). Edit `.env`:

- `MRFPIPELINE_PROVIDER_CATALOG_DIR` — catalog directory (enricher export)
- `MRFPIPELINE_SERVICES_FILE` — selector CSV with header
  `billing_code_type,billing_code`
- `MRFPIPELINE_MRF_RESIDENT_CAPACITY` — concurrent raw MRF sources (default
  `2`). Raise this only if you also scale the `mrf` service.

Confirm:

```text
test -f local/provider-catalog/manifest.json && echo catalog_ok
test -f local/services.csv && echo services_ok
```

## Build the image

Preferred:

```text
eval "$(ssh-agent -s)"
ssh-add <path-to-private-module-key>
export GOPRIVATE=github.com/EnotPoloskun/*,github.com/enotpoloskun/mrfconsumer
DOCKER_BUILDKIT=1 docker compose -f docker-compose.yml build
```

Direct:

```text
DOCKER_BUILDKIT=1 docker build --ssh default --tag mrfpipeline:local .
```

If Compose build exits with a Buildx context error (common with Colima):

```text
docker buildx build --ssh default --progress=plain --load --tag mrfpipeline:local .
```

Confirm `docker images mrfpipeline:local` shows a real image before migrate.

After dependency updates, rebuild this shared image and recreate every worker
role, preserving configured replica counts. Stop old workers before the
cutover to avoid mixing parser/consumer versions. The current pairing requires
parser `1.3.0`; older parsed outputs need reparsing, not manifest relabeling.
See [parser input compatibility](README.md#parser-input-compatibility).

The image contains the statically built `mrfpipeline` binary and the official
DuckDB CLI at `/usr/local/bin/duckdb`, both pinned to the repository contract;
DuckDB is exact `v1.5.5` for both `linux/amd64` and `linux/arm64` builds.
DuckDB is not a Compose service, has no port or persistent volume, and never
runs in a worker. CI verifies `mrfpipeline --version`, `duckdb --version`, and
the static binary after the final image build.

After a code change, rebuild the tag, then recreate running services so they
pick up the new image (see [Reload a rebuilt image](#reload-a-rebuilt-image)).

Pushing to a private registry (for example GHCR) is optional and is not
required to run locally. Tag and push only after `docker login`:

```text
docker tag mrfpipeline:local ghcr.io/<namespace>/mrfpipeline:<git-sha>
docker push ghcr.io/<namespace>/mrfpipeline:<git-sha>
```

Keep that package private. The image includes compiled private modules.

## First start

```text
docker compose -f docker-compose.yml --profile operator run --rm cli migrate
docker compose -f docker-compose.yml up -d control
docker compose -f docker-compose.yml logs -f --tail=50 control
```

Wait for a JSON line with `msg=worker_started`, `kind=runtime`, and
`role=control`. Ctrl-C stops following logs; it does not stop control.

Then start workers. Each `mrf` container runs one parse at a time. Scale `mrf`
to the number of parallel parses you want, and set
`MRFPIPELINE_MRF_RESIDENT_CAPACITY` to at least that number (then recreate
control so it rereads `.env`):

```text
docker compose -f docker-compose.yml --profile workers up -d --scale mrf=2 mrf consumer
```

Three parallel parses:

```text
# in .env: MRFPIPELINE_MRF_RESIDENT_CAPACITY=3
docker compose -f docker-compose.yml up -d --force-recreate --no-deps control
docker compose -f docker-compose.yml --profile workers up -d --scale mrf=3 mrf consumer
```

Or use the wrappers, which keep in-flight `mrf` containers:

```text
scripts/scale-mrf.sh 3
scripts/scale-consumer.sh 1
```

Scale consumer replicas:

```text
docker compose -f docker-compose.yml --profile workers up -d --scale consumer=2 mrf consumer
```

Default Compose still starts one consumer replica; scale explicitly when needed.

Plain `docker compose up -d` starts only PostgreSQL, migrate, and control.
`mrf` and `consumer` are behind the `workers` profile. River UI is behind
the `ui` profile.

## Operator CLI (one-shot `cli` service)

These are the Compose forms of `mrfpipeline migrate`, `discover`, `stats`,
`month status`, `month sources set-total`, `month activate`, `filters status`,
`filters build`, `retry`, and `reconcile`.

```text
docker compose -f docker-compose.yml --profile operator run --rm cli migrate
docker compose -f docker-compose.yml --profile operator run --rm cli discover --payer uhc --collection-month <YYYY-MM> --limit 1 --mrf-source-limit 3
docker compose -f docker-compose.yml --profile operator run --rm cli stats
docker compose -f docker-compose.yml --profile operator run --rm cli stats --payer uhc --collection-month <YYYY-MM>
docker compose -f docker-compose.yml --profile operator run --rm cli month status --payer uhc --collection-month <YYYY-MM>
docker compose -f docker-compose.yml --profile operator run --rm cli month status
docker compose -f docker-compose.yml --profile operator run --rm cli month sources set-total --payer uhc --collection-month <YYYY-MM> --total <N|all>
docker compose -f docker-compose.yml --profile operator run --rm cli filters status --payer uhc --collection-month <YYYY-MM>
docker compose -f docker-compose.yml --profile operator run --rm cli filters build --payer uhc --collection-month <YYYY-MM>
docker compose -f docker-compose.yml --profile operator run --rm cli retry --stage <kind> --id <domain-id>
docker compose -f docker-compose.yml --profile operator run --rm cli month activate --payer uhc --collection-month <YYYY-MM>
```

Supported `--payer` values: `uhc`, `optum`, `cigna`, `molina`, `oscar`, `anthem`. `--limit` is `N` or `all`.
`--mrf-source-limit all` selects every known MRF source for the month.

Full-month example:

```text
docker compose -f docker-compose.yml --profile operator run --rm cli discover --payer optum --collection-month 2026-09 --limit all --mrf-source-limit all
```

## Filter catalog operations

Build before activation and rerun the build explicitly after any stale race:

```text
docker compose -f docker-compose.yml --profile operator run --rm --no-deps cli filters status --payer uhc --collection-month <YYYY-MM>
docker compose -f docker-compose.yml --profile operator run --rm --no-deps cli filters build --payer uhc --collection-month <YYYY-MM>
docker compose -f docker-compose.yml --profile operator run --rm --no-deps cli month activate --payer uhc --collection-month <YYYY-MM>
```

The final image help smoke checks are:

```text
docker compose -f docker-compose.yml --profile operator run --rm --no-deps cli filters status --help
docker compose -f docker-compose.yml --profile operator run --rm --no-deps cli filters build --help
docker compose -f docker-compose.yml --profile operator run --rm --no-deps cli filters measure --help
```

The measurement wrapper is private and disposable only. Run it outside the
Compose service when its normalized environment points at the private
warehouse and database:

```text
scripts/measure-filter-build.sh <payer> <YYYY-MM>
scripts/lookup-filter-measure.sh <payer> <YYYY-MM> <CPT|HCPCS> <code> <plan-id> <file>
```

Measurement refuses an existing ready/published target, never activates, emits
only its exact sanitized 17-field JSON, and uses platform-supported
`/usr/bin/time`. Lookup output is private `EXPLAIN (ANALYZE, BUFFERS)` evidence
from fixed parameterized serving queries; it never changes PostgreSQL.

Published catalogs are immutable. Reactivating an inactive month rolls back to
its exact published generation; activating a new month makes the previous
month inactive. Back up PostgreSQL and the warehouse together. Restore both,
run `reconcile` and targeted `month status`/`filters status`, then verify the
active relation before serving.

`--limit` is additional TOC files and is always required on `discover` (`N` or `all`).
`--mrf-source-limit` / `set-total --total` is the cumulative MRF source
target. It is required while the release target is still `unset`.

On a later `discover` for the same building month, omit `--mrf-source-limit`
to reuse the durable target. If the CLI rejects that with `mrf_source_limit`,
pass the same current target explicitly (for example `--mrf-source-limit 40`).
A different value fails `source_target_conflict`.

`retry --stage` is one of: `discovery.run`, `toc.download`, `toc.parse`,
`toc.import`, `mrf.download`, `mrf.parse`, `consumer.ingest`,
`consumer.attach_plans`. `--id` is the domain row, not a River job id.

`mrf.parse`, `mrf.download`, and `toc.download` have four attempts including
the first. A terminal download (retry exhaustion, HTTP 404 or 410, or River UI cancel)
deletes unpublished download bytes and matching staging; MRF cleanup then frees
the resident slot. Recreate/SIGTERM stays an interruption and keeps bytes for
resume. After a terminal parse, the worker deletes unpublished parsed output,
parser staging, and the raw download, then frees the resident slot. The source
stays selected and failed. `retry --stage mrf.download --id <source-id>`
redownloads from scratch; `retry --stage mrf.parse --id <source-id>`
rematerializes the download when bytes are gone.

## Admit more MRF sources

Raise the cumulative target when PostgreSQL already has unused known sources:

```text
docker compose -f docker-compose.yml --profile operator run --rm cli month sources set-total --payer uhc --collection-month <YYYY-MM> --total <N|all>
docker compose -f docker-compose.yml --profile operator run --rm cli month status --payer uhc --collection-month <YYYY-MM>
```

`--total` must be greater than the current numeric target. `all` cannot be
changed back to a number. Leave workers running; control selects the next
prefix and fills free slots.

If `mrf_sources_known` is still below the new target, admit more TOC files
(`--limit` is additional TOCs, not a reset):

```text
docker compose -f docker-compose.yml --profile operator run --rm cli discover --payer uhc --collection-month <YYYY-MM> --limit 11 --mrf-source-limit <current-target>
```

Repeat `discover` with a larger `--limit` until the intended TOC/source
inventory is present. First activation freezes discovery/TOC inventory.
Numeric targets remain valid for incremental activation; `partial:true` and
`mrf_source_target_partial` continue to report incomplete full-month coverage.

## Reconcile

Stop only control. Leave `mrf` and `consumer` running:

```text
docker compose -f docker-compose.yml stop control
docker compose -f docker-compose.yml --profile operator run --rm cli reconcile
docker compose -f docker-compose.yml up -d control
```

Use this after deploying terminal-parse slot release so already-failed parses
that still hold slots are repaired. Do not delete slot rows by SQL.

## Reload a rebuilt image

Rebuild `mrfpipeline:local`, then recreate workers. Do not recreate `mrf`
while a large parse is still running unless you accept interrupting it.

```text
DOCKER_BUILDKIT=1 docker compose -f docker-compose.yml build
docker compose -f docker-compose.yml --profile workers up -d --no-build --force-recreate --no-deps --scale mrf=3 mrf consumer
docker compose -f docker-compose.yml stop control
docker compose -f docker-compose.yml --profile operator run --rm cli migrate
docker compose -f docker-compose.yml --profile operator run --rm cli reconcile
docker compose -f docker-compose.yml up -d --no-build --force-recreate --no-deps control
```

Confirm every pipeline container is on the new image:

```text
docker compose -f docker-compose.yml ps
docker inspect --format '{{.Name}} {{.Image}}' \
  mrfpipeline-control-1 mrfpipeline-mrf-1 mrfpipeline-mrf-2 mrfpipeline-mrf-3 mrfpipeline-consumer-1
```

## Status, logs, stalled work

```text
docker compose -f docker-compose.yml --profile operator run --rm cli month status --payer uhc --collection-month <YYYY-MM>
docker compose -f docker-compose.yml logs -f --tail=50 control mrf consumer
docker compose -f docker-compose.yml ps
docker compose -f docker-compose.yml exec -T postgres psql -U mrfpipeline -d mrfpipeline -v ON_ERROR_STOP=1 < scripts/stalled-work.sql
```

`mrfpipeline stats` is the read-only operational count table. `month status` is the readiness check. Worker logs never print URLs.

Watch progress:

```text
docker compose -f docker-compose.yml --profile operator run --rm cli stats --payer <payer> --collection-month <YYYY-MM>
docker compose -f docker-compose.yml --profile operator run --rm cli month status --payer <payer> --collection-month <YYYY-MM>
```

`stats` is stage counts. `month status` is slots, blockers, and whether the
month can be activated.

## River UI (optional)

The `ui` profile runs official River UI on http://localhost:8080 bound to
loopback. It uses Compose PostgreSQL on the private network
(`RIVER_SCHEMA=mrfpipeline_river`). It is off until you start it:

```text
docker compose -f docker-compose.yml --profile ui up -d riverui
```

Pause and resume queues. Cancelling `mrf.download`, `mrf.parse`, or
`toc.download` fails that stage and deletes unpublished download bytes. Do not
retry or delete jobs, and do not cancel other kinds. Keep using `month status`
for domain readiness.

Stop the UI without touching workers:

```text
docker compose -f docker-compose.yml --profile ui stop riverui
```

## Stop and tear down

```text
docker compose -f docker-compose.yml --profile workers stop consumer mrf control
```

`docker compose -f docker-compose.yml down` removes containers and the
network but keeps named volumes (PostgreSQL, artifacts, warehouse).
