# Docker Compose operator commands

Copy-paste Compose wrappers for the same verbs as the host `mrfpipeline`
binary. Product behavior, flags, and blockers stay in
[`README.md`](README.md). The file for this topology is
[`docker-compose.story21.yml`](docker-compose.story21.yml).

Run every command from the repository root. Repeat
`-f docker-compose.story21.yml`, or export
`COMPOSE_FILE=docker-compose.story21.yml`. Compose does not take a host
pipeline database URL; workers and the `cli` service use unpublished
Compose PostgreSQL.

## Do not

- `--scale consumer=2` (the extra replica exits `worker_busy`)
- `docker compose down -v` unless deleting the disposable PostgreSQL and
  artifact volumes is intentional
- `month activate` on a numeric MRF sample (`mrf_source_target` is a number,
  not `all`)
- retry or delete River jobs from River UI; do not cancel kinds other than
  `mrf.download` / `mrf.parse` (those two fail the stage and free the slot)
- `DELETE` from `mrf_materialization_slots`
- change the catalog or selector while a warehouse already exists

## Inputs

```text
cp .env.example .env
mkdir -p local/provider-catalog
cp config/services.csv.example local/services.csv
```

Copy those files **before** any Compose command that mounts them. Put a real
`mrfenricher` catalog under `local/provider-catalog/` (must contain
`manifest.json`). Edit `.env`:

- `MRFPIPELINE_PROVIDER_CATALOG_DIR` — catalog directory
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
DOCKER_BUILDKIT=1 docker compose -f docker-compose.story21.yml build
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

After a code change, rebuild the tag, then recreate running services so they
pick up the new image (see [Reload a rebuilt image](#reload-a-rebuilt-image)).

## First start

```text
docker compose -f docker-compose.story21.yml --profile operator run --rm cli migrate
docker compose -f docker-compose.story21.yml up -d control
docker compose -f docker-compose.story21.yml logs -f --tail=50 control
```

Wait for a JSON line with `msg=worker_started`, `kind=runtime`, and
`role=control`. Ctrl-C stops following logs; it does not stop control.

Then start workers. Each `mrf` container runs one parse at a time. Scale `mrf`
to the number of parallel parses you want, and set
`MRFPIPELINE_MRF_RESIDENT_CAPACITY` to at least that number (then recreate
control so it rereads `.env`):

```text
docker compose -f docker-compose.story21.yml --profile workers up -d --scale mrf=2 mrf consumer
```

Three parallel parses:

```text
# in .env: MRFPIPELINE_MRF_RESIDENT_CAPACITY=3
docker compose -f docker-compose.story21.yml up -d --force-recreate --no-deps control
docker compose -f docker-compose.story21.yml --profile workers up -d --scale mrf=3 mrf consumer
```

Leave consumer at one replica.

## Operator CLI (one-shot `cli` service)

These are the Compose forms of `mrfpipeline migrate`, `discover`,
`month status`, `month sources set-total`, `month activate`, `retry`, and
`reconcile`.

```text
docker compose -f docker-compose.story21.yml --profile operator run --rm cli migrate
docker compose -f docker-compose.story21.yml --profile operator run --rm cli discover --payer uhc --collection-month <YYYY-MM> --limit 1 --mrf-source-limit 3
docker compose -f docker-compose.story21.yml --profile operator run --rm cli month status --payer uhc --collection-month <YYYY-MM>
docker compose -f docker-compose.story21.yml --profile operator run --rm cli month status
docker compose -f docker-compose.story21.yml --profile operator run --rm cli month sources set-total --payer uhc --collection-month <YYYY-MM> --total <N|all>
docker compose -f docker-compose.story21.yml --profile operator run --rm cli retry --stage <kind> --id <domain-id>
docker compose -f docker-compose.story21.yml --profile operator run --rm cli month activate --payer uhc --collection-month <YYYY-MM>
```

`--limit` is additional TOC files and is always required on `discover`.
`--mrf-source-limit` / `set-total --total` is the cumulative MRF source
target. It is required while the release target is still `unset`.

On a later `discover` for the same building month, omit `--mrf-source-limit`
to reuse the durable target. If the CLI rejects that with `mrf_source_limit`,
pass the same current target explicitly (for example `--mrf-source-limit 40`).
A different value fails `source_target_conflict`.

`retry --stage` is one of: `discovery.run`, `toc.download`, `toc.parse`,
`toc.import`, `mrf.download`, `mrf.parse`, `consumer.ingest`,
`consumer.attach_plans`. `--id` is the domain row, not a River job id.

`mrf.parse` has four attempts including the first. After a terminal parse,
the worker deletes unpublished parsed output, parser staging, and the raw
download, then frees the resident slot. The source stays selected and failed.
`retry --stage mrf.parse --id <source-id>` rematerializes the download when
bytes are gone.

## Admit more MRF sources

Raise the cumulative target when PostgreSQL already has unused known sources:

```text
docker compose -f docker-compose.story21.yml --profile operator run --rm cli month sources set-total --payer uhc --collection-month <YYYY-MM> --total <N|all>
docker compose -f docker-compose.story21.yml --profile operator run --rm cli month status --payer uhc --collection-month <YYYY-MM>
```

`--total` must be greater than the current numeric target. `all` cannot be
changed back to a number. Leave workers running; control selects the next
prefix and fills free slots.

If `mrf_sources_known` is still below the new target, admit more TOC files
(`--limit` is additional TOCs, not a reset):

```text
docker compose -f docker-compose.story21.yml --profile operator run --rm cli discover --payer uhc --collection-month <YYYY-MM> --limit 11 --mrf-source-limit <current-target>
```

Repeat `discover` with a larger `--limit` until `mrf_sources_selected` matches
the target. Status stays `building` with `mrf_source_target_partial` until
target is `all` and every selected source drains.

## Reconcile

Stop only control. Leave `mrf` and `consumer` running:

```text
docker compose -f docker-compose.story21.yml stop control
docker compose -f docker-compose.story21.yml --profile operator run --rm cli reconcile
docker compose -f docker-compose.story21.yml up -d control
```

Use this after deploying terminal-parse slot release so already-failed parses
that still hold slots are repaired. Do not delete slot rows by SQL.

## Reload a rebuilt image

Rebuild `mrfpipeline:local`, then recreate workers. Do not recreate `mrf`
while a large parse is still running unless you accept interrupting it.

```text
DOCKER_BUILDKIT=1 docker compose -f docker-compose.story21.yml build
docker compose -f docker-compose.story21.yml --profile workers up -d --no-build --force-recreate --no-deps --scale mrf=3 mrf consumer
docker compose -f docker-compose.story21.yml stop control
docker compose -f docker-compose.story21.yml --profile operator run --rm cli migrate
docker compose -f docker-compose.story21.yml --profile operator run --rm cli reconcile
docker compose -f docker-compose.story21.yml up -d --no-build --force-recreate --no-deps control
```

Confirm every pipeline container is on the new image:

```text
docker compose -f docker-compose.story21.yml ps
docker inspect --format '{{.Name}} {{.Image}}' \
  mrfpipeline-control-1 mrfpipeline-mrf-1 mrfpipeline-mrf-2 mrfpipeline-mrf-3 mrfpipeline-consumer-1
```

## Status, logs, stalled work

```text
docker compose -f docker-compose.story21.yml --profile operator run --rm cli month status --payer uhc --collection-month <YYYY-MM>
docker compose -f docker-compose.story21.yml logs -f --tail=50 control mrf consumer
docker compose -f docker-compose.story21.yml ps
docker compose -f docker-compose.story21.yml exec -T postgres psql -U mrfpipeline -d mrfpipeline -v ON_ERROR_STOP=1 < scripts/stalled-work.sql
```

`month status` is the readiness check. Worker logs never print URLs.

## River UI (optional)

Pause and resume queues. Cancelling `mrf.download` or `mrf.parse` fails the
stage and frees the slot. Do not retry or delete jobs, and do not cancel
other kinds.

```text
docker run --rm -p 8080:8080 --network <project>_default \
  -e DATABASE_URL=<same URL as the worker> \
  -e RIVER_SCHEMA=mrfpipeline_river \
  ghcr.io/riverqueue/riverui:latest
```

`<project>` is usually `mrfpipeline`. Copy `DATABASE_URL` from
`MRFPIPELINE_DATABASE_URL` in `docker-compose.story21.yml`. Open
http://localhost:8080. Keep using `month status` for domain readiness.

## Stop and tear down

```text
docker compose -f docker-compose.story21.yml --profile workers stop consumer mrf control
```

`docker compose -f docker-compose.story21.yml down` removes containers and the
network but keeps named volumes (PostgreSQL, artifacts, warehouse).
