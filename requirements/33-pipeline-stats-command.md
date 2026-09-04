# Story 33: Pipeline stats command

## Status

Not implemented.

## User story

As a pipeline operator, I want one read-only CLI table of TOC, MRF, ingest,
and plan-attachment counts by status, payer, and month so I can see in-flight
work without reading `month status` readiness JSON or running diagnostic SQL.

## Goal

Add a top-level `stats` command that prints domain-row counts for every known
pipeline file/snapshot/batch, optionally filtered by payer and/or collection
month.

`month status` remains the authoritative release-readiness check. `stats` is
the operational overview: how many domain rows are `blocked`, `pending`,
`running`, `succeeded`, or `failed` at each processing stage.

## Dependencies and supersession

This story depends on the feed-free domain schema (Stories 14–16), monthly
releases (Story 18), bounded source admission (Story 21), and additive plan
attachment (Story 12).

It does not change `month status`, `filters status`, readiness blockers,
admission, or any worker. It does not replace the redacted SQL constants in
`internal/reconcile/status.go`; those remain documentation and diagnostic
queries. `stats` may use new dedicated aggregate queries.

It preserves redaction, exclusive-lease rules for mutating commands, and the
hand-rolled CLI parser from Story 01.

## Product decisions

- The verb is `stats`, not a top-level `status`. `month status` and
  `filters status` already own that word for readiness and catalog reports.
- Default stdout is a text table. `--json` prints the same rows as a compact
  JSON array.
- Population is **all known** domain rows, not only sources admitted in
  `monthly_release_mrf_sources`. Unselected sources still appear. This can
  disagree with `month status` under a numeric MRF target, and that is
  intended.
- MRF download/parse are attributed through `mrf_snapshots`. A shared source
  is one stats row per payer that has a snapshot for that source/month.
  Download and parse statuses are identical across those payers because they
  live on the single `mrf_sources` row.
- `consumer.attach_plans` is a stage in the same table. Its domain status has
  no `blocked` value; the `blocked` column is `0`.
- `--payer` and `--collection-month` are independent optional filters.
- A missing payer/month is a successful empty result, not `release_not_found`.
- No schema or migration change.

## Command surface

Add one top-level command:

```text
mrfpipeline stats [--payer <payer>] [--collection-month <YYYY-MM>] [--json]
```

Accepted forms:

```text
mrfpipeline stats
mrfpipeline stats --payer <payer>
mrfpipeline stats --collection-month <YYYY-MM>
mrfpipeline stats --payer <payer> --collection-month <YYYY-MM>
mrfpipeline stats --json
mrfpipeline stats --payer <payer> --json
mrfpipeline stats --collection-month <YYYY-MM> --json
mrfpipeline stats --payer <payer> --collection-month <YYYY-MM> [--json]
mrfpipeline stats --help
```

`--json` is a valueless switch. `--json=true`, `--json true`, a value after
`--json`, or a repeated `--json` is a usage error.

Unknown flags, positional arguments, single-dash flags, `--`, empty flag
arguments, and duplicate `--payer` / `--collection-month` are usage errors.

Payer syntax is `ValidatePayerIdentifier` (any valid generic identifier), not
discovery's UHC-only `ValidatePayer`. Collection month is `YYYY-MM`, stored
and queried as the first day of that month, matching `month status`.

`stats` is read-only, requires only `MRFPIPELINE_DATABASE_URL`, validates the
current application schema, and does not acquire the worker lease. It may run
while `work` is active. The result is a point-in-time database snapshot and
may become stale immediately.

The command does not inspect the artifact root, warehouse, provider catalog,
services CSV, River queue depths, or filter catalogs. It does not start,
repair, retry, download, parse, consume, or attach work.

Root `--help` and `stats --help` name the command. Help is informational and
must not read the environment.

## Stages and units

Emit these stages in this order when the backing table has at least one
matching domain row for that payer and month:

| Stage | Backing row | Status column |
|---|---|---|
| `toc.download` | `toc_files` | `download_status` |
| `toc.parse` | `toc_files` | `parse_status` |
| `toc.import` | `toc_files` | `import_status` |
| `mrf.download` | `mrf_sources` joined to `mrf_snapshots` | `mrf_sources.download_status` |
| `mrf.parse` | `mrf_sources` joined to `mrf_snapshots` | `mrf_sources.parse_status` |
| `consumer.ingest` | `mrf_snapshots` | `consume_status` |
| `consumer.attach_plans` | `plan_attachment_batches` joined to `mrf_snapshots` | `plan_attachment_batches.status` |

Do not emit a stage for a payer/month that has zero backing rows. Do not
synthesize all-zero TOC/MRF/consumer rows just because a `monthly_releases`
row exists. Do not include `discovery.run`, resident slots, River
`retryable`, filter catalogs, or a totals/summary row.

Status buckets are the domain CHECK values:

```text
blocked | pending | running | succeeded | failed
```

There is no domain status named downloaded, parsed, imported, processed, or
in progress. `download_status = succeeded` means downloaded. `running` is
in-progress work. Downstream stages remain `blocked` until the previous stage
succeeds.

`consumer.attach_plans` CHECK values are `pending`, `running`, `succeeded`,
and `failed` only. Print `blocked` as `0`. Batch count is smaller than ingest
count because a batch exists only after ingest succeeds.

`total` is the number of backing rows for that stage and payer/month. It
equals `blocked + pending + running + succeeded + failed`.

Join `mrf_sources` to `mrf_snapshots` on `mrf_source_id` and matching
`collection_month`. Unique `(mrf_source_id, payer_id, collection_month)`
means one snapshot per payer per source, so counting those join rows counts
each known source once per payer. Do not restrict the join to
`monthly_release_mrf_sources`.

Orphan `mrf_sources` rows with no snapshot are absent from per-payer MRF
stats.

## Table output

Without `--json`, write an aligned text table to stdout with a header and
one data row per stage/payer/month:

```text
stage                  payer  month    blocked  pending  running  succeeded  failed  total
toc.download           uhc    2026-08        0        3        2        110      5    120
toc.parse              uhc    2026-08        5        2        1        107      5    120
toc.import             uhc    2026-08        8        1        0        106      5    120
mrf.download           uhc    2026-08       12       40        8        900     20    980
mrf.parse              uhc    2026-08      460       10        4        486     20    980
consumer.ingest        uhc    2026-08      494        6        2        470      8    980
consumer.attach_plans  uhc    2026-08        0        4        1        460      5    470
```

Exact columns, in order:

```text
stage  payer  month  blocked  pending  running  succeeded  failed  total
```

Header labels are those exact strings. `payer` is `payer_id`. `month` is
`YYYY-MM`. Numeric columns are right-aligned. Text columns are left-aligned.
Separate columns with spaces so they form a readable table; do not emit TSV
or CSV as the default format. End stdout with one trailing newline.

Sort rows by `payer_id` ascending, `collection_month` ascending, then the
stage order in the table above. Do not sort stages alphabetically.

When no rows match, still print the header line and a trailing newline.
That is success.

Do not colorize, do not print URLs, paths, SQL, numeric domain IDs, River
job IDs, failure codes, or blocker strings.

## JSON output

With `--json`, write one compact JSON array plus a trailing newline. Do not
pretty-print. Each element is one table row:

```json
[{"stage":"toc.download","payer_id":"uhc","collection_month":"2026-08","blocked":0,"pending":3,"running":2,"succeeded":110,"failed":5,"total":120}]
```

Field names:

```text
stage
payer_id
collection_month
blocked
pending
running
succeeded
failed
total
```

Counts are JSON numbers. An empty result is:

```json
[]
```

JSON field order need not match the table, but the array order matches the
table sort. JSON must not add readiness fields, retry counts, slot counts, or
blocker arrays. It is the same information as the table.

## Queries and redaction

Queries return payer identifiers, collection months, stage names, and
nonnegative counts only. They must not select `source_url`, artifact paths,
plan names, or job argument payloads.

Existing status indexes on the domain status columns are sufficient. Do not
add indexes in this story.

`stats` may execute a small number of `GROUP BY payer_id, collection_month`
aggregates with `count(*) FILTER (WHERE status = ...)`. It must not scan
warehouse files or join River except that this story does not join River at
all.

## CLI, help, and documentation

Amend:

- `internal/cli` command parse/execute/help/hint for `stats`
- root help command list
- `README.md` command list and the Status queries section, stating that
  `stats` is the operational count table and `month status` remains the
  targeted readiness check
- `DOCKER.md` operator command list with a Compose `cli stats` example
- `requirements/DESIGN.md` operator-command list and the note that
  `migrate` / `stats` / `discover` / `retry` / no-flag `month status` /
  targeted `month status` / `filters status` need only the database URL
  (`stats` and both `month status` forms are database-only)

Packaging tests that snapshot help text or Compose CLI examples must include
`stats`.

## Errors, streams, and exit status

Reuse the Story 01 / current CLI exit contract:

| Exit | When |
|---|---|
| 0 | Successful table or JSON, including an empty result |
| 1 | Stdout write failure or uncategorized error |
| 2 | Usage or invalid configuration (missing/invalid database URL, invalid payer identifier, invalid month) |
| 3 | Database error |

`stats` does not return `jobs.ErrJob` for a missing release. A payer or month
with no matching domain rows is exit 0.

Standard error receives the error line and, for usage/config, the hint:

```text
Try 'mrfpipeline stats --help'.
```

## Required tests

- Parse every accepted form above, including `--json` before or after payer
  and month flags, `--payer=uhc`, and `--collection-month=2026-08`.
- Reject only `--payer`, empty flag values, unknown flags, positional
  arguments, `--json true`, `--json=true`, duplicate `--json`, and
  `stats` plus `--help` plus other flags (help remains an exact informational
  invocation, matching other commands).
- `stats --help` and root help mention `stats` and do not read the
  environment.
- Table formatter: header, alignment, sort, trailing newline, empty header
  only.
- JSON formatter: compact array, trailing newline, empty `[]\n`, same row
  order as the table.
- Integration: seed TOC files, a shared MRF source with snapshots for two
  payers, an unselected source (snapshot exists, no
  `monthly_release_mrf_sources` row), ingest statuses, and a plan-attachment
  batch. Assert:
  - TOC stage totals equal TOC file counts for that payer/month
  - the shared source increments `mrf.download` / `mrf.parse` for both payers
  - the unselected source is included
  - `consumer.ingest` totals equal snapshot counts
  - `consumer.attach_plans` `blocked` is 0 and totals equal batch counts
  - `--payer` omits the other payer
  - `--collection-month` omits other months
  - unknown payer/month prints an empty successful result
- Query redaction: stats SQL contains no `source_url`.
- `month status` parse, help, and JSON shapes are unchanged.

Do not require a project-wide test suite beyond the packages this command
touches. Skip unrelated formatters.

## Acceptance criteria

- `mrfpipeline stats` is a documented, tested, database-only operator command.
- Default output is the stage/payer/month count table specified above.
- `--json` is the same rows as a compact JSON array.
- Filters `--payer` and `--collection-month` work independently.
- Counts use all known TOC files, snapshot-joined MRF sources, snapshots, and
  plan-attachment batches.
- Shared MRF sources appear once per payer.
- `consumer.attach_plans` is in the same table with `blocked` 0.
- `month status` is unchanged and remains the readiness command.
- No URLs, paths, or secrets appear in output, logs, or stats SQL.
- No migration, worker, or lease change.

## Non-goals

- Extending or replacing `month status` JSON.
- Discovery-run counts, River retry/queue depths, resident-slot occupancy,
  stalled-age, filter-catalog status, or warehouse file inspection.
- Pretty-printed JSON, `--format`, CSV, or a pager.
- Totals/summary rows or percentage columns.
- Per-file / per-URL listings.
- New indexes, tables, or status enum values.
- Counting admitted sources only, or counting shared MRF files once globally
  with a blank payer column.
