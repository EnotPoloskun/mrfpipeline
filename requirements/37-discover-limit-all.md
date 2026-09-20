# Story 37: Discover `--limit all`

## Status

Implemented.

## User story

As a pipeline operator, I want `mrfpipeline discover --limit all` so one
discovery run can admit every new TOC URL in the current listing without
picking a number larger than the listing.

## Context

`--limit` is required and is a per-run admission cap on **new** TOC URLs.
Known TOCs do not consume it. Distinct new URLs beyond the cap increment
`overflow_count` and can be admitted on a later run. Unlimited TOC admission
was deferred in Stories 01 and 05 so version 1 could start with a controlled
real-data test instead of the full payer listing.

`--mrf-source-limit all` already exists (Story 21). It is a **durable monthly
release** target for MRF sources, not a TOC cap. It does not admit TOC files.
`--limit all` does not select MRF sources. Both flags remain independent.

`mrfdiscoverer` already returns the complete current listing. The pipeline is
what cuts it. `discovery_runs.toc_limit` is already nullable; Story 02 reserved
null for unlimited runs. Version 1 never writes null today.

This story is **not** the deferred chunked-admission design. Repeated numeric
`--limit` already stages admission. `--limit all` is the opt-in opposite:
admit every new first-occurrence URL in **this listing**. A later listing that
grows still needs another `discover`.

## Goal

Accept the exact token `all` as a `--limit` value, in addition to a positive
integer:

```text
mrfpipeline discover --payer <uhc|optum|cigna> --collection-month <YYYY-MM> --limit <N|all> [--mrf-source-limit <N|all>]
```

`--limit` stays required. Omitting it stays a usage error. `--limit all` is
never the default.

Numeric `--limit` behavior is unchanged. `--mrf-source-limit` is unchanged.

## Dependencies and compatibility

Depends on implemented Stories 01, 05, 21, and 36.

No sibling pin bump. No warehouse, catalog, Compose, download-policy, parse,
or ingest change. No new migration: store unlimited runs as `toc_limit NULL`,
which the existing checks already allow (`toc_limit IS NULL OR toc_limit > 0`,
and `admitted_count <= toc_limit` only when `toc_limit` is present).

## Supersession

This story supersedes living current-contract language that says unlimited
TOC `--limit` is out of scope, including:

- Story 01 “Omitted or unlimited discovery is deferred until a later
  chunked-admission story” as operator-facing current contract (omission stays
  invalid; `--limit all` becomes valid).
- Story 05 “`--limit` is required and is a positive `int64`. Omitted or
  unlimited discovery is deferred” and “The single admission transaction is
  appropriate only because the run is bounded” as a current prohibition on
  `--limit all`.
- `internal/cli/help.go` “Unlimited discovery is out of version 1 scope” and
  “`--limit` Positive int64 of newly admitted TOCs”.
- README Limitations “Required `--limit`” as implying numeric-only.
- DESIGN.md “`--limit` is required and must be a positive `int64`”,
  “`discovery_runs.toc_limit` … version 1 never writes null”, and non-goal
  “Omitted or unlimited discovery admission”.

It preserves:

- required `--limit` (must be present);
- numeric `--limit` as a positive int that still must fit a PostgreSQL
  `integer`;
- known TOCs never consume the limit;
- overflow counting for numeric runs;
- one discovery run = one payer + one collection month + one `--limit`;
- `--mrf-source-limit` as a separate durable MRF target;
- public-HTTPS download policy;
- Story 05 atomicity: no TOC rows or counts commit until the complete listing
  succeeds.

Historical Stories 01, 02, and 05 stay as originally written.

## Product decisions

- `--limit all` admits every distinct **new** first-occurrence TOC URL in this
  run’s listing. `overflow_count` is `0`. Existing TOCs are observed and do
  not consume a cap.
- It is per-run, not a durable month-wide TOC target. Unlike
  `--mrf-source-limit all`, later listing growth or later `discover` does not
  inherit an “admit every future TOC” policy. Another `--limit all` (or a
  numeric limit) is required to admit newly appeared URLs.
- Exact token `all`, matching `--mrf-source-limit`. Reject `All`, `ALL`,
  `all `, empty, `0`, and every current invalid numeric form. No aliases.
- Store `discovery_runs.toc_limit` as SQL `NULL` for `--limit all`. Do not use
  `0`, `-1`, or `2147483647` as a sentinel.
- Enqueue JSON keeps field `toc_limit` in the same position. Numeric runs
  still report a positive integer. Unlimited runs report JSON `null`:

```json
{"discovery_run_id":41,"river_job_id":9001,"payer_id":"optum","collection_month":"2026-08","toc_limit":null}
```

  Do not omit the field. Do not send the string `"all"`.
- Keep **one** admission transaction. Story 05’s “bounded TX” note was the
  reason `all` was deferred, not a reason to invent paging here. If the
  listing is too large to insert TOCs, memberships, and `toc.download` jobs
  in one transaction, the attempt fails and retries as today. Chunked
  in-worker admission is a later story.
- `--limit all` does not change listing fetch: `mrfdiscoverer` already returns
  the full list into memory.
- `--limit all` does not bypass `--mrf-source-limit`. A numeric MRF target
  still blocks unselected sources. Processing every MRF still requires
  `--mrf-source-limit all` (or `month sources set-total --total all`).
- TOC downloads still have no resident-capacity bound. Control drains
  `toc_download` at the existing queue concurrency (currently 4). Peak TOC
  disk is in-flight downloads/parses, not the full listing at once. MRF disk
  remains bounded by resident capacity and the MRF target.
- `TestRealUHCAcceptance` stays a small numeric `--limit`. This story does not
  add a required live full-listing acceptance run.

## Concerns (accepted, not blockers)

These are why version 1 required a numeric cap. They remain true; `--limit all`
is an explicit operator opt-in.

1. **Fan-out.** One TOC can reference many MRFs. Admitting every TOC can
   create thousands of `mrf_sources` rows even when most stay `blocked` by a
   numeric `--mrf-source-limit`.
2. **One large admission transaction.** Every new TOC row, membership, and
   download job commits together. A huge listing can make that transaction
   long. Failure rolls back the whole attempt.
3. **Not “parse everything”.** `--limit all` without `--mrf-source-limit all`
   still materializes only the selected MRF sources.
4. **Not durable.** A later listing with new TOC URLs is not auto-admitted.

Do not add TOC resident capacity, auto-`set-total`, or listing caching in this
story.

## Command surface

`--limit` remains required. Accepted values:

```text
--limit 1
--limit 10000
--limit all
```

Rejected (exit 2, `FieldLimit` in the error unless the flag is omitted, which
stays usage):

- omitted `--limit`
- empty `--limit` / `--limit=`
- `All`, `ALL`, `all `
- `0`, `+5`, hex, overflow past int64, overflow past PostgreSQL `integer` for
  numeric values

`--mrf-source-limit all` remains valid and independent.

## Implementation

- `internal/config`: accept `all` for `--limit` without breaking numeric
  callers. Prefer a dedicated validator used by `runDiscover` (same shape as
  `ValidateSourceTarget`, field `limit`) rather than overloading
  `ValidateLimit` if that keeps numeric tests simpler. Error text for a
  well-formed but invalid token must say `positive integer or all`, not
  “positive integer” only.
- `internal/cli/cli.go` `runDiscover`: stop requiring a positive int before
  enqueue; pass unlimited as null into discovery.
- `internal/discovery/enqueue.go`: insert `toc_limit NULL` for `all`; allow
  `limit < 1` only for that kind. Numeric still must be `1..MaxInt32`.
- `internal/discovery/report.go`: `TOCLimit` must encode JSON `null` when
  unlimited. `FormatReport` must accept a null limit.
- `internal/discovery/admit.go`: when `toc_limit` is null, never take the
  overflow branch; every new first-occurrence URL is admitted.
- `internal/cli/help.go` and README Limitations / discover paragraph.
- DESIGN.md living operator text listed under Supersession.
- Do not rewrite historical Stories 01, 02, or 05.

## Required tests

- `ValidateLimit` / the new discover-limit validator: `all` succeeds; `All`,
  `ALL`, `all `, `""`, `0` fail with `FieldLimit`. Numeric cases stay as
  today, including PostgreSQL integer overflow.
- Discover parse: `--limit all` with a valid payer/month reaches database
  (same `ErrDatabase` pattern as numeric success-before-connect). `--limit
  All` is exit 2. Omitted `--limit` stays usage.
- Enqueue/report: unlimited run stores `toc_limit` null and prints
  `"toc_limit":null`. Numeric report is unchanged.
- Admission integration (or existing discovery tests extended): a listing of
  several new URLs with `--limit all` inserts all of them, `overflow_count=0`,
  one download job each. The same listing with `--limit 1` still overflows.
  Known URLs still do not consume the unlimited cap.
- Help/root help: `--limit` documents `N|all` and no longer says unlimited
  discovery is out of scope.
- Schema tests already covering null `toc_limit` and `admitted_count <=
  toc_limit` only when present stay green. No new migration.

Do not require a project-wide suite beyond `internal/config`, `internal/cli`,
and `internal/discovery`. Skip live payer listings.

## Documentation

Update current operator text only:

- Discover help: `--limit` is a positive integer or `all`. Unlimited TOC
  admission is opt-in for this run; it does not select MRF sources.
- README Limitations: required `--limit` may be `N` or `all`.
- DESIGN.md: document status stays Stories 01–36 implemented plus this story
  specified; `--limit` may be `all`; `toc_limit` null means this run had no
  TOC cap; non-goal becomes “omitted `--limit`” only; cross-story acceptance
  item 2 still requires `--limit`, and records overflow only for numeric
  limits.

Do not claim a live full-listing Optum/UHC run was executed. Do not claim
`--limit all` processes every MRF.

## Acceptance criteria

1. `discover --limit all` passes semantic validation with the same required
   `--payer` and `--collection-month` rules as numeric `--limit`.
2. Omitted `--limit`, `--limit All`, and `--limit 0` still fail.
3. An unlimited run stores `toc_limit` null, reports `"toc_limit":null`, and
   admits every new first-occurrence URL in that listing with
   `overflow_count=0`.
4. Numeric `--limit` still admits at most N new TOCs and still records
   overflow.
5. `--mrf-source-limit` behavior is unchanged. `--limit all` does not select
   MRF sources.
6. Help and README no longer say unlimited discovery is out of version 1
   scope.
7. No migration, warehouse layout, Compose, or sibling-module change.

## Non-goals

- Omitting `--limit`.
- Case-insensitive `all`, aliases, or `--limit unlimited`.
- A durable month-wide TOC admission target (auto-admit future listing URLs).
- Chunked in-worker admission / paging the listing into several transactions.
- TOC resident capacity or changing `toc_download` MaxWorkers.
- Changing `--mrf-source-limit`, download policy, parse, ingest, catalog, or
  Compose topology.
- A live full-listing acceptance test.
- Rewriting historical Stories 01–05 as if they originally shipped `--limit
  all`.
