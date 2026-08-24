# Story 14: Feed-free domain schema

## Status

Implemented.

## User story

As a pipeline operator, I want MRF snapshots identified by source, payer, and
collection month without a synthetic feed record so that durable pipeline state
does not claim a cross-month identity the payer data cannot support.

## Product decision

Remove `mrf_feeds` and every `feed_id`/`mrf_feed_id` dependency from the active
pipeline model.

The remaining identities are sufficient:

```text
mrf_source   = one exact MRF URL + collection month capture and lifecycle
mrf_snapshot = one parsed source + payer + collection month
output_id    = mrf-<mrf_snapshots.id>
```

The snapshot unique key becomes:

```text
(mrf_source_id, payer_id, collection_month)
```

Several distinct MRF sources may exist for one payer/month. The pipeline does
not infer that they are replacements, duplicates, or logical networks. One
exact URL reused by several TOCs in the same collection month produces one
source capture. Reusing that URL in another month produces a new source,
download, and parse. No content hash, ETag, byte comparison, or URL
normalization is involved.

## Dependencies and supersession

This story depends on the implemented Stories 01–13 and the approved
`mrfconsumer` Story 22 feed-free `2.0.0` contract.

Stories 14–17 are one atomic breaking delivery batch. Consumer `2.0.0` must be
available before the batch begins. The four stories may be implemented as
separate commits, but none is independently mergeable, releasable, or
deployable. The only supported repository state is after all four are complete.
Do not add an adapter, compatibility mode, feature flag, dual consumer version,
or temporary feed placeholder between them.

It supersedes Stories 02, 08, 11, 12, and 13 wherever they require:

- `mrf_feeds`;
- `mrf_snapshots.mrf_feed_id`;
- `feed_id=mrf-source-<source-id>`;
- payer lookup through a feed row;
- source/feed/month snapshot uniqueness; or
- feed values in claims, reports, logs, reconciliation, or status SQL.

All exact URL handling, numeric primary keys, output-ID formatting, plan
identity, stage lifecycle, and River separation remain. The operational-context
and redaction boundary is clarified below.

## Compatibility and migration boundary

Add the next contiguous application migration:

```text
internal/database/migrations/0002_feed_free_domain.sql
```

Migration `0001` remains immutable.

The pipeline and consumer change is rebuild-only for populated deployments.
Before making a destructive or ambiguous transformation, migration `0002`
must require that no domain data has been admitted beyond empty initialized
tables. In particular, any discovery, TOC, MRF source, snapshot, plan, or plan
batch row causes migration to fail transactionally with `ErrDatabase` and a
safe instruction to use a new pipeline database and warehouse.

This rule avoids pretending that `1.5.0` consumer publications and succeeded
plan batches can be converted to `2.0.0` by editing PostgreSQL. The migration
does not delete, merge, reset, or reinterpret existing rows.

On an empty initialized schema, migration `0002`:

1. removes foreign keys and constraints that reference `mrf_feeds`;
2. replaces `mrf_snapshots.mrf_feed_id` with required `payer_id`;
3. adds required `collection_month` to `mrf_sources` and replaces global URL
   uniqueness with monthly exact-URL uniqueness;
4. creates the new snapshot unique key and source/month relationship;
5. removes `mrf_feeds`;
6. removes obsolete indexes tied to the old relationships; and
7. installs the generic payer constraints below.

A fresh database applies `0001` then `0002`. Running `migrate` again remains a
no-op. There is no down migration, compatibility view, legacy column, or data
copy command.

## Payer identifier foundation

Database payer fields become structurally generic even though the only
implemented discovery adapter remains UHC.

Every payer column uses:

```text
^[a-z0-9][a-z0-9._-]{0,127}$
```

Remove exact `payer_id = 'uhc'` checks from domain tables. The `discover` CLI
and discovery worker continue accepting only `--payer uhc` until a later payer
adapter story. This separation permits release/control rows and test fixtures
for several payers without claiming that the current binary can discover them.

Do not infer payer from a TOC, MRF URL, reporting entity, plan, filename, or
directory.

## Revised `mrf_sources`

One `mrf_sources` row is one monthly capture of one exact MRF URL. Its download
and parse lifecycle remains plan- and payer-independent.

Add required `collection_month date`, constrained to the first day of its
month. Replace `mrf_sources_source_url_key` with a named unique constraint on:

```text
(source_url, collection_month)
```

The same exact URL referenced by several TOCs or payers in one month reuses one
source row, download, parsed output, and numeric source artifact identity. The
same URL referenced in another month creates a new source row and lifecycle.
Different exact URLs always remain different source rows.

Add a named uniqueness constraint on `(id, collection_month)` so snapshots can
enforce source/month agreement with a composite foreign key. Source URL and
collection month are immutable after insertion.

The accepted limitation is confined to one collection month: if the bytes at
one exact URL change again after that month's capture succeeds, the pipeline
does not detect or automatically recapture them. Do not add hashes, ETags,
last-modified identity, byte comparison, or URL normalization.

## Revised `mrf_snapshots`

The exact active columns are:

| Column | Type | Contract |
|---|---|---|
| `id` | `bigint` identity | Primary key and source for `mrf-<id>`. |
| `mrf_source_id` | `bigint` | Required reference to `mrf_sources`. |
| `payer_id` | `text` | Required caller/discovery payer namespace. |
| `collection_month` | `date` | Required first day of month. |
| `consume_status` | `text` | Existing stage vocabulary. |
| `consume_river_job_id` | `bigint` nullable | Existing positive-ID rule. |
| `failure_code` | `text` nullable | Existing safe failure contract. |
| `created_at` | `timestamptz` | Existing creation timestamp. |
| `updated_at` | `timestamptz` | Existing mutable-state timestamp. |

The exact unique constraint is named and covers:

```text
(mrf_source_id, payer_id, collection_month)
```

Use a composite `ON DELETE RESTRICT` foreign key:

```text
(mrf_source_id, collection_month)
    -> mrf_sources(id, collection_month)
```

This makes source/snapshot month agreement structural. Payer and month are
immutable after insertion. Application code names inserted columns and never
depends on physical order.

Do not add `feed_id`, logical network ID, product ID, source hash, URL hash,
warehouse path, or consumer output ID as stored columns.

## Revised relationships

`toc_mrf_plan_associations`, `mrf_plans`, attachment batches/items, and River
arguments continue referencing the numeric snapshot ID. Their schemas and
grains remain unchanged.

Queries needing payer/month obtain them directly from `mrf_snapshots`. Claims
lock snapshot then source; there is no third feed-row lock. Remove old lock
ordering and invariant checks involving a feed row.

Validated payer IDs, collection months, derived output IDs, numeric domain/job
IDs, aggregate counts, statuses, and blocker codes may appear as structured
operational context. URLs, paths, filenames, plans, provider/service values,
credentials, response content, and raw errors remain redacted.

## Go domain changes

Remove feed-specific:

- row/claim structs;
- insert/select/update helpers;
- `formatFeedID` and validation helpers;
- SQL joins and lock steps;
- test factories and fixture fields;
- error context and logs; and
- reconciliation ownership comparisons.

Snapshot claim state carries `PayerID` and `CollectionMonth` directly. River
arguments remain numeric IDs only and do not gain payer or month.

## Required tests

Add or revise tests proving:

- migration `0002` is contiguous, embedded, transactional, and idempotent;
- a populated v1 domain schema is rejected without mutation;
- a fresh schema contains no `mrf_feeds` table, feed column, feed constraint,
  or feed index;
- payer constraints accept representative valid identifiers and reject empty,
  uppercase, whitespace, and overlength values;
- the production discover command still rejects non-UHC payers;
- duplicate `(exact URL, month)` sources are rejected;
- the same exact URL in different months creates distinct sources;
- duplicate `(source, payer, month)` snapshots are rejected;
- the same source/month for different payers is accepted;
- a snapshot month differing from its source month is rejected;
- different sources for one payer/month are accepted;
- snapshot output ID remains `mrf-<positive-id>`;
- all snapshot-dependent joins continue using numeric snapshot ID;
- operational context and redaction follow the boundary above; and
- migration ledger validation and PostgreSQL 15 requirements remain green.

Tests assert schema keys, constraints, and observable identity behavior. They
do not scan source text for words such as `sha`, `hash`, or `digest`.

## Acceptance criteria

- The active database model contains no feed table, feed column, feed value, or
  feed-derived identifier.
- Snapshot identity is exact source + payer + collection month.
- MRF source capture identity is exact URL + collection month.
- Existing populated `1.5.0` state is never silently converted.
- A new database supports future multiple-payer release state while discovery
  remains explicitly UHC-only.
- Numeric source/snapshot identity and same-month exact URL deduplication remain
  deterministic.

## Out of scope

- Consumer invocation or warehouse schema changes beyond the database model.
- Monthly release state, activation, readiness, rollback, or query serving.
- A non-UHC discovery adapter.
- Migration of populated databases or consumer warehouses.
- Stable logical network/product identity.
- Detecting more than one content change at an exact URL inside one collection
  month.
- Content hashes, ETags, byte comparison, or URL normalization.
