# Story 28: Release filter catalog database contract

## Status

Proposed.

## User story

As a query-service operator, I want every published output generation to have a
versioned PostgreSQL filter catalog so a future public web process can load one
complete active output relation and its matching filters without discovering
values from Parquet during an HTTP request.

## Context

Story 27 makes publication incremental. One payer/month has durable
`monthly_release_outputs`, and each checkpoint that adds outputs increments
`monthly_releases.publication_generation`. Repeating activation may therefore
produce several immutable generations for the same monthly release. The web
catalog must match one exact generation; a catalog keyed only by payer/month
would silently mix old filters with newly published outputs.

The Parquet warehouse remains the analytical source of truth. PostgreSQL stores
only compact filter catalogs and release/output relationships. It does not
store negotiated-rate facts or precomputed statistics for arbitrary filter
combinations.

This story creates the database contract only. Stories 29–30 extract and
populate it, Story 31 makes publication require it, and Story 32 owns final
operational acceptance.

## Dependencies

Implemented Stories 14–27, especially:

- Story 17 release activation and exact active-output handoff;
- Story 20 sponsor-independent plan identity;
- Story 21 plan-independent ingestion and additive attachment;
- Story 27 durable incremental publication generations; and
- exact `mrfconsumer 2.0.0` warehouse/query semantics.

## Goal

Migration `0009_release_filter_catalog.sql` creates a separate `mrfweb` schema
in the existing pipeline PostgreSQL database. The pipeline owns writes. A
future web role receives read-only access outside this story's credential
management.

The schema records:

1. one building, ready, failed, or published catalog header for an exact
   payer/month/publication generation;
2. the exact output set represented by that catalog;
3. release billing codes and source-provided labels;
4. code-scoped modifier, place-of-service, billing-class, setting, and
   negotiation-arrangement values;
5. canonical sponsor-independent plans and their output relationships;
6. output- and billing-code-scoped networks; and
7. release-reachable taxonomy, state, and state/city values.

No ZIP, NPI, expiration-date, rate aggregate, histogram, or arbitrary filter
combination is materialized.

## Product decisions

- Use the same PostgreSQL database and a separate `mrfweb` schema. Do not add a
  second database, connection string, transaction coordinator, or service.
- Logical release identity remains `(payer_id, collection_month)`. Story 27's
  existing positive `publication_generation` distinguishes additive output
  generations for that month. Do not add a second product-level release or
  revision model.
- A private catalog `id` is a staging/publication key, not another release
  identity.
- Catalog rows are immutable after their header becomes `published`.
- A `ready` prospective catalog may be replaced before publication when its
  candidate output set becomes stale. A `published` catalog is never replaced.
- The active catalog is the `published` catalog whose payer/month/generation
  exactly matches the active `monthly_releases` row.
- PostgreSQL contains compact query controls, not warehouse facts or every
  possible aggregate.
- Postal code and exact NPI remain caller-supplied text values. Expiration is
  not a public filter.
- Billing-code identity is the exact pair `(billing_code_type, billing_code)`.
- Canonical plan identity is the complete five-field tuple
  `(plan_name, issuer_name, plan_id_type, plan_id, plan_market_type)`. Sponsor
  is excluded.
- Networks retain their output and billing-code relationship. Do not create a
  direct plan/network cross-product table.
- Option counts are standard negotiated-price observation counts, not provider
  counts, unless a column is explicitly named `provider_count`.

## Publication-generation identity

For a monthly release currently at generation `N`:

- its existing published output set is every `monthly_release_outputs` row for
  the payer/month;
- a prospective checkpoint with at least one new publishable output targets
  generation `N + 1`;
- the catalog for `N + 1` represents the union of all previously published
  outputs and all currently publishable new outputs;
- an idempotent checkpoint with no new output stays at `N` and reuses the
  published catalog for `N`; and
- inactive rollback uses the published catalog for the inactive release's
  current stored generation.

A catalog output fingerprint is lowercase hexadecimal SHA-256 over the exact
candidate output relation sorted by ASCII `output_id`. Each row contributes:

```text
payer_id + ASCII unit separator (0x1f)
+ YYYY-MM collection month + 0x1f
+ output_id + newline (0x0a)
```

Pipeline lexical contracts exclude the delimiters from identifiers, so this
encoding is unambiguous. The empty set is invalid for catalog construction.
The fingerprint is evidence and a fast comparison; Story 31 also compares the
exact output rows and never trusts the digest alone.

## PostgreSQL schema

### `mrfweb.release_catalogs`

Exact columns:

```text
id                              bigint generated always as identity primary key
payer_id                        text not null
collection_month                date not null
publication_generation          bigint not null
status                          text not null default 'building'
output_fingerprint              text not null
output_count                    bigint not null
provider_catalog_schema_version bigint
provider_catalog_release_month  date
billing_code_count              bigint
code_filter_value_count         bigint
plan_count                      bigint
plan_output_count               bigint
output_code_network_count       bigint
provider_filter_value_count     bigint
failure_code                    text
created_at                      timestamptz not null default transaction_timestamp()
completed_at                    timestamptz
published_at                    timestamptz
```

Constraints:

- unique `(payer_id, collection_month, publication_generation)`;
- foreign key `(payer_id, collection_month)` to
  `mrfpipeline.monthly_releases` with `ON DELETE RESTRICT`;
- payer uses the existing lower-ASCII pipeline identifier contract;
- collection month is the first day of its month;
- publication generation is positive;
- status is exactly `building`, `ready`, `failed`, or `published`;
- output fingerprint is exactly 64 lowercase hexadecimal characters;
- output count is positive;
- provider catalog schema version is positive when non-null;
- provider catalog release month is a first-of-month date when non-null;
- every optional row-count column is nonnegative when non-null;
- `building` has null `failure_code`, `completed_at`, and `published_at`;
- `ready` has null `failure_code`, non-null `completed_at`, null
  `published_at`, complete provider-catalog identity, and all count columns
  non-null;
- `failed` has a nonempty `failure_code`, non-null `completed_at`, null
  `published_at`, and may retain null aggregate counts;
- `published` has null `failure_code`, non-null `completed_at`, non-null
  `published_at`, complete provider-catalog identity, and all count columns
  non-null; and
- `published_at >= completed_at >= created_at` where those values exist.

`failure_code` is a fixed sanitized application code. It never stores raw SQL,
DuckDB output, paths, identifiers from source rows, or exception text.

### `mrfweb.release_outputs`

Exact columns:

```text
catalog_id      bigint not null
mrf_snapshot_id bigint not null
output_id       text not null
```

Constraints:

- primary key `(catalog_id, output_id)`;
- unique `(catalog_id, mrf_snapshot_id)`;
- foreign key `catalog_id` to `release_catalogs(id)` with `ON DELETE CASCADE`;
- foreign key `mrf_snapshot_id` to `mrfpipeline.mrf_snapshots(id)` with
  `ON DELETE RESTRICT`; and
- output ID uses the existing 1–128 lower-ASCII identifier contract.

The population path additionally verifies `output_id = 'mrf-' ||
mrf_snapshot_id` and that snapshot payer/month equals the catalog header.
Cross-table checks that PostgreSQL cannot express as an ordinary row check are
application invariants proven by Story 30 tests and rechecked by Story 31.

### `mrfweb.release_billing_codes`

Exact columns:

```text
catalog_id                  bigint not null
billing_code_type           text not null
billing_code                text not null
billing_code_type_version   text
warehouse_service_name      text
warehouse_service_description text
observation_count           bigint not null
unmodified_observation_count bigint not null
```

Constraints:

- primary key `(catalog_id, billing_code_type, billing_code)`;
- foreign key `catalog_id` with `ON DELETE CASCADE`;
- code type and code are nonempty and contain no CR/LF;
- optional version/name/description values are null or nonempty and contain no
  CR/LF;
- observation count is positive; and
- unmodified count is nonnegative and no greater than observation count.

Names are explicitly source-provided warehouse labels, not authoritative CPT
or HCPCS definitions. Story 29 defines deterministic conflict resolution. No
external code dictionary or licensing workflow is introduced.

### `mrfweb.release_code_filter_values`

Exact columns:

```text
catalog_id       bigint not null
billing_code_type text not null
billing_code     text not null
filter_kind      text not null
filter_value     text not null
display_label    text not null
observation_count bigint not null
```

Constraints:

- primary key
  `(catalog_id, billing_code_type, billing_code, filter_kind, filter_value)`;
- foreign key `(catalog_id, billing_code_type, billing_code)` to
  `release_billing_codes` with `ON DELETE CASCADE`;
- filter kind is exactly `modifier`, `place_of_service`, `billing_class`,
  `setting`, or `negotiation_arrangement`;
- filter value and display label are nonempty and contain no CR/LF; and
- observation count is positive.

`CSTM-00` uses display label `Broad or unspecified place of service`. Other
values initially use their exact stored value as the display label. A future
reference dictionary may change display metadata without changing filter
identity.

The UI constants `No modifier`, `All observations`, and `Has any modifier` are
not catalog rows. `release_billing_codes.unmodified_observation_count` carries
the no-modifier count.

### `mrfweb.release_plans`

Exact columns:

```text
id               bigint generated always as identity
catalog_id       bigint not null
plan_name         text not null
issuer_name       text not null
plan_id_type      text not null
plan_id           text not null
plan_market_type  text not null
search_text       text not null
```

Constraints:

- primary key `(catalog_id, id)`;
- foreign key `catalog_id` with `ON DELETE CASCADE`;
- unique canonical five-field tuple within one catalog;
- every text value is nonempty and contains no CR/LF;
- plan ID type is exactly `ein` or `hios`;
- market type is exactly `group` or `individual`; and
- `search_text` is the single-space concatenation of plan name, issuer name,
  plan ID type, plan ID, and market type after trimming only outer ASCII
  whitespace from each source field, then applying Go 1.26
  `strings.ToLower`. Source identity fields themselves remain exact and are
  not lowercased or otherwise normalized. Future callers normalize the search
  prefix with the same operation.

Add indexes suitable for catalog-scoped deterministic pagination and prefix
search:

- `(catalog_id, plan_name COLLATE "C", issuer_name COLLATE "C",
  plan_id_type COLLATE "C", plan_id COLLATE "C",
  plan_market_type COLLATE "C")`; and
- `(catalog_id, search_text COLLATE "C" text_pattern_ops, id)`.

Required identity pagination orders every text field with `COLLATE "C"` in the
same tuple order, then uses `id` only as a final stable tie-break.

Do not install `pg_trgm`, full-text search, fuzzy matching, or a generic search
framework in this story.

### `mrfweb.release_plan_outputs`

Exact columns:

```text
catalog_id bigint not null
plan_id    bigint not null
output_id  text not null
```

Constraints:

- primary key `(catalog_id, plan_id, output_id)`;
- foreign key `(catalog_id, plan_id)` to `release_plans` with
  `ON DELETE CASCADE`; and
- foreign key `(catalog_id, output_id)` to `release_outputs` with
  `ON DELETE CASCADE`.

One canonical plan may reach several outputs. Several plans may reach one
output. These are intended relationships, not duplicates to collapse across
identity fields.

### `mrfweb.release_output_code_networks`

Exact columns:

```text
catalog_id        bigint not null
output_id         text not null
billing_code_type text not null
billing_code      text not null
network_name      text not null
observation_count bigint not null
```

Constraints:

- primary key
  `(catalog_id, output_id, billing_code_type, billing_code, network_name)`;
- foreign key `(catalog_id, output_id)` to `release_outputs` with
  `ON DELETE CASCADE`;
- foreign key `(catalog_id, billing_code_type, billing_code)` to
  `release_billing_codes` with `ON DELETE CASCADE`;
- network name is nonempty and contains no CR/LF; and
- observation count is positive.

Add an index on
`(catalog_id, billing_code_type, billing_code, output_id, network_name)` for
network discovery after selected plan outputs are deduplicated.

### `mrfweb.release_provider_filter_values`

Exact columns:

```text
catalog_id    bigint not null
filter_kind   text not null
parent_value  text not null default ''
filter_value  text not null
provider_count bigint not null
```

Constraints:

- primary key `(catalog_id, filter_kind, parent_value, filter_value)`;
- foreign key `catalog_id` with `ON DELETE CASCADE`;
- filter kind is exactly `taxonomy`, `state`, or `city`;
- filter and parent values contain no CR/LF;
- taxonomy/state rows require empty `parent_value`;
- city rows require a nonempty state in `parent_value`;
- filter value is nonempty; and
- provider count is positive.

City filter values retain the warehouse's lowercase exact value. Display title
casing belongs to the future web layer and does not change identity.

## Stable read-only views

Create these ordinary PostgreSQL views in `mrfweb`:

### `mrfweb.active_release_catalogs`

Expose one row per active payer only when a matching `published` catalog exists
for the exact `(payer_id, collection_month, publication_generation)`:

```text
catalog_id
payer_id
collection_month
publication_generation
output_fingerprint
output_count
provider_catalog_schema_version
provider_catalog_release_month
published_at
```

The view is gated by symmetric exact set equality between authoritative
`monthly_release_outputs`/derived output IDs and catalog `release_outputs`
using both `(mrf_snapshot_id, output_id)`. A missing or extra row on either
side suppresses the catalog completely. Header output count/fingerprint alone
cannot satisfy the view.

Do not select a latest catalog, repair a missing generation, or fall back to a
ready/building/failed catalog.

### `mrfweb.active_release_outputs`

Join `active_release_catalogs` to authoritative
`mrfpipeline.monthly_release_outputs` and `mrfpipeline.mrf_snapshots`, exposing:

```text
catalog_id
payer_id
collection_month
publication_generation
output_id
```

Output ID is the existing exact `mrf-<snapshot-id>` value. The view does not
infer membership from all snapshots or from `mrfweb.release_outputs` alone.
Story 31 guarantees that authoritative membership and catalog outputs are the
same set before publication.

The migration grants no password and creates no login role. Deployment may
grant a future web role `USAGE` on schema `mrfweb` and `SELECT` on its serving
views/tables. The runtime web role must not receive `INSERT`, `UPDATE`,
`DELETE`, `TRUNCATE`, DDL, or privileges on pipeline/River operational tables
beyond an explicitly required stable view.

## Migration and existing data

Migration `0009` is additive and does not rewrite releases, snapshots, jobs,
warehouse files, or Story 27 output membership.

Existing active/inactive releases are not assigned fabricated catalog rows.
After migration:

- they retain their current state and publication generation;
- active catalog views return no row for them until `filters build` creates and
  Story 31 publishes a matching catalog;
- Story 30 must support backfilling the current exact membership of an existing
  active or inactive release; and
- Story 31 defines cutover behavior before catalog-aware publication becomes
  mandatory.

Do not infer labels/counts in SQL migration, scan the warehouse from migration,
or mark an empty catalog ready.

## Transaction and mutation rules

- Child rows may be inserted only while a catalog is `building`.
- The application, not a trigger framework, enforces the insertion-state check
  under a locked catalog row.
- Final validation and `building → ready` occur in one transaction.
- Failure cleanup deletes partial child rows and changes the header to `failed`
  in one transaction.
- Retrying the same unpublished generation may delete its `failed`, stale
  `building`, or stale `ready` header with cascading child rows, then create a
  new building header.
- A `published` header and its children are immutable and never cascade-deleted
  by normal commands or reconciliation.
- Story 31 alone performs `ready → published` in the same transaction that
  publishes the matching output generation.
- No database trigger derives filters, changes release status, or starts work.

## Required tests

- Migration creates exactly the `mrfweb` schema objects, constraints, indexes,
  and views listed here.
- A second migration run is handled by the existing migration system and does
  not duplicate objects.
- Every invalid status, generation, count, fingerprint, empty value, parent
  combination, and cross-catalog child relationship is rejected.
- Canonical duplicate plans in one catalog are rejected; identical plans in
  separate catalog generations are accepted.
- One plan may map to several catalog outputs and one output may map to several
  plans.
- One output/code may map to several networks.
- City requires a state parent; state/taxonomy prohibit one.
- `CSTM-00` is stored as identity `CSTM-00` with its required display label.
- Active views return nothing without an exact published catalog, return the
  exact matching generation when present, and ignore later ready candidates.
- Active output view membership equals `monthly_release_outputs`, including
  several outputs first published in different Story 27 generations.
- Migration leaves existing release/output rows unchanged.

## Acceptance criteria

- One exact schema supports catalog staging, readiness, publication,
  replacement before publication, immutable published history, and rollback.
- Publication generation is the only catalog version dimension within one
  payer/month.
- Every plan/network relationship retains output lineage.
- Every enumerable PostgreSQL filter is release- or code-scoped as required.
- No ZIP, NPI, expiration, arbitrary aggregate, or direct plan/network
  cross-product table exists.
- Stable active views fail closed when the active generation lacks a published
  catalog.
- The migration does not read or modify the warehouse.

## Non-goals

- Running DuckDB or populating any catalog row.
- Changing consumer warehouse schema or `mrfconsumer` behavior.
- Changing Story 27 publication behavior before Story 31.
- Adding the web server, HTTP API, templates, JavaScript, or UI.
- Creating or managing a web login/password.
- Importing an external CPT/HCPCS dictionary.
- Fuzzy plan search, ZIP autocomplete, NPI directory, expiration filtering, or
  historical public browsing.
- Materialized rate statistics or filter-combination cubes.

## Documentation

Update `README.md` and `requirements/DESIGN.md` to describe this as a planned
Stories 28–32 contract until implementation is complete. Do not list proposed
commands as currently available.

## Implementation notes

Expected files when implemented:

- `internal/database/migrations/0009_release_filter_catalog.sql`;
- migration integration assertions in `internal/database`;
- stable schema/view column assertions; and
- no new production dependency outside existing PostgreSQL for this story.

Use concrete SQL and typed Go test fixtures. Do not introduce a schema builder,
ORM, repository framework, migration abstraction, or generic catalog model.
