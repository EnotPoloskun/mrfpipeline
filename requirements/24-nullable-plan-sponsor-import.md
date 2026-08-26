# Story 24: Nullable plan sponsor on TOC import

## Status

Specified.

## User story

As a pipeline operator, I want `toc.import` to accept a TOC association whose
`plan_id_type` is `ein` and whose `plan_sponsor_name` is null, so that real
payer TOC files already parsed by `mrftocparser` still create MRF sources,
snapshots, and canonical plans instead of failing the whole import as
`toc_import_row_invalid`.

## Context

`mrftocparser` already treats a missing or unusable EIN `plan_sponsor_name` as
a warning and still writes a valid association row with a null sponsor. That
is the parser output contract (`missing_required` or `invalid_value`, then a
finished row).

Pipeline Story 02 and Story 08 currently reject that row. Go validation
returns `toc_import_row_invalid` before any domain write. PostgreSQL CHECKs on
`toc_mrf_plan_associations` and `mrf_plans` also require EIN to have a
nonempty sponsor. Story 12's `plans.json` encoder refuses to emit EIN with a
null sponsor.

Live UHC TOC files hit that gate: EIN plans with `plan_sponsor_name` null,
otherwise valid HTTPS locations and plan identity fields. River retries the
same bytes until the import attempt budget is exhausted. Those TOCs never
contribute unique MRF URLs.

This story changes only the pipeline's sponsor-required rule. It does not
change `mrftocparser`, `mrfparser`, discovery, download, parse, or consume.

## Dependencies

- Implemented Stories 02, 08, 12, 13, 16, and 23.
- The pinned `mrftocparser` package already emits null EIN sponsor. This
  story does not change it.
- The pinned `mrfconsumer` package currently rejects EIN with a missing
  sponsor on `AttachPlans` JSON (`plan_sponsor_name is required for ein`).
  This story does not implement that consumer change. See
  [Sibling landing constraint](#sibling-landing-constraint).

## Goal

1. Treat null `plan_sponsor_name` as valid for both `ein` and `hios` on TOC
   association import.
2. Keep empty-string sponsor invalid for every `plan_id_type`.
3. Store a null EIN sponsor on `toc_mrf_plan_associations` and allow a null
   EIN sponsor on `mrf_plans`.
4. Keep HIOS projection on `mrf_plans` as null even when the TOC supplied a
   sponsor.
5. Keep first-nonempty EIN sponsor when one exists; a later nonempty value
   may fill a stored null and must not replace a stored nonempty value.
6. Emit `"plan_sponsor_name":null` from Story 12's encoder when the stored
   EIN sponsor is null. Never emit `""`.
7. After deploy, operator `retry` of failed `toc.import` jobs is enough to
   finish those TOCs. Do not auto-retry them from this story.

## Non-goals

- Changing `mrftocparser`, `mrfparser`, `mrfdiscoverer`, or `mrfenricher`.
- Implementing the `mrfconsumer` EIN-null acceptance change. That is a
  sibling story. This repo may later pin such a consumer version; it must
  not rewrite consumer behavior here.
- Inferring, copying, or fabricating a sponsor from `issuer_name`,
  `plan_name`, EIN, filename, URL, or reporting entity.
- Treating empty string as null. Empty remains invalid.
- Changing plan identity. Canonical `mrf_plans` uniqueness stays the
  five-field tuple that excludes sponsor. Provenance uniqueness still
  includes sponsor, including null.
- Unselecting sources, raising `set-total`, or changing MRF resident
  capacity.
- Rewriting Stories 02, 08, or 12 as if they originally allowed EIN-null.
  Current DESIGN.md and README must describe the new rule.
- Mutating a live Compose sample as part of landing the story.
- Hand-deleting River jobs or SQL-updating plan rows to invent a sponsor.

## Product decisions

- A missing CMS EIN sponsor is source metadata, not a reason to drop the
  association or the MRF URL. Null and nonempty sponsor remain distinct
  provenance identities and distinct stored values.
- Empty string is never a sponsor. JSON `""` must not appear in
  `plans.json`.
- HIOS canonical rows still store and emit null sponsor. A TOC HIOS
  sponsor remains only on `toc_mrf_plan_associations`.
- EIN canonical rows store the first nonempty sponsor when one is imported
  for that five-field identity. If every imported row for that identity has
  a null sponsor, the canonical row stores null.
- Story 02's sentence “EIN requires a nonempty sponsor,” Story 08's
  “EIN has a nonempty sponsor,” and Story 12's “EIN emits the stored
  required nonempty sponsor as a JSON string” are superseded by this story
  for current behavior only.
- `mrf_plans.plan_sponsor_name` is no longer “one valid operational sponsor
  used only to satisfy consumer JSON validation.” It is the stored source
  sponsor after the HIOS-null and first-nonempty-EIN rules, and it may be
  null for EIN.

## Sibling landing constraint

`toc.import` can land and be tested without a new consumer. Plan attachment
cannot: Story 12 still calls `mrfconsumer.AttachPlans` with the encoded
document.

Until a consumer version accepts EIN with JSON-null sponsor:

- Encoder unit tests in this repo must still prove EIN-null emits JSON
  null.
- Do not add an integration test that invokes `AttachPlans` with EIN-null
  against the currently pinned consumer (it would fail for the sibling
  reason, not a pipeline bug).
- Do not deploy this pipeline image onto a warehouse that will attach
  EIN-null `mrf_plans` rows unless that consumer pin is already in
  `go.mod`.

A later pipeline change may pin the accepting consumer. That pin is not
required to specify this story, and this story does not implement it.

## Required behavior

### Association row validation

`toc.import` first-pass row validation must accept:

- `plan_id_type = ein` and `plan_sponsor_name` null
- `plan_id_type = hios` and `plan_sponsor_name` null or nonempty

It must still reject:

- empty-string sponsor for `ein` or `hios`
- invalid UTF-8 sponsor when present
- every other existing invalid plan, location, filename, or additional-JSON
  rule from Story 08

A TOC whose associations are all EIN with null sponsor, and that otherwise
matches the parser output contract, must import successfully. The failure
code `toc_import_row_invalid` must not be used for null EIN sponsor alone.

Do not warn, log, or store parser warning text. Parser warnings already
happened at `toc.parse`.

### PostgreSQL CHECKs

Add application migration `0007` after `0006`. Do not rewrite `0001`.

Replace `toc_mrf_plan_associations_sponsor_check` so sponsor is null or a
nonempty string for both `ein` and `hios`:

```text
plan_sponsor_name IS NULL OR plan_sponsor_name <> ''
```

Replace `mrf_plans_sponsor_check` so:

```text
HIOS: plan_sponsor_name IS NULL
EIN:  plan_sponsor_name IS NULL OR plan_sponsor_name <> ''
```

Existing live rows already satisfy the new CHECKs (EIN-null could not have
been inserted). The migration must not require an empty database.

Uniqueness is unchanged:

- `toc_mrf_plan_associations_unique_key` still uses
  `UNIQUE NULLS NOT DISTINCT` including `plan_sponsor_name`
- `mrf_plans_identity_key` still excludes sponsor

### Canonical EIN sponsor

`canonicalSponsor` for EIN is the association's sponsor pointer, which may
be nil. HIOS remains nil.

When inserting `mrf_plans`:

- A new five-field identity is inserted with that canonical sponsor
  (possibly null).
- On conflict with an existing identity, keep a stored nonempty EIN
  sponsor. If the stored EIN sponsor is null and the incoming canonical
  sponsor is nonempty, fill the stored null with that nonempty value.
- Never replace a nonempty EIN sponsor with a different nonempty value.
- Never replace a nonempty EIN sponsor with null.
- Never write empty string.

Provenance still inserts every accepted source row, including EIN-null and
EIN-nonempty variants of the same five-field identity.

### `plans.json` encoder

When `plan_id_type` is `ein` and stored `plan_sponsor_name` is null, emit
JSON null for `plan_sponsor_name`, the same member shape already used for
HIOS.

Keep:

- exact six-member field order
- compact JSON, `SetEscapeHTML(false)`, one trailing newline
- HIOS always JSON null
- nonempty EIN sponsor emitted as a JSON string, preserved exactly
- empty string is an invariant failure

An existing `plans.json` is still reusable only when its bytes equal the
newly rendered canonical document. No live `plans.json` can already contain
EIN-null because those imports never succeeded.

### Retry and reconcile

No new reconcile repair. Failed `toc.import` remains operator `retry`
(`cli retry --stage toc.import --id <toc_file_id>`). After this code and
migration are running, that retry must succeed for a TOC whose only former
defect was EIN-null sponsor, using the already-parsed association Parquet.

Do not invent a sponsor during retry. Do not skip association rows. Do not
cancel or delete River jobs from the UI.

River jobs still in `retryable` on an old image will keep failing until
workers run the new code. Recreating workers after migrate is the deploy
path already documented for other stories.

### Status and admission

Unchanged except that those TOCs can reach `import_status=succeeded` and
then create or reuse `mrf_sources` / `mrf_snapshots` under the existing
Story 16 rules. Extra URLs over a numeric `mrf_source_target` stay
`blocked_by_limit`. This story does not raise the target.

## Documentation

When implemented, update current operator/architecture text:

- DESIGN.md document status and the Stories 14–N current-architecture list
  include this story.
- DESIGN.md attach section: EIN may emit JSON-null sponsor; empty string is
  still forbidden.
- Story 08 current validation bullet and Story 02 current CHECK text are
  superseded here; do not rewrite those historical files.
- README packaging contract strings stay intact.

Do not document inferred sponsors or a new CLI flag.

## Required tests

Do not run formatters, linters, or the full suite while implementing. Land
tests that prove the contract; the operator can run `go test -p 1 ./...`
once.

### Unit / policy

- Association validation accepts EIN with nil sponsor and HIOS with nil
  sponsor.
- Association validation still rejects empty-string sponsor for EIN and
  HIOS.
- Encoder emits `"plan_sponsor_name":null` for EIN with nil sponsor and for
  HIOS.
- Encoder still emits the exact nonempty EIN sponsor string when present.
- Encoder still rejects empty-string sponsor.

### PostgreSQL integration

Run against a disposable `mrfpipeline_test_` database, serial as required by
the DB-suite contract.

- Migration 0007 allows INSERT of EIN-null into `toc_mrf_plan_associations`
  and `mrf_plans`.
- Empty-string sponsor is still rejected on both tables.
- HIOS `mrf_plans` still rejects a nonempty stored sponsor.
- A TOC output whose associations are EIN with null sponsor imports:
  provenance rows stored, canonical `mrf_plans` row has null sponsor, MRF
  source/snapshot created or reused, `import_status=succeeded`.
- Two provenance rows that differ only by EIN null vs nonempty sponsor both
  insert; canonical identity stays one row.
- First nonempty EIN sponsor wins: null then nonempty fills the canonical
  null; nonempty then a different nonempty keeps the first; nonempty then
  null keeps the nonempty.
- HIOS TOC sponsor still projects to null on `mrf_plans` and is preserved
  on provenance.
- Existing import tests that use EIN with a nonempty sponsor remain valid.

Do not add `AttachPlans` EIN-null integration against the currently pinned
consumer.

## Acceptance criteria

1. `toc.import` succeeds for parser-valid EIN associations with null
   `plan_sponsor_name`.
2. Empty-string sponsor is still `toc_import_row_invalid` / CHECK failure.
3. Migration 0007 is idempotent via the existing migrate command and does
   not require an empty database.
4. Canonical HIOS sponsor remains null; canonical EIN sponsor is null or
   the first nonempty imported value.
5. Encoder emits JSON null for stored-null EIN and never emits `""`.
6. Operator retry of a previously failed `toc.import` whose bytes were only
   EIN-null sponsor succeeds on the new image without re-parsing the TOC
   source when parsed output is still present.
7. `mrftocparser` and `mrfparser` are unchanged.
8. DESIGN current-architecture text includes this story after
   implementation; historical Stories 02/08/12 are not rewritten.
9. No `AttachPlans` EIN-null integration is added until a consumer pin
   accepts that document.

## Implementation notes

Prefer replacing the two CHECKs in a small `0007` migration, relaxing
`validateAssocRow`, using `COALESCE` (or equivalent) on the `mrf_plans`
conflict path so first-nonempty EIN still holds, and dropping the encoder's
EIN-required-nonempty branch while keeping the empty-string invariant.

Do not SQL-delete or hand-update live `mrf_plans` / association rows as
part of landing. After deploy: `cli migrate`, recreate workers so they
load the new image, then `cli retry` for domain-failed or exhausted
`toc.import` jobs.
