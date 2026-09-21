# Story 40: Month-aware discoverer pin and Oscar payer

## Status

Implemented.

## User story

As a pipeline operator, I want `discover` to pass the release month into the
pinned `mrfdiscoverer` and to accept `oscar`, so listings are filtered to
that month and Oscar TOC objects can be admitted.

## Context

Pipeline Story 38 still calls:

```go
mrfdiscoverer.Discover(ctx, mrfdiscoverer.Config{Payer: payer})
```

`mrfdiscoverer` Story 06 makes `Config.Month` required (`mm-yyyy`, for
example `09-2026`). A missing month is `ErrInvalidConfig`. Story 06 also
filters the current listing to TOC files whose derived reporting month
equals that value.

`mrfdiscoverer` Story 07 adds exact identifier `oscar` and constructs the
day-01 Oscar TOC object for that month.

Pipeline `--collection-month` remains `YYYY-MM` and is stored as the first
of that month. Do not change the operator flag form. Convert at the
discoverer boundary only.

## Goal

1. Pin `github.com/EnotPoloskun/mrfdiscoverer` to a revision that includes
   Stories 06 and 07. No `replace` directive.
2. Pass the discovery run's collection month into `Discover`.
3. Expand the production `discover` adapter allowlist to:

```text
uhc
optum
cigna
molina
oscar
```

Keep `--limit` required (`N` or `all`). Keep `--mrf-source-limit` unchanged.

## Dependencies and supersession

Depends on implemented Stories 36–38 and on `mrfdiscoverer` Stories 06–07.

Do not rewrite historical Stories 36–38. This story is the living contract
for production discovery adapters and for forwarding month into the pinned
discoverer.

## Product decisions

- Pipeline CLI and PostgreSQL stay `YYYY-MM`. `mrfdiscoverer.Config.Month`
  is `mm-yyyy`. `DefaultDiscover` converts `2026-09` to `09-2026`.
- `DiscoverFunc` takes `(ctx, payer, collectionMonth string)` where
  `collectionMonth` is pipeline `YYYY-MM`. Fakes in tests use that form.
- Conversion uses the stored `date` year and month, not a string parse of
  a URL.
- An invalid stored month cannot be converted and maps through existing
  `ErrInvalidConfig` handling (`domain_invariant`). That is a programming
  / database invariant, not an operator typo path (`ValidateCollectionMonth`
  already rejected bad CLI values).
- Production `discover --payer` accepts exactly the five identifiers above.
  Exact lowercase; no aliases (`Oscar`, `OSCAR`, `oscar-health` stay invalid).
- Help usage is `--payer <uhc|optum|cigna|molina|oscar>`.
- Oscar discovery may return zero or one TOC URL. Empty success still
  admits nothing and is not a listing error.

## Command surface

Accepted:

```text
mrfpipeline discover --payer oscar --collection-month 2026-09 --limit all --mrf-source-limit all
```

Rejected (exit 2, `FieldPayer`): `Oscar`, `OSCAR`, `oscar-health`.

`--collection-month` remains required and `YYYY-MM`. Do not add `--month`.

## Implementation

- Pin `mrfdiscoverer` and freeze it in `go.mod` / `cmd/mrfpipeline` pin tests.
- `internal/discovery`: `DefaultDiscover` sets `Config.Payer` and
  `Config.Month`; `Worker.execute` passes `run.PayerID` and the run's
  collection month formatted as `YYYY-MM`.
- `internal/config.ValidatePayer` accepts `oscar`.
- Help, README, USAGE, DOCKER, and DESIGN living operator text.
- Do not rewrite historical Stories 36–38.

## Required tests

- `TestValidatePayer` accepts `oscar` and rejects `Oscar`, `OSCAR`,
  `oscar-health`.
- Discover parse/help tests: `oscar` is valid; help names `oscar`.
- Conversion: `2026-09` → `09-2026`.
- Worker integration fake receives payer `uhc` and month `2026-08` (the
  fixture month), not `08-2026`.
- Pin test expects the Story 06/07 `mrfdiscoverer` pseudo-version.

## Acceptance

1. `discover --payer oscar --collection-month 2026-09 --limit all` passes
   payer and month validation.
2. `DefaultDiscover` calls `mrfdiscoverer.Discover` with `Payer` and
   `Month` in `mm-yyyy`.
3. Off-month TOC URLs are no longer returned by the pinned discoverer and
   therefore are not admitted.
4. Help, README, USAGE, and DOCKER name `oscar` with the other adapters.
5. No migration, warehouse layout, or Compose topology change.

## Non-goals

- Changing `--collection-month` to `mm-yyyy`.
- Exporting a supported-payer list from `mrfdiscoverer`.
- Filtering again in `admit()` by filename.
- Download policy, parse, ingest, catalog, or Compose topology changes.
