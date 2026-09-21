# Story 38: Molina discoverer payer

## Status

Implemented.

## User story

As a pipeline operator, I want `mrfpipeline discover --payer molina` to
enqueue a Molina listing using the pinned `mrfdiscoverer` that already
knows that identifier.

## Context

`mrfdiscoverer` Story 05 adds exact identifier `molina` and lists TOC URLs
from Molina's Pricing Transparency HTML page. Pipeline Story 36 still
duplicates the previous three-payer private switch (`uhc`, `optum`,
`cigna`) in `config.ValidatePayer`. `DefaultDiscover` already forwards the
stored `payer_id` to `mrfdiscoverer.Discover`.

`stats`, `month`, and `filters` already accept `molina` through
`ValidatePayerIdentifier`. Only production `discover` rejects it.

## Goal

Pin `github.com/EnotPoloskun/mrfdiscoverer` to a revision that includes
Molina listing, and expand the production `discover` adapter allowlist to:

```text
uhc
optum
cigna
molina
```

Keep `--limit` required (`N` or `all`). Keep `--mrf-source-limit` unchanged.
Do not add a fifth payer or a supported-payer export from `mrfdiscoverer`.

## Dependencies and compatibility

Depends on implemented Stories 36–37 and on `mrfdiscoverer` Story 05.

The worker integration does not change:

```go
mrfdiscoverer.Discover(ctx, mrfdiscoverer.Config{Payer: payer})
```

No pipeline migration. No warehouse, catalog, Compose, download-policy, or
parser/consumer pin change.

## Supersession

This story supersedes living current-contract language that lists only
`uhc`, `optum`, and `cigna` as production discovery adapters, including
Story 36's three-payer allowlist as operator-facing current contract.
Historical Stories 36–37 remain a record of that delivery.

## Product decisions

- Production `discover --payer` accepts exactly `uhc`, `optum`, `cigna`,
  and `molina`. Those strings remain duplicated here to match the pinned
  discoverer's private switch.
- `ValidatePayer` still calls `ValidatePayerIdentifier` first, then rejects
  any identifier outside the four. `ValidatePayerIdentifier` is not an
  adapter allowlist.
- The invalid-adapter error names the accepted identifiers, for example
  `must be an exact adapter identifier: uhc, optum, cigna, or molina`.
- Help usage is `--payer <uhc|optum|cigna|molina>`.
- Exact lowercase identifiers; no case folding or aliases (`Molina`,
  `MOLINA`, `molina-healthcare` stay invalid).
- `discover` still enqueues and returns. A Molina run lists Molina TOC URLs.

## Command surface

Accepted:

```text
mrfpipeline discover --payer molina --collection-month <YYYY-MM> --limit <N|all> [--mrf-source-limit <N|all>]
```

Rejected (exit 2, `FieldPayer`): `Molina`, `MOLINA`, `molina-healthcare`,
`aetna`, and any other identifier the pinned discoverer does not list.

## Implementation

- Pin `github.com/EnotPoloskun/mrfdiscoverer` to the Molina listing
  revision and freeze it in `go.mod` / `cmd/mrfpipeline` pin tests.
- `internal/config/config.go`: accept `molina` in `ValidatePayer`.
- `internal/cli/help.go`, README Limitations, USAGE, DOCKER, and DESIGN
  living operator text.
- Do not rewrite historical Stories 36–37.

## Required tests

- `TestValidatePayer` accepts `molina`.
- `TestValidatePayer` rejects `Molina`, `MOLINA`, and `molina-healthcare`.
- Discover parse/help tests: `molina` is a valid payer value; help names
  `molina`.
- Pin test expects the Molina `mrfdiscoverer` pseudo-version.

## Acceptance criteria

1. `discover --payer molina` passes payer validation with the same required
   `--collection-month` and `--limit` rules as `uhc`.
2. `discover --payer Molina` fails with exit 2 and `FieldPayer`.
3. `DefaultDiscover` still passes the stored payer string to
   `mrfdiscoverer.Discover`.
4. Help, README, USAGE, and DOCKER name `molina` with the other adapters.
5. No migration, warehouse layout, or Compose topology change.

## Non-goals

- A payer other than `uhc`, `optum`, `cigna`, and `molina`.
- Exporting a supported-payer list from `mrfdiscoverer`.
- Changing download policy, parse, ingest, catalog, or Compose topology.
- Recurring discovery, aliases, or inferring payer from URLs/hostnames.
