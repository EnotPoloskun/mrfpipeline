# Story 36: Discoverer-supported production payers

## Status

Implemented.

## User story

As a pipeline operator, I want `mrfpipeline discover --payer` to accept every
payer identifier the pinned `mrfdiscoverer` already lists so I can enqueue an
Optum or Cigna run without a pipeline-only UHC gate.

## Context

`mrfdiscoverer` version 1 lists TOC URLs for exact identifiers `uhc`, `optum`,
and `cigna`. `DefaultDiscover` already forwards the stored `payer_id` to
`mrfdiscoverer.Discover`. The domain schema, monthly releases, stats, month
control, and filters already treat `payer_id` as a generic identifier.

The only production UHC-only gate is `config.ValidatePayer`, called from
`runDiscover`. It currently requires the exact string `uhc` after
`ValidatePayerIdentifier` succeeds. `stats`, `month`, and `filters` use
`ValidatePayerIdentifier` and already accept `optum` and `cigna`.

Stories 01, 05, 14, 16, README Limitations, discover help, and DESIGN current
operator text still say production discovery is UHC-only. That living contract
is what this story replaces. Historical story text stays as originally
written.

## Goal

Expand the production `discover` adapter allowlist to the exact lowercase
identifiers the pinned `mrfdiscoverer` already supports:

```text
uhc
optum
cigna
```

Keep `--limit` required. Keep `--mrf-source-limit` as Story 21 defined it
(`N` or `all`). Do not add a fourth payer, a discoverer export, a sibling
pin bump, or a live Optum/Cigna acceptance run in this story.

## Dependencies and compatibility

Depends on implemented Stories 01, 05, 14–16, and 21, and on the already
pinned `github.com/EnotPoloskun/mrfdiscoverer`.

The worker integration does not change:

```go
mrfdiscoverer.Discover(ctx, mrfdiscoverer.Config{Payer: payer})
```

`payer` is the validated CLI value stored on the discovery run. Do not copy
listing logic, execute the `mrfdiscoverer` CLI, or add a second HTTP
implementation in this repository.

No pipeline migration. No warehouse, catalog, Compose, download-policy, or
parser/consumer pin change.

## Supersession

This story supersedes living current-contract language that says production
discovery is UHC-only, including:

- Story 01 `--payer` “Version 1 accepts only `uhc`” as operator-facing
  current contract (the historical story remains a record of version 1
  delivery).
- Story 05 “A payer other than UHC” as a current non-goal of `discover`.
- Story 14 required test “the production discover command still rejects
  non-UHC payers” and acceptance “discovery remains explicitly UHC-only”.
- Story 16 “The production discovery adapter is still UHC only.”
- README Limitations “Discovery is UHC-only”.
- `internal/cli/help.go` “Enqueue one bounded UHC discovery run” and
  “only uhc”.
- DESIGN.md non-goal “A payer other than UHC”, the context diagram labeled
  only “UHC blob listing”, and cross-story acceptance “A bounded UHC
  discovery”.

It preserves:

- required positive `--limit` (no omitted or unlimited TOC admission);
- exact lowercase identifiers; no case folding or aliases
  (`UHC`, `united-healthcare`, `UnitedHealthcare` stay invalid);
- `ValidatePayerIdentifier` as the generic stored-identifier check used by
  `stats` / `month` / `filters`;
- Story 05 admission, overflow, ordinal, and enqueue behavior;
- public-HTTPS download policy (not a UHC-host allowlist);
- one discovery run = one payer + one collection month + one `--limit`.

## Product decisions

- Production `discover --payer` accepts exactly `uhc`, `optum`, and `cigna`.
  Those three strings are duplicated in this repository to match the pinned
  discoverer’s private switch. Do not add a public supported-payer export to
  `mrfdiscoverer` in this story. A later discoverer payer still needs a later
  pipeline story.
- `ValidatePayer` remains the production adapter allowlist. It still calls
  `ValidatePayerIdentifier` first, then rejects any identifier outside the
  three. `ValidatePayerIdentifier` itself does not become an adapter
  allowlist.
- The invalid-adapter error keeps `FieldPayer` and names the accepted
  identifiers, for example `must be an exact adapter identifier: uhc, optum, or
  cigna`. Do not keep `must be the exact adapter identifier uhc`.
- Help usage may keep `--payer uhc` as a worked example only if the required
  flag text lists `uhc`, `optum`, and `cigna`. Prefer
  `--payer <uhc|optum|cigna>` on the usage line.
- `discover` still enqueues and returns. It does not list or download in the
  CLI process. The worker already passes the stored payer to
  `mrfdiscoverer`; an Optum run lists Optum TOC URLs, a Cigna run lists Cigna
  TOC URLs.
- Shared-warehouse and same-URL two-payer snapshot rules from Stories 14–16
  already apply. This story does not change them.
- `TestRealUHCAcceptance` stays UHC. This story does not add a required live
  Optum or Cigna acceptance test.

## Command surface

`discover` flags, environment, and exit codes stay Story 01 / 21. Only the
accepted `--payer` values change.

Accepted:

```text
mrfpipeline discover --payer uhc --collection-month <YYYY-MM> --limit <count> [--mrf-source-limit <N|all>]
mrfpipeline discover --payer optum --collection-month <YYYY-MM> --limit <count> [--mrf-source-limit <N|all>]
mrfpipeline discover --payer cigna --collection-month <YYYY-MM> --limit <count> [--mrf-source-limit <N|all>]
```

Rejected (exit 2, `FieldPayer` in the error):

- omitted `--payer`
- empty `--payer`
- `UHC`, `OPTUM`, `Cigna`, `united-healthcare`, `aetna`, and any other
  identifier the pinned discoverer does not list
- leading/trailing whitespace

`--limit all` remains invalid. `--mrf-source-limit all` remains valid.

## Implementation

- `internal/config/config.go`: change `ValidatePayer` to accept `uhc`,
  `optum`, and `cigna` after `ValidatePayerIdentifier`. Keep the comment
  accurate (production adapter identifiers, not “only uhc”).
- `internal/cli/cli.go`: `runDiscover` already calls `ValidatePayer`; do not
  add a second allowlist there.
- `internal/cli/help.go`: root command description, discover usage, and
  `--payer` required-flag text.
- `internal/discovery/discovery.go`: no behavior change. Do not hard-code
  `uhc` in `DefaultDiscover`.
- README Limitations and DESIGN.md living operator text listed under
  Supersession.
- Do not rewrite historical Stories 01, 05, 13, 14, or 16 as if they
  originally shipped Optum/Cigna discovery.

## Required tests

- `TestValidatePayer` accepts `uhc`, `optum`, and `cigna`.
- `TestValidatePayer` rejects `UHC`, `OPTUM`, `Cigna`, `united-healthcare`,
  `aetna`, `uhc `, ` uhc`, and `""`, still wrapping `ErrInvalidConfig` and
  including `FieldPayer`. The error text for a well-formed but unsupported
  identifier names the accepted adapters and does not say “only uhc”.
- `TestValidatePayerIdentifier` still accepts generic identifiers such as
  `aetna` (stored-identifier check is not the adapter allowlist).
- Discover command parse/help tests: `optum` and `cigna` are valid payer
  values; `aetna` is still a usage/config error. `--help` and root help name
  `uhc`, `optum`, and `cigna` and no longer say “only uhc” or “bounded UHC
  discovery”.
- Replace any remaining production-discover test that requires rejecting
  `optum` / `cigna` as non-UHC. Keep rejecting identifiers the discoverer
  does not support.
- Do not require a live listing, TOC download, or warehouse ingest for Optum
  or Cigna in this story.

Do not require a project-wide test suite beyond `internal/config` and
`internal/cli` (plus any discover-parse tests that currently freeze UHC-only).

## Documentation

Update current operator text only:

- README Limitations: discovery accepts `uhc`, `optum`, and `cigna`; month
  control and import remain the generic payer contract.
- `internal/cli/help.go` as above.
- DESIGN.md: document status stays Stories 01–35 implemented plus this story
  specified; non-goal “A payer other than UHC” becomes “A payer the pinned
  `mrfdiscoverer` does not list”; context diagram is a payer listing, not
  “UHC blob listing” only; cross-story acceptance item 2 is a bounded
  discovery for a supported payer identifier, still requiring `--limit`.

Do not claim unlimited discovery. Do not claim a live Optum run was executed.

## Acceptance criteria

1. `discover --payer optum` and `discover --payer cigna` pass payer
   validation with the same required `--collection-month` and `--limit`
   rules as `uhc`.
2. `discover --payer aetna` and `discover --payer UHC` fail with exit 2 and
   `FieldPayer`.
3. `stats` / `month` / `filters` payer validation is unchanged
   (`ValidatePayerIdentifier`).
4. `--limit` is still required and still rejects `all`.
5. `DefaultDiscover` still passes the stored payer string to
   `mrfdiscoverer.Discover`.
6. Help and README no longer say production discovery is UHC-only.
7. No migration, warehouse layout, Compose, or sibling-module change.

## Non-goals

- A payer other than `uhc`, `optum`, and `cigna`.
- Exporting a supported-payer list from `mrfdiscoverer`.
- Omitted or unlimited TOC `--limit`.
- Changing `--mrf-source-limit`, download policy, parse, ingest, catalog, or
  Compose topology.
- Starting an Optum or Cigna discovery run, worker scale, or shared-warehouse
  cutover as part of this story.
- Recurring discovery, aliases, or inferring payer from URLs/hostnames.
- Rewriting historical Stories 01–16 as originally multi-payer discovery.
