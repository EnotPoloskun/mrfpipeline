# Story 01: Project foundation and CLI contract

## Status

Implemented.

## User story

As a pipeline operator, I want one small `mrfpipeline` executable with a
stable configuration and command contract so that later stories can add
PostgreSQL state, River workers, and MRF processing without redefining the
application boundary.

## Goal

Create the Go module, executable, configuration validation, command parsing,
cancellation, diagnostics, and version behavior for the pipeline project.

This story establishes three operational commands:

```text
mrfpipeline migrate
mrfpipeline work
mrfpipeline discover --payer uhc --collection-month <YYYY-MM> --limit <count>
```

The commands are placeholders in Story 01. After successful configuration
validation they return a private not-implemented error without opening a
database, creating an artifact directory, contacting a payer, or starting a
worker. Later stories replace those terminal errors with concrete behavior.

## Scope decisions

- The Go module path is `github.com/enotpoloskun/mrfpipeline`.
- The module declares Go `1.26`.
- The initial build targets are `darwin/arm64`, `linux/amd64`, and
  `linux/arm64`.
- The executable is named `mrfpipeline` and lives under `cmd/mrfpipeline`.
- Version 1 is an operator-facing executable. Story 01 does not expose a
  public orchestration framework or require an importable root-package API.
- Runtime configuration comes from exact environment variables. Version 1
  has no configuration-file format and does not accept secrets through CLI
  flags.
- `migrate` will apply application and River database migrations in a later
  story. Other commands never perform implicit database migration.
- `work` will run background workers in a later story.
- `discover` will enqueue one background discovery run in a later story. It
  will not synchronously execute the complete download and parsing pipeline.
- Version 1 supports only the payer identifier `uhc`.
- Discovery is manually triggered through the CLI or an external scheduler.
  An internal recurring scheduler is outside the initial pipeline scope.
- Automatic `mrfenricher` execution, a UI, an HTTP server, and payers other
  than UHC are outside this story and the initial pipeline implementation.

## Module and repository boundary

Initialize the repository as:

```text
module github.com/enotpoloskun/mrfpipeline

go 1.26
```

The initial repository contains:

```text
cmd/mrfpipeline/
internal/cli/
internal/config/
requirements/
go.mod
README.md
```

Story 01 adds a short `README.md` that states the project is requirements-first,
names the three current commands, and links to `requirements/`. It does not
document worker operations that do not exist yet. Story 13 replaces it with
operational documentation. Root and command `--help` in this story mention only
`migrate`, `work`, and `discover`. Story 13 amends root help to add `reconcile`
and `retry`.

The exact internal package split may remain smaller when that is clearer, but
the command entry point stays thin. Process argument parsing, signal setup,
and stream selection belong to the command or CLI adapter. Environment and
semantic configuration validation belong to one reusable internal
configuration path rather than being duplicated in each command.

Story 01 uses the Go standard library only. It must not add River, a PostgreSQL
driver, a migration package, a CLI framework, a logging framework, a sibling
MRF module, or a Parquet dependency before concrete behavior needs it.

## Commands

### Database migration

```text
mrfpipeline migrate
```

`migrate` accepts no flags or positional arguments. It requires only database
configuration. A later story makes the operation explicit and repeatable;
`work` and `discover` do not migrate automatically.

### Background workers

```text
mrfpipeline work
```

`work` accepts no flags or positional arguments in Story 01. It requires the
complete worker configuration. Queue selection, concurrency, polling, River
client creation, and shutdown behavior are defined when the worker runtime is
implemented.

### Discovery trigger

```text
mrfpipeline discover \
  --payer uhc \
  --collection-month 2026-08 \
  --limit 5
```

`--payer`, `--collection-month`, and `--limit` are required. Omitted or
unlimited discovery is deferred until a later chunked-admission story. The
Story 02 `toc_limit` column may remain nullable for that future compatibility;
version 1 never writes null and never accepts a missing `--limit`.

| Flag | Meaning |
|---|---|
| `--payer` | Exact payer adapter identifier. Version 1 accepts only `uhc`. |
| `--collection-month` | Caller-selected month assigned to the discovered TOC records, exactly `YYYY-MM`. It is not inferred from URLs or payer contents. |
| `--limit` | Required positive `int64` limiting how many newly discovered TOCs this run may admit into the pipeline. Story 05 defines selection, overflow counting, and admission semantics. |

Scalar flags accept both `--name value` and `--name=value`. A scalar flag may
appear more than once; its final occurrence is used. A flag is syntactically
present when it appears at least once, even when its final value is empty.
Semantic validation applies to the final value.

The command has no positional arguments. Single-dash aliases, short flags,
and a standalone `--` are not supported.

## Runtime environment

The executable reads only these pipeline-specific environment variables:

| Variable | `migrate` | `work` | `discover` | Meaning |
|---|:---:|:---:|:---:|---|
| `MRFPIPELINE_DATABASE_URL` | required | required | required | PostgreSQL connection string used by later database and River stories. |
| `MRFPIPELINE_ARTIFACT_ROOT` | ignored | required | ignored | Local root owned by the pipeline for downloads and intermediate parser artifacts. |
| `MRFPIPELINE_WAREHOUSE_PATH` | ignored | required | ignored | Local `mrfconsumer` warehouse root. |
| `MRFPIPELINE_PROVIDER_CATALOG_PATH` | ignored | required | ignored | Local provider catalog prepared manually through `mrfenricher`. |
| `MRFPIPELINE_SERVICES_PATH` | ignored | required | ignored | Local service-selector CSV passed to `mrfparser`. |

An ignored variable does not participate in validation for that command.
This lets an operator run migrations or enqueue discovery without having the
worker's local files mounted on that machine.

Version 1 does not read `.env` files, infer alternate variable names, accept a
database URL as a command-line flag, or scan arbitrary environment variables.
An empty required environment value is missing configuration.

`MRFPIPELINE_DATABASE_URL` is treated as an opaque secret in Story 01. It must
be nonempty and valid UTF-8 and must not contain NUL, carriage-return, or
newline bytes. Story 02 delegates full PostgreSQL connection-string parsing to
the selected PostgreSQL driver. Story 01 does not normalize, connect with, or
echo it.

## Local-path configuration

Each required local path is accepted lexically when it:

- Is nonempty valid UTF-8.
- Contains no NUL, carriage-return, or newline byte.
- Does not begin with URI-like `scheme://` syntax.

For this contract, a URI-like prefix is an ASCII scheme matching
`[A-Za-z][A-Za-z0-9+.-]*` followed by `://`. Values beginning with `s3://`,
`http://`, `https://`, and `file://` are therefore invalid local paths. A colon
without the following `//`, such as `disk:artifacts`, remains an ordinary path
character on platforms where it can be normalized.

Normalize required paths independently, in environment-table order, using:

1. `filepath.Abs`.
2. `filepath.Clean`.

Story 01 does not call `filepath.EvalSymlinks`, open or stat a path, require it
to exist, create it, or modify its permissions. Story 04 defines operational
filesystem types, containment, ownership, staging, and cleanup safety before
the pipeline writes artifacts.

Configuration errors identify the field or environment-variable name but do
not include the configured value.

## Discovery-value validation

The payer is an exact adapter identifier. Version 1 accepts only:

```text
uhc
```

The value is not trimmed or case-folded. `UHC`, `united-healthcare`, and
`uhc ` are invalid.

`collection_month` is exactly four decimal year digits, `-`, and a two-digit
month:

- Year is `0001` through `9999`.
- Month is `01` through `12`.
- Year `0000`, timestamps, surrounding whitespace, and alternate spellings
  are invalid.

`limit` is required and must be a positive base-10 integer that fits in a
signed 64-bit integer (`int64`), independent of the platform `int` size. `0`,
negative values, a leading plus sign, surrounding whitespace, decimals,
exponents, empty values, and values that overflow `int64` are invalid. Leading
zeroes are accepted by command parsing and have their ordinary decimal meaning;
Story 05 stores and reports the resulting integer rather than the original
spelling.

The limit is an operator safety control for bounded discovery. It is not an
identity value or a request to truncate the payer listing before exact URL
deduplication. The documented live progression is one newly admitted TOC, then
two, then five, with ten only after measuring fan-out and disk. Story 05
defines its application to newly admitted TOCs and `overflow_count`.

## Validation and placeholder behavior

Configuration validation is deterministic and fail-fast.

### `migrate`

1. Reject a nil context as a programming error.
2. Return entry cancellation when already canceled.
3. Validate `MRFPIPELINE_DATABASE_URL`.
4. Check cancellation again.
5. Return the private migration-not-implemented error.

### `work`

1. Reject a nil context as a programming error.
2. Return entry cancellation when already canceled.
3. Validate `MRFPIPELINE_DATABASE_URL`.
4. Validate and normalize `MRFPIPELINE_ARTIFACT_ROOT`.
5. Validate and normalize `MRFPIPELINE_WAREHOUSE_PATH`.
6. Validate and normalize `MRFPIPELINE_PROVIDER_CATALOG_PATH`.
7. Validate and normalize `MRFPIPELINE_SERVICES_PATH`.
8. Check cancellation again.
9. Return the private worker-not-implemented error.

### `discover`

1. Reject a nil context as a programming error.
2. Return entry cancellation when already canceled.
3. Validate `MRFPIPELINE_DATABASE_URL`.
4. Validate payer.
5. Validate collection month.
6. Validate the required limit.
7. Check cancellation again.
8. Return the private discovery-enqueue-not-implemented error.

Entry cancellation takes precedence over configuration errors. When the
context was not canceled on entry, the first configuration failure wins even
if cancellation races with validation. After valid configuration, the second
cancellation check takes precedence over the private placeholder error.

The internal configuration package declares one sentinel used throughout the
executable:

```go
var ErrInvalidConfig = errors.New("invalid configuration")
```

It is exported from the internal package for the CLI adapter and focused
tests, but Story 01 does not promise it as an external module API. Every
semantic environment, path, payer, month, or limit error wraps that sentinel.
Command-line syntax errors do not need to wrap it. Cancellation and private
placeholder errors must not match it.

Every placeholder operation is side-effect free. It does not:

- Open a PostgreSQL connection.
- Run a migration.
- Initialize River.
- Read or create any configured path.
- Contact UnitedHealthcare.
- Enqueue, reserve, or execute a job.
- Import a sibling MRF tool.

## CLI informational behavior

These exact invocations succeed without reading runtime environment
configuration or performing operational work:

```text
mrfpipeline --help
mrfpipeline migrate --help
mrfpipeline work --help
mrfpipeline discover --help
mrfpipeline --version
```

`mrfpipeline --version` writes exactly:

```text
mrfpipeline dev
```

including one trailing newline. The version defaults to `dev` and may be
replaced for release builds with a linker flag.

Help and version are terminal informational operations only in the exact forms
above. Additional flags or positional arguments make them usage errors.

Root help explains:

- That the program orchestrates the MRF discovery, parsing, consumption, and
  plan-attachment pipeline.
- That PostgreSQL stores durable state and River runs background jobs in later
  stories.
- That `mrfenricher` execution is manual and not started by the pipeline.
- The `migrate`, `work`, and `discover` commands.

Command help explains the command's future effect and required environment.
Discovery help also explains that `collection_month` is a caller-supplied
label, `--limit` is required, unlimited discovery is out of version 1 scope,
and success will enqueue background work rather than wait for the entire
pipeline. Help in this story does not mention `reconcile` or `retry`.

## CLI errors, streams, and exit status

No command, an unknown command, or an unexpected root argument is a root usage
error and prints this exact hint to standard error:

```text
Try 'mrfpipeline --help'.
```

An unknown flag, missing required flag, or unexpected positional argument for
a known command is a command usage error. It prints the corresponding exact
hint:

```text
Try 'mrfpipeline migrate --help'.
Try 'mrfpipeline work --help'.
Try 'mrfpipeline discover --help'.
```

An error matching `ErrInvalidConfig` uses the current command's hint. The CLI
does not print the complete help text automatically.

Streams and initial exit statuses are:

| Outcome | Standard output | Exit status |
|---|---|---:|
| Help or version | Informational text | `0` |
| Usage or invalid configuration | Empty | `2` |
| Cancellation | Empty | `1` |
| Story 01 placeholder result | Empty | `1` |
| Help/version write failure or other reporting failure | Possibly partial informational text | `1` |

Human-readable diagnostics and help hints go to standard error. Other than the
version line and help-hint lines, exact diagnostic prose and help layout are
implementation-defined and tests do not pin them.

The command converts `SIGINT` and `SIGTERM` into context cancellation for
operational commands. Informational commands do not install workers, connect
to PostgreSQL, or inspect the filesystem.

No diagnostic, panic assertion, test failure produced from production error
text, or future log field may expose:

- The database URL or any substring derived from it.
- Configured local paths.
- Environment contents.
- Discovered TOC or MRF URLs.
- Credentials, query parameters, signed tokens, or HTTP response bodies.
- Plan values, negotiated rates, provider values, or raw source rows.

Identifiers and collection months may appear in successful machine-readable
reports added by later stories, but Story 01 has no successful operational
report.

## Minimal design constraints

- Keep the executable and configuration code concrete and small.
- Do not create generic `Job`, `Repository`, `Storage`, `Downloader`, payer,
  parser, consumer, or workflow interfaces in anticipation of later stories.
- Do not add a dependency-injection container, plugin system, service locator,
  event bus, or generic state-machine framework.
- Do not implement a custom scheduler or queue. River is introduced when
  PostgreSQL persistence exists.
- Do not shell out to or import `mrfdiscoverer`, `mrftocparser`, `mrfparser`,
  `mrfconsumer`, or `mrfenricher` in Story 01.
- Do not create database tables, local artifacts, warehouse files, temporary
  files, lock files, PID files, sockets, or network listeners.
- Do not add hashes, URL-derived IDs, filename-derived IDs, or placeholder
  identity algorithms.
- Do not infer collection month or payer from a URL.
- Do not run provider enrichment automatically.

## Required tests

Add focused tests proving:

- The module path and Go version are exact.
- The command builds for the three initial targets.
- The five informational invocations succeed without required environment.
- Development version output is exactly `mrfpipeline dev\n`.
- Root and per-command usage errors have the correct exit status and help
  hint.
- Operational commands reject unsupported flags, single-dash flags,
  positional arguments, and invalid help/version combinations.
- Repeated discovery flags use the final occurrence.
- Each command requires exactly the environment values listed for it and
  ignores worker-only variables for `migrate` and `discover`.
- Database configuration rejects empty, invalid UTF-8, NUL, carriage return,
  and newline values without exposing the supplied value.
- Worker path configuration covers relative paths, absolute cleaning,
  URI-like prefixes, NUL, carriage return, newline, invalid UTF-8, and
  normalization failure.
- Payer validation accepts only exact `uhc`.
- Month validation accepts boundary years and valid months and rejects year
  `0000`, invalid months, whitespace, and noncanonical forms.
- Limit parsing covers missing `--limit`, `1`, representative values `2`, `5`,
  and `10`, leading zeroes, zero, negative, plus-prefixed, noninteger, empty,
  and values overflowing `int64`.
- A nil context panics before validation or side effects; the panic text is
  not asserted.
- Entry cancellation precedes invalid configuration, and post-validation
  cancellation precedes the placeholder error.
- Every semantic configuration failure matches `ErrInvalidConfig`; syntax,
  cancellation, and private placeholder errors do not.
- Configuration errors and CLI diagnostics contain field names but no raw
  database or path values.
- Every operational invocation in Story 01 leaves PostgreSQL, the network,
  and configured filesystem paths untouched.
- `go test ./...`, `go test -race ./...`, and `go vet ./...` succeed.

## Acceptance criteria

- The repository is a Go module at
  `github.com/enotpoloskun/mrfpipeline` declaring Go `1.26`.
- The `mrfpipeline` binary builds and exposes `migrate`, `work`, `discover`,
  help, and version commands.
- Runtime environment and discovery values follow the exact validation and
  redaction rules above.
- Story 01 operational commands are side-effect-free placeholders and never
  report success.
- `README.md` is a short requirements-first placeholder linking to
  `requirements/`.
- `--limit` is syntactically required and parsed as a positive `int64`.
- Command parsing and configuration are implemented once and are ready for
  later database, River, discovery, and worker stories.
- No database, queue, payer request, artifact, parser, consumer, enrichment,
  or UI behavior is implemented prematurely.

## Dependencies

None.

## Non-goals

- PostgreSQL connections, migrations, tables, or transactions.
- River migrations, clients, queues, workers, job arguments, or retries.
- Payer discovery or scheduled discovery.
- HTTP downloads.
- TOC or MRF parsing.
- Parquet input or output.
- Consumer warehouse ingestion or plan attachment.
- Automatic provider enrichment.
- Artifact cleanup, recovery, or retention.
- An HTTP API, UI, metrics server, or recurring scheduler.
- Supporting a payer other than UHC.
