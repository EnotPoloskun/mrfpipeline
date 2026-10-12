# Story 42: Exec the MRF parser CLI from `mrf.parse`

## Status

Implemented.

## User story

As a pipeline operator, I want `mrf.parse` to run the Rust `mrfparser`
binary instead of calling the Go `mrfparser.Parse` library in-process, so
the parser can be swapped without rewriting River, PostgreSQL, or
`mrfconsumer`.

## Context

Story 10 implements `mrf.parse` by linking `github.com/EnotPoloskun/mrfparser`
and calling:

```go
cfg := mrfparser.DefaultConfig()
cfg.Input = input
cfg.Output = output
cfg.Services = configuredServicesPath
cfg.TempDir = sourceOwnedParserTemp
cfg.OnProgress = throttledParseProgress
err := mrfparser.Parse(ctx, cfg)
```

Story 10 forbids executing the parser CLI, scraping stderr, or importing
parser `internal` packages. That remains the record of what it shipped.

The Rust `mrfparser` sibling now publishes the same local `1.3.0` contract
(seven datasets, `manifest.json`, `processing_stats.json`, default
`negotiated` prices, service-selector CSV). Logical rows match the Go
oracle. Parquet bytes, empty-list versus SQL `NULL` on optional list
columns, `stored_bytes` (+4 encoding peek), and warning-example order may
differ; Story 10 completion checks and `mrfconsumer` do not byte-diff
Parquet or require example order.

The Compose image contains only `mrfpipeline` and DuckDB. A library pin
cannot load the Rust parser. The production worker must exec a Linux
binary.

## Dependencies and supersession

Depends on implemented pipeline Stories 10, 23, and 39, and on a pinned
revision of the Rust `mrfparser` sibling that implements its Stories 01–09
(local CLI, exact `1.3.0` output, no S3, no `--memory-limit`).

Do not rewrite historical Stories 10 or 39. Those remain the record of
what they shipped. This story is the living contract for how production
`mrf.parse` invokes the parser. Update current `DESIGN.md`, `README.md`,
`DOCKER.md`, `USAGE.md`, and the `mrf` image.

## Goal

1. Production `mrf.parse` execs `mrfparser parse` with the same local
   paths Story 10 already computes. It does not call
   `mrfparser.Parse`.
2. Map child exit codes onto the existing failure codes. Do not inspect
   parser stderr or stdout text.
3. On cancellation, send SIGTERM to the parser process group, wait up to
   `jobs.GracefulStop` (30s), then SIGKILL. Reap the child before the
   worker returns.
4. Keep Story 10 `validateCompletedOutput` and Story 23/39 Terminal
   cleanup unchanged.
5. Put a Linux `mrfparser` on `PATH` in the shared pipeline image so the
   `mrf` role can exec it.

## Non-goals

- Rewriting `mrfpipeline`, `mrfconsumer`, TOC, discovery, ingest, or
  plan attachment in Rust.
- Changing layout, manifest, or `processing_stats` validation.
- Byte-identical Parquet, warning-example order, or `stored_bytes`
  equality with the Go writer.
- A progress callback, scraping `mrfparser:` logs, or storing parser
  percent in PostgreSQL.
- `--memory-limit`, `--target-file-size`, `--row-group-rows`,
  `--negotiated-types`, `--plans`, or `--overwrite`.
- Changing `MaxAttempts` (still 4), queue concurrency (still 1),
  `JobTimeout`, or Rescue.
- A new failure code.
- Unpinning `github.com/EnotPoloskun/mrfparser` from `go.mod` (tests and
  fixture generators may still call the Go library).
- Importing parser `internal` packages.
- Windows process groups.
- Embedding the git revision in `mrfparser --version`.

## Product decisions

Locked for this story:

- **Exec is the production parse.** `mrfparse.DefaultParse` starts the
  CLI. Injected `ParseFunc` fakes remain for tests that do not run a
  binary.
- **One child, argv only.** Start it with `exec.Command(path, args...)`.
  Never run it through a shell or build a command string; paths go in as
  separate arguments.
- **Same flags as today's config.** Required:
  `--input`, `--output`, `--services`, `--temp-dir`. `--temp-dir` is the
  source-owned directory from `PrepareMRFParserTemp`. The binary still
  creates `mrfparser-<pid>-…` under that parent. Do not pass
  `--temp-disk-limit` unless the pinned CLI default changes; keep the
  parser 10 GiB default. Do not pass `--negotiated-types` (pipeline
  default remains `negotiated` only).
- **Exit codes, never parser text.** `jobs.Failure` still stores only the
  code. Hostile CLI diagnostics must not appear in the returned error or
  `failure_code`.

  | Child | Pipeline |
  | ---: | --- |
  | `0` | Validate completed output as today. |
  | `4` | `mrf_parse_input_invalid` (JSON / unsafe structure / unsupported schema, including a compressed stream that fails partway through). Immediate, Story 39. |
  | `7` | `ctx.Err()` when the job context is done; otherwise `mrf_parse_execution_failed`. |
  | `1`, `2`, `3`, `5`, `6`, signal, missing wait status | `mrf_parse_execution_failed` (retryable). |

  Exit `3` covers failures before the JSON stream starts: the input or
  selector cannot be opened or read, a bad gzip header, or an unsupported
  ZIP container. It stays retryable, matching Story 39's
  non-`ErrInvalidInput` class.

  A gzip stream or ZIP member that is truncated or corrupt after decoding
  starts exits `4`, not `3`. The parser reports it as invalid JSON at the
  JSON path where the stream failed. This is not a regression: the
  in-process Go library reports the same truncated-gzip failure as
  `ErrInvalidJSON`, so today's `mapWorkError` already returns
  `mrf_parse_input_invalid`. A retry would read the same bytes.

  Exit `2` must not happen with the flags this worker passes; it is still
  redacted and retryable rather than a new invariant code.

  If the job context is done after the child is reaped, return
  `ctx.Err()` even when the child also exited `4`. Cancellation wins.
- **SIGTERM then SIGKILL.** Do not use `exec.CommandContext` as the only
  cancel path (that is SIGKILL). Start the child in its own process
  group (`Setpgid`). On `ctx` cancel or runtime stop: SIGTERM to the
  group (`Kill(-pid)`), wait `GracefulStop`, then SIGKILL the group, then
  `Wait`. Do not leave an orphan parser holding the resident slot. The
  SIGTERM-to-SIGKILL wait is an unexported package variable that tests
  set to a short duration; production uses `jobs.GracefulStop`. It is not
  an environment knob.
- **Child dies with the pipeline on Linux.** `Setpgid` moves the child
  out of the pipeline's process group, so a SIGKILLed pipeline would
  leave it running. On Linux, also set `Pdeathsig: SIGKILL`, behind a
  `linux` build tag. Linux sends that signal when the *thread* that
  started the child exits. The goroutine that holds `parseMu` calls
  `runtime.LockOSThread`, then `Start`, then `Wait`, and unlocks the
  thread only after `Wait` returns. `Start` and `Wait` must not move to
  another goroutine; the cancel path may signal the group from any
  goroutine. On other hosts a hard-killed pipeline can leave the parser
  running; Story 23 reconcile still removes its files.
- **Container stop time.** Docker's default stop timeout is 10 s, shorter
  than `GracefulStop`. Set `stop_grace_period: 45s` on the `mrf` service
  so SIGTERM, the 30 s wait, and SIGKILL finish before Docker kills the
  container. The Rust parser normally exits within a second of SIGTERM;
  30 s is a ceiling.
- **Serialize the child, not GOMEMLIMIT.** Keep one in-flight parser
  child per process (existing mutex or equivalent around start/wait).
  Queue concurrency 1 is still not that contract: River 0.39 rescue does
  not terminate the existing worker goroutine or its child. The Go
  process-wide memory limit no longer applies.
- **No `OnProgress`.** Do not fail parse because progress is absent.
  Child stderr is inherited by the container so `mrfparser: progress …`
  may appear in Docker logs. Do not parse those lines. The worker may
  emit one existing `progress` log when parse starts (`phase=mrf_parse`,
  no percent).

  Inherited stderr also carries the parser's one-line failure
  diagnostic. It can contain a local path and a JSON pointer built from
  source object keys. That is accepted for container logs only. Never
  copy child stderr or stdout into the returned error, `failure_code`,
  PostgreSQL, or a structured pipeline log field.
- **Selector and source URIs unchanged.** Pass the same generated
  absolute input, output, and services paths Story 10 already uses. Do
  not change `validateCompletedOutput`. The CLI must write
  `source.kind=local`, `selection.mode=service_csv`, and canonical local
  URIs that already pass that check.
- **Binary location.** Optional `MRFPIPELINE_MRFPARSER_PATH` is an
  absolute local path to the executable. When unset, look up `mrfparser`
  on `PATH`. When `ParseMRF` is nil, the `mrf` role fails startup if the
  resolved path is missing or not executable. When a test injects
  `ParseMRF`, startup does not look for the binary, so `go test` never
  needs Cargo. Control and consumer roles do not require it. The
  resolved path is operator-trusted. `mrfparser --version` prints the
  crate version (`0.1.0` today), not the revision, so it is a smoke test
  rather than a pin check. The pin is the Rust revision in the
  Dockerfile.
- **Image ships the binary.** The shared `mrfpipeline:local` image
  installs `/usr/local/bin/mrfparser` next to `mrfpipeline` and `duckdb`.
  Build it in a Docker stage from the private Rust sibling at a revision
  pinned in the Dockerfile, using the existing SSH agent. Do not copy
  parser source or a compiler into the final image. Host `go test` and
  `go build` must not require Cargo.

  Use a Rust `bookworm` builder so the binary links the same glibc as
  the `debian:bookworm-slim` runtime. Pin it to an exact version and
  digest (`rust:<version>-bookworm@sha256:…`), not a floating `1` tag.
  The version must be at least 1.88: the locked `time` 0.3.55 crate
  requires it, so `rust:1.80-bookworm` does not build. Bump the pin
  together with the parser revision. Build with
  `cargo build --release --locked` against the committed `Cargo.lock`.
  Compiling arrow, parquet, and zstd under QEMU emulation is very slow;
  cross-compile from `--platform=$BUILDPLATFORM` (zstd needs a C cross
  compiler for the target) or build each architecture on a native
  runner.

## Required behavior

`internal/mrfparse`:

- `DefaultParse` execs the CLI with the config fields listed above and
  maps the wait result through a new mapper that does not call
  `errors.Is(err, mrfparser.ErrInvalidInput)` for production children.
- `callParse` still holds the process mutex around the full child
  lifetime, including injected fakes.
- `Worker.parse` still prepares temp, resets incomplete output, validates
  completed output, deletes the download, and removes parser temp.
- Do not change claim, snapshot success, Terminal, or selector
  immutability.

`internal/work`:

- `mrf` role start resolves the parser executable before River starts,
  only when `ParseMRF` is nil.
- Keep `ParseMRF` injection for tests.
- Tests that reach the production default today must inject the Go
  library instead, or they would start needing the binary:
  `TestIntegrationRuntimeDownloadsAndParses` (sets
  `ParseMRF: mrfparser.Parse`) and the two direct `DefaultParse` calls in
  `internal/mrfparse/mrfparse_test.go` (call `mrfparser.Parse`).

Dockerfile:

- Multi-arch `linux/amd64` and `linux/arm64` `mrfparser` matching the
  existing DuckDB arch switch.
- Pin the Rust sibling revision next to the Go module pins as
  documentation in README.

## Tests

Unit:

- Mapper: exit `4` → `mrf_parse_input_invalid`, no path/URL in the error.
- Mapper: exit `1` / `3` / `5` / `6` and unknown wait →
  `mrf_parse_execution_failed` and still redacts.
- Mapper: exit `7` with cancelled context → `context.Canceled`; with an
  expired deadline → `context.DeadlineExceeded`.
- Mapper: exit `7` with a live context → `mrf_parse_execution_failed`.
- Mapper: exit `4` with cancelled context → `context.Canceled`.
- Linux: the built command sets `Setpgid` and `Pdeathsig: SIGKILL`.
- Production argument list contains `--input`, `--output`, `--services`,
  `--temp-dir` and does not contain `--memory-limit`,
  `--negotiated-types`, `--target-file-size`, or `--row-group-rows`.
- `jobs.ParserMutexRequired` remains true; comment records child
  lifetime, not GOMEMLIMIT.

Exec fakes use the Go test binary as the child: the test runs
`os.Args[0]` with an environment switch that selects the fake behavior
(the `os/exec` helper-process pattern). No shell scripts and no Cargo:

- Exit `0` after writing a minimal valid `1.3.0` layout → worker
  validates and succeeds.
- Exit `4` → attempt 1, River `cancelled`,
  `failure_code=mrf_parse_input_invalid`, Story 23 occupancy cleanup
  (same fixture shape as Story 39).
- Trap SIGTERM and exit `7` when the job context is cancelled →
  `context.Canceled`, not `mrf_parse_input_invalid`.
- Ignore SIGTERM until SIGKILL → with the test's short kill wait, the
  worker returns after that wait without leaving the fake running.
- Two overlapping `DefaultParse` calls in one process do not run two
  children at once.

Startup:

- `mrf` role with nil `ParseMRF` and a missing parser path fails before
  River starts.
- `mrf` role with an injected `ParseMRF` starts without the parser
  binary.
- `control` role starts without the parser binary.

Packaging:

- Dockerfile copies `/usr/local/bin/mrfparser`.
- Pin assertion documents the Rust revision the image builds.
- The Rust builder image is pinned by digest, not a floating tag.
- `docker-compose.yml` sets `stop_grace_period` on `mrf` to at least
  `GracefulStop` plus 15 s.

Existing `ParseFunc` tests stay on the Go fake/library seam. Do not
convert every integration test to exec.

Historical Stories 10 and 39 are unchanged files.

## Documentation

`DESIGN.md`:

- Production MRF parse execs the Rust CLI; it does not call
  `mrfparser.Parse` in-process.
- Process mutex serializes the child because River rescue does not kill
  it. There is no process-wide parser memory limit.
- Story sequence row 42.
- Document status includes Story 42 once implemented.

`README.md` / `DOCKER.md` / `USAGE.md`:

- Image contains `mrfparser`.
- Cutover: rebuild the image, stop old `mrf` workers, start new ones
  with the same replica count. Do not mix an old in-process parser with
  a new consumer on the same sources; completed `1.3.0` output remains
  reusable.
- Go module pin remains for tests/fixtures; production parse does not
  use it.
- Parser RSS is no longer a 1 GiB Go target. Replica sizing is
  operational, not a new environment knob.
- The `mrf` service's `stop_grace_period` and why it exceeds
  `GracefulStop`.
- `mrfparser --version` is a smoke test; the Dockerfile revision is the
  pin.

## Acceptance

1. A successful CLI child produces output that passes today's
   `validateCompletedOutput` and can be ingested by the pinned
   `mrfconsumer`.
2. Exit `4` is immediate `mrf_parse_input_invalid` with Story 23
   cleanup. Exit `5` still retries as `mrf_parse_execution_failed`.
3. Cancelling `mrf.parse` SIGTERMs the parser group and does not leave a
   child after the worker returns. `docker compose stop mrf` during a
   parse ends with the child reaped and exit `7`, not a container kill.
4. The published Linux image can run `mrfparser --version` and
   `mrfpipeline` without a host Go/Cargo toolchain.
5. With the image's binary, a truncated `.json.gz` fails on attempt 1 as
   `mrf_parse_input_invalid`, the same as the in-process Go parser today.
6. Historical Stories 10 and 39 are unchanged files.

## Cutover note

Empty list versus SQL `NULL` on `service_codes`,
`billing_code_modifiers`, and `npis` is valid for those optional list
columns. Prove one selector-CSV ingest through `mrfconsumer` before
switching production `mrf` replicas. Do not treat Parquet file size as
the cutover check.
