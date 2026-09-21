# Story 39: Immediate MRF parse failure for frozen source input

## Status

Implemented.

## User story

As an operator, I want an MRF whose downloaded bytes cannot be parsed
because of JSON or CMS schema errors to fail on this attempt, run Story 23
occupancy cleanup, and free the resident slot, so poison files do not
retry or pin capacity.

## Context

Story 34 made frozen identity and completed-output validation immediate,
and explicitly left `mrf_parse_execution_failed` retryable: the public
parser had no stable taxonomy, so that code mixed poison files with
OOM/disk. Immediate fail would also run Story 23 Terminal (delete raw,
release the slot) on a maybe-transient error.

`mrfparser` Story 36 now exports:

```text
ErrInvalidInput          // public frozen-input category
ErrInvalidJSON
ErrDuplicateKey
ErrUnsafeStructure
ErrUnsupportedSchema
IsSourceError
```

`Parse` wraps CMS source failures and a non-object JSON root with
`ErrInvalidInput`. Resource `LimitError` stays outside that category.

Today `internal/mrfparse.mapWorkError` still collapses unknown parser
errors to `mrf_parse_execution_failed`. Molina files with
`billing_code_type: null` therefore retry four times and hold a slot.

TOC already has this split: `mrftocparser.ErrInvalidInput` →
`toc_parse_input_invalid` (immediate).

## Dependencies and supersession

Depends on implemented Stories 10, 23, and 34, and on pinned `mrfparser`
Story 36.

Do not rewrite historical Stories 10 or 34. Those remain the record of
what they shipped. This story is the living contract for parse-time
source-input failure. Update current `DESIGN.md`, `README.md`,
`failure_test.go`, and the parser pin.

## Goal

1. Pin `github.com/EnotPoloskun/mrfparser` to a revision that includes
   Story 36. No `replace` directive.
2. Add allowed failure code `mrf_parse_input_invalid`.
3. Map `mrfparser.ErrInvalidInput` and `mrfparser.IsSourceError` to that
   code in `mapWorkError`.
4. Add the code to `isImmediateFail`.
5. Keep `mrf_parse_execution_failed` retryable (OOM, disk, unknown parser
   text, I/O).
6. Reuse Story 23 Terminal. Immediate input-invalid must delete
   unpublished parsed output, parser temp, and raw download, set download
   `blocked`, `ReleaseSlotTx`, and `WakeTx`, exactly as output-invalid
   already does.
7. After deploy, the next still-retryable River attempt that now returns
   this code becomes terminal on that attempt. No extra reconcile pass.

## Non-goals

- Fail-fast of `mrf_parse_execution_failed`.
- A new Terminal hook, occupancy path, or download rematerialize rule.
- Mapping resource/OOM to a new `mrf_parse_resource_failed` code.
- ZIP/gzip/transport parser errors (not `ErrInvalidInput`).
- Unselecting the failed source or substituting another URL.
- A PostgreSQL enum/check migration (`failure_code` is unconstrained
  nonempty text).
- Changing `MaxAttempts` (still 4 for `mrf.parse`).
- Rewriting Story 10/34 requirement text.

## Product decisions

Locked for this story:

- **All public frozen source-input errors are immediate.** Mapper uses
  `errors.Is(err, mrfparser.ErrInvalidInput) || mrfparser.IsSourceError(err)`
  so both real `Parse` wrapping and test fakes that return a sentinel
  classify the same way.
- **Resource and unknown errors stay `mrf_parse_execution_failed` and
  retry.** Do not inspect parser error strings.
- **Story 23 cleanup runs.** Immediate fail is `failAssignedTerminal`:
  persist the code, Terminal, `JobCancel`. Valid completed 1.2.0 output
  still blocks occupancy cleanup (unchanged Story 10/23).
- **Redaction unchanged.** `jobs.Failure` stores only the code. Hostile
  parser text must not appear in the returned error or `failure_code`.
- **No DB migration.**

## Required behavior

`jobs.FailureMRFParseInputInvalid = "mrf_parse_input_invalid"`.

Add it to `allowedFailureCode` and `isImmediateFail` next to the other
`mrf.parse` codes.

`mapWorkError` order:

1. `nil` / cancel / deadline unchanged.
2. Already-classified job failures, including the new code, pass through.
3. `ErrInvalidInput` / `IsSourceError` → `mrf_parse_input_invalid`.
4. `artifact.ErrArtifact` → `domain_invariant`.
5. Everything else → `mrf_parse_execution_failed`.

Do not import `mrfparser/internal`.

## Tests

Unit:

- `mapWorkError(mrfparser.ErrUnsafeStructure)` and a wrapped
  `ErrInvalidInput` → `mrf_parse_input_invalid`, no source path/URL in
  the error text.
- `mapWorkError` of hostile unknown text still →
  `mrf_parse_execution_failed` and still redacts.
- `failure_test.go`: new code is allowed and immediate;
  `mrf_parse_execution_failed` still retries.
- `lifecycle_test.go`: `FailureMRFParseInputInvalid` is terminal on
  attempt 1 (Terminal + `JobCancel`); execution_failed still does not
  cancel on attempt 1.

Integration (same occupancy fixture as
`TestIntegrationImmediateOutputInvalidReleasesSlot`):

- Fake `Parse` returns `mrfparser.ErrUnsafeStructure`.
- Attempt 1, River `cancelled`, `parse_status=failed`,
  `failure_code=mrf_parse_input_invalid`, download `blocked`, raw and
  unpublished parsed absent, slot released, a waiting source can be
  admitted.
- Existing fourth-failure / hostile-text tests still use
  `mrf_parse_execution_failed`, keep the slot until exhaustion, and do
  not become immediate.

Pin assertion in `cmd/mrfpipeline/main_test.go` matches `go.mod`.

## Documentation

`DESIGN.md`:

- Document status Stories 01–39.
- Immediate-failure section: `mrf_parse_input_invalid` is immediate;
  `mrf_parse_execution_failed` stays retryable.
- Story sequence row 39.

`README.md`: poison JSON/schema parse fails immediately as
`mrf_parse_input_invalid` with Story 23 cleanup; execution/OOM/disk still
retries.

## Acceptance

1. A source whose parser return is `ErrInvalidInput` / `IsSourceError`
   fails once, stores `mrf_parse_input_invalid`, runs Terminal cleanup,
   and does not retry.
2. A source whose parser return is unknown or a resource failure still
   retries as `mrf_parse_execution_failed` and still redacts.
3. Pipeline compiles against the pinned public parser only.
4. Historical Stories 10 and 34 are unchanged files.
