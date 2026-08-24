# Story 19: Release-aware reconciliation, acceptance, and documentation

## Status

Implemented.

## User story

As an operator and maintainer, I want feed-free monthly releases included in
reconciliation, operational acceptance, and documentation so that build,
activation, cross-payer serving, rollback, and recovery form one trustworthy
pipeline contract before a UI is added.

## Goal

Complete Stories 14–18 by:

1. making reconciliation and retry release-aware without mutating active
   control state automatically;
2. proving two-month build/activation/rollback behavior;
3. proving the complete multi-payer active-output relation used by unfiltered
   queries;
4. verifying the exact consumer `2.0.0` feed-free warehouse boundary;
5. documenting operator status, cutover, query, backup, and rebuild procedures;
   and
6. removing obsolete feed, sticky-TOC, and consumer-current guidance.

This story adds no UI, query server, recurring scheduler, provider enrichment,
warehouse rewrite, or automatic activation.

## Dependencies and supersession

This story depends on Stories 14–18 and supersedes Story 13's final acceptance
and documentation where it describes:

- source-based feeds;
- sticky TOC month across URL reuse;
- consumer `1.5.0`;
- `current_*` serving;
- source/feed/month status SQL; or
- a pipeline with no monthly release state.

Story 13's exclusive lease, exact retry, safe cleanup, capacity monitoring, and
live-acceptance safety guards remain normative. Its redaction rules remain
except for the explicitly authorized structured control-plane values below.

## Release-aware reconciliation

Safe reconciliation continues running under the exclusive worker lease. It may
repair admitted domain work but never creates, activates, deactivates, seals,
reopens, or deletes a monthly release.

For `building` releases, retain all existing safe repairs:

- missing predecessor/successor transitions;
- missing/orphaned current River jobs;
- orphaned running claims;
- eligible blocked snapshot scheduling;
- unassigned plan batching;
- crash-window completed publication acknowledgement; and
- safe artifact cleanup.

For `active` or `inactive` releases:

- do not admit new discovery or create work not already represented by durable
  domain rows;
- do not create plans, batches, batch items, or `consumer.attach_plans` jobs;
- do not acknowledge an out-of-band plan part as a supported sealed-release
  change;
- existing nonterminal rows indicate an invariant violation because activation
  requires readiness;
- do not silently repair that violation into a changed sealed release;
- report a safe `sealed_release_inconsistent` failure and leave control state
  unchanged; and
- never auto-fall back to another month.

An operator investigates external corruption or unsupported mutation. The
pipeline does not deactivate an unhealthy release automatically.

A sealed-release domain inconsistency is recorded and does not abort safe work
for unrelated building releases. Reconciliation:

1. records each affected durable domain row at most once per pass in the
   aggregate `sealed_release_inconsistency_count` report field, with sanitized
   structured logs containing only the failure code, stage context, and
   numeric domain ID;
2. continues every safe building-release repair and reconciliation page;
3. completes remaining independent cleanup; and
4. returns a successful nonzero summary when safe continuation is possible.
   Infrastructure failures still return an error.

Infrastructure failures that prevent safe continuation, such as lost database
connectivity, may still abort immediately.

Reconciliation pages and locks use feed-free snapshot/source relationships.
Every query that groups TOCs includes collection month. No reconciliation SQL
references `mrf_feeds`, `mrf_feed_id`, or a feed-formatted identifier.

## Retry policy

Explicit `retry` remains available for failed work in a `building` release and
preserves exact numeric identity/artifacts.

A stage belonging to `active` or `inactive` release is not retryable in place;
the command fails safely with `sealed_release_retry_forbidden`. Activation
could not have succeeded with a failed stage, so such state indicates later
external/invariant damage. This avoids modifying an operator-approved monthly
release.

One monthly source capture may be shared by several payer snapshots for that
same month. If the source is failed, normal retry is allowed only when every
dependent release is still building. If any dependent release is sealed, fail
safely and require operator investigation rather than changing a shared
prerequisite beneath immutable published history. Sources are not shared across
collection months.

Attachment retry continues reusing its frozen batch ID and JSON for building
releases. It never creates a replacement batch for the same items.

## Cleanup and retention

Existing cleanup remains artifact-owner and manifest gated. Release history
adds no warehouse deletion:

- never delete consumer snapshots for inactive months;
- never delete plan-association parts;
- never delete or rewrite `warehouse.json` or provider catalog;
- never delete shared successful MRF parsed output;
- safe TOC/download/plan-JSON cleanup remains allowed after its existing
  terminal-success gates; and
- rollback relies on warehouse and PostgreSQL state, not retained TOC download
  artifacts.

Backups must include PostgreSQL, the complete append-only warehouse, shared
parsed MRF output, and the pinned provider catalog identity. Restoring only the
warehouse without its active release mapping is insufficient for serving.

## Query handoff contract

The pipeline does not execute analytical DuckDB queries in production. It
exposes the complete active `(payer_id, collection_month, output_id)` relation
through PostgreSQL and `month status`.

A future query service must join active releases to their immutable snapshots
in one PostgreSQL statement at startup or control-plane refresh. It validates
the complete relation against the consumer warehouse as required by consumer
Story 23 and atomically publishes only verified serving state. For example:

```text
uhc   2026-09  mrf-72
uhc   2026-09  mrf-73
aetna 2026-08  mrf-41
```

Semantics:

- each request captures the already verified relation without rescanning
  warehouse metadata;
- payer-filtered request: use that payer's active output rows;
- request without payer filter: use every active output row;
- payer with no active row: contribute no data;
- historical request: deliberately bypass active state and label/group months;
  and
- one request never rereads serving state between subqueries.

Relationship and plan views join the exact selected output-ID set. Values are
bound/registered, never interpolated. The query service must not recreate
membership by selecting every consumer ingestion for the payer/month.

Query planning, partition pruning, output-glob construction, and performance
acceptance belong to consumer Story 23/24 or the future query-service project,
not `mrfpipeline`.

## Permanent synthetic acceptance

Use focused hermetic PostgreSQL/local-consumer tests rather than one scenario
that duplicates consumer and future query-service acceptance:

### Release lifecycle

- Build and activate August for UHC.
- Build September while August remains active.
- Attempt activation with one deliberate blocker and require no pointer change.
- Resolve it through supported processing/retry, activate September atomically,
  then reactivate August as rollback.
- Require warehouse bytes/counts unchanged by activation and rollback.
- Require each sealed release's plan-association part inventory to remain
  unchanged through activation, reconciliation, inactivity, and rollback;
  reactivating the same release restores its same plan-filtered result set.

### Monthly capture behavior

- Reuse one exact TOC URL across August/September and require two TOC captures.
- Reuse one exact MRF URL across August/September and require two source
  captures, downloads, parses, snapshots, and publications.
- Use overlapping same-month TOCs and require one same-month source parse with
  complete additive plan batches.

### Multi-payer control state

- Represent a second valid payer through controlled domain fixtures.
- Keep the two payers on different active months.
- Require the deterministic complete active-output relation and no unrelated
  same-month consumer output lacking a pipeline snapshot.

### Reconciliation and idempotency

- Introduce a sealed inconsistency and unrelated repairable building work.
- Require the sealed release unchanged, the building repair completed, and a
  nonzero sanitized summary.
- Restart/reconcile and require no duplicate domain rows, jobs, publications,
  plans, or releases.

### Consumer selection smoke test

Run one small consumer integration query using the exact active output-ID set.
Require selected active facts/plans and exclusion of an inactive output plus an
unrelated same-payer/month output. Consumer view semantics, provider/network
filtering, fact multiplication, query plans, and partition pruning remain owned
by consumer Story 23/24 or the future query-service project.

Use hand-computed small fixture expectations. Ordinary pipeline acceptance does
not profile DuckDB or impose a query-performance contract.

## Live UHC acceptance

Update the opt-in `TestRealUHCAcceptance` contract to the new database and
consumer versions. It continues using dedicated disposable roots/database and
small explicit limits.

At minimum it must:

- create one building release for the supplied month;
- complete bounded discovery through plan attachment;
- require readiness;
- activate that month;
- verify active status survives worker restart and converged reconciliation;
- verify a repeated activation is idempotent; and
- emit only sanitized counts/timings.

A second real month is optional because it may require waiting for payer
publication and large duplicate work. The focused permanent synthetic tests
remain the mandatory two-month/rollback oracle.

Ordinary `go test ./...` never contacts UHC. All existing disposable database,
dedicated-root, symlink, timeout, maximum-limit, and report-path guards remain.

## Status and operator diagnostics

Update status SQL and README examples to include:

- release counts by status;
- complete active payer/month/output relation;
- building release readiness blockers;
- feed-free snapshot counts by payer/month/consume status; and
- monthly TOC counts using the new identity.

Default diagnostics may print validated payer/month, derived output IDs,
numeric domain/job IDs, aggregate counts, statuses, and blocker codes as
structured operational context. They never print URLs, paths, filenames, plans,
provider/service values, credentials, response content, or raw River errors.
Successful no-flag `month status` prints validated payer/month/output control
values as its explicit query-handoff purpose; the targeted readiness form need
not expose output IDs.

Retain the separately labeled authorized numeric-ID URL-debug query.

## Documentation updates

Update:

- repository `README.md`;
- `requirements/DESIGN.md`;
- all current CLI help/examples;
- status SQL and operational runbook sections;
- live acceptance instructions; and
- cross-repository consumer links/version notes.

Current documentation must explain:

- feed IDs are gone;
- TOC capture identity is payer + month + exact URL;
- MRF source capture identity is exact URL + collection month;
- consumer is exact feed-free `2.0.0`;
- one month builds while another remains active;
- activation seals and atomically switches one payer;
- rollback reactivates sealed immutable history;
- active and inactive releases have frozen plan-association sets; new plans or
  attachment work require a newly built release;
- multiple payers may have different active months;
- a query without payer filter applies the complete active output relation and
  does not infer membership from every warehouse output in the active month;
- no automatic latest-month/current view is supported;
- active release corruption is not silently repaired; and
- populated old database/warehouse state is rebuild-only.

The root README and requirements design describe Stories 14–19 as the current
feed-free target. Story 01–13 populated state and consumer `1.5.0` warehouses
remain rebuild-only.

## Required checks

Run:

```text
go test ./...
go test -race ./...
go vet ./...
```

PostgreSQL integration must cover migrations `0002`–`0004`, concurrent
activation/discovery gates, reconciliation, and retry. Consumer integration
must use the exact pinned parser/consumer sibling revisions. One small consumer
query proves exact output-ID selection and exclusion; pipeline tests do not
inspect DuckDB plans or profiles.

Documentation link checks include Stories 14–19 and all cross-repository links.
Review Mermaid diagrams for obsolete feed/current nodes.

Tests assert resulting schemas, keys, constraints, scheduling, recognition, and
release behavior. They do not scan source text for words such as `sha`, `hash`,
or `digest`.

## Acceptance criteria

- Reconciliation preserves sealed releases and never changes active state.
- Reconciliation and retry never add plan associations to active or inactive
  releases.
- Two-month activation and rollback are exact, atomic, restart-safe, and do not
  rewrite warehouse data.
- The complete active-output relation supports different months per payer,
  scopes payerless queries correctly, and excludes unrelated same-month
  warehouse outputs.
- Feed/current/sticky-month terminology is removed from the active target
  contract.
- Exact consumer `2.0.0`, plan readiness, structured operational context,
  redaction, cleanup, and retry invariants remain proven.
- The project is operationally ready for a future read-only UI/query service.

## Out of scope

- UI or query-service implementation.
- Global synchronized multi-payer release generations.
- Automated recurring discovery, activation, or rollback.
- Adding/correcting data in a sealed month.
- Warehouse compaction, deletion, migration, or plan correction.
- Historical provider-catalog reconstruction.
- Performance SLA or materialized aggregate implementation.
