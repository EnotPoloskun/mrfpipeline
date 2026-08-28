# Story 27: Incremental release publication

## Status

Implemented.

## User story

As a pipeline operator, I want to publish the completed subset of a monthly
release while the remaining known MRF files continue processing, then publish
newly completed outputs later, so development and production use the same
activated-output contract without requiring every upstream file to succeed.

## Goal

Change `month activate` from one all-or-nothing snapshot derivation into an
explicit additive publication checkpoint:

1. First activation freezes discovery and TOC-derived source/plan inventory.
2. It atomically switches the payer's active month and persists only the
   currently consumed, plan-ready, warehouse-valid outputs.
3. Already-known MRF download, parse, consume, and plan-attachment work remains
   executable while the release is active.
4. Repeating activation appends newly ready outputs to the same release.
5. Existing published membership is never removed or inferred from all
   snapshots in the month.
6. Terminal TOC/MRF file failures are reported and omitted rather than blocking
   publication.

The behavior is identical in development, staging, and production. There is no
force flag, environment switch, or direct-SQL publication path.

## Publication boundary

One first activation closes the inventory-producing stages for that month:

- no new discovery run;
- no new or retried `toc.download`, `toc.parse`, or `toc.import` work;
- no source-target increase after activation; and
- no later TOC-derived plan additions.

First activation therefore requires:

- a configured numeric or `all` MRF target;
- the selected-source membership to match that target against the known source
  inventory;
- at least one succeeded discovery run and no failed/nonterminal discovery run;
- at least one TOC;
- every TOC stage to be terminal; and
- at least one publishable output.

A terminal TOC stage may be failed. Pending or running discovery/TOC work still
rejects first activation because it could change source or plan inventory.

## Publishable output

A snapshot is publishable only when all of the following hold under the locked
release transaction:

- its MRF source belongs to `monthly_release_mrf_sources` for the payer/month;
- `consume_status = 'succeeded'`;
- at least one succeeded attachment batch has `added_plan_count > 0`;
- no attachment batch is pending, running, or failed;
- every `mrf_plans` row is assigned through a succeeded attachment batch; and
- the consumer base publication and exact positive plan-part inventory validate
  against the configured warehouse.

A planless snapshot, an incomplete output, or an output with a consumer/plan
publication failure is never inserted into release membership.

## Failure policy

Terminal file failures do not block the checkpoint:

- `toc.download`;
- `toc.parse`;
- `toc.import`;
- `mrf.download`; and
- `mrf.parse`.

They remain failed domain records and are counted in the activation result.
Selected MRF failures may be retried while the release is active because MRF
materialization remains open. TOC retries are closed after first activation
because the TOC inventory is frozen.

Consumer-ingest and plan-attachment failures remain publication blockers. They
represent defects in producing the warehouse/query contract, not ordinary
unusable upstream files. Pending/running consumer work is simply omitted until
a later activation.

## Durable membership

Migration `0008_incremental_release_publication.sql` adds:

- `monthly_releases.publication_generation`; and
- `monthly_release_outputs` keyed by
  `(payer_id, collection_month, mrf_snapshot_id)` with first generation and
  publication timestamp.

The migration backfills existing active/inactive releases with generation `1`
and their previously valid complete snapshot membership.

`ListActiveOutputs`, no-selector `month status`, reconciliation status SQL, and
active-publication audits read `monthly_release_outputs`. They must never expose
every `mrf_snapshots` row merely because its monthly release is active.

Each successful checkpoint with new outputs increments
`publication_generation`. An idempotent checkpoint adds zero rows, retains the
generation, and does not change the activation timestamp. The activation JSON
reports:

- `publication_generation`;
- `output_count`;
- `added_output_count`;
- `skipped_output_count`;
- `terminal_file_failure_count`; and
- `previous_collection_month`.

## Transaction and concurrency

Activation locks all monthly release rows for the payer before selecting or
validating candidates. It recomputes the exact publishable set under that lock,
validates the same set in the warehouse, and inserts only those snapshot IDs.
It does not trust a pre-lock candidate list.

Stage claims use the same release-row ordering:

- discovery and TOC stages require `building`;
- MRF download/parse, consumer ingest, and plan attachment accept `building` or
  `active`; and
- every stage rejects `inactive`.

This prevents first activation from racing a TOC claim while allowing known MRF
work to finish after publication begins.

## Repeated activation and rollback

For an active release, repeating `month activate` validates all previously
published outputs plus the currently publishable unpublished outputs, inserts
only new membership rows, and leaves earlier rows unchanged.

Switching to another month marks the prior active release inactive. An inactive
release is frozen. Reactivation validates and restores its exact persisted
membership; it does not derive a new set from the month's snapshots.

Plan-set mutation checks use each output's `published_at`, not the release's
first activation timestamp. Plan rows and parts for unpublished outputs may be
created while MRF work continues; plan mutation after that output was published
is a sealed inconsistency.

## Operator behavior

`database_ready` remains the strict full-completion signal. It may remain false
because of `mrf_source_target_partial`, pending MRF work, or terminal file
failures even when an incremental checkpoint is valid.

The operator runs the same command for first and later checkpoints:

```text
mrfpipeline month activate --payer <payer> --collection-month <YYYY-MM>
```

Before the first call, the operator must finish the intended discovery/TOC
inventory because that call closes it. Later calls publish newly ready MRF
outputs without stopping MRF or consumer workers.

## Acceptance criteria

- Numeric targets can publish a ready subset while `partial:true` remains
  visible.
- Pending MRF work continues after first activation.
- First activation rejects pending/running discovery or TOC work.
- A terminal TOC/MRF file failure is counted and omitted.
- A planless snapshot is omitted.
- Consumer/attachment failure rejects the checkpoint without membership or
  active-pointer mutation.
- Repeated activation appends only newly ready output IDs and increments the
  generation exactly once.
- Idempotent activation changes neither generation nor activation timestamp.
- Active-output queries return only persisted membership.
- Inactive rollback restores the same membership.
- Warehouse preflight failure leaves the previous active release unchanged.
- Existing active/inactive releases are backfilled safely by migration `0008`.

## Non-goals

- Environment-specific development behavior.
- Publishing pending or unvalidated outputs.
- Continuing discovery/TOC import after first activation.
- Removing an already published output.
- Automatically retrying terminal file failures.
- Ignoring consumer or plan-publication failure.
- Rewriting Parquet, materializing DuckDB state, or adding a query server.
- Deploying or migrating the current live Compose stack as part of this story.
