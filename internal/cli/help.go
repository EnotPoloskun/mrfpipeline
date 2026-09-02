package cli

const rootHelp = `mrfpipeline orchestrates the MRF discovery, parsing, consumption, and
plan-attachment pipeline.

PostgreSQL stores durable state. River background jobs use a separate
schema applied by migrate. mrfenricher execution is manual and is not
started by the pipeline.

Usage:
  mrfpipeline migrate
  mrfpipeline work --role <control|mrf|consumer>
  mrfpipeline discover --payer uhc --collection-month <YYYY-MM> --limit <count> [--mrf-source-limit <N|all>]
  mrfpipeline reconcile
  mrfpipeline retry --stage <job-kind> --id <domain-id>
  mrfpipeline month status [--payer <payer> --collection-month <YYYY-MM>]
  mrfpipeline month sources set-total --payer <payer> --collection-month <YYYY-MM> --total <N|all>
  mrfpipeline month activate --payer <payer> --collection-month <YYYY-MM>
  mrfpipeline filters build --payer <payer> --collection-month <YYYY-MM>
  mrfpipeline filters status --payer <payer> --collection-month <YYYY-MM>
  mrfpipeline filters measure --payer <payer> --collection-month <YYYY-MM>
  mrfpipeline --help
  mrfpipeline --version

Commands:
  migrate   Apply application and River database migrations
  work      Run background workers
  discover  Enqueue one bounded UHC discovery run
  reconcile Repair building-release gaps and report sealed inconsistencies
  retry     Reopen one exact failed stage with a fresh River series
  month     Inspect and activate monthly serving releases
  filters   Build and inspect release filter catalogs
`

const migrateHelp = `Usage:
  mrfpipeline migrate
  mrfpipeline migrate --help

Apply application and River database migrations. The operation is
explicit and repeatable; work and discover do not migrate automatically.

Required environment:
  MRFPIPELINE_DATABASE_URL  PostgreSQL connection string
`

const workHelp = `Usage:
  mrfpipeline work --role <control|mrf|consumer>
  mrfpipeline work --help

Run one explicit worker role until canceled. The control role owns discovery,
TOC work, reconciliation, and the shared resident-slot scheduler. The mrf
role owns MRF download/parse, and consumer owns consumer writes.

Required environment:
  MRFPIPELINE_DATABASE_URL
  MRFPIPELINE_ARTIFACT_ROOT
  MRFPIPELINE_MRF_RESIDENT_CAPACITY (control only)
  MRFPIPELINE_WAREHOUSE_PATH (control and consumer)
  MRFPIPELINE_PROVIDER_CATALOG_PATH (control and consumer)
  MRFPIPELINE_SERVICES_PATH

The control role must be the only control process. The consumer role is
serialized because the current warehouse writer is serialized. MRF download
has two workers and MRF parse has one worker per process; the database-owned
resident capacity is the shared download-plus-parse disk bound.
`

const discoverHelp = `Usage:
  mrfpipeline discover --payer uhc --collection-month <YYYY-MM> --limit <count> [--mrf-source-limit <N|all>]
  mrfpipeline discover --help

Enqueue one background discovery run. Success reports the durable run and
job identifiers rather than waiting for listing or download.

collection_month selects the caller-supplied building monthly release that
owns discovered TOC captures. It is not inferred from URLs or payer contents.

--limit is required and must be a positive integer. Unlimited discovery is
out of version 1 scope.

--mrf-source-limit is required for a new or unset release. Omit it only when
an existing release already has an initialized target; omission then reuses
that durable target. It accepts a positive integer or all. The target is
cumulative and can later be increased with month sources set-total.

Required flags:
  --payer              Exact payer adapter identifier (only uhc)
  --collection-month   Caller-selected month, exactly YYYY-MM
  --limit              Positive int64 of newly admitted TOCs

Required environment:
  MRFPIPELINE_DATABASE_URL  PostgreSQL connection string
`

const reconcileHelp = `Usage:
  mrfpipeline reconcile
  mrfpipeline reconcile --help

Acquire the singleton control lease and repair only safe gaps in building
releases: missing successor jobs, orphaned current jobs, plan-batch
scheduling, and eligible artifact cleanup. Stop the control worker before
running this command; MRF and consumer roles may remain running. Sealed-
release inconsistencies are reported without mutation; terminal failed stages
use retry.

Required environment:
  MRFPIPELINE_DATABASE_URL
  MRFPIPELINE_ARTIFACT_ROOT
  MRFPIPELINE_WAREHOUSE_PATH
  MRFPIPELINE_PROVIDER_CATALOG_PATH
  MRFPIPELINE_SERVICES_PATH
`

const retryHelp = `Usage:
  mrfpipeline retry --stage <job-kind> --id <domain-id>
  mrfpipeline retry --help

Reopen one exact failed domain stage and insert a replacement River job. The
command does not run the job and may run while worker roles are active. It
coordinates through the release and exact source/output execution locks. The
same stage identity is retained. An attachment retry reuses the
frozen plan batch rather than creating a new batch for its items.

For mrf.parse, automatic retries reuse the raw file and resident slot. A
terminal parse cleanup deletes unpublished parsed output, parser staging, and
raw bytes, releases the slot, and keeps the source selected and failed.
Cancelling mrf.download, mrf.parse, or toc.download from River UI fails that
stage and deletes unpublished download bytes; download retry exhaustion does
the same. Recreate/SIGTERM stays an interruption and keeps bytes for resume.
A parse retry reuses valid raw bytes when present; when bytes are gone it
reopens download and waits for admission before redownloading. New mrf.parse,
mrf.download, and toc.download jobs have four attempts including the first.
HTTP 404 is not retried. Other production stages retain eight.

Retries for active or inactive releases fail with
sealed_release_retry_forbidden.

Required flags:
  --stage   One production job kind
  --id      Positive domain row ID

Required environment:
  MRFPIPELINE_DATABASE_URL
  MRFPIPELINE_ARTIFACT_ROOT (mrf.parse only)

`

const monthHelp = `Usage:
  mrfpipeline month status
  mrfpipeline month status --payer <payer> --collection-month <YYYY-MM>
  mrfpipeline month activate --payer <payer> --collection-month <YYYY-MM>
  mrfpipeline month --help

Inspect monthly serving state or atomically publish ready outputs.
`

const monthStatusHelp = `Usage:
  mrfpipeline month status
  mrfpipeline month status --payer <payer> --collection-month <YYYY-MM>
  mrfpipeline month status --help

Status is read-only and requires only MRFPIPELINE_DATABASE_URL.
`

const monthActivateHelp = `Usage:
  mrfpipeline month activate --payer <payer> --collection-month <YYYY-MM>
  mrfpipeline month activate --help

Activation locks the payer's release rows and validates the consumer
environment. First activation requires terminal discovery/TOC work and freezes
that inventory. Numeric targets, terminal TOC/MRF file failures, and pending
MRF work are allowed. Only consumed, plan-ready outputs are published; repeat
activation to append newly ready outputs while MRF work continues. Consumer or
plan-attachment failures reject the checkpoint. Control lease is not required.
`

const monthSourcesSetTotalHelp = `Usage:
  mrfpipeline month sources set-total --payer <payer> --collection-month <YYYY-MM> --total <N|all>

Increase the cumulative MRF source admission target for a building release.
The target cannot be decreased and all cannot be changed back to a number.
`

const filtersHelp = `Usage:
  mrfpipeline filters build --payer <payer> --collection-month <YYYY-MM>
  mrfpipeline filters status --payer <payer> --collection-month <YYYY-MM>
  mrfpipeline filters measure --payer <payer> --collection-month <YYYY-MM>
  mrfpipeline filters --help

Build or inspect the exact filter catalog for a release publication
generation. Build is synchronous and does not activate the release. Measurement
is an opt-in fresh-build operation for private disposable environments.
`

const filtersBuildHelp = `Usage:
  mrfpipeline filters build --payer <payer> --collection-month <YYYY-MM>
  mrfpipeline filters build --help

Build one complete ready PostgreSQL filter catalog for the exact current or
prospective publication generation. The command does not activate a release.
It requires the database, artifact root, warehouse, provider catalog, and
services paths.
`

const filtersStatusHelp = `Usage:
  mrfpipeline filters status --payer <payer> --collection-month <YYYY-MM>
  mrfpipeline filters status --help

Read current publication and filter-catalog state from PostgreSQL only.
This command does not inspect warehouse files or run DuckDB.
`

const filtersMeasureHelp = `Usage:
  mrfpipeline filters measure --payer <payer> --collection-month <YYYY-MM>
  mrfpipeline filters measure --help

Measure one fresh filter-catalog build in a disposable private environment.
The command never activates a release and refuses an existing target catalog.
`

func helpFor(command string) string {
	switch command {
	case cmdMigrate:
		return migrateHelp
	case cmdWork:
		return workHelp
	case cmdDiscover:
		return discoverHelp
	case cmdReconcile:
		return reconcileHelp
	case cmdRetry:
		return retryHelp
	case cmdMonth:
		return monthHelp
	case cmdFilters:
		return filtersHelp
	default:
		return rootHelp
	}
}
