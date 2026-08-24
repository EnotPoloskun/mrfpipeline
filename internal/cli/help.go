package cli

const rootHelp = `mrfpipeline orchestrates the MRF discovery, parsing, consumption, and
plan-attachment pipeline.

PostgreSQL stores durable state. River background jobs use a separate
schema applied by migrate. mrfenricher execution is manual and is not
started by the pipeline.

Usage:
  mrfpipeline migrate
  mrfpipeline work
  mrfpipeline discover --payer uhc --collection-month <YYYY-MM> --limit <count>
  mrfpipeline reconcile
  mrfpipeline retry --stage <job-kind> --id <domain-id>
  mrfpipeline month status [--payer <payer> --collection-month <YYYY-MM>]
  mrfpipeline month activate --payer <payer> --collection-month <YYYY-MM>
  mrfpipeline --help
  mrfpipeline --version

Commands:
  migrate    Apply application and River database migrations
  work       Run background workers
  discover   Enqueue one bounded UHC discovery run
  reconcile  Repair building-release gaps and report sealed inconsistencies
  retry      Reopen one exact failed stage with a fresh River series
  month      Inspect and activate monthly serving releases
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
  mrfpipeline work
  mrfpipeline work --help

Run background workers. This command validates the complete worker
configuration, acquires the exclusive worker lease, runs the same safe
reconciliation as reconcile, then starts the discovery, TOC download, TOC
parse, TOC import, MRF download, MRF parse, and consumer ingest and attach
queues, and runs until canceled.

Required environment:
  MRFPIPELINE_DATABASE_URL
  MRFPIPELINE_ARTIFACT_ROOT
  MRFPIPELINE_WAREHOUSE_PATH
  MRFPIPELINE_PROVIDER_CATALOG_PATH
  MRFPIPELINE_SERVICES_PATH
`

const discoverHelp = `Usage:
  mrfpipeline discover --payer uhc --collection-month <YYYY-MM> --limit <count>
  mrfpipeline discover --help

Enqueue one background discovery run. Success reports the durable run and
job identifiers rather than waiting for listing or download.

collection_month selects the caller-supplied building monthly release that
owns discovered TOC captures. It is not inferred from URLs or payer contents.

--limit is required and must be a positive integer. Unlimited discovery is
out of version 1 scope.

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

Acquire the exclusive worker lease and repair only safe gaps in building
releases: missing successor jobs, orphaned current jobs, plan-batch
scheduling, and eligible artifact cleanup. Sealed-release inconsistencies
are reported without mutation; terminal failed stages use retry.

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

Reopen one exact failed domain stage and insert a replacement River
job. The command does not run the job. The worker must be stopped.
The same stage identity is retained. An attachment retry reuses the
frozen plan batch rather than creating a new batch for its items.
Retries for active or inactive releases fail with
sealed_release_retry_forbidden.

Required flags:
  --stage   One production job kind
  --id      Positive domain row ID

Required environment:
  MRFPIPELINE_DATABASE_URL  PostgreSQL connection string
`

const monthHelp = `Usage:
  mrfpipeline month status
  mrfpipeline month status --payer <payer> --collection-month <YYYY-MM>
  mrfpipeline month activate --payer <payer> --collection-month <YYYY-MM>
  mrfpipeline month --help

Inspect monthly serving state or atomically activate a ready release.
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

Activation requires the complete worker environment and the worker lease.
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
	default:
		return rootHelp
	}
}
