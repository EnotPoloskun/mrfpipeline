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
  mrfpipeline --help
  mrfpipeline --version

Commands:
  migrate   Apply application and River database migrations
  work      Run background workers
  discover  Enqueue one bounded UHC discovery run
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
configuration, starts the discovery, TOC download, TOC parse, TOC import,
MRF download, MRF parse, and consumer ingest and attach queues, and runs until canceled.

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

collection_month is a caller-supplied label assigned to discovered TOC
records. It is not inferred from URLs or payer contents.

--limit is required and must be a positive integer. Unlimited discovery is
out of version 1 scope.

Required flags:
  --payer              Exact payer adapter identifier (only uhc)
  --collection-month   Caller-selected month, exactly YYYY-MM
  --limit              Positive int64 of newly admitted TOCs

Required environment:
  MRFPIPELINE_DATABASE_URL  PostgreSQL connection string
`

func helpFor(command string) string {
	switch command {
	case cmdMigrate:
		return migrateHelp
	case cmdWork:
		return workHelp
	case cmdDiscover:
		return discoverHelp
	default:
		return rootHelp
	}
}
