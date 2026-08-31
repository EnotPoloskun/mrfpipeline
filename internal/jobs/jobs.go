package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
)

// ErrJob is the sentinel wrapped by River client, insert, start, stop,
// argument decoding, and runtime failures after lower-level text is redacted.
var ErrJob = errors.New("background job operation failed")

const (
	FailureInvalidArguments                  = "invalid_job_arguments"
	FailureMissingRecord                     = "missing_domain_record"
	FailureDomainInvariant                   = "domain_invariant"
	FailureAttemptsExhausted                 = "job_attempts_exhausted"
	FailureDiscoveryListing                  = "discovery_listing_failed"
	FailureDiscoveryResultInvalid            = "discovery_result_invalid"
	FailureDiscoveryDatabase                 = "discovery_database_failed"
	FailureTOCDownload                       = "toc_download_failed"
	FailureTOCDownloadNotFound               = "toc_download_not_found"
	FailureMRFDownload                       = "mrf_download_failed"
	FailureMRFDownloadNotFound               = "mrf_download_not_found"
	FailureTOCParseInputInvalid              = "toc_parse_input_invalid"
	FailureTOCParseResourceFailed            = "toc_parse_resource_failed"
	FailureTOCParseOutputFailed              = "toc_parse_output_failed"
	FailureTOCParseOutputInvalid             = "toc_parse_output_invalid"
	FailureTOCParseCleanupFailed             = "toc_parse_cleanup_failed"
	FailureMRFParseExecutionFailed           = "mrf_parse_execution_failed"
	FailureMRFParseOutputInvalid             = "mrf_parse_output_invalid"
	FailureMRFParseSelectorChanged           = "mrf_parse_selector_changed"
	FailureMRFParseCleanupFailed             = "mrf_parse_cleanup_failed"
	FailureMRFParseDatabaseFailed            = "mrf_parse_database_failed"
	FailureTOCImportManifestInvalid          = "toc_import_manifest_invalid"
	FailureTOCImportSchemaInvalid            = "toc_import_schema_invalid"
	FailureTOCImportRowInvalid               = "toc_import_row_invalid"
	FailureTOCImportOrderInvalid             = "toc_import_order_invalid"
	FailureTOCImportDatabaseFailed           = "toc_import_database_failed"
	FailureTOCImportInvariant                = "toc_import_invariant"
	FailureConsumerIngestConfigInvalid       = "consumer_ingest_config_invalid"
	FailureConsumerIngestInputInvalid        = "consumer_ingest_input_invalid"
	FailureConsumerIngestOutputFailed        = "consumer_ingest_output_failed"
	FailureConsumerIngestOutputInvalid       = "consumer_ingest_output_invalid"
	FailureConsumerIngestProviderChanged     = "consumer_ingest_provider_changed"
	FailureConsumerIngestDatabaseFailed      = "consumer_ingest_database_failed"
	FailurePlanAttachConfigInvalid           = "plan_attach_config_invalid"
	FailurePlanAttachInputInvalid            = "plan_attach_input_invalid"
	FailurePlanAttachOutputFailed            = "plan_attach_output_failed"
	FailurePlanAttachOutputInvalid           = "plan_attach_output_invalid"
	FailurePlanAttachDatabaseFailed          = "plan_attach_database_failed"
	FailurePlanAttachInvariant               = "plan_attach_invariant"
	FailureWorkerLeaseUnavailable            = "worker_lease_unavailable"
	FailureWorkerLeaseLost                   = "worker_lease_lost"
	FailureRiverTerminalWithoutResult        = "river_terminal_without_domain_result"
	FailureReconciliationDatabaseFailed      = "reconciliation_database_failed"
	FailureArtifactReconciliationFailed      = "artifact_reconciliation_failed"
	FailureRetryStageNotFailed               = "retry_stage_not_failed"
	FailureRetryStageInvariant               = "retry_stage_invariant"
	FailureReleaseNotFound                   = "release_not_found"
	FailureReleaseNotReady                   = "release_not_ready"
	FailureSealedReleaseInconsistent         = "sealed_release_inconsistent"
	FailureSealedReleaseRetryForbidden       = "sealed_release_retry_forbidden"
	FailureWorkerBusy                        = "worker_busy"
	FailureStageExecutionBusy                = "stage_execution_busy"
	FailureStageExecutionInterrupted         = "stage_execution_interrupted"
	FailureCapacityBelowHeld                 = "capacity_below_held_slots"
	FailureRuntimeRootMismatch               = "runtime_artifact_root_mismatch"
	FailureSourceTargetDecrease              = "source_target_decrease"
	FailureSourceTargetConflict              = "source_target_conflict"
	FailureSourceTargetUnset                 = "source_target_unset"
	FailureFilterCatalogConfigInvalid        = "filter_catalog_config_invalid"
	FailureFilterCatalogWarehouseInvalid     = "filter_catalog_warehouse_invalid"
	FailureFilterCatalogDuckDBUnavailable    = "filter_catalog_duckdb_unavailable"
	FailureFilterCatalogDuckDBVersionInvalid = "filter_catalog_duckdb_version_invalid"
	FailureFilterCatalogQueryFailed          = "filter_catalog_query_failed"
	FailureFilterCatalogValueInvalid         = "filter_catalog_value_invalid"
	FailureFilterCatalogProtocolInvalid      = "filter_catalog_protocol_invalid"
	FailureFilterCatalogCancelled            = "filter_catalog_cancelled"
	// FailureJobBookkeeping is emitted only in logs when retry or terminal
	// bookkeeping cannot be persisted. It is not a domain failure code.
	FailureJobBookkeeping = "job_bookkeeping_failed"
	// FailureJobLifecycle is emitted only in logs when claim/success lifecycle
	// bookkeeping fails without a more specific fixed failure classification.
	FailureJobLifecycle = "job_lifecycle_failed"

	MaxAttempts    = 8
	RescueAfter    = 24 * time.Hour
	GracefulStop   = 30 * time.Second
	ProgressEvery  = 30 * time.Second
	JobTimeoutNone = -1 * time.Nanosecond
	RiverSchema    = database.RiverSchema
)

type failCodeError struct {
	code string
}

func (e *failCodeError) Error() string {
	return ErrJob.Error() + ": " + e.code
}

func (e *failCodeError) Unwrap() error { return ErrJob }

func jobErr(op string) error {
	if allowedFailureCode(op) {
		return &failCodeError{code: op}
	}
	return fmt.Errorf("%w: %s", ErrJob, op)
}

// Failure returns a redacted job error. Known codes are persisted on
// terminal failure; other operations wrap ErrJob only.
func Failure(code string) error {
	return jobErr(code)
}

func allowedFailureCode(code string) bool {
	switch code {
	case FailureInvalidArguments, FailureMissingRecord, FailureDomainInvariant, FailureAttemptsExhausted,
		FailureDiscoveryListing, FailureDiscoveryResultInvalid, FailureDiscoveryDatabase,
		FailureTOCDownload, FailureTOCDownloadNotFound, FailureMRFDownload, FailureMRFDownloadNotFound,
		FailureTOCParseInputInvalid, FailureTOCParseResourceFailed, FailureTOCParseOutputFailed,
		FailureTOCParseOutputInvalid, FailureTOCParseCleanupFailed,
		FailureMRFParseExecutionFailed, FailureMRFParseOutputInvalid, FailureMRFParseSelectorChanged,
		FailureMRFParseCleanupFailed, FailureMRFParseDatabaseFailed,
		FailureTOCImportManifestInvalid, FailureTOCImportSchemaInvalid, FailureTOCImportRowInvalid,
		FailureTOCImportOrderInvalid, FailureTOCImportDatabaseFailed, FailureTOCImportInvariant,
		FailureConsumerIngestConfigInvalid, FailureConsumerIngestInputInvalid, FailureConsumerIngestOutputFailed,
		FailureConsumerIngestOutputInvalid, FailureConsumerIngestProviderChanged, FailureConsumerIngestDatabaseFailed,
		FailurePlanAttachConfigInvalid, FailurePlanAttachInputInvalid, FailurePlanAttachOutputFailed,
		FailurePlanAttachOutputInvalid, FailurePlanAttachDatabaseFailed, FailurePlanAttachInvariant,
		FailureWorkerLeaseUnavailable, FailureWorkerLeaseLost, FailureRiverTerminalWithoutResult,
		FailureReconciliationDatabaseFailed, FailureArtifactReconciliationFailed,
		FailureRetryStageNotFailed, FailureRetryStageInvariant,
		FailureReleaseNotFound, FailureReleaseNotReady,
		FailureSealedReleaseInconsistent, FailureSealedReleaseRetryForbidden,
		FailureWorkerBusy, FailureStageExecutionBusy, FailureStageExecutionInterrupted, FailureCapacityBelowHeld,
		FailureRuntimeRootMismatch, FailureSourceTargetDecrease, FailureSourceTargetConflict, FailureSourceTargetUnset,
		FailureFilterCatalogConfigInvalid, FailureFilterCatalogWarehouseInvalid, FailureFilterCatalogDuckDBUnavailable,
		FailureFilterCatalogDuckDBVersionInvalid, FailureFilterCatalogQueryFailed, FailureFilterCatalogValueInvalid,
		FailureFilterCatalogProtocolInvalid, FailureFilterCatalogCancelled:

		return true
	default:
		return false
	}
}

func isImmediateFail(err error) bool {
	return isFailure(err, FailureInvalidArguments) ||
		isFailure(err, FailureMissingRecord) ||
		isFailure(err, FailureDomainInvariant) ||
		isFailure(err, FailureTOCDownloadNotFound) ||
		isFailure(err, FailureMRFDownloadNotFound) ||
		isFailure(err, FailureSealedReleaseInconsistent) ||
		isFailure(err, FailureSealedReleaseRetryForbidden)
}

func terminalFailureCode(err error) string {
	var f *failCodeError
	if errors.As(err, &f) && allowedFailureCode(f.code) {
		return f.code
	}
	return FailureAttemptsExhausted
}

func isFailure(err error, code string) bool {
	var f *failCodeError
	return errors.As(err, &f) && f.code == code
}

// IsFailure reports whether err is a coded terminal/retryable job failure.
func IsFailure(err error, code string) bool {
	return isFailure(err, code)
}

// IsRecognizedFailureCode reports whether code is a fixed persisted/runtime code.
func IsRecognizedFailureCode(code string) bool {
	return allowedFailureCode(code)
}

func classifyJob(ctx context.Context, op string, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, database.ErrDatabase) {
		return fmt.Errorf("%w: %w: %s", ErrJob, database.ErrDatabase, op)
	}
	return jobErr(op)
}
