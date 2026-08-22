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
	FailureInvalidArguments         = "invalid_job_arguments"
	FailureMissingRecord            = "missing_domain_record"
	FailureDomainInvariant          = "domain_invariant"
	FailureAttemptsExhausted        = "job_attempts_exhausted"
	FailureDiscoveryListing         = "discovery_listing_failed"
	FailureDiscoveryResultInvalid   = "discovery_result_invalid"
	FailureDiscoveryDatabase        = "discovery_database_failed"
	FailureTOCDownload              = "toc_download_failed"
	FailureTOCParseInputInvalid     = "toc_parse_input_invalid"
	FailureTOCParseResourceFailed   = "toc_parse_resource_failed"
	FailureTOCParseOutputFailed     = "toc_parse_output_failed"
	FailureTOCParseOutputInvalid    = "toc_parse_output_invalid"
	FailureTOCParseCleanupFailed    = "toc_parse_cleanup_failed"
	FailureTOCImportManifestInvalid = "toc_import_manifest_invalid"
	FailureTOCImportSchemaInvalid   = "toc_import_schema_invalid"
	FailureTOCImportRowInvalid      = "toc_import_row_invalid"
	FailureTOCImportOrderInvalid    = "toc_import_order_invalid"
	FailureTOCImportDatabaseFailed  = "toc_import_database_failed"
	FailureTOCImportInvariant       = "toc_import_invariant"

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
		FailureTOCDownload,
		FailureTOCParseInputInvalid, FailureTOCParseResourceFailed, FailureTOCParseOutputFailed,
		FailureTOCParseOutputInvalid, FailureTOCParseCleanupFailed,
		FailureTOCImportManifestInvalid, FailureTOCImportSchemaInvalid, FailureTOCImportRowInvalid,
		FailureTOCImportOrderInvalid, FailureTOCImportDatabaseFailed, FailureTOCImportInvariant:
		return true
	default:
		return false
	}
}

func isImmediateFail(err error) bool {
	return isFailure(err, FailureInvalidArguments) ||
		isFailure(err, FailureMissingRecord) ||
		isFailure(err, FailureDomainInvariant)
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
