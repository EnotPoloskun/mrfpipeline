package jobs

import (
	"errors"
	"testing"
)

func TestFailureCodes(t *testing.T) {
	t.Parallel()
	for _, code := range []string{
		FailureDiscoveryListing,
		FailureDiscoveryResultInvalid,
		FailureDiscoveryDatabase,
		FailureTOCDownload,
		FailureTOCDownloadNotFound,
		FailureMRFDownload,
		FailureMRFDownloadNotFound,
		FailureTOCParseInputInvalid,
		FailureTOCParseResourceFailed,
		FailureTOCParseOutputFailed,
		FailureTOCParseOutputInvalid,
		FailureTOCParseCleanupFailed,
		FailureMRFParseExecutionFailed,
		FailureMRFParseOutputInvalid,
		FailureMRFParseSelectorChanged,
		FailureMRFParseCleanupFailed,
		FailureMRFParseDatabaseFailed,
		FailureTOCImportManifestInvalid,
		FailureTOCImportSchemaInvalid,
		FailureTOCImportRowInvalid,
		FailureTOCImportOrderInvalid,
		FailureTOCImportDatabaseFailed,
		FailureTOCImportInvariant,
		FailureConsumerIngestConfigInvalid,
		FailureConsumerIngestInputInvalid,
		FailureConsumerIngestOutputFailed,
		FailureConsumerIngestOutputInvalid,
		FailureConsumerIngestPublicationInvalid,
		FailureConsumerIngestProviderChanged,
		FailureConsumerIngestDatabaseFailed,
		FailurePlanAttachConfigInvalid,
		FailurePlanAttachInputInvalid,
		FailurePlanAttachOutputFailed,
		FailurePlanAttachOutputInvalid,
		FailurePlanAttachDatabaseFailed,
		FailurePlanAttachInvariant,
		FailureDomainInvariant,
		FailureWorkerLeaseUnavailable,
		FailureWorkerLeaseLost,
		FailureRiverTerminalWithoutResult,
		FailureReconciliationDatabaseFailed,
		FailureArtifactReconciliationFailed,
		FailureRetryStageNotFailed,
		FailureRetryStageInvariant,
		FailureReleaseNotFound,
		FailureReleaseNotReady,
		FailureSealedReleaseInconsistent,
		FailureSealedReleaseRetryForbidden,
	} {
		err := Failure(code)
		if !errors.Is(err, ErrJob) || !IsFailure(err, code) {
			t.Fatalf("%s: %v", code, err)
		}
		if terminalFailureCode(err) != code {
			t.Fatalf("terminal %s", code)
		}
	}
	if terminalFailureCode(jobErr("work")) != FailureAttemptsExhausted {
		t.Fatal("unknown work error")
	}
	for _, code := range []string{
		FailureInvalidArguments,
		FailureMissingRecord,
		FailureDomainInvariant,
		FailureTOCDownloadNotFound,
		FailureMRFDownloadNotFound,
		FailureSealedReleaseInconsistent,
		FailureSealedReleaseRetryForbidden,
		FailureTOCParseInputInvalid,
		FailureTOCParseOutputInvalid,
		FailureMRFParseOutputInvalid,
		FailureMRFParseSelectorChanged,
		FailureTOCImportManifestInvalid,
		FailureTOCImportSchemaInvalid,
		FailureTOCImportRowInvalid,
		FailureTOCImportOrderInvalid,
		FailureTOCImportInvariant,
		FailureConsumerIngestConfigInvalid,
		FailureConsumerIngestInputInvalid,
		FailureConsumerIngestProviderChanged,
		FailureConsumerIngestPublicationInvalid,
		FailurePlanAttachConfigInvalid,
		FailurePlanAttachInputInvalid,
		FailurePlanAttachInvariant,
	} {
		if !isImmediateFail(Failure(code)) {
			t.Fatalf("%s should be immediate", code)
		}
	}
	for _, code := range []string{
		FailureDiscoveryListing,
		FailureDiscoveryResultInvalid,
		FailureDiscoveryDatabase,
		FailureTOCDownload,
		FailureMRFDownload,
		FailureTOCParseResourceFailed,
		FailureTOCParseOutputFailed,
		FailureTOCParseCleanupFailed,
		FailureMRFParseExecutionFailed,
		FailureMRFParseCleanupFailed,
		FailureMRFParseDatabaseFailed,
		FailureTOCImportDatabaseFailed,
		FailureConsumerIngestOutputFailed,
		FailureConsumerIngestOutputInvalid,
		FailureConsumerIngestDatabaseFailed,
		FailurePlanAttachOutputFailed,
		FailurePlanAttachOutputInvalid,
		FailurePlanAttachDatabaseFailed,
		FailureReconciliationDatabaseFailed,
		FailureArtifactReconciliationFailed,
		FailureWorkerLeaseUnavailable,
		FailureRiverTerminalWithoutResult,
		FailureRetryStageNotFailed,
	} {
		if isImmediateFail(Failure(code)) {
			t.Fatalf("%s should retry", code)
		}
	}
}
