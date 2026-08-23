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
		FailureMRFDownload,
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
		FailureConsumerIngestProviderChanged,
		FailureConsumerIngestDatabaseFailed,
		FailurePlanAttachConfigInvalid,
		FailurePlanAttachInputInvalid,
		FailurePlanAttachOutputFailed,
		FailurePlanAttachOutputInvalid,
		FailurePlanAttachDatabaseFailed,
		FailurePlanAttachInvariant,
		FailureDomainInvariant,
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
	if !isImmediateFail(Failure(FailureDomainInvariant)) {
		t.Fatal("invariant should be immediate")
	}
	if isImmediateFail(Failure(FailureDiscoveryListing)) {
		t.Fatal("listing should retry")
	}
	if isImmediateFail(Failure(FailureTOCDownload)) {
		t.Fatal("toc download should retry")
	}
	if isImmediateFail(Failure(FailureMRFDownload)) {
		t.Fatal("mrf download should retry")
	}
	if isImmediateFail(Failure(FailureTOCParseInputInvalid)) {
		t.Fatal("toc parse input should retry")
	}
	if isImmediateFail(Failure(FailureMRFParseExecutionFailed)) {
		t.Fatal("mrf parse execution should retry")
	}
	if isImmediateFail(Failure(FailureMRFParseOutputInvalid)) {
		t.Fatal("mrf parse output should retry")
	}
	if isImmediateFail(Failure(FailureMRFParseSelectorChanged)) {
		t.Fatal("mrf parse selector should retry")
	}
	if isImmediateFail(Failure(FailureMRFParseCleanupFailed)) {
		t.Fatal("mrf parse cleanup should retry")
	}
	if isImmediateFail(Failure(FailureMRFParseDatabaseFailed)) {
		t.Fatal("mrf parse database should retry")
	}
	if isImmediateFail(Failure(FailureTOCImportManifestInvalid)) {
		t.Fatal("toc import manifest should retry")
	}
	if isImmediateFail(Failure(FailureTOCImportInvariant)) {
		t.Fatal("toc import invariant should retry")
	}
	if isImmediateFail(Failure(FailureConsumerIngestConfigInvalid)) {
		t.Fatal("consumer ingest config should retry")
	}
	if isImmediateFail(Failure(FailureConsumerIngestInputInvalid)) {
		t.Fatal("consumer ingest input should retry")
	}
	if isImmediateFail(Failure(FailureConsumerIngestOutputFailed)) {
		t.Fatal("consumer ingest output failed should retry")
	}
	if isImmediateFail(Failure(FailureConsumerIngestOutputInvalid)) {
		t.Fatal("consumer ingest output invalid should retry")
	}
	if isImmediateFail(Failure(FailureConsumerIngestProviderChanged)) {
		t.Fatal("consumer ingest provider should retry")
	}
	if isImmediateFail(Failure(FailureConsumerIngestDatabaseFailed)) {
		t.Fatal("consumer ingest database should retry")
	}
	if isImmediateFail(Failure(FailurePlanAttachConfigInvalid)) {
		t.Fatal("plan attach config should retry")
	}
	if isImmediateFail(Failure(FailurePlanAttachInputInvalid)) {
		t.Fatal("plan attach input should retry")
	}
	if isImmediateFail(Failure(FailurePlanAttachOutputFailed)) {
		t.Fatal("plan attach output failed should retry")
	}
	if isImmediateFail(Failure(FailurePlanAttachOutputInvalid)) {
		t.Fatal("plan attach output invalid should retry")
	}
	if isImmediateFail(Failure(FailurePlanAttachDatabaseFailed)) {
		t.Fatal("plan attach database should retry")
	}
	if isImmediateFail(Failure(FailurePlanAttachInvariant)) {
		t.Fatal("plan attach invariant should retry")
	}
}
