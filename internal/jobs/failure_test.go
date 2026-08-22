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
		FailureTOCImportManifestInvalid,
		FailureTOCImportSchemaInvalid,
		FailureTOCImportRowInvalid,
		FailureTOCImportOrderInvalid,
		FailureTOCImportDatabaseFailed,
		FailureTOCImportInvariant,
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
	if isImmediateFail(Failure(FailureTOCImportManifestInvalid)) {
		t.Fatal("toc import manifest should retry")
	}
	if isImmediateFail(Failure(FailureTOCImportInvariant)) {
		t.Fatal("toc import invariant should retry")
	}
}
