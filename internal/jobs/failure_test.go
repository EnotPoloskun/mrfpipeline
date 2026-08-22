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
}
