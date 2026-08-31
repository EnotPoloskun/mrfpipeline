package release

import (
	"strings"
	"testing"
)

func TestOutputFingerprintUsesASCIIOutputOrder(t *testing.T) {
	targets := []Target{
		{PayerID: "payer", CollectionMonth: "2026-08", OutputID: "mrf-20", SnapshotID: 20},
		{PayerID: "payer", CollectionMonth: "2026-08", OutputID: "mrf-3", SnapshotID: 3},
	}
	first := OutputFingerprint(targets)
	second := OutputFingerprint([]Target{targets[1], targets[0]})
	if first != second || len(first) != 64 || first != strings.ToLower(first) {
		t.Fatalf("fingerprint=%q repeat=%q", first, second)
	}
}

func TestFilterCatalogLockKeyIsStableAndDomainSeparated(t *testing.T) {
	first := FilterCatalogLockKey("payer", "2026-08")
	if first != FilterCatalogLockKey("payer", "2026-08") {
		t.Fatal("lock key is not stable")
	}
	if first == FilterCatalogLockKey("payer", "2026-09") || first == FilterCatalogLockKey("other", "2026-08") {
		t.Fatal("lock key is not domain separated")
	}
}
