package reconcile

import (
	"os"
	"testing"
)

func TestRealAcceptanceSkippedWithoutOptIn(t *testing.T) {
	t.Parallel()
	if os.Getenv(EnvRealAcceptance) == "1" &&
		os.Getenv(EnvTestDatabase) != "" &&
		os.Getenv("MRFPIPELINE_ARTIFACT_ROOT") != "" &&
		os.Getenv("MRFPIPELINE_WAREHOUSE_PATH") != "" &&
		os.Getenv("MRFPIPELINE_PROVIDER_CATALOG_PATH") != "" &&
		os.Getenv("MRFPIPELINE_SERVICES_PATH") != "" {
		t.Skip("authorized live acceptance is operator-supervised")
	}
	if _, err := CheckAcceptanceGuards(os.Getenv); err == nil {
		t.Fatal("ordinary go test must not enable live acceptance")
	}
}
