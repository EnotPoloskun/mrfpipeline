package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/config"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

func TestIntegrationMonthActivateLeaseContention(t *testing.T) {
	testEnv, pool := testDiscoverEnv(t)
	ctx := context.Background()
	month := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO mrfpipeline.monthly_releases (payer_id, collection_month)
VALUES ('uhc', $1)`, month); err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	artifactRoot := filepath.Join(root, "artifacts")
	warehouse := filepath.Join(root, "warehouse")
	catalog := filepath.Join(root, "catalog")
	services := filepath.Join(root, "services.csv")
	for _, dir := range []string{artifactRoot, warehouse, catalog} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(catalog, "manifest.json"), []byte(`{"schema_version":1,"release_month":"2026-08"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(services, []byte("service_id\n"), 0600); err != nil {
		t.Fatal(err)
	}
	env := envMap(map[string]string{
		config.EnvDatabaseURL:         testEnv(config.EnvDatabaseURL),
		config.EnvArtifactRoot:        artifactRoot,
		config.EnvWarehousePath:       warehouse,
		config.EnvProviderCatalogPath: catalog,
		config.EnvServicesPath:        services,
	})

	lease, err := database.AcquireWorkerLease(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Release(context.Background()) }()

	code, stdout, stderr := runCLI(ctx, []string{
		"month", "activate", "--payer", "uhc", "--collection-month", "2026-08",
	}, env)
	if code != 4 || stdout != "" || !strings.Contains(stderr, jobs.FailureWorkerLeaseUnavailable) {
		t.Fatalf("exit %d stdout=%q stderr=%q", code, stdout, stderr)
	}

	var status string
	var sealedAt, lastActivatedAt *time.Time
	if err := pool.QueryRow(ctx, `
SELECT status, sealed_at, last_activated_at
FROM mrfpipeline.monthly_releases
WHERE payer_id = 'uhc' AND collection_month = $1`, month).Scan(&status, &sealedAt, &lastActivatedAt); err != nil {
		t.Fatal(err)
	}
	if status != "building" || sealedAt != nil || lastActivatedAt != nil {
		t.Fatalf("lease contention mutated release: status=%q sealed_at=%v last_activated_at=%v", status, sealedAt, lastActivatedAt)
	}
}
