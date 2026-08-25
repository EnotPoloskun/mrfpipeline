package work

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/parquet-go/parquet-go"
)

func TestQueuesForRole(t *testing.T) {
	t.Parallel()
	tests := []struct {
		role string
		want map[string]int
	}{
		{role: "control", want: map[string]int{
			jobs.QueueControl:     1,
			jobs.QueueDiscovery:   1,
			jobs.QueueTOCDownload: 4,
			jobs.QueueTOCParse:    2,
			jobs.QueueTOCImport:   2,
		}},
		{role: "mrf", want: map[string]int{
			jobs.QueueMRFDownload: 2,
			jobs.QueueMRFParse:    1,
		}},
		{role: "consumer", want: map[string]int{jobs.QueueConsumer: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			got := QueuesForRole(tt.role)
			if len(got) != len(tt.want) {
				t.Fatalf("queues for %s = %v", tt.role, got)
			}
			for queue, maxWorkers := range tt.want {
				if got[queue].MaxWorkers != maxWorkers {
					t.Fatalf("%s %s max workers = %d, want %d", tt.role, queue, got[queue].MaxWorkers, maxWorkers)
				}
			}
		})
	}
	if got := QueuesForRole("unknown"); got != nil {
		t.Fatalf("unknown role queues = %v", got)
	}
}

func TestWorkerLeaseLossLogIsRuntimeOnly(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logWorkerLeaseLost(jobs.NewLogger(&buf))
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record); err != nil {
		t.Fatal(err)
	}
	if record["msg"] != "worker_lease_lost" || record["kind"] != "runtime" || record["failure"] != jobs.FailureWorkerLeaseLost {
		t.Fatalf("record %v", record)
	}
	for _, field := range []string{"queue", "job_id", "domain_id", "attempt", "outcome"} {
		if _, ok := record[field]; ok {
			t.Fatalf("runtime field %s present: %v", field, record)
		}
	}
}

func TestWorkerStartedLogIncludesAttachedRole(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logWorkerStarted(jobs.NewLogger(&buf).With("role", "mrf"))
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record); err != nil {
		t.Fatal(err)
	}
	if record["msg"] != "worker_started" || record["kind"] != "runtime" || record["role"] != "mrf" {
		t.Fatalf("record %v", record)
	}
}

func TestRunRequiresServices(t *testing.T) {
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	err = Runtime{Role: "control", Pool: &pgxpool.Pool{}, Workspace: ws}.Run(context.Background())
	if err == nil {
		t.Fatal("expected services failure")
	}
}

func TestRunRequiresExplicitRole(t *testing.T) {
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	err = Runtime{Pool: &pgxpool.Pool{}, Workspace: ws}.Run(context.Background())
	if !jobs.IsFailure(err, jobs.FailureInvalidArguments) {
		t.Fatalf("role error = %v", err)
	}
}

func TestRunRequiresCatalogAndWarehouse(t *testing.T) {
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	svc := filepath.Join(t.TempDir(), "services.csv")
	if err := os.WriteFile(svc, []byte("billing_code_type,billing_code\nCPT,99213\n"), 0600); err != nil {
		t.Fatal(err)
	}
	err = Runtime{Pool: &pgxpool.Pool{}, Workspace: ws, ServicesPath: svc}.Run(context.Background())
	if err == nil {
		t.Fatal("expected catalog failure")
	}
	cat := filepath.Join(t.TempDir(), "catalog")
	if err := os.Mkdir(cat, 0700); err != nil {
		t.Fatal(err)
	}
	writeWorkCatalog(t, cat)
	err = Runtime{Pool: &pgxpool.Pool{}, Workspace: ws, ServicesPath: svc, ProviderCatalogPath: cat}.Run(context.Background())
	if err == nil {
		t.Fatal("expected warehouse failure")
	}
}

type workCatalogProvider struct {
	NPI        string  `parquet:"npi"`
	State      *string `parquet:"state,optional"`
	City       *string `parquet:"city,optional"`
	PostalCode *string `parquet:"postal_code,optional"`
}

type workCatalogTaxonomy struct {
	NPI          string `parquet:"npi"`
	TaxonomyCode string `parquet:"taxonomy_code"`
}

func writeWorkCatalog(t testing.TB, root string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "providers"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "provider_taxonomies"), 0700); err != nil {
		t.Fatal(err)
	}
	state, city, zip := "fl", "miami", "33101"
	writeWorkParquet(t, filepath.Join(root, "providers", "part-00000.parquet"), []workCatalogProvider{{
		NPI: "1111111111", State: &state, City: &city, PostalCode: &zip,
	}})
	writeWorkParquet(t, filepath.Join(root, "provider_taxonomies", "part-00000.parquet"), []workCatalogTaxonomy{{
		NPI: "1111111111", TaxonomyCode: "207Q00000X",
	}})
	man, err := json.Marshal(map[string]any{
		"schema_version": 1, "release_month": "2026-08",
		"providers":           map[string]any{"path": "providers", "rows": 1},
		"provider_taxonomies": map[string]any{"path": "provider_taxonomies", "rows": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), append(man, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func writeWorkParquet[T any](t testing.TB, path string, rows []T) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[T](f)
	if _, err := w.Write(rows); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}
