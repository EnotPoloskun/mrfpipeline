package jobs

import (
	"errors"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
)

func TestProductionPolicy(t *testing.T) {
	t.Parallel()
	cfg := ProductionPolicy()
	if cfg.Schema != database.RiverSchema {
		t.Fatalf("schema %q", cfg.Schema)
	}
	if cfg.MaxAttempts != 8 {
		t.Fatalf("attempts %d", cfg.MaxAttempts)
	}
	if cfg.JobTimeout != -1 {
		t.Fatalf("timeout %s is not infinite", cfg.JobTimeout)
	}
	if cfg.RescueStuckJobsAfter != 24*time.Hour {
		t.Fatalf("rescue %s", cfg.RescueStuckJobsAfter)
	}
	if cfg.SoftStopTimeout != 30*time.Second {
		t.Fatalf("grace %s", cfg.SoftStopTimeout)
	}
	if cfg.RetryPolicy != nil {
		t.Fatal("retry must stay River default")
	}
	if cfg.FetchCooldown != 0 || cfg.FetchPollInterval != 0 {
		t.Fatal("fetch must stay River default")
	}
	if cfg.CompletedJobRetentionPeriod != 0 || cfg.DiscardedJobRetentionPeriod != 0 {
		t.Fatal("retention must stay River default")
	}
	if cfg.PeriodicJobs != nil {
		t.Fatal("no periodic jobs")
	}
	want := map[string]int{
		QueueDiscovery: 1, QueueTOCDownload: 4, QueueTOCParse: 2, QueueTOCImport: 2,
		QueueMRFDownload: 2, QueueMRFParse: 1, QueueConsumer: 1,
	}
	if len(cfg.Queues) != len(want) {
		t.Fatalf("queues %v", cfg.Queues)
	}
	for name, n := range want {
		if cfg.Queues[name].MaxWorkers != n {
			t.Fatalf("%s workers %d", name, cfg.Queues[name].MaxWorkers)
		}
	}
}

func TestInsertTxRequiresCallerTx(t *testing.T) {
	t.Parallel()
	_, err := InsertTx(t.Context(), nil, nil, DiscoveryRunArgs{DiscoveryRunID: 1})
	if !errors.Is(err, ErrJob) {
		t.Fatalf("got %v", err)
	}
}
