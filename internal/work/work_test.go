package work

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestQueues(t *testing.T) {
	t.Parallel()
	q := Queues()
	if len(q) != 6 {
		t.Fatalf("queues %v", q)
	}
	if q[jobs.QueueDiscovery].MaxWorkers != 1 {
		t.Fatalf("discovery %d", q[jobs.QueueDiscovery].MaxWorkers)
	}
	if q[jobs.QueueTOCDownload].MaxWorkers != 4 {
		t.Fatalf("toc_download %d", q[jobs.QueueTOCDownload].MaxWorkers)
	}
	if q[jobs.QueueTOCParse].MaxWorkers != 2 {
		t.Fatalf("toc_parse %d", q[jobs.QueueTOCParse].MaxWorkers)
	}
	if q[jobs.QueueTOCImport].MaxWorkers != 2 {
		t.Fatalf("toc_import %d", q[jobs.QueueTOCImport].MaxWorkers)
	}
	if q[jobs.QueueMRFDownload].MaxWorkers != 2 {
		t.Fatalf("mrf_download %d", q[jobs.QueueMRFDownload].MaxWorkers)
	}
	if q[jobs.QueueMRFParse].MaxWorkers != 1 {
		t.Fatalf("mrf_parse %d", q[jobs.QueueMRFParse].MaxWorkers)
	}
	if _, ok := q[jobs.QueueConsumer]; ok {
		t.Fatal("must not consume consumer")
	}
}

func TestRunRequiresServices(t *testing.T) {
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	err = Runtime{Pool: &pgxpool.Pool{}, Workspace: ws}.Run(context.Background())
	if err == nil {
		t.Fatal("expected services failure")
	}
}
