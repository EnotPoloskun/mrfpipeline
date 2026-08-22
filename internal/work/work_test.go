package work

import (
	"testing"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

func TestQueues(t *testing.T) {
	t.Parallel()
	q := Queues()
	if len(q) != 5 {
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
	if _, ok := q[jobs.QueueMRFParse]; ok {
		t.Fatal("must not consume mrf_parse")
	}
	if _, ok := q[jobs.QueueConsumer]; ok {
		t.Fatal("must not consume consumer")
	}
}
