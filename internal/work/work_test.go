package work

import (
	"testing"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

func TestQueues(t *testing.T) {
	t.Parallel()
	q := Queues()
	if len(q) != 2 {
		t.Fatalf("queues %v", q)
	}
	if q[jobs.QueueDiscovery].MaxWorkers != 1 {
		t.Fatalf("discovery %d", q[jobs.QueueDiscovery].MaxWorkers)
	}
	if q[jobs.QueueTOCDownload].MaxWorkers != 4 {
		t.Fatalf("toc_download %d", q[jobs.QueueTOCDownload].MaxWorkers)
	}
	if _, ok := q[jobs.QueueTOCParse]; ok {
		t.Fatal("must not consume toc_parse")
	}
}
