package jobs

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestProgressAllowlistAndThrottle(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	p := NewProgress(NewLogger(&buf))
	base := time.Unix(1_700_000_000, 0).UTC()
	err := p.Log(ProgressParams{JobID: 9, Kind: KindMRFParse, Queue: QueueMRFParse, Phase: "mrf_parse", CopiedBytes: 10, TotalBytes: 100, now: base})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Log(ProgressParams{JobID: 9, Kind: KindMRFParse, Queue: QueueMRFParse, Phase: "mrf_parse", CopiedBytes: 20, TotalBytes: 100, now: base.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if err := p.Log(ProgressParams{JobID: 9, Kind: KindMRFParse, Queue: QueueMRFParse, Phase: "mrf_parse", CopiedBytes: 50, TotalBytes: 100, now: base.Add(31 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if err := p.Log(ProgressParams{JobID: 9, Kind: KindMRFParse, Queue: QueueMRFParse, Phase: "mrf_parse", CopiedBytes: 100, TotalBytes: 100, Done: true, now: base.Add(32 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if err := p.Log(ProgressParams{JobID: 9, Kind: KindMRFParse, Queue: QueueMRFParse, Phase: "not_a_phase", CopiedBytes: 1}); !errors.Is(err, ErrJob) {
		t.Fatalf("unknown phase: %v", err)
	}
	for _, phase := range []string{
		"download", "toc_destination_prepared", "toc_input_parsed", "toc_parquet_closed",
		"mrf_parse", "validating_input", "provider_relationships", "rate_facts", "publishing",
	} {
		if err := NewProgress(NewLogger(io.Discard)).Log(ProgressParams{JobID: 1, Kind: KindTOCDownload, Queue: QueueTOCDownload, Phase: phase, CopiedBytes: 0}); err != nil {
			t.Fatalf("phase %s: %v", phase, err)
		}
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("lines %d %q", len(lines), buf.String())
	}
	for _, line := range lines {
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatal(err)
		}
		if obj["msg"] != "progress" {
			t.Fatalf("msg %v", obj["msg"])
		}
		if obj["phase"] != "mrf_parse" {
			t.Fatalf("phase %v", obj)
		}
		if _, ok := obj["discovery_run_id"]; ok {
			t.Fatalf("domain id leaked: %v", obj)
		}
		if strings.Contains(line, "https://") || strings.Contains(line, "/tmp/") {
			t.Fatalf("leaked: %s", line)
		}
		for k := range obj {
			switch k {
			case "time", "level", "msg", "kind", "queue", "job_id", "phase", "percent", "copied_bytes", "total_bytes":
			default:
				t.Fatalf("unexpected field %s", k)
			}
		}
	}
}

func TestProgressOmitsPercentWhenUnknown(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	p := NewProgress(NewLogger(&buf))
	if err := p.Log(ProgressParams{JobID: 3, Kind: KindTOCDownload, Queue: QueueTOCDownload, Phase: "download", CopiedBytes: 12}); err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &obj); err != nil {
		t.Fatal(err)
	}
	if _, ok := obj["percent"]; ok {
		t.Fatalf("percent present: %v", obj)
	}
	if obj["copied_bytes"] != float64(12) {
		t.Fatalf("copied %v", obj["copied_bytes"])
	}
}

func TestLoggerDropsErrorText(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log := NewLogger(&buf)
	log.Error("job_error", "kind", KindDiscoveryRun, "job_id", int64(4), "attempt", 2, "failure", FailureAttemptsExhausted, "err", "postgres://user:supersecret@example.invalid/db")
	line := buf.String()
	if strings.Contains(line, "supersecret") || strings.Contains(line, "example.invalid") {
		t.Fatalf("leaked: %s", line)
	}
	if !strings.Contains(line, FailureAttemptsExhausted) {
		t.Fatalf("missing classification: %s", line)
	}
}
