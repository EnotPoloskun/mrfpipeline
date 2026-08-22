package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/EnotPoloskun/mrfdiscoverer"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/riverqueue/river"
)

func TestFormatReport(t *testing.T) {
	t.Parallel()
	text, err := FormatReport(Report{
		DiscoveryRunID:  41,
		RiverJobID:      9001,
		PayerID:         "uhc",
		CollectionMonth: "2026-08",
		TOCLimit:        5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if text != "{\"discovery_run_id\":41,\"river_job_id\":9001,\"payer_id\":\"uhc\",\"collection_month\":\"2026-08\",\"toc_limit\":5}\n" {
		t.Fatalf("got %q", text)
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(text)), &obj); err != nil {
		t.Fatal(err)
	}
	if obj["toc_limit"] == nil {
		t.Fatal("toc_limit must not be null")
	}
	if _, ok := obj["toc_limit"].(float64); !ok {
		t.Fatalf("toc_limit type %T", obj["toc_limit"])
	}
	keys := make([]string, 0, len(obj))
	dec := json.NewDecoder(strings.NewReader(strings.TrimSpace(text)))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		t.Fatalf("object: %v %v", tok, err)
	}
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k.(string))
		var skip any
		if err := dec.Decode(&skip); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"discovery_run_id", "river_job_id", "payer_id", "collection_month", "toc_limit"}
	if len(keys) != len(want) {
		t.Fatalf("keys %v", keys)
	}
	for i, k := range want {
		if keys[i] != k {
			t.Fatalf("keys %v", keys)
		}
	}
}

func TestFirstOccurrencesPreservesOrdinalsAndExactURLs(t *testing.T) {
	t.Parallel()
	files := []TOCFile{
		{URL: "https://example.invalid/a"},
		{URL: "https://example.invalid/A"},
		{URL: "https://example.invalid/a"},
		{URL: "https://example.invalid/b"},
		{URL: "https://example.invalid/a"},
	}
	got, err := firstOccurrences(files)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("len %d", len(got))
	}
	if got[0].URL != files[0].URL || got[0].Ordinal != 0 {
		t.Fatalf("first %+v", got[0])
	}
	if got[1].URL != files[1].URL || got[1].Ordinal != 1 {
		t.Fatalf("case-sensitive %+v", got[1])
	}
	if got[2].URL != files[3].URL || got[2].Ordinal != 3 {
		t.Fatalf("gap %+v", got[2])
	}
	if int64(len(files)) != 5 {
		t.Fatal("discovered includes duplicates")
	}
}

func TestEmptyListing(t *testing.T) {
	t.Parallel()
	got, err := firstOccurrences(nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("%v %v", got, err)
	}
	got, err = firstOccurrences([]TOCFile{})
	if err != nil || len(got) != 0 {
		t.Fatalf("%v %v", got, err)
	}
}

func TestInvalidURLFailsWithoutExposure(t *testing.T) {
	t.Parallel()
	secret := "https://secret.example.invalid/toc\nnext"
	_, err := firstOccurrences([]TOCFile{
		{URL: "https://example.invalid/ok"},
		{URL: secret},
	})
	if !jobs.IsFailure(err, jobs.FailureDiscoveryResultInvalid) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "example.invalid") {
		t.Fatalf("exposed url: %v", err)
	}
	if _, err := firstOccurrences([]TOCFile{{URL: ""}}); !jobs.IsFailure(err, jobs.FailureDiscoveryResultInvalid) {
		t.Fatalf("empty: %v", err)
	}
	if _, err := firstOccurrences([]TOCFile{{URL: "https://x.invalid/\x00"}}); !jobs.IsFailure(err, jobs.FailureDiscoveryResultInvalid) {
		t.Fatalf("nul: %v", err)
	}
}

func TestMapDiscoverError(t *testing.T) {
	t.Parallel()
	if err := mapDiscoverError(context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if err := mapDiscoverError(context.DeadlineExceeded); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline: %v", err)
	}
	if err := mapDiscoverError(mrfdiscoverer.ErrListing); !jobs.IsFailure(err, jobs.FailureDiscoveryListing) {
		t.Fatalf("listing: %v", err)
	}
	if err := mapDiscoverError(mrfdiscoverer.ErrInvalidConfig); !jobs.IsFailure(err, jobs.FailureDomainInvariant) {
		t.Fatalf("config: %v", err)
	}
	err := mapDiscoverError(mrfdiscoverer.ErrListing)
	if strings.Contains(err.Error(), "uhc") || strings.Contains(err.Error(), "http") {
		t.Fatalf("exposed: %v", err)
	}
}

func TestWorkRegistersOnlyDiscovery(t *testing.T) {
	t.Parallel()
	q := Queues()
	if len(q) != 1 {
		t.Fatalf("queues %v", q)
	}
	if q[jobs.QueueDiscovery].MaxWorkers != 1 {
		t.Fatalf("discovery workers %d", q[jobs.QueueDiscovery].MaxWorkers)
	}
	if _, ok := q[jobs.QueueTOCDownload]; ok {
		t.Fatal("must not consume toc_download")
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, &Worker{})
}

func TestLimitSelectionCounts(t *testing.T) {
	t.Parallel()
	files := []TOCFile{
		{URL: "known-1"},
		{URL: "new-A"},
		{URL: "new-A"},
		{URL: "new-B"},
		{URL: "new-C"},
		{URL: "new-D"},
	}
	first, err := firstOccurrences(files)
	if err != nil {
		t.Fatal(err)
	}
	known := map[string]bool{"known-1": true}
	var existing, admitted, overflow int64
	const limit = 2
	for _, hit := range first {
		if known[hit.URL] {
			existing++
			continue
		}
		if admitted >= limit {
			overflow++
			continue
		}
		admitted++
	}
	if int64(len(files)) != 6 {
		t.Fatal("discovered includes the duplicate")
	}
	if existing != 1 || admitted != 2 || overflow != 2 {
		t.Fatalf("existing=%d admitted=%d overflow=%d", existing, admitted, overflow)
	}
	if existing+admitted+overflow > int64(len(files)) {
		t.Fatal("sum exceeds discovered")
	}
	if first[0].Ordinal != 0 || first[1].Ordinal != 1 || first[2].Ordinal != 3 {
		t.Fatalf("ordinals %+v", first)
	}
}
