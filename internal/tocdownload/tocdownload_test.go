package tocdownload

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/riverqueue/river"
)

func TestClassifyClaim(t *testing.T) {
	t.Parallel()
	job := int64(72)
	same := job
	other := int64(9)
	cases := []struct {
		name     string
		download string
		parse    string
		stored   *int64
		want     string
		err      string
	}{
		{"succeeded ignores parse", jobs.StatusSucceeded, jobs.StatusPending, &same, jobs.ClaimNoop, ""},
		{"failed", jobs.StatusFailed, jobs.StatusBlocked, &same, jobs.ClaimNoop, ""},
		{"stale pending", jobs.StatusPending, jobs.StatusBlocked, &other, jobs.ClaimNoop, ""},
		{"nil job", jobs.StatusPending, jobs.StatusBlocked, nil, jobs.ClaimNoop, ""},
		{"pending blocked", jobs.StatusPending, jobs.StatusBlocked, &same, jobs.ClaimWork, ""},
		{"running blocked", jobs.StatusRunning, jobs.StatusBlocked, &same, jobs.ClaimWork, ""},
		{"pending parse not blocked", jobs.StatusPending, jobs.StatusPending, &same, "", jobs.FailureDomainInvariant},
		{"blocked download", jobs.StatusBlocked, jobs.StatusBlocked, &same, "", jobs.FailureDomainInvariant},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := classifyClaim(tc.download, tc.parse, tc.stored, job)
			if tc.err != "" {
				if !jobs.IsFailure(err, tc.err) {
					t.Fatalf("got %v", err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %s %v want %s", got, err, tc.want)
			}
		})
	}
}

func TestMapDownloadError(t *testing.T) {
	t.Parallel()
	if err := mapDownloadError(context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if err := mapDownloadError(context.DeadlineExceeded); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline: %v", err)
	}
	secret := "https://secret.example.invalid/toc-72"
	err := mapDownloadError(errors.Join(artifact.ErrDownload, errors.New(secret)))
	if !jobs.IsFailure(err, jobs.FailureTOCDownload) {
		t.Fatalf("download: %v", err)
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "72") {
		t.Fatalf("exposed: %v", err)
	}
	err = mapDownloadError(fmt.Errorf("%w: %w", artifact.ErrDownload, artifact.ErrHTTPNotFound))
	if !jobs.IsFailure(err, jobs.FailureTOCDownloadNotFound) {
		t.Fatalf("not found: %v", err)
	}
	err = mapDownloadError(errors.Join(artifact.ErrArtifact, errors.New("/tmp/artifacts/toc/toc-72/download")))
	if !jobs.IsFailure(err, jobs.FailureTOCDownload) {
		t.Fatalf("artifact: %v", err)
	}
	if strings.Contains(err.Error(), "toc-72") || strings.Contains(err.Error(), "/tmp") {
		t.Fatalf("exposed path: %v", err)
	}
}

func TestWorkerRegistration(t *testing.T) {
	t.Parallel()
	args := jobs.TOCDownloadArgs{}
	if args.Kind() != jobs.KindTOCDownload {
		t.Fatal("kind")
	}
	if args.InsertOpts().Queue != jobs.QueueTOCDownload || args.InsertOpts().MaxAttempts != 4 {
		t.Fatal("queue")
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, &Worker{})
}
