package mrfdownload

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

func TestClassifyClaim(t *testing.T) {
	t.Parallel()
	job := int64(151)
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
	secret := "https://secret.example.invalid/mrf-source-151?token=abc"
	err := mapDownloadError(errors.Join(artifact.ErrDownload, errors.New(secret)))
	if !jobs.IsFailure(err, jobs.FailureMRFDownload) {
		t.Fatalf("download: %v", err)
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "151") {
		t.Fatalf("exposed: %v", err)
	}
	err = mapDownloadError(errors.Join(artifact.ErrArtifact, errors.New("/tmp/artifacts/mrf/mrf-source-151/download")))
	if !jobs.IsFailure(err, jobs.FailureMRFDownload) {
		t.Fatalf("artifact: %v", err)
	}
	if strings.Contains(err.Error(), "mrf-source-151") || strings.Contains(err.Error(), "/tmp") {
		t.Fatalf("exposed path: %v", err)
	}
}

func TestArgsAndGeneratedPath(t *testing.T) {
	t.Parallel()
	args := jobs.MRFDownloadArgs{MRFSourceID: 151}
	if args.Kind() != jobs.KindMRFDownload || args.InsertOpts().Queue != jobs.QueueMRFDownload {
		t.Fatal("args")
	}
	if (jobs.MRFParseArgs{MRFSourceID: 151}).Kind() != jobs.KindMRFParse {
		t.Fatal("parse kind")
	}
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := ws.DownloadDataPath(artifact.KindMRF, 151)
	if err != nil || !strings.HasSuffix(p, filepath.Join("mrf", "mrf-source-151", "download", "data")) {
		t.Fatalf("path %s %v", p, err)
	}
	name, err := artifact.RecordDirName(artifact.KindMRF, 151)
	if err != nil || name != "mrf-source-151" {
		t.Fatalf("name %s %v", name, err)
	}
}

func TestDownloadUsesStoredURLAndMRFKind(t *testing.T) {
	t.Parallel()
	var host, path string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, path = r.Host, r.URL.Path
		_, _ = w.Write([]byte("body"))
	}))
	t.Cleanup(ts.Close)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	dl := artifact.NewTestDownloader(ws, jobs.NewProgress(jobs.NewLogger(bytes.NewBuffer(nil))),
		func(context.Context, string) ([]net.IP, error) {
			return []net.IP{net.ParseIP("203.0.113.10")}, nil
		},
		func(ctx context.Context, network, address string) (net.Conn, error) {
			var nd net.Dialer
			return nd.DialContext(ctx, "tcp", ts.Listener.Addr().String())
		},
	)
	w := &Worker{Downloader: dl}
	if err := w.download(context.Background(), downloadJob(9, 151), "http://files.test/exact-source"); err != nil {
		t.Fatal(err)
	}
	if host != "files.test" || path != "/exact-source" {
		t.Fatalf("request %s %s", host, path)
	}
	n, err := ws.InspectDownload(artifact.KindMRF, 151)
	if err != nil || n != 4 {
		t.Fatalf("inspect %d %v", n, err)
	}
}

func TestProgressLiveVersusReuse(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte("x"), 4096))
	}))
	t.Cleanup(ts.Close)
	ws, err := artifact.Init(context.Background(), filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	var live bytes.Buffer
	dl := artifact.NewTestDownloader(ws, jobs.NewProgress(jobs.NewLogger(&live)),
		func(context.Context, string) ([]net.IP, error) {
			return []net.IP{net.ParseIP("203.0.113.10")}, nil
		},
		func(ctx context.Context, network, address string) (net.Conn, error) {
			var nd net.Dialer
			return nd.DialContext(ctx, "tcp", ts.Listener.Addr().String())
		},
	)
	w := &Worker{Downloader: dl}
	if err := w.download(context.Background(), downloadJob(9, 4), "http://files.test/data"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(live.String(), `"phase":"download"`) {
		t.Fatalf("live progress %s", live.String())
	}
	if strings.Contains(live.String(), "files.test") || strings.Contains(live.String(), "mrf-source") {
		t.Fatalf("leaked %s", live.String())
	}
	var reuse bytes.Buffer
	dl2 := artifact.NewTestDownloader(ws, jobs.NewProgress(jobs.NewLogger(&reuse)),
		func(context.Context, string) ([]net.IP, error) {
			t.Fatal("reuse resolved")
			return nil, nil
		},
		func(context.Context, string, string) (net.Conn, error) {
			t.Fatal("reuse dialed")
			return nil, errors.New("dial")
		},
	)
	if err := (&Worker{Downloader: dl2}).download(context.Background(), downloadJob(9, 4), "http://files.test/data"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(reuse.String(), "download") {
		t.Fatalf("reuse progress %s", reuse.String())
	}
}

func TestWorkerRegistration(t *testing.T) {
	t.Parallel()
	workers := river.NewWorkers()
	river.AddWorker(workers, &Worker{})
}

func downloadJob(jobID, sourceID int64) *river.Job[jobs.MRFDownloadArgs] {
	return &river.Job[jobs.MRFDownloadArgs]{
		JobRow: &rivertype.JobRow{ID: jobID},
		Args:   jobs.MRFDownloadArgs{MRFSourceID: sourceID},
	}
}
