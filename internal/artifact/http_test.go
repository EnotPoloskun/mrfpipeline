package artifact

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

const publicTestIP = "203.0.113.10"

func hookDownloader(t *testing.T, ws *Workspace, ts *httptest.Server, header time.Duration, logBuf *bytes.Buffer) *Downloader {
	t.Helper()
	if header <= 0 {
		header = headerTimeout
	}
	var tlsCfg *tls.Config
	if ts != nil && strings.HasPrefix(ts.URL, "https://") && ts.Certificate() != nil {
		pool := x509.NewCertPool()
		pool.AddCert(ts.Certificate())
		tlsCfg = &tls.Config{RootCAs: pool, ServerName: "example.com"}
	}
	var progress *jobs.Progress
	if logBuf != nil {
		progress = jobs.NewProgress(jobs.NewLogger(logBuf))
	}
	d := newDownloader(ws, progress, header, tlsCfg,
		func(context.Context, string) ([]net.IP, error) {
			return []net.IP{net.ParseIP(publicTestIP)}, nil
		},
		func(ctx context.Context, network, address string) (net.Conn, error) {
			var nd net.Dialer
			return nd.DialContext(ctx, "tcp", ts.Listener.Addr().String())
		},
	)
	return d
}

func testURL(path string) string {
	if path == "" {
		path = "/data"
	}
	return "http://files.test" + path
}

func testProg() ProgressID {
	return ProgressID{JobID: 9, Kind: jobs.KindTOCDownload, Queue: jobs.QueueTOCDownload}
}

func TestDownloadStoresBodiesAndHeaders(t *testing.T) {
	t.Parallel()
	bodies := [][]byte{
		[]byte(`{"ok":true}`),
		{0x1f, 0x8b, 0x08, 0x00, 0x01, 0x02},
		{},
		bytes.Repeat([]byte("a"), CopyBufferSize+100),
	}
	for i, body := range bodies {
		body := body
		t.Run(string(rune('a'+i)), func(t *testing.T) {
			t.Parallel()
			var sawUA, sawAE atomic.Bool
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("User-Agent") == userAgent {
					sawUA.Store(true)
				}
				if r.Header.Get("Accept-Encoding") == acceptEncoding {
					sawAE.Store(true)
				}
				w.Header().Set("Content-Encoding", "gzip")
				_, _ = w.Write(body)
			}))
			t.Cleanup(ts.Close)
			var logs bytes.Buffer
			ws := mustInit(t, filepath.Join(t.TempDir(), "ws"))
			d := hookDownloader(t, ws, ts, 0, &logs)
			res, err := d.Download(context.Background(), KindTOC, int64(i+1), testURL("/data"), testProg())
			if err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(res.DataPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, body) || res.ByteCount != int64(len(body)) {
				t.Fatalf("bytes %d vs %d", res.ByteCount, len(body))
			}
			if !sawUA.Load() || !sawAE.Load() {
				t.Fatal("headers")
			}
			if strings.Contains(logs.String(), "https://") || strings.Contains(logs.String(), "files.test") || strings.Contains(logs.String(), ws.Root) {
				t.Fatalf("leaked: %s", logs.String())
			}
		})
	}
}

func TestDownloadChunkedAndContentLength(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/chunked" {
			fl, ok := w.(http.Flusher)
			if !ok {
				t.Fatal("flusher")
			}
			w.Header().Set("Transfer-Encoding", "chunked")
			fl.Flush()
			_, _ = w.Write([]byte("hel"))
			fl.Flush()
			_, _ = w.Write([]byte("lo"))
			return
		}
		if r.URL.Path == "/short" {
			w.Header().Set("Content-Length", "10")
			_, _ = w.Write([]byte("nope"))
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)
	ws := mustInit(t, filepath.Join(t.TempDir(), "ws"))
	d := hookDownloader(t, ws, ts, 0, nil)
	res, err := d.Download(context.Background(), KindTOC, 1, testURL("/chunked"), testProg())
	if err != nil || res.ByteCount != 5 {
		t.Fatalf("chunked %+v %v", res, err)
	}
	if _, err := d.Download(context.Background(), KindTOC, 2, testURL("/short"), testProg()); !errors.Is(err, ErrDownload) {
		t.Fatalf("length: %v", err)
	}
	final := filepath.Join(ws.Root, dirTOC, "toc-2", dirDownload)
	if _, err := os.Lstat(final); !os.IsNotExist(err) {
		t.Fatal("published short body")
	}
}

func TestDownloadReuseAndIncomplete(t *testing.T) {
	t.Parallel()
	var hits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("abc"))
	}))
	t.Cleanup(ts.Close)
	ws := mustInit(t, filepath.Join(t.TempDir(), "ws"))
	d := hookDownloader(t, ws, ts, 0, nil)
	res, err := d.Download(context.Background(), KindMRF, 4, testURL("/data"), testProg())
	if err != nil {
		t.Fatal(err)
	}
	again, err := d.Download(context.Background(), KindMRF, 4, testURL("/data"), testProg())
	if err != nil || again.ByteCount != res.ByteCount {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatalf("hits %d", hits.Load())
	}
	dir := filepath.Join(ws.Root, dirMRF, "mrf-source-5", dirDownload)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, fileData), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	res, err = d.Download(context.Background(), KindMRF, 5, testURL("/data"), testProg())
	if err != nil || res.ByteCount != 3 {
		t.Fatalf("%+v %v", res, err)
	}
	got, _ := os.ReadFile(res.DataPath)
	if string(got) != "abc" {
		t.Fatalf("got %q", got)
	}
}

func TestDownloadNoFinalUntilRename(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		_, _ = w.Write([]byte("xyz"))
	}))
	t.Cleanup(ts.Close)
	ws := mustInit(t, filepath.Join(t.TempDir(), "ws"))
	d := hookDownloader(t, ws, ts, 0, nil)
	errc := make(chan error, 1)
	go func() {
		_, err := d.Download(context.Background(), KindTOC, 9, testURL("/data"), testProg())
		errc <- err
	}()
	<-started
	final := filepath.Join(ws.Root, dirTOC, "toc-9", dirDownload)
	if _, err := os.Lstat(final); !os.IsNotExist(err) {
		t.Fatal("final exists during transfer")
	}
	close(release)
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestDownloadStatusAndNoRetry(t *testing.T) {
	t.Parallel()
	var hits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("secret body"))
	}))
	t.Cleanup(ts.Close)
	ws := mustInit(t, filepath.Join(t.TempDir(), "ws"))
	d := hookDownloader(t, ws, ts, 0, nil)
	err := error(nil)
	_, err = d.Download(context.Background(), KindTOC, 1, testURL("/data"), testProg())
	if !errors.Is(err, ErrDownload) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "500") {
		t.Fatalf("leaked: %v", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("retried %d", hits.Load())
	}
}

func TestDownloadCancelAndHeaderTimeout(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slowhdr" {
			time.Sleep(200 * time.Millisecond)
			_, _ = w.Write([]byte("late"))
			return
		}
		time.Sleep(time.Second)
		_, _ = w.Write([]byte("slow"))
	}))
	t.Cleanup(ts.Close)
	ws := mustInit(t, filepath.Join(t.TempDir(), "ws"))
	d := hookDownloader(t, ws, ts, 50*time.Millisecond, nil)
	_, err := d.Download(context.Background(), KindTOC, 1, testURL("/slowhdr"), testProg())
	if !errors.Is(err, ErrDownload) || errors.Is(err, context.Canceled) {
		t.Fatalf("header timeout: %v", err)
	}
	started := make(chan struct{})
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		time.Sleep(time.Second)
		_, _ = w.Write([]byte("slow"))
	}))
	t.Cleanup(ts2.Close)
	ctx, cancel := context.WithCancel(context.Background())
	d2 := hookDownloader(t, ws, ts2, time.Minute, nil)
	errc := make(chan error, 1)
	go func() {
		_, err := d2.Download(ctx, KindTOC, 2, testURL("/slow"), testProg())
		errc <- err
	}()
	<-started
	cancel()
	err = <-errc
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrDownload) {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(ws.Root, dirTOC, "toc-1", dirDownload)); !os.IsNotExist(err) {
		t.Fatal("timeout published")
	}
	if _, err := os.Lstat(filepath.Join(ws.Root, dirTOC, "toc-2", dirDownload)); !os.IsNotExist(err) {
		t.Fatal("cancel published")
	}
}

func TestURLPolicyAndRedirects(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	hops := map[string]string{
		"/r1": "/r2", "/r2": "/r3", "/r3": "/r4", "/r4": "/r5", "/r5": "/ok",
	}
	for from, to := range hops {
		from, to := from, to
		mux.HandleFunc(from, func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, to, http.StatusFound)
		})
	}
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("done")) })
	mux.HandleFunc("/rel", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/ok")
		w.WriteHeader(http.StatusFound)
	})
	mux.HandleFunc("/six1", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/six2", http.StatusFound) })
	mux.HandleFunc("/six2", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/six3", http.StatusFound) })
	mux.HandleFunc("/six3", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/six4", http.StatusFound) })
	mux.HandleFunc("/six4", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/six5", http.StatusFound) })
	mux.HandleFunc("/six5", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/six6", http.StatusFound) })
	mux.HandleFunc("/six6", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/ok", http.StatusFound) })
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	ws := mustInit(t, filepath.Join(t.TempDir(), "ws"))
	d := hookDownloader(t, ws, ts, 0, nil)
	res, err := d.Download(context.Background(), KindTOC, 1, testURL("/r1"), testProg())
	if err != nil || res.ByteCount != 4 {
		t.Fatalf("five redirects %+v %v", res, err)
	}
	res, err = d.Download(context.Background(), KindTOC, 2, testURL("/rel"), testProg())
	if err != nil || res.ByteCount != 4 {
		t.Fatalf("relative %+v %v", res, err)
	}
	if _, err := d.Download(context.Background(), KindTOC, 3, testURL("/six1"), testProg()); !errors.Is(err, ErrDownload) {
		t.Fatalf("sixth: %v", err)
	}
	rejects := []string{
		"ftp://files.test/x",
		"http://user:pass@files.test/x",
		"http://files.test/\x00",
		"not a url",
	}
	for _, raw := range rejects {
		_, err := d.Download(context.Background(), KindTOC, 8, raw, testProg())
		if !errors.Is(err, ErrDownload) {
			t.Fatalf("%s: %v", raw, err)
		}
		if strings.Contains(err.Error(), "files.test") || strings.Contains(err.Error(), "pass") {
			t.Fatalf("leaked: %v", err)
		}
	}
}

func TestHTTPSDowngradeRejected(t *testing.T) {
	t.Parallel()
	httpTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("downgrade request sent")
	}))
	t.Cleanup(httpTS.Close)
	httpsTS := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, httpTS.URL, http.StatusFound)
	}))
	t.Cleanup(httpsTS.Close)
	ws := mustInit(t, filepath.Join(t.TempDir(), "ws"))
	d := hookDownloader(t, ws, httpsTS, 0, nil)
	_, err := d.Download(context.Background(), KindTOC, 1, "https://files.test/start", testProg())
	if !errors.Is(err, ErrDownload) {
		t.Fatalf("got %v", err)
	}
}

func TestForbiddenAddresses(t *testing.T) {
	t.Parallel()
	ws := mustInit(t, filepath.Join(t.TempDir(), "ws"))
	addrs := [][]net.IP{
		{net.ParseIP("127.0.0.1")},
		{net.ParseIP("0.0.0.0")},
		{net.ParseIP("169.254.1.1")},
		{net.ParseIP("224.0.0.1")},
		{net.ParseIP("10.1.2.3")},
		{net.ParseIP("::1")},
		{net.ParseIP("fe80::1")},
		{net.ParseIP("fc00::1")},
		{net.ParseIP("ff02::1")},
		{net.ParseIP("::ffff:10.0.0.1")},
		{net.ParseIP("8.8.8.8"), net.ParseIP("10.0.0.1")},
	}
	for i, set := range addrs {
		set := set
		d := newDownloader(ws, nil, time.Second, nil,
			func(context.Context, string) ([]net.IP, error) { return set, nil },
			func(context.Context, string, string) (net.Conn, error) {
				t.Fatal("dialed forbidden")
				return nil, errors.New("dial")
			},
		)
		_, err := d.Download(context.Background(), KindTOC, int64(i+1), testURL("/x"), testProg())
		if !errors.Is(err, ErrDownload) {
			t.Fatalf("%v: %v", set, err)
		}
	}
	d := newDownloader(ws, nil, time.Second, nil, nil, nil)
	_, err := d.Download(context.Background(), KindTOC, 99, "http://127.0.0.1/x", testProg())
	if !errors.Is(err, ErrDownload) {
		t.Fatalf("literal: %v", err)
	}
}

func TestProgressOmitsPercentAndSecrets(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fl := w.(http.Flusher)
		w.Header().Set("Transfer-Encoding", "chunked")
		fl.Flush()
		_, _ = w.Write([]byte("abcdef"))
	}))
	t.Cleanup(ts.Close)
	var logs bytes.Buffer
	ws := mustInit(t, filepath.Join(t.TempDir(), "ws"))
	d := hookDownloader(t, ws, ts, 0, &logs)
	if _, err := d.Download(context.Background(), KindTOC, 1, testURL("/data"), testProg()); err != nil {
		t.Fatal(err)
	}
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal(line, &obj); err != nil {
			t.Fatal(err)
		}
		if obj["msg"] == "progress" {
			if _, ok := obj["percent"]; ok {
				t.Fatalf("percent: %v", obj)
			}
			if obj["phase"] != "download" {
				t.Fatalf("phase %v", obj)
			}
		}
		s := string(line)
		if strings.Contains(s, "files.test") || strings.Contains(s, "/tmp") || strings.Contains(s, "https://") {
			t.Fatalf("leaked %s", s)
		}
	}
	_ = CopyBufferSize
}

func TestCopyBufferSize(t *testing.T) {
	t.Parallel()
	if CopyBufferSize != 256*1024 {
		t.Fatalf("buffer %d", CopyBufferSize)
	}
}

func TestCanceledContextNotDownload(t *testing.T) {
	t.Parallel()
	ws := mustInit(t, filepath.Join(t.TempDir(), "ws"))
	d := NewDownloader(ws, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := d.Download(ctx, KindTOC, 1, testURL("/x"), testProg())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestIsForbiddenIP(t *testing.T) {
	t.Parallel()
	if !isForbiddenIP(net.ParseIP("192.168.1.1")) || !isForbiddenIP(net.ParseIP("172.16.0.1")) {
		t.Fatal("rfc1918")
	}
	if isForbiddenIP(net.ParseIP("8.8.8.8")) || isForbiddenIP(net.ParseIP("1.1.1.1")) {
		t.Fatal("public")
	}
	if isForbiddenIP(net.ParseIP("100.64.0.1")) {
		t.Fatal("cgnat must not be extra-blocked")
	}
}
