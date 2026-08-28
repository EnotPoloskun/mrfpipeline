package artifact

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

// ProgressID is the Story 03 progress identity for one executing River job.
type ProgressID struct {
	JobID int64
	Kind  string
	Queue string
}

// DownloadResult is a published local data file.
type DownloadResult struct {
	DataPath  string
	ByteCount int64
}

// Download publishes or reuses one TOC/MRF download leaf. A complete existing
// download is reused without an HTTP request or progress logs.
func (d *Downloader) Download(ctx context.Context, kind string, id int64, rawURL string, prog ProgressID) (res DownloadResult, err error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return DownloadResult{}, err
	}
	if d == nil || d.ws == nil || d.client == nil {
		return DownloadResult{}, artErr("downloader")
	}
	if _, err := parsePublicURL(rawURL); err != nil {
		return DownloadResult{}, err
	}
	if _, err := d.ws.ensureRecord(kind, id); err != nil {
		return DownloadResult{}, err
	}
	final, err := d.ws.downloadPath(kind, id)
	if err != nil {
		return DownloadResult{}, err
	}
	if complete, n, err := d.ws.inspectDownloadDir(final); err != nil {
		return DownloadResult{}, err
	} else if complete {
		return DownloadResult{DataPath: filepath.Join(final, fileData), ByteCount: n}, nil
	} else {
		info, lerr := os.Lstat(final)
		if lerr == nil {
			if isSymlink(info) {
				return DownloadResult{}, artErr("symlink")
			}
			if err := d.ws.RemoveDownload(kind, id); err != nil {
				return DownloadResult{}, err
			}
		} else if !os.IsNotExist(lerr) {
			return DownloadResult{}, artErr("stat")
		}
	}
	if err := d.ws.cleanMatchingStaging(kind, id); err != nil {
		return DownloadResult{}, err
	}

	prefix, err := stagingPrefix(kind, id)
	if err != nil {
		return DownloadResult{}, err
	}
	staging, err := os.MkdirTemp(d.ws.StagingDir(), prefix)
	if err != nil {
		return DownloadResult{}, artErr("create")
	}
	defer func() {
		if staging == "" {
			return
		}
		if cerr := removeExactDir(staging); cerr != nil && err != nil {
			err = joinErr(err, artErr("cleanup"))
		} else if cerr != nil && err == nil {
			err = cerr
		}
	}()

	if _, err := d.transfer(ctx, staging, rawURL, prog); err != nil {
		return DownloadResult{}, err
	}
	if err := verifyAbsentOrTake(d.ws, final, staging); err != nil {
		return DownloadResult{}, err
	}
	complete, got, ierr := d.ws.inspectDownloadDir(final)
	if ierr != nil {
		return DownloadResult{}, ierr
	}
	if !complete {
		return DownloadResult{}, artErr("conflict")
	}
	if err := removeExactDir(staging); err != nil {
		return DownloadResult{}, err
	}
	staging = ""
	return DownloadResult{DataPath: filepath.Join(final, fileData), ByteCount: got}, nil
}

func verifyAbsentOrTake(ws *Workspace, final, staging string) error {
	if err := ws.verifyChain(final, false); err != nil {
		return err
	}
	_, err := os.Lstat(final)
	if err == nil {
		complete, _, ierr := ws.inspectDownloadDir(final)
		if ierr != nil {
			return ierr
		}
		if complete {
			return nil
		}
		return artErr("conflict")
	}
	if !os.IsNotExist(err) {
		return artErr("stat")
	}
	if err := os.Rename(staging, final); err != nil {
		if _, lerr := os.Lstat(final); lerr == nil {
			complete, _, ierr := ws.inspectDownloadDir(final)
			if ierr != nil {
				return ierr
			}
			if complete {
				return nil
			}
			return artErr("conflict")
		}
		return artErr("rename")
	}
	return nil
}

func (d *Downloader) transfer(ctx context.Context, staging, rawURL string, prog ProgressID) (int64, error) {
	dataPath := filepath.Join(staging, fileData)
	f, err := os.OpenFile(dataPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fileMode)
	if err != nil {
		return 0, artErr("create")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		_ = f.Close()
		return 0, classifyDL(ctx, "request", err)
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept-Encoding", acceptEncoding)
	resp, err := d.client.Do(req)
	if err != nil {
		_ = f.Close()
		return 0, classifyDL(ctx, "request", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		_ = f.Close()
		return 0, fmt.Errorf("%w: %w", ErrDownload, ErrHTTPNotFound)
	}
	if resp.StatusCode != http.StatusOK {
		return closeFail(f, statusClass(resp.StatusCode))
	}
	total := int64(0)
	if resp.ContentLength > 0 || (resp.ContentLength == 0 && hasContentLength(resp)) {
		if resp.ContentLength < 0 {
			return closeFail(f, "length")
		}
		total = resp.ContentLength
	}
	if prog.JobID > 0 {
		_ = d.progress.Log(jobs.ProgressParams{
			JobID: prog.JobID, Kind: prog.Kind, Queue: prog.Queue,
			Phase: "download", CopiedBytes: 0, TotalBytes: total,
		})
	}
	pw := &progressWriter{w: f, p: d.progress, id: prog, total: total}
	buf := make([]byte, CopyBufferSize)
	n, copyErr := io.CopyBuffer(pw, resp.Body, buf)
	closeErr := f.Close()
	if copyErr != nil {
		return 0, classifyDL(ctx, "copy", copyErr)
	}
	if closeErr != nil {
		return 0, artErr("close")
	}
	if bodyErr := resp.Body.Close(); bodyErr != nil {
		return 0, classifyDL(ctx, "close", bodyErr)
	}
	if total > 0 || hasContentLength(resp) {
		if n != resp.ContentLength {
			return 0, dlErr("length")
		}
	}
	if prog.JobID > 0 {
		_ = d.progress.Log(jobs.ProgressParams{
			JobID: prog.JobID, Kind: prog.Kind, Queue: prog.Queue,
			Phase: "download", CopiedBytes: n, TotalBytes: total, Done: true,
		})
	}
	man, err := downloadManifestJSON(n)
	if err != nil {
		return 0, err
	}
	if err := writeExclusive(filepath.Join(staging, fileManifest), man); err != nil {
		return 0, err
	}
	return n, nil
}

func hasContentLength(resp *http.Response) bool {
	if resp == nil {
		return false
	}
	return resp.Header.Get("Content-Length") != ""
}

func closeFail(f *os.File, op string) (int64, error) {
	_ = f.Close()
	return 0, dlErr(op)
}

func statusClass(code int) string {
	switch {
	case code >= 400 && code <= 499:
		return "http_4xx"
	case code >= 500 && code <= 599:
		return "http_5xx"
	default:
		return "http_status"
	}
}

type progressWriter struct {
	w     io.Writer
	p     *jobs.Progress
	id    ProgressID
	total int64
	n     int64
}

func (pw *progressWriter) Write(b []byte) (int, error) {
	n, err := pw.w.Write(b)
	pw.n += int64(n)
	if pw.id.JobID > 0 && pw.p != nil {
		_ = pw.p.Log(jobs.ProgressParams{
			JobID: pw.id.JobID, Kind: pw.id.Kind, Queue: pw.id.Queue,
			Phase: "download", CopiedBytes: pw.n, TotalBytes: pw.total,
		})
	}
	return n, err
}

func (w *Workspace) cleanMatchingStaging(kind string, id int64) error {
	dir := w.StagingDir()
	if err := w.verifyChain(dir, true); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return artErr("read")
	}
	for _, e := range entries {
		gotKind, gotID, ok := parseStagingName(e.Name())
		if !ok || gotKind != kind || gotID != id {
			continue
		}
		p := filepath.Join(dir, e.Name())
		info, err := os.Lstat(p)
		if err != nil {
			return artErr("stat")
		}
		if isSymlink(info) || !info.IsDir() {
			return artErr("staging")
		}
		if err := removeExactDir(p); err != nil {
			return err
		}
	}
	return nil
}
