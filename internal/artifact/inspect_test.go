package artifact

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestParserInspectAndReset(t *testing.T) {
	t.Parallel()
	ws := mustInit(t, filepath.Join(t.TempDir(), "ws"))
	if _, err := ws.ensureRecord(KindTOC, 7); err != nil {
		t.Fatal(err)
	}
	state, err := ws.InspectParsed(KindTOC, 7)
	if err != nil || state != ParsedAbsent {
		t.Fatalf("%s %v", state, err)
	}
	if err := ws.ResetParsed(KindTOC, 7); err != nil {
		t.Fatal(err)
	}
	state, err = ws.InspectParsed(KindTOC, 7)
	if err != nil || state != ParsedEmpty {
		t.Fatalf("empty %s %v", state, err)
	}
	if err := os.WriteFile(filepath.Join(ws.Root, dirTOC, "toc-7", dirParsed, "partial"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	state, err = ws.InspectParsed(KindTOC, 7)
	if err != nil || state != ParsedIncomplete {
		t.Fatalf("incomplete %s %v", state, err)
	}
	other := filepath.Join(ws.Root, dirTOC, "toc-8")
	if err := os.MkdirAll(filepath.Join(other, dirParsed), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(other, dirParsed, "keep"), []byte("y"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ws.ResetParsed(KindTOC, 7); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(other, dirParsed, "keep")); err != nil {
		t.Fatal("touched sibling")
	}
	state, err = ws.InspectParsed(KindTOC, 7)
	if err != nil || state != ParsedEmpty {
		t.Fatalf("reset %s %v", state, err)
	}
	if err := os.WriteFile(filepath.Join(ws.Root, dirTOC, "toc-7", dirParsed, fileManifest), []byte(`{"ok":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	state, err = ws.InspectParsed(KindTOC, 7)
	if err != nil || state != ParsedManifestPresent {
		t.Fatalf("present %s %v", state, err)
	}
	if err := ws.ResetParsed(KindTOC, 7); err == nil {
		t.Fatal("removed manifest_present")
	}
	if _, err := os.Lstat(filepath.Join(ws.Root, dirTOC, "toc-7", dirParsed, fileManifest)); err != nil {
		t.Fatal("deleted completed parse")
	}
}

func TestRemoveDownloadIdempotent(t *testing.T) {
	t.Parallel()
	ws := mustInit(t, filepath.Join(t.TempDir(), "ws"))
	if err := ws.RemoveDownload(KindMRF, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.ensureRecord(KindMRF, 3); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(ws.Root, dirMRF, "mrf-source-3", dirDownload)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, fileData), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ws.RemoveDownload(KindMRF, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(dir); !os.IsNotExist(err) {
		t.Fatal("download remained")
	}
	if _, err := os.Lstat(filepath.Join(ws.Root, dirMRF, "mrf-source-3")); err != nil {
		t.Fatal("removed id directory")
	}
}

func TestInspectDownloadRequiresCompletedLeaf(t *testing.T) {
	t.Parallel()
	ws := mustInit(t, filepath.Join(t.TempDir(), "ws"))
	if _, err := ws.InspectDownload(KindTOC, 4); !errors.Is(err, ErrArtifact) {
		t.Fatalf("absent: %v", err)
	}
	if _, err := ws.ensureRecord(KindTOC, 4); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(ws.Root, dirTOC, "toc-4", dirDownload)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.InspectDownload(KindTOC, 4); !errors.Is(err, ErrArtifact) {
		t.Fatalf("empty dir: %v", err)
	}
	body := []byte("toc-bytes")
	if err := os.WriteFile(filepath.Join(dir, fileData), body, 0600); err != nil {
		t.Fatal(err)
	}
	man, err := downloadManifestJSON(int64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, fileManifest), man, 0600); err != nil {
		t.Fatal(err)
	}
	n, err := ws.InspectDownload(KindTOC, 4)
	if err != nil || n != int64(len(body)) {
		t.Fatalf("complete %d %v", n, err)
	}
	if err := os.WriteFile(filepath.Join(dir, fileManifest), []byte(`{"schema_version":"9.0.0","byte_count":9}`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.InspectDownload(KindTOC, 4); !errors.Is(err, ErrArtifact) {
		t.Fatalf("unsupported schema: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(dir, fileManifest)); err != nil {
		t.Fatal("inspect deleted unsupported schema")
	}
}

func TestSymlinkRejectedOnInspect(t *testing.T) {
	t.Parallel()
	ws := mustInit(t, filepath.Join(t.TempDir(), "ws"))
	if _, err := ws.ensureRecord(KindTOC, 1); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "outside")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(ws.Root, dirTOC, "toc-1", dirParsed)
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.InspectParsed(KindTOC, 1); err == nil {
		t.Fatal("followed symlink")
	}
}
