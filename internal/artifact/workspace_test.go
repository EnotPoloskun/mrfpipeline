package artifact

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func mustInit(t *testing.T, root string) *Workspace {
	t.Helper()
	ws, err := Init(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

func TestInitAbsentAndRepeat(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "artifacts")
	ws := mustInit(t, root)
	if ws.Root == "" {
		t.Fatal("empty physical root")
	}
	raw, err := os.ReadFile(filepath.Join(ws.Root, markerName))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != string(markerJSON()) {
		t.Fatalf("marker %q", raw)
	}
	for _, name := range fixedDirs {
		if err := requireRealDir(filepath.Join(ws.Root, name)); err != nil {
			t.Fatal(err)
		}
	}
	again := mustInit(t, root)
	if again.Root != ws.Root {
		t.Fatalf("physical %q vs %q", again.Root, ws.Root)
	}
	if again.StagingDir() != filepath.Join(ws.Root, stagingName) {
		t.Fatal("staging")
	}
}

func TestInitEmptyRootAndTmpMarker(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "empty")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	mustInit(t, root)

	root2 := filepath.Join(t.TempDir(), "tmpmark")
	if err := os.Mkdir(root2, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root2, markerTmpName), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	mustInit(t, root2)
	if _, err := os.Lstat(filepath.Join(root2, markerTmpName)); !os.IsNotExist(err) {
		t.Fatal("tmp marker remained")
	}
}

func TestInitRejectsUnrecognized(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		prep func(string)
	}{
		{"ds_store", func(root string) {
			os.Mkdir(root, 0700)
			os.WriteFile(filepath.Join(root, ".DS_Store"), []byte("x"), 0644)
		}},
		{"unmarked", func(root string) {
			os.Mkdir(root, 0700)
			os.WriteFile(filepath.Join(root, "notes"), []byte("x"), 0644)
		}},
		{"bad version", func(root string) {
			os.Mkdir(root, 0700)
			os.WriteFile(filepath.Join(root, markerName), []byte(`{"application":"mrfpipeline","schema_version":"9.0.0"}`+"\n"), 0600)
		}},
		{"file root", func(root string) {
			os.WriteFile(root, []byte("x"), 0600)
		}},
		{"symlink root", func(root string) {
			target := root + "-target"
			os.Mkdir(target, 0700)
			os.Symlink(target, root)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := filepath.Join(t.TempDir(), "root")
			tc.prep(root)
			_, err := Init(context.Background(), root)
			if !errors.Is(err, ErrArtifact) {
				t.Fatalf("got %v", err)
			}
			if strings.Contains(err.Error(), root) {
				t.Fatalf("leaked path: %v", err)
			}
			if tc.name == "ds_store" {
				if _, err := os.Lstat(filepath.Join(root, ".DS_Store")); err != nil {
					t.Fatal("deleted extra entry")
				}
			}
			if tc.name == "symlink root" {
				info, err := os.Lstat(root)
				if err != nil || !isSymlink(info) {
					t.Fatal("rewrote symlink root")
				}
			}
		})
	}
}

func TestInitRejectsSymlinkParent(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	real := filepath.Join(base, "real")
	if err := os.Mkdir(real, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(link, "artifacts")
	_, err := Init(context.Background(), root)
	if !errors.Is(err, ErrArtifact) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), root) || strings.Contains(err.Error(), link) {
		t.Fatalf("leaked path: %v", err)
	}
	if _, err := os.Lstat(root); !os.IsNotExist(err) {
		t.Fatal("created root through symlink parent")
	}
	if _, err := os.Lstat(filepath.Join(real, "artifacts")); !os.IsNotExist(err) {
		t.Fatal("created root inside symlink target")
	}
}

func TestInitRepairsMissingFixedDir(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "ws")
	mustInit(t, root)
	if err := os.Remove(filepath.Join(root, stagingName)); err != nil {
		t.Fatal(err)
	}
	mustInit(t, root)
	if err := requireRealDir(filepath.Join(root, stagingName)); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(root, dirTOC)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, dirTOC), []byte("nope"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Init(context.Background(), root); !errors.Is(err, ErrArtifact) {
		t.Fatalf("wrong type: %v", err)
	}
}

func TestInitCanceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Init(ctx, filepath.Join(t.TempDir(), "x"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if errors.Is(err, ErrArtifact) {
		t.Fatal("cancel wrapped artifact")
	}
}
