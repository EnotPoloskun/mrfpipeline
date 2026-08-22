package main

import (
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
}

func TestModulePathAndGoVersion(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.HasPrefix(text, "module github.com/enotpoloskun/mrfpipeline\n") {
		t.Fatalf("module path: %s", text)
	}
	if !strings.Contains(text, "\ngo 1.26\n") && !strings.HasSuffix(strings.TrimSpace(text), "\ngo 1.26") {
		t.Fatalf("go version: %s", text)
	}
	if !strings.Contains(text, "github.com/jackc/pgx/v5 v5.9.2") {
		t.Fatalf("expected pinned pgx: %s", text)
	}
	if !strings.Contains(text, "github.com/riverqueue/river v0.39.0") {
		t.Fatalf("expected pinned river: %s", text)
	}
	if !strings.Contains(text, "github.com/riverqueue/river/riverdriver/riverpgxv5 v0.39.0") {
		t.Fatalf("expected pinned riverpgxv5: %s", text)
	}
	if !strings.Contains(text, "github.com/EnotPoloskun/mrfdiscoverer v0.0.0-20260821182039-590460f1448b") {
		t.Fatalf("expected pinned mrfdiscoverer: %s", text)
	}
	if !strings.Contains(text, "github.com/EnotPoloskun/mrftocparser v0.0.0-20260822213145-f80bb070f2b5") {
		t.Fatalf("expected pinned mrftocparser: %s", text)
	}
	if strings.Contains(text, "\nreplace ") || strings.HasPrefix(text, "replace ") {
		t.Fatal("go.mod must not contain a replace directive")
	}
}

func TestBuildTargets(t *testing.T) {
	root := repoRoot(t)
	targets := []struct{ goos, goarch string }{
		{"darwin", "arm64"},
		{"linux", "amd64"},
		{"linux", "arm64"},
	}
	for _, tc := range targets {
		t.Run(tc.goos+"_"+tc.goarch, func(t *testing.T) {
			t.Parallel()
			out := filepath.Join(t.TempDir(), "mrfpipeline")
			cmd := exec.Command("go", "build", "-o", out, "./cmd/mrfpipeline")
			cmd.Dir = root
			cmd.Env = append(os.Environ(),
				"GOOS="+tc.goos,
				"GOARCH="+tc.goarch,
				"CGO_ENABLED=0",
			)
			got, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("build: %v\n%s", err, got)
			}
		})
	}
}

func TestNoForbiddenProductionImports(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	common := []string{
		"database/sql",
		"net/http",
	}
	cliForbidden := append(append([]string{}, common...),
		"github.com/jackc/pgx",
		"github.com/riverqueue/river",
		"net",
	)
	dirs := []struct {
		path      string
		forbidden []string
	}{
		{filepath.Join(root, "cmd", "mrfpipeline"), cliForbidden},
		{filepath.Join(root, "internal", "cli"), cliForbidden},
		{filepath.Join(root, "internal", "config"), cliForbidden},
		{filepath.Join(root, "internal", "database"), common},
		{filepath.Join(root, "internal", "jobs"), common},
		{filepath.Join(root, "internal", "discovery"), common},
		{filepath.Join(root, "internal", "tocdownload"), common},
		{filepath.Join(root, "internal", "tocparse"), common},
		{filepath.Join(root, "internal", "work"), common},
		{filepath.Join(root, "internal", "artifact"), []string{
			"database/sql",
			"github.com/jackc/pgx",
			"github.com/riverqueue/river",
		}},
	}
	fset := token.NewFileSet()
	for _, dir := range dirs {
		pkgs, err := parser.ParseDir(fset, dir.path, func(info os.FileInfo) bool {
			return !strings.HasSuffix(info.Name(), "_test.go")
		}, parser.ImportsOnly)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatal(err)
		}
		for _, pkg := range pkgs {
			for _, file := range pkg.Files {
				for _, spec := range file.Imports {
					path := strings.Trim(spec.Path.Value, `"`)
					for _, bad := range dir.forbidden {
						if path == bad || strings.HasPrefix(path, bad+"/") {
							t.Fatalf("%s imports %s", fset.File(file.Pos()).Name(), path)
						}
					}
				}
			}
		}
	}
}
