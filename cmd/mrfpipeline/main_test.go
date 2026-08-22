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
	if strings.Contains(text, "\nrequire") {
		t.Fatal("Story 01 must not add module dependencies")
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
	forbidden := []string{
		"database/sql",
		"net",
		"net/http",
		"github.com/jackc/pgx",
		"riverqueue.com/river",
	}
	dirs := []string{
		filepath.Join(root, "cmd", "mrfpipeline"),
		filepath.Join(root, "internal", "cli"),
		filepath.Join(root, "internal", "config"),
	}
	fset := token.NewFileSet()
	for _, dir := range dirs {
		pkgs, err := parser.ParseDir(fset, dir, func(info os.FileInfo) bool {
			return !strings.HasSuffix(info.Name(), "_test.go")
		}, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, pkg := range pkgs {
			for _, file := range pkg.Files {
				for _, spec := range file.Imports {
					path := strings.Trim(spec.Path.Value, `"`)
					for _, bad := range forbidden {
						if path == bad || strings.HasPrefix(path, bad+"/") {
							t.Fatalf("%s imports %s", fset.File(file.Pos()).Name(), path)
						}
					}
				}
			}
		}
	}
}
