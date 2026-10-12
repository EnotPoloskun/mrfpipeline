package mrfparse

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

func TestLookupParserMissing(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	missing := filepath.Join(t.TempDir(), "no-such-mrfparser")
	_, err := LookupParser(missing)
	if err == nil {
		t.Fatal("expected missing parser")
	}
	if jobs.IsFailure(err, jobs.FailureMRFParseExecutionFailed) || jobs.IsFailure(err, jobs.FailureMRFParseInputInvalid) {
		t.Fatalf("startup error classified as parse failure: %v", err)
	}
	if strings.Contains(err.Error(), missing) {
		t.Fatalf("leaked path %v", err)
	}
	_, err = LookupParser("")
	if err == nil {
		t.Fatal("expected PATH lookup failure")
	}
	if strings.Contains(err.Error(), "mrfparser") {
		t.Fatalf("leaked name %v", err)
	}
}

func TestLookupParserRejectsDirectory(t *testing.T) {
	dir := t.TempDir()
	_, err := LookupParser(dir)
	if err == nil {
		t.Fatal("expected directory rejection")
	}
	if strings.Contains(err.Error(), dir) {
		t.Fatalf("leaked path %v", err)
	}
}

func TestLookupParserAcceptsExecutable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mrfparser")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0755); err != nil {
		t.Fatal(err)
	}
	got, err := LookupParser(path)
	if err != nil {
		t.Fatal(err)
	}
	if got == "" {
		t.Fatal("empty path")
	}
}
