package mrfparse

import (
	"strings"
	"testing"

	"github.com/EnotPoloskun/mrfparser"
)

func TestParserCLIArgs(t *testing.T) {
	t.Parallel()
	cfg := mrfparser.Config{
		Input:    "/in/data",
		Output:   "/out/parsed",
		Services: "/etc/services.csv",
		TempDir:  "/tmp/mrf-source-1",
	}
	args := parserCLIArgs(cfg)
	got := strings.Join(args, "\x00")
	for _, want := range []string{"--input", "--output", "--services", "--temp-dir", cfg.Input, cfg.Output, cfg.Services, cfg.TempDir} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in %v", want, args)
		}
	}
	if args[0] != "parse" {
		t.Fatalf("command %v", args)
	}
	joined := strings.Join(args, " ")
	for _, forbid := range []string{"--memory-limit", "--negotiated-types", "--target-file-size", "--row-group-rows"} {
		if strings.Contains(joined, forbid) {
			t.Fatalf("forbidden %s in %v", forbid, args)
		}
	}
	if strings.Contains(joined, "sh -c") || strings.Contains(joined, "/bin/sh") {
		t.Fatalf("shell %v", args)
	}
}
