package cli

import (
	"context"
	"strings"
	"testing"
)

func TestFiltersHelpIsInformational(t *testing.T) {
	for _, args := range [][]string{
		{"filters", "--help"},
		{"filters", "build", "--help"},
		{"filters", "status", "--help"},
		{"filters", "measure", "--help"},
	} {
		parsed, err := parse(args)
		if err != nil || !parsed.help || parsed.helpText == "" {
			t.Fatalf("args=%v parsed=%+v err=%v", args, parsed, err)
		}
	}
	code, stdout, stderr := runCLI(context.Background(), []string{"filters", "build", "--help"}, fatalEnv(t))
	if code != 0 || stdout == "" || stderr != "" {
		t.Fatalf("exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
}

func TestFiltersParserRejectsDuplicatesAndExtraArguments(t *testing.T) {
	cases := [][]string{
		{"filters", "build", "--payer", "payer", "--payer", "other", "--collection-month", "2026-08"},
		{"filters", "build", "--payer=payer", "--payer=other", "--collection-month=2026-08"},
		{"filters", "status", "--payer=payer", "--collection-month=2026-08", "--collection-month=2026-09"},
		{"filters", "measure", "--payer", "payer", "--payer", "other", "--collection-month", "2026-08"},
		{"filters", "measure", "--payer=payer", "--payer=other", "--collection-month=2026-08"},
		{"filters", "measure", "--payer=payer", "--collection-month=2026-08", "--collection-month=2026-09"},
		{"filters", "status", "--payer", "payer", "--collection-month", "2026-08", "--unknown", "x"},
		{"filters", "status", "--payer", "payer", "--collection-month", "2026-08", "positional"},
		{"filters", "status", "--payer", "payer"},
		{"filters", "build", "--payer=", "--collection-month", "2026-08"},
		{"filters", "status", "--payer", "", "--collection-month", "2026-08"},
		{"filters", "build", "--payer", "payer", "--collection-month="},
		{"filters", "status", "--payer", "payer", "--collection-month", ""},
		{"filters", "measure", "--payer", "payer"},
	}
	for _, args := range cases {
		_, err := parse(args)
		if err == nil || !isUsage(err) {
			t.Fatalf("args=%v err=%v", args, err)
		}
		if !strings.Contains(hint(commandOf(err)), "filters") {
			t.Fatalf("args=%v hint=%q", args, hint(commandOf(err)))
		}
	}
}
