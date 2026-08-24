package cli

import (
	"errors"
	"testing"

	"github.com/enotpoloskun/mrfpipeline/internal/config"
)

func TestParseMonthCommands(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		args       []string
		wantAction string
		wantPayer  string
		wantMonth  string
	}{
		{"status all", []string{"month", "status"}, monthStatus, "", ""},
		{"status target", []string{"month", "status", "--payer", "aetna", "--collection-month", "2026-08"}, monthStatus, "aetna", "2026-08"},
		{"activate", []string{"month", "activate", "--payer=aetna", "--collection-month=2026-08"}, monthActivate, "aetna", "2026-08"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parse(tc.args)
			if err != nil {
				t.Fatal(err)
			}
			if got.command != cmdMonth || got.monthAction != tc.wantAction || got.payer != tc.wantPayer || got.month != tc.wantMonth {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

func TestParseMonthCommandRejectsIncompleteTargets(t *testing.T) {
	t.Parallel()
	cases := [][]string{
		{"month"},
		{"month", "status", "--payer", "aetna"},
		{"month", "status", "--collection-month", "2026-08"},
		{"month", "status", "--payer", "aetna", "extra"},
		{"month", "activate", "--payer", "aetna"},
		{"month", "activate", "--payer", "aetna", "--collection-month", "2026-08", "extra"},
		{"month", "unknown"},
	}
	for _, args := range cases {
		if _, err := parse(args); err == nil || isUsage(err) == false {
			t.Fatalf("%v: got %v", args, err)
		}
	}
}

func TestParseMonthDate(t *testing.T) {
	t.Parallel()
	if got, err := parseMonthDate("2026-08"); err != nil || got.Format("2006-01-02") != "2026-08-01" {
		t.Fatalf("got %v, %v", got, err)
	}
	if _, err := parseMonthDate("2026-13"); err == nil || !errors.Is(err, config.ErrInvalidConfig) {
		t.Fatalf("got %v", err)
	}
	if _, err := parseMonthDate("2026-08-01"); err == nil || !errors.Is(err, config.ErrInvalidConfig) {
		t.Fatalf("got %v", err)
	}
	if err := config.ValidatePayerIdentifier("aetna"); err != nil {
		t.Fatal(err)
	}
}
