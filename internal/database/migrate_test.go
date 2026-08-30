package database

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
)

func TestBundledRiverVersion(t *testing.T) {
	t.Parallel()
	got := bundledRiverVersion()
	if got != ExpectedRiverVersion {
		t.Fatalf("rivermigrate main line is %d; story expects %d — stop and update the story", got, ExpectedRiverVersion)
	}
}

func TestRequirePostgres15(t *testing.T) {
	t.Parallel()
	err := requirePostgres15(149999)
	if !errors.Is(err, ErrDatabase) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), "149999") {
		t.Fatalf("version leaked: %v", err)
	}
	if err := requirePostgres15(150000); err != nil {
		t.Fatal(err)
	}
	if err := requirePostgres15(160004); err != nil {
		t.Fatal(err)
	}
}

func TestParseConfigFailureIsDatabase(t *testing.T) {
	t.Parallel()
	secret := "postgres://user:supersecret@127.0.0.1/%zz"
	_, err := Migrate(context.Background(), secret)
	if !errors.Is(err, ErrDatabase) {
		t.Fatalf("got %v", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if strings.Contains(err.Error(), "supersecret") || strings.Contains(err.Error(), secret) {
		t.Fatalf("exposed url: %v", err)
	}
}

func TestCanceledContextIsNotDatabase(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Migrate(ctx, "postgres://user:supersecret@127.0.0.1:1/db")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if errors.Is(err, ErrDatabase) {
		t.Fatal("cancellation must not wrap ErrDatabase")
	}
}

func TestResultJSON(t *testing.T) {
	t.Parallel()
	got, err := FormatResult(Result{ApplicationVersion: 6, AppliedMigrationCount: 6, RiverVersion: 6, AppliedRiverMigrationCount: 6})
	if err != nil {
		t.Fatal(err)
	}
	if got != "{\"application_version\":6,\"applied_migration_count\":6,\"river_version\":6,\"applied_river_migration_count\":6}\n" {
		t.Fatalf("got %q", got)
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSuffix(got, "\n")), &obj); err != nil {
		t.Fatal(err)
	}
	if len(obj) != 4 {
		t.Fatalf("fields %v", obj)
	}
	zero, err := FormatResult(Result{ApplicationVersion: 6, AppliedMigrationCount: 0, RiverVersion: 6, AppliedRiverMigrationCount: 0})
	if err != nil {
		t.Fatal(err)
	}
	if zero != "{\"application_version\":6,\"applied_migration_count\":0,\"river_version\":6,\"applied_river_migration_count\":0}\n" {
		t.Fatalf("got %q", zero)
	}
}

func TestEmbeddedMigrations(t *testing.T) {
	t.Parallel()
	files, err := loadEmbeddedMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 9 {
		t.Fatalf("got %d files", len(files))
	}
	if files[0].Version != 1 || files[0].Name != "0001_initial_domain.sql" {
		t.Fatalf("got %+v", files[0])
	}
	if files[1].Version != 2 || files[1].Name != "0002_feed_free_domain.sql" {
		t.Fatalf("got %+v", files[1])
	}
	if files[2].Version != 3 || files[2].Name != "0003_monthly_toc_captures.sql" {
		t.Fatalf("got %+v", files[2])
	}
	if files[3].Version != 4 || files[3].Name != "0004_monthly_releases.sql" || files[4].Version != 5 || files[4].Name != "0005_bounded_worker_scaling.sql" || files[5].Version != 6 || files[5].Name != "0006_terminal_mrf_parse_slot_release.sql" || files[6].Version != 7 || files[6].Name != "0007_nullable_plan_sponsor.sql" || files[7].Version != 8 || files[7].Name != "0008_incremental_release_publication.sql" || files[8].Version != 9 || files[8].Name != "0009_release_filter_catalog.sql" {
		t.Fatalf("got %+v", files[3:])
	}
	if files[0].SQL == "" {
		t.Fatal("empty sql")
	}
	if err := validateMigrationSet(files); err != nil {
		t.Fatal(err)
	}
}

func TestValidateLedger(t *testing.T) {
	t.Parallel()
	files := []migrationFile{{Version: 1, Name: "0001_initial_domain.sql"}, {Version: 2, Name: "0002_feed_free_domain.sql"}, {Version: 3, Name: "0003_monthly_toc_captures.sql"}, {Version: 4, Name: "0004_monthly_releases.sql"}, {Version: 5, Name: "0005_bounded_worker_scaling.sql"}, {Version: 6, Name: "0006_terminal_mrf_parse_slot_release.sql"}, {Version: 7, Name: "0007_nullable_plan_sponsor.sql"}, {Version: 8, Name: "0008_incremental_release_publication.sql"}, {Version: 9, Name: "0009_release_filter_catalog.sql"}}
	if err := validateLedger(nil, files); err != nil {
		t.Fatal(err)
	}
	if err := validateLedger([]ledgerRow{{Version: 1, Name: "0001_initial_domain.sql"}}, files); err != nil {
		t.Fatal(err)
	}
	cases := [][]ledgerRow{
		{{Version: 1, Name: "wrong.sql"}},
		{{Version: 2, Name: "0002_future.sql"}},
		{{Version: 1, Name: "0001_initial_domain.sql"}, {Version: 3, Name: "0003_gap.sql"}},
		{{Version: 1, Name: "0001_initial_domain.sql"}, {Version: 1, Name: "0001_initial_domain.sql"}},
	}
	for i, rows := range cases {
		err := validateLedger(rows, files)
		if !errors.Is(err, ErrDatabase) {
			t.Fatalf("case %d: got %v", i, err)
		}
		if strings.Contains(err.Error(), ".sql") || strings.Contains(err.Error(), strconv.Itoa(rows[0].Version)) {
			t.Fatalf("case %d leaked ledger detail: %v", i, err)
		}
	}
}

func TestConnectFailureRedactsURL(t *testing.T) {
	t.Parallel()
	secret := "postgres://user:supersecret@127.0.0.1:1/db"
	_, err := Migrate(context.Background(), secret)
	if !errors.Is(err, ErrDatabase) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), "supersecret") || strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("exposed connection detail: %v", err)
	}
}
