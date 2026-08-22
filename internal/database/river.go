package database

import (
	"context"
	"io"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newRiverMigrator(pool *pgxpool.Pool) (*rivermigrate.Migrator[pgx.Tx], error) {
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), &rivermigrate.Config{
		Line:   "main",
		Logger: discardLogger(),
		Schema: RiverSchema,
	})
	if err != nil {
		return nil, dbErr("river migrate")
	}
	return migrator, nil
}

func applyRiverMigrations(ctx context.Context, pool *pgxpool.Pool) (int, int, error) {
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+RiverSchema); err != nil {
		return 0, 0, classify(ctx, "create river schema", err)
	}
	migrator, err := newRiverMigrator(pool)
	if err != nil {
		return 0, 0, err
	}
	res, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	if err != nil {
		return 0, 0, classify(ctx, "river migrate", err)
	}
	existing, err := migrator.ExistingVersions(ctx)
	if err != nil {
		return 0, 0, classify(ctx, "river version", err)
	}
	if err := validateRiverLedger(migrator, existing); err != nil {
		return 0, 0, err
	}
	version := currentRiverVersion(existing)
	if version != ExpectedRiverVersion {
		return 0, 0, dbErr("river version")
	}
	return version, len(res.Versions), nil
}

func validateRiverLedger(migrator *rivermigrate.Migrator[pgx.Tx], existing []rivermigrate.Migration) error {
	known := make(map[int]bool, len(migrator.AllVersions()))
	for _, v := range migrator.AllVersions() {
		known[v.Version] = true
	}
	seen := make(map[int]bool, len(existing))
	for i, row := range existing {
		if row.Version < 1 || seen[row.Version] || !known[row.Version] {
			return dbErr("invalid river ledger")
		}
		seen[row.Version] = true
		if i == 0 && row.Version != 1 {
			return dbErr("invalid river ledger")
		}
		if i > 0 && row.Version != existing[i-1].Version+1 {
			return dbErr("invalid river ledger")
		}
	}
	return nil
}

func currentRiverVersion(existing []rivermigrate.Migration) int {
	max := 0
	for _, row := range existing {
		if row.Version > max {
			max = row.Version
		}
	}
	return max
}

func bundledRiverVersion() int {
	migrator, err := rivermigrate.New(riverpgxv5.New(nil), &rivermigrate.Config{
		Line:   "main",
		Logger: discardLogger(),
		Schema: RiverSchema,
	})
	if err != nil {
		return 0
	}
	return currentRiverVersion(migrator.AllVersions())
}
