package database

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Result is the compact migrate success report.
type Result struct {
	ApplicationVersion         int `json:"application_version"`
	AppliedMigrationCount      int `json:"applied_migration_count"`
	RiverVersion               int `json:"river_version"`
	AppliedRiverMigrationCount int `json:"applied_river_migration_count"`
}

type ledgerRow struct {
	Version int
	Name    string
}

// FormatResult returns compact JSON plus a trailing newline.
func FormatResult(result Result) (string, error) {
	raw, err := json.Marshal(result)
	if err != nil {
		return "", dbErr("encode result")
	}
	return string(raw) + "\n", nil
}

// Migrate applies pending application migrations to the given database URL.
func Migrate(ctx context.Context, databaseURL string) (Result, error) {
	files, err := loadEmbeddedMigrations()
	if err != nil {
		return Result{}, err
	}
	return applyMigrations(ctx, databaseURL, files)
}

func applyMigrations(ctx context.Context, databaseURL string, files []migrationFile) (Result, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return Result{}, classify(ctx, "parse config", err)
	}
	cfg.MaxConns = maxMigrateConns

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return Result{}, classify(ctx, "connect", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return Result{}, classify(ctx, "ping", err)
	}

	var versionText string
	if err := pool.QueryRow(ctx, "SELECT current_setting('server_version_num')").Scan(&versionText); err != nil {
		return Result{}, classify(ctx, "server version", err)
	}
	versionNum, err := strconv.Atoi(versionText)
	if err != nil {
		return Result{}, dbErr("server version")
	}
	if err := requirePostgres15(versionNum); err != nil {
		return Result{}, err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return Result{}, classify(ctx, "acquire", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1, $2)", MigrationLockClass, MigrationLockObject); err != nil {
		return Result{}, classify(ctx, "lock", err)
	}
	unlocked := false
	defer func() {
		if unlocked {
			return
		}
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1, $2)", MigrationLockClass, MigrationLockObject)
	}()

	if _, err := conn.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS mrfpipeline`); err != nil {
		return Result{}, classify(ctx, "create schema", err)
	}
	if _, err := conn.Exec(ctx, `
CREATE TABLE IF NOT EXISTS mrfpipeline.schema_migrations (
    version integer PRIMARY KEY,
    name text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT schema_migrations_version_check CHECK (version > 0)
)`); err != nil {
		return Result{}, classify(ctx, "create ledger", err)
	}

	rows, err := conn.Query(ctx, `
SELECT version, name
FROM mrfpipeline.schema_migrations
ORDER BY version`)
	if err != nil {
		return Result{}, classify(ctx, "read ledger", err)
	}
	defer rows.Close()
	var applied []ledgerRow
	for rows.Next() {
		var row ledgerRow
		if err := rows.Scan(&row.Version, &row.Name); err != nil {
			return Result{}, classify(ctx, "read ledger", err)
		}
		applied = append(applied, row)
	}
	if err := rows.Err(); err != nil {
		return Result{}, classify(ctx, "read ledger", err)
	}
	rows.Close()

	if err := validateLedger(applied, files); err != nil {
		return Result{}, err
	}

	have := make(map[int]bool, len(applied))
	maxApplied := 0
	for _, row := range applied {
		have[row.Version] = true
		if row.Version > maxApplied {
			maxApplied = row.Version
		}
	}

	appliedCount := 0
	for _, file := range files {
		if have[file.Version] {
			continue
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return Result{}, classify(ctx, "begin migration", err)
		}
		if err := execSimple(ctx, tx, file.SQL); err != nil {
			_ = tx.Rollback(ctx)
			if file.Version == 2 && isPopulatedFeedFreeMigration(err) {
				return Result{}, dbErr("migration requires a new pipeline database and warehouse")
			}
			return Result{}, classify(ctx, "apply migration", err)
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO mrfpipeline.schema_migrations (version, name)
VALUES ($1, $2)`, file.Version, file.Name); err != nil {
			_ = tx.Rollback(ctx)
			return Result{}, classify(ctx, "record migration", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return Result{}, classify(ctx, "commit migration", err)
		}
		appliedCount++
		if file.Version > maxApplied {
			maxApplied = file.Version
		}
	}

	riverVersion, riverApplied, err := applyRiverMigrations(ctx, pool)
	if err != nil {
		return Result{}, err
	}

	if _, err := conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1, $2)", MigrationLockClass, MigrationLockObject); err != nil {
		return Result{}, dbErr("unlock")
	}
	unlocked = true

	return Result{
		ApplicationVersion:         maxApplied,
		AppliedMigrationCount:      appliedCount,
		RiverVersion:               riverVersion,
		AppliedRiverMigrationCount: riverApplied,
	}, nil
}

func isPopulatedFeedFreeMigration(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "P0001" &&
		pgErr.Message == "feed-free migration requires a new pipeline database and warehouse"
}

func validateLedger(rows []ledgerRow, files []migrationFile) error {
	byVersion := make(map[int]migrationFile, len(files))
	for _, file := range files {
		byVersion[file.Version] = file
	}
	seen := make(map[int]bool, len(rows))
	for i, row := range rows {
		if row.Version < 1 || seen[row.Version] {
			return dbErr("invalid ledger")
		}
		seen[row.Version] = true
		if i == 0 && row.Version != 1 {
			return dbErr("invalid ledger")
		}
		if i > 0 && row.Version != rows[i-1].Version+1 {
			return dbErr("invalid ledger")
		}
		file, ok := byVersion[row.Version]
		if !ok || file.Name != row.Name {
			return dbErr("invalid ledger")
		}
	}
	return nil
}

func execSimple(ctx context.Context, tx pgx.Tx, sql string) error {
	return tx.Conn().PgConn().Exec(ctx, sql).Close()
}
