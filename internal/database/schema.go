package database

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ValidateCurrent reports whether application and River schemas are exactly
// current for this binary. It is read-only and never migrates.
func ValidateCurrent(ctx context.Context, pool *pgxpool.Pool) error {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if pool == nil {
		return dbErr("validate schema")
	}
	if err := requireSchema(ctx, pool, ApplicationSchema); err != nil {
		return err
	}
	if err := requireSchema(ctx, pool, RiverSchema); err != nil {
		return err
	}
	if err := validateApplicationCurrent(ctx, pool); err != nil {
		return err
	}
	return validateRiverCurrent(ctx, pool)
}

func requireSchema(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	var ok bool
	err := pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM information_schema.schemata WHERE schema_name = $1
)`, schema).Scan(&ok)
	if err != nil {
		return classify(ctx, "validate schema", err)
	}
	if !ok {
		return dbErr("run mrfpipeline migrate")
	}
	return nil
}

func validateApplicationCurrent(ctx context.Context, pool *pgxpool.Pool) error {
	files, err := loadEmbeddedMigrations()
	if err != nil {
		return err
	}
	rows, err := pool.Query(ctx, `
SELECT version, name
FROM mrfpipeline.schema_migrations
ORDER BY version`)
	if err != nil {
		return classify(ctx, "validate schema", err)
	}
	defer rows.Close()
	var applied []ledgerRow
	for rows.Next() {
		var row ledgerRow
		if err := rows.Scan(&row.Version, &row.Name); err != nil {
			return classify(ctx, "validate schema", err)
		}
		applied = append(applied, row)
	}
	if err := rows.Err(); err != nil {
		return classify(ctx, "validate schema", err)
	}
	if err := validateLedger(applied, files); err != nil {
		return dbErr("run mrfpipeline migrate")
	}
	if len(applied) != len(files) {
		return dbErr("run mrfpipeline migrate")
	}
	return nil
}

func validateRiverCurrent(ctx context.Context, pool *pgxpool.Pool) error {
	migrator, err := newRiverMigrator(pool)
	if err != nil {
		return err
	}
	existing, err := migrator.ExistingVersions(ctx)
	if err != nil {
		return classify(ctx, "validate schema", err)
	}
	if err := validateRiverLedger(migrator, existing); err != nil {
		return dbErr("run mrfpipeline migrate")
	}
	if currentRiverVersion(existing) != ExpectedRiverVersion {
		return dbErr("run mrfpipeline migrate")
	}
	res, err := migrator.Validate(ctx, nil)
	if err != nil {
		return classify(ctx, "validate schema", err)
	}
	if res == nil || !res.OK {
		return dbErr("run mrfpipeline migrate")
	}
	return nil
}
