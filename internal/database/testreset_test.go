package database

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func resetPipelineSchemas(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return dbErr("reset schema")
	}
	for _, schema := range []string{RiverSchema, ApplicationSchema, "mrfpipeline_test"} {
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			return classify(ctx, "reset schema", err)
		}
	}
	return nil
}
