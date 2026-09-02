package filtercatalog

import (
	"context"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/consumeringest"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Measurement is the sanitized output of an opt-in real-warehouse build.
// PeakRSSBytes is supplied by the command wrapper using the platform's
// supported /usr/bin/time mode; the in-process helper never guesses it.
type Measurement struct {
	WarehouseSchemaVersion       string `json:"warehouse_schema_version"`
	ProviderCatalogSchemaVersion int64  `json:"provider_catalog_schema_version"`
	ProviderCatalogReleaseMonth  string `json:"provider_catalog_release_month"`
	PublicationGeneration        int64  `json:"publication_generation"`
	CandidateOutputCount         int64  `json:"candidate_output_count"`
	StandardFactCount            int64  `json:"standard_fact_count"`
	BillingCodeCount             int64  `json:"billing_code_count"`
	CodeFilterValueCount         int64  `json:"code_filter_value_count"`
	PlanCount                    int64  `json:"plan_count"`
	PlanOutputCount              int64  `json:"plan_output_count"`
	OutputCodeNetworkCount       int64  `json:"output_code_network_count"`
	ProviderFilterValueCount     int64  `json:"provider_filter_value_count"`
	DuckDBWallTimeMS             int64  `json:"duckdb_wall_time_ms"`
	PostgresPopulationWallTimeMS int64  `json:"postgres_population_wall_time_ms"`
	TotalWallTimeMS              int64  `json:"total_wall_time_ms"`
	PeakRSSBytes                 int64  `json:"peak_rss_bytes"`
	CatalogDatabaseBytes         int64  `json:"catalog_database_bytes"`
}

// Measure performs one fresh build and an internal unchanged repeat. It never
// activates a release and refuses any pre-existing generation before Build can
// replace it. The caller supplies the same normalized BuildParams used by the
// ordinary filters build command.
func Measure(ctx context.Context, params BuildParams) (Measurement, error) {
	if ctx == nil {
		panic("filtercatalog: nil context")
	}
	if params.Pool == nil {
		return Measurement{}, jobs.Failure(jobs.FailureInvalidArguments)
	}
	candidate, err := release.SelectCatalogCandidate(ctx, params.Pool, params.PayerID, params.CollectionMonth)
	if err != nil {
		return Measurement{}, err
	}
	var exists bool
	if err := params.Pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM mrfweb.release_catalogs
    WHERE payer_id = $1 AND collection_month = $2::date
      AND publication_generation = $3
)`, candidate.PayerID, candidate.CollectionMonth+"-01", candidate.TargetPublicationGeneration).Scan(&exists); err != nil {
		return Measurement{}, jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
	}
	if exists {
		return Measurement{}, jobs.Failure(jobs.FailureFilterCatalogMeasurePrecondition)
	}
	beforeBytes, err := catalogRelationBytes(ctx, params.Pool)
	if err != nil {
		return Measurement{}, jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
	}
	var totalStart, duckStart, duckDone, populationStart, populationDone time.Time
	measured := params
	measured.Observer = func(phase BuildPhase) {
		now := time.Now()
		switch phase {
		case BuildPhaseTotalStart:
			totalStart = now
		case BuildPhaseDuckDBStart:
			duckStart = now
		case BuildPhaseDuckDBDone:
			duckDone = now
		case BuildPhasePopulationStart:
			populationStart = now
		case BuildPhasePopulationDone:
			populationDone = now
		}
	}
	built, err := Build(ctx, measured)
	totalDone := time.Now()
	if err != nil {
		return Measurement{}, err
	}
	if totalStart.IsZero() || duckStart.IsZero() || duckDone.IsZero() || populationStart.IsZero() || populationDone.IsZero() {
		return Measurement{}, jobs.Failure(jobs.FailureFilterCatalogPopulationFailed)
	}
	afterBytes, err := catalogRelationBytes(ctx, params.Pool)
	if err != nil || afterBytes < beforeBytes {
		return Measurement{}, jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
	}
	providerSchema, providerMonth, err := consumeringest.InspectCatalogIdentity(params.ProviderCatalogPath)
	if err != nil {
		return Measurement{}, jobs.Failure(jobs.FailureFilterCatalogWarehouseInvalid)
	}
	repeated, err := Build(ctx, params)
	if err != nil {
		return Measurement{}, err
	}
	if !repeated.Unchanged || !sameBuildCounts(built, repeated) {
		return Measurement{}, jobs.Failure(jobs.FailureFilterCatalogInconsistent)
	}
	return Measurement{
		WarehouseSchemaVersion:       warehouseSchemaVersion,
		ProviderCatalogSchemaVersion: providerSchema,
		ProviderCatalogReleaseMonth:  providerMonth,
		PublicationGeneration:        built.PublicationGeneration,
		CandidateOutputCount:         built.OutputCount,
		StandardFactCount:            built.StandardFactCount,
		BillingCodeCount:             built.BillingCodeCount,
		CodeFilterValueCount:         built.CodeFilterValueCount,
		PlanCount:                    built.PlanCount,
		PlanOutputCount:              built.PlanOutputCount,
		OutputCodeNetworkCount:       built.OutputCodeNetworkCount,
		ProviderFilterValueCount:     built.ProviderFilterValueCount,
		DuckDBWallTimeMS:             elapsedMilliseconds(duckDone, duckStart),
		PostgresPopulationWallTimeMS: elapsedMilliseconds(populationDone, populationStart),
		TotalWallTimeMS:              elapsedMilliseconds(totalDone, totalStart),
		CatalogDatabaseBytes:         afterBytes - beforeBytes,
	}, nil
}

func sameBuildCounts(a, b BuildResult) bool {
	return a.PayerID == b.PayerID && a.CollectionMonth == b.CollectionMonth &&
		a.PublicationGeneration == b.PublicationGeneration && a.CatalogID == b.CatalogID &&
		a.CatalogStatus == b.CatalogStatus && a.OutputCount == b.OutputCount &&
		a.StandardFactCount == b.StandardFactCount && a.BillingCodeCount == b.BillingCodeCount &&
		a.CodeFilterValueCount == b.CodeFilterValueCount && a.PlanCount == b.PlanCount &&
		a.PlanOutputCount == b.PlanOutputCount && a.OutputCodeNetworkCount == b.OutputCodeNetworkCount &&
		a.ProviderFilterValueCount == b.ProviderFilterValueCount
}

func elapsedMilliseconds(end, start time.Time) int64 {
	if end.Before(start) {
		return 0
	}
	return end.Sub(start).Milliseconds()
}

func catalogRelationBytes(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var bytes int64
	err := pool.QueryRow(ctx, `
SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0)::bigint
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'mrfweb' AND c.relkind IN ('r', 'p')`).Scan(&bytes)
	return bytes, err
}
