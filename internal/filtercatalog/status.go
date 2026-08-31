package filtercatalog

import (
	"context"
	"errors"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// StatusResult is the database-only release and catalog status report.
type StatusResult struct {
	PayerID                          string  `json:"payer_id"`
	CollectionMonth                  string  `json:"collection_month"`
	ReleaseStatus                    string  `json:"release_status"`
	CurrentPublicationGeneration     int64   `json:"current_publication_generation"`
	CurrentCatalogID                 *int64  `json:"current_catalog_id"`
	CurrentCatalogStatus             *string `json:"current_catalog_status"`
	CurrentCatalogOutputCount        *int64  `json:"current_catalog_output_count"`
	ProspectiveCandidateValid        bool    `json:"prospective_candidate_valid"`
	ProspectivePublicationGeneration *int64  `json:"prospective_publication_generation"`
	ProspectiveCatalogID             *int64  `json:"prospective_catalog_id"`
	ProspectiveCatalogStatus         *string `json:"prospective_catalog_status"`
	ProspectiveCatalogOutputCount    *int64  `json:"prospective_catalog_output_count"`
}

type statusCatalog struct {
	ID          int64
	Status      string
	OutputCount int64
}

// Status reads only pipeline PostgreSQL state. It never accesses warehouse
// files, local paths, or DuckDB.
func Status(ctx context.Context, pool *pgxpool.Pool, payer string, month time.Time) (StatusResult, error) {
	if ctx == nil {
		panic("filtercatalog: nil context")
	}
	if pool == nil {
		return StatusResult{}, jobs.Failure(jobs.FailureInvalidArguments)
	}
	var result StatusResult
	result.PayerID = payer
	result.CollectionMonth = month.Format("2006-01")
	var generation int64
	if err := pool.QueryRow(ctx, `
SELECT status, publication_generation
FROM mrfpipeline.monthly_releases
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&result.ReleaseStatus, &generation); errors.Is(err, pgx.ErrNoRows) {
		return StatusResult{}, jobs.Failure(jobs.FailureReleaseNotFound)
	} else if err != nil {
		if ctx.Err() != nil {
			return StatusResult{}, ctx.Err()
		}
		return StatusResult{}, jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
	}
	result.CurrentPublicationGeneration = generation
	if (result.ReleaseStatus == release.Active || result.ReleaseStatus == release.Inactive) && generation == 0 {
		return StatusResult{}, jobs.Failure(jobs.FailureDomainInvariant)
	}
	current, err := readStatusCatalog(ctx, pool, payer, month, generation)
	if err != nil {
		return StatusResult{}, err
	}
	setStatusCatalog(&result.CurrentCatalogID, &result.CurrentCatalogStatus, &result.CurrentCatalogOutputCount, current)

	candidate, err := release.SelectCatalogCandidate(ctx, pool, payer, month)
	if err != nil {
		if ctx.Err() != nil {
			return StatusResult{}, ctx.Err()
		}
		if errors.Is(err, database.ErrDatabase) {
			return StatusResult{}, jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
		}
		if jobs.IsFailure(err, jobs.FailureReleaseNotFound) {
			return StatusResult{}, err
		}
		if jobs.IsFailure(err, jobs.FailureDomainInvariant) {
			return StatusResult{}, err
		}
		return result, nil
	}
	result.ProspectiveCandidateValid = true
	generationCopy := candidate.TargetPublicationGeneration
	result.ProspectivePublicationGeneration = &generationCopy
	prospective, err := readStatusCatalog(ctx, pool, payer, month, candidate.TargetPublicationGeneration)
	if err != nil {
		return StatusResult{}, err
	}
	setStatusCatalog(&result.ProspectiveCatalogID, &result.ProspectiveCatalogStatus, &result.ProspectiveCatalogOutputCount, prospective)
	return result, nil
}

func readStatusCatalog(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, payer string, month time.Time, generation int64) (*statusCatalog, error) {
	if generation <= 0 {
		return nil, nil
	}
	var catalog statusCatalog
	err := q.QueryRow(ctx, `
SELECT id, status, output_count
FROM mrfweb.release_catalogs
WHERE payer_id = $1 AND collection_month = $2 AND publication_generation = $3`, payer, month, generation).Scan(&catalog.ID, &catalog.Status, &catalog.OutputCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
	}
	return &catalog, nil
}

func setStatusCatalog(id **int64, status **string, outputCount **int64, catalog *statusCatalog) {
	if catalog == nil {
		return
	}
	catalogID := catalog.ID
	catalogStatus := catalog.Status
	count := catalog.OutputCount
	*id = &catalogID
	*status = &catalogStatus
	*outputCount = &count
}
