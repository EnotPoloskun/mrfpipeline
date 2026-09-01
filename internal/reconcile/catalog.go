package reconcile

import (
	"context"
	"errors"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/consumeringest"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/mrfparse"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5"
)

func auditFilterCatalogs(ctx context.Context, p Params, report *Report) error {
	schema, monthText, filesystemOK := int64(0), "", false
	var recognized bool
	ws, err := consumeringest.InspectWarehouse(p.WarehousePath)
	if err == nil {
		catalog, e := consumeringest.InspectCatalog(p.ProviderCatalogPath)
		if e == nil {
			schema, monthText, recognized = ws.RecognizedCatalog()
			catalogSchema, catalogMonth, identityErr := consumeringest.InspectCatalogIdentity(catalog.Path)
			filesystemOK = recognized && identityErr == nil && catalogSchema == schema && catalogMonth == monthText
			if filesystemOK {
				services, e := mrfparse.InspectServices(p.ServicesPath)
				if e != nil || consumeringest.CheckWarehouseCatalog(ws, catalog, p.Workspace.Root, services.Path) != nil {
					filesystemOK = false
				}
			}
		}
	}
	rows, err := p.Pool.Query(ctx, `SELECT payer_id,collection_month,status,publication_generation FROM mrfpipeline.monthly_releases WHERE status IN ('active','inactive') ORDER BY payer_id,collection_month`)
	if err != nil {
		return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	defer rows.Close()
	for rows.Next() {
		var payer, status string
		var month time.Time
		var generation int64
		if err := rows.Scan(&payer, &month, &status, &generation); err != nil {
			return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
		}
		targets, err := release.ListPublishedTargets(ctx, p.Pool, payer, month)
		if err != nil {
			return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
		}
		var cs string
		err = p.Pool.QueryRow(ctx, `SELECT status FROM mrfweb.release_catalogs WHERE payer_id=$1 AND collection_month=$2 AND publication_generation=$3`, payer, month, generation).Scan(&cs)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
		}
		if errors.Is(err, pgx.ErrNoRows) || cs != "published" {
			if status == release.Inactive {
				report.FilterCatalogBackfillRequiredCount++
			} else {
				report.SealedReleaseInconsistencyCount++
			}
			continue
		}
		audit, err := release.AuditPublishedCatalog(ctx, p.Pool, payer, month, generation, targets)
		if err != nil {
			if ctx.Err() != nil {
				return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
			}
			report.SealedReleaseInconsistencyCount++
			continue
		}
		if !filesystemOK {
			report.SealedReleaseInconsistencyCount++
			continue
		}
		want, e := time.Parse("2006-01", monthText)
		if e != nil || audit.ProviderSchemaVersion == nil || *audit.ProviderSchemaVersion != schema || audit.ProviderReleaseMonth == nil || !audit.ProviderReleaseMonth.Equal(want) {
			report.SealedReleaseInconsistencyCount++
		} else if e := ValidateActivationTargets(ctx, p.Pool, p.WarehousePath, payer, month, targets); e != nil {
			if ctx.Err() != nil {
				return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
			}
			report.SealedReleaseInconsistencyCount++
		}
	}
	if err := rows.Err(); err != nil {
		return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	return nil
}
