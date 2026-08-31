package filtercatalog

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/consumeringest"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// BuildParams describes one explicit catalog build. Preflight is expected to
// be the existing activation target preflight and runs while the build lock is
// held, before any catalog state is created.
type BuildParams struct {
	Pool                *pgxpool.Pool
	PayerID             string
	CollectionMonth     time.Time
	WarehousePath       string
	ProviderCatalogPath string
	Preflight           func([]release.Target) error
}

// BuildResult is the complete sanitized build result.
type BuildResult struct {
	PayerID                  string `json:"payer_id"`
	CollectionMonth          string `json:"collection_month"`
	PublicationGeneration    int64  `json:"publication_generation"`
	CatalogID                int64  `json:"catalog_id"`
	CatalogStatus            string `json:"catalog_status"`
	OutputCount              int64  `json:"output_count"`
	StandardFactCount        int64  `json:"standard_fact_count"`
	BillingCodeCount         int64  `json:"billing_code_count"`
	CodeFilterValueCount     int64  `json:"code_filter_value_count"`
	PlanCount                int64  `json:"plan_count"`
	PlanOutputCount          int64  `json:"plan_output_count"`
	OutputCodeNetworkCount   int64  `json:"output_code_network_count"`
	ProviderFilterValueCount int64  `json:"provider_filter_value_count"`
	Unchanged                bool   `json:"unchanged"`
}

type catalogHeader struct {
	ID                       int64
	PayerID                  string
	CollectionMonth          time.Time
	PublicationGeneration    int64
	Status                   string
	OutputFingerprint        string
	OutputCount              int64
	StandardFactCount        *int64
	ProviderSchemaVersion    *int64
	ProviderReleaseMonth     *time.Time
	BillingCodeCount         *int64
	CodeFilterValueCount     *int64
	PlanCount                *int64
	PlanOutputCount          *int64
	OutputCodeNetworkCount   *int64
	ProviderFilterValueCount *int64
	FailureCode              *string
	CreatedAt                time.Time
	CompletedAt              *time.Time
	PublishedAt              *time.Time
}

type catalogOutput struct {
	SnapshotID      int64
	OutputID        string
	PayerID         string
	CollectionMonth time.Time
}

type catalogPlan struct {
	ID int64
	planIdentity
	SearchText string
	Outputs    []string
}

type catalogInspection struct {
	Header          catalogHeader
	Outputs         []catalogOutput
	Projection      planProjection
	Counts          catalogCounts
	ReferencesValid bool
}

type catalogCounts struct {
	Billing   int64
	Code      int64
	Plans     int64
	PlanOut   int64
	Networks  int64
	Providers int64
}

func Build(ctx context.Context, params BuildParams) (BuildResult, error) {
	if ctx == nil {
		panic("filtercatalog: nil context")
	}
	if err := ctx.Err(); err != nil {
		return BuildResult{}, err
	}
	if params.Pool == nil {
		return BuildResult{}, jobs.Failure(jobs.FailureInvalidArguments)
	}
	conn, err := params.Pool.Acquire(ctx)
	if err != nil {
		return BuildResult{}, buildDatabaseError(ctx)
	}
	defer conn.Release()
	monthText := params.CollectionMonth.Format("2006-01")
	locked, err := tryCatalogLock(ctx, conn, release.FilterCatalogLockKey(params.PayerID, monthText))
	if err != nil {
		return BuildResult{}, buildDatabaseError(ctx)
	}
	if !locked {
		return BuildResult{}, jobs.Failure(jobs.FailureFilterCatalogBusy)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1::bigint)", release.FilterCatalogLockKey(params.PayerID, monthText))
	}()

	candidate, snapshot, err := buildPhaseOne(ctx, conn, params)
	if err != nil {
		return BuildResult{}, mapBuildError(err)
	}
	identity, err := inspectWarehouseIdentity(params.WarehousePath, params.ProviderCatalogPath)
	if err != nil {
		return BuildResult{}, err
	}
	if params.Preflight != nil {
		if err := params.Preflight(candidate.Targets); err != nil {
			return BuildResult{}, mapBuildError(err)
		}
	}
	inspection, found, err := inspectExistingCatalog(ctx, conn, candidate, snapshot, identity)
	if err != nil {
		return BuildResult{}, mapBuildError(err)
	}
	if found && inspection.Header.Status == "published" && !catalogInternallyValid(inspection) {
		return BuildResult{}, jobs.Failure(jobs.FailureFilterCatalogInconsistent)
	}
	if found && inspection.Header.Status == "published" {
		if catalogMatchesCandidate(inspection, candidate, snapshot, identity) {
			result, err := resultFromHeader(inspection.Header, true)
			if err != nil {
				return BuildResult{}, err
			}
			return result, nil
		}
		return BuildResult{}, jobs.Failure(jobs.FailureFilterCatalogPublishedMismatch)
	}
	if found && inspection.Header.Status == "ready" && catalogInternallyValid(inspection) && catalogMatchesCandidate(inspection, candidate, snapshot, identity) {
		result, err := resultFromHeader(inspection.Header, true)
		if err != nil {
			return BuildResult{}, err
		}
		return result, nil
	}

	duck, err := ResolveDuckDB(ctx)
	if err != nil {
		return BuildResult{}, err
	}
	catalogID, err := createBuildingCatalog(ctx, conn, candidate, found && inspection.Header.Status != "published", inspection.Header.ID)
	if err != nil {
		return BuildResult{}, mapBuildError(err)
	}

	extracted, err := Extract(ctx, duck, Params{
		WarehousePath:     params.WarehousePath,
		OutputFingerprint: candidate.OutputFingerprint,
		Outputs:           targetsToOutputs(candidate.Targets),
	})
	if err != nil {
		return BuildResult{}, finishFailedBuild(ctx, conn, catalogID, err)
	}
	if extracted.OutputFingerprint != candidate.OutputFingerprint {
		return BuildResult{}, finishFailedBuild(ctx, conn, catalogID, jobs.Failure(jobs.FailureFilterCatalogPlanInvalid))
	}

	phaseThree, err := rereadPlanSnapshot(ctx, conn, candidate.Targets)
	if err != nil {
		return BuildResult{}, finishFailedBuild(ctx, conn, catalogID, err)
	}
	if !equalPlanSnapshots(snapshot, phaseThree) {
		return BuildResult{}, finishFailedBuild(ctx, conn, catalogID, jobs.Failure(jobs.FailureFilterCatalogPlanInvalid))
	}
	projection, err := projectPlans(phaseThree)
	if err != nil {
		return BuildResult{}, finishFailedBuild(ctx, conn, catalogID, err)
	}
	buildResult, err := populateCatalog(ctx, conn, catalogID, candidate, identity, extracted, projection)
	if err != nil {
		return BuildResult{}, finishFailedBuild(ctx, conn, catalogID, err)
	}
	return buildResult, nil
}

func buildPhaseOne(ctx context.Context, conn *pgxpool.Conn, params BuildParams) (release.CatalogCandidate, planSnapshot, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return release.CatalogCandidate{}, planSnapshot{}, buildDatabaseError(ctx)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	candidate, err := release.SelectCatalogCandidate(ctx, tx, params.PayerID, params.CollectionMonth)
	if err != nil {
		return release.CatalogCandidate{}, planSnapshot{}, err
	}
	snapshot, err := capturePlanSnapshot(ctx, tx, candidate.Targets)
	if err != nil {
		return release.CatalogCandidate{}, planSnapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return release.CatalogCandidate{}, planSnapshot{}, buildDatabaseError(ctx)
	}
	return candidate, snapshot, nil
}

func tryCatalogLock(ctx context.Context, conn *pgxpool.Conn, key int64) (bool, error) {
	var locked bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1::bigint)", key).Scan(&locked); err != nil {
		return false, err
	}
	return locked, nil
}

func targetsToOutputs(targets []release.Target) []Output {
	out := make([]Output, 0, len(targets))
	for _, target := range targets {
		out = append(out, Output{PayerID: target.PayerID, CollectionMonth: target.CollectionMonth, OutputID: target.OutputID, SnapshotID: target.SnapshotID})
	}
	return out
}

func inspectWarehouseIdentity(warehousePath, catalogPath string) (warehouseIdentity, error) {
	state, err := consumeringest.InspectWarehouse(warehousePath)
	if err != nil {
		return warehouseIdentity{}, jobs.Failure(jobs.FailureFilterCatalogWarehouseInvalid)
	}
	schema, monthText, ok := state.RecognizedCatalog()
	if !ok {
		return warehouseIdentity{}, jobs.Failure(jobs.FailureFilterCatalogWarehouseInvalid)
	}
	catalog, err := consumeringest.InspectCatalog(catalogPath)
	if err != nil {
		return warehouseIdentity{}, jobs.Failure(jobs.FailureFilterCatalogWarehouseInvalid)
	}
	manifestSchema, manifestMonth, err := consumeringest.InspectCatalogIdentity(catalog.Path)
	if err != nil || manifestSchema != schema || manifestMonth != monthText {
		return warehouseIdentity{}, jobs.Failure(jobs.FailureFilterCatalogWarehouseInvalid)
	}
	month, err := time.Parse("2006-01", monthText)
	if err != nil || month.Day() != 1 {
		return warehouseIdentity{}, jobs.Failure(jobs.FailureFilterCatalogWarehouseInvalid)
	}
	return warehouseIdentity{SchemaVersion: schema, ReleaseMonth: month}, nil
}

type warehouseIdentity struct {
	SchemaVersion int64
	ReleaseMonth  time.Time
}

func inspectExistingCatalog(ctx context.Context, q planQueryer, candidate release.CatalogCandidate, snapshot planSnapshot, identity warehouseIdentity) (catalogInspection, bool, error) {
	var header catalogHeader
	err := q.QueryRow(ctx, `
SELECT id, payer_id, collection_month, publication_generation, status,
       output_fingerprint, output_count, standard_fact_count,
       provider_catalog_schema_version, provider_catalog_release_month,
       billing_code_count, code_filter_value_count, plan_count,
       plan_output_count, output_code_network_count,
       provider_filter_value_count, failure_code, created_at, completed_at, published_at
FROM mrfweb.release_catalogs
WHERE payer_id = $1 AND collection_month = $2 AND publication_generation = $3`,
		candidate.PayerID, candidate.CollectionMonth+"-01", candidate.TargetPublicationGeneration).Scan(
		&header.ID, &header.PayerID, &header.CollectionMonth, &header.PublicationGeneration, &header.Status,
		&header.OutputFingerprint, &header.OutputCount, &header.StandardFactCount,
		&header.ProviderSchemaVersion, &header.ProviderReleaseMonth,
		&header.BillingCodeCount, &header.CodeFilterValueCount, &header.PlanCount,
		&header.PlanOutputCount, &header.OutputCodeNetworkCount,
		&header.ProviderFilterValueCount, &header.FailureCode, &header.CreatedAt, &header.CompletedAt, &header.PublishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return catalogInspection{}, false, nil
	}
	if err != nil {
		return catalogInspection{}, false, buildDatabaseError(ctx)
	}
	inspection := catalogInspection{Header: header}
	if header.Status != "ready" && header.Status != "published" {
		return inspection, true, nil
	}
	outputs, err := readCatalogOutputs(ctx, q, header.ID)
	if err != nil {
		return catalogInspection{}, false, err
	}
	inspection.Outputs = outputs
	counts, err := readCatalogCounts(ctx, q, header.ID)
	if err != nil {
		return catalogInspection{}, false, err
	}
	inspection.Counts = counts
	refsOK, err := catalogReferencesValid(ctx, q, header.ID)
	if err != nil {
		return catalogInspection{}, false, err
	}
	inspection.ReferencesValid = refsOK
	projection, err := readCatalogProjection(ctx, q, header.ID)
	if err != nil {
		if header.Status == "ready" && (jobs.IsFailure(err, jobs.FailureFilterCatalogInconsistent) || jobs.IsFailure(err, jobs.FailureFilterCatalogPlanInvalid)) {
			return inspection, true, nil
		}
		if header.Status == "published" && jobs.IsFailure(err, jobs.FailureFilterCatalogPlanInvalid) {
			return catalogInspection{}, false, jobs.Failure(jobs.FailureFilterCatalogInconsistent)
		}
		return catalogInspection{}, false, err
	}
	inspection.Projection = projection
	return inspection, true, nil
}

func readCatalogOutputs(ctx context.Context, q planQueryer, catalogID int64) ([]catalogOutput, error) {
	rows, err := q.Query(ctx, `
SELECT o.mrf_snapshot_id, o.output_id, s.payer_id, s.collection_month
FROM mrfweb.release_outputs o
JOIN mrfpipeline.mrf_snapshots s ON s.id = o.mrf_snapshot_id
WHERE o.catalog_id = $1
ORDER BY o.output_id COLLATE "C"`, catalogID)
	if err != nil {
		return nil, buildDatabaseError(ctx)
	}
	defer rows.Close()
	var outputs []catalogOutput
	for rows.Next() {
		var output catalogOutput
		if err := rows.Scan(&output.SnapshotID, &output.OutputID, &output.PayerID, &output.CollectionMonth); err != nil {
			return nil, buildDatabaseError(ctx)
		}
		outputs = append(outputs, output)
	}
	if err := rows.Err(); err != nil {
		return nil, buildDatabaseError(ctx)
	}
	return outputs, nil
}
func readCatalogCounts(ctx context.Context, q planQueryer, catalogID int64) (catalogCounts, error) {
	var counts catalogCounts
	for _, item := range []struct {
		query string
		dest  *int64
	}{
		{`SELECT count(*) FROM mrfweb.release_billing_codes WHERE catalog_id = $1`, &counts.Billing},
		{`SELECT count(*) FROM mrfweb.release_code_filter_values WHERE catalog_id = $1`, &counts.Code},
		{`SELECT count(*) FROM mrfweb.release_plans WHERE catalog_id = $1`, &counts.Plans},
		{`SELECT count(*) FROM mrfweb.release_plan_outputs WHERE catalog_id = $1`, &counts.PlanOut},
		{`SELECT count(*) FROM mrfweb.release_output_code_networks WHERE catalog_id = $1`, &counts.Networks},
		{`SELECT count(*) FROM mrfweb.release_provider_filter_values WHERE catalog_id = $1`, &counts.Providers},
	} {
		if err := q.QueryRow(ctx, item.query, catalogID).Scan(item.dest); err != nil {
			return catalogCounts{}, buildDatabaseError(ctx)
		}
	}
	return counts, nil
}

func catalogReferencesValid(ctx context.Context, q planQueryer, catalogID int64) (bool, error) {
	checks := []string{
		`SELECT count(*) FROM mrfweb.release_code_filter_values v WHERE v.catalog_id = $1 AND NOT EXISTS (SELECT 1 FROM mrfweb.release_billing_codes b WHERE b.catalog_id = v.catalog_id AND b.billing_code_type = v.billing_code_type AND b.billing_code = v.billing_code)`,
		`SELECT count(*) FROM mrfweb.release_output_code_networks n WHERE n.catalog_id = $1 AND NOT EXISTS (SELECT 1 FROM mrfweb.release_billing_codes b WHERE b.catalog_id = n.catalog_id AND b.billing_code_type = n.billing_code_type AND b.billing_code = n.billing_code)`,
		`SELECT count(*) FROM mrfweb.release_output_code_networks n WHERE n.catalog_id = $1 AND NOT EXISTS (SELECT 1 FROM mrfweb.release_outputs o WHERE o.catalog_id = n.catalog_id AND o.output_id = n.output_id)`,
	}
	for _, query := range checks {
		var missing int64
		if err := q.QueryRow(ctx, query, catalogID).Scan(&missing); err != nil {
			return false, buildDatabaseError(ctx)
		}
		if missing != 0 {
			return false, nil
		}
	}
	return true, nil
}

func readCatalogProjection(ctx context.Context, q planQueryer, catalogID int64) (planProjection, error) {
	plansByID := make(map[int64]*catalogPlan)
	rows, err := q.Query(ctx, `
SELECT p.id, p.plan_name, p.issuer_name, p.plan_id_type, p.plan_id, p.plan_market_type, p.search_text, o.output_id
FROM mrfweb.release_plans p
LEFT JOIN mrfweb.release_plan_outputs o
  ON o.catalog_id = p.catalog_id AND o.plan_id = p.id
WHERE p.catalog_id = $1
ORDER BY p.plan_name COLLATE "C", p.issuer_name COLLATE "C", p.plan_id_type COLLATE "C", p.plan_id COLLATE "C", p.plan_market_type COLLATE "C", p.id, o.output_id COLLATE "C"`, catalogID)
	if err != nil {
		return planProjection{}, buildDatabaseError(ctx)
	}
	for rows.Next() {
		var plan catalogPlan
		var output *string
		if err := rows.Scan(&plan.ID, &plan.PlanName, &plan.IssuerName, &plan.PlanIDType, &plan.PlanID, &plan.PlanMarketType, &plan.SearchText, &output); err != nil {
			rows.Close()
			return planProjection{}, buildDatabaseError(ctx)
		}
		if err := validatePlanIdentity(plan.planIdentity); err != nil {
			rows.Close()
			return planProjection{}, err
		}
		want, err := searchText(plan.planIdentity)
		if err != nil || plan.SearchText != want {
			rows.Close()
			return planProjection{}, jobs.Failure(jobs.FailureFilterCatalogInconsistent)
		}
		existing := plansByID[plan.ID]
		if existing == nil {
			existing = &plan
			plansByID[plan.ID] = existing
		}
		if output != nil {
			existing.Outputs = append(existing.Outputs, *output)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return planProjection{}, buildDatabaseError(ctx)
	}
	rows.Close()
	plans := make([]catalogPlan, 0, len(plansByID))
	for _, plan := range plansByID {
		plans = append(plans, *plan)
	}
	projection := planProjection{Plans: make([]canonicalPlan, 0, len(plans))}
	for _, plan := range plans {
		projection.Plans = append(projection.Plans, canonicalPlan{planIdentity: plan.planIdentity, Outputs: append([]string(nil), plan.Outputs...)})
	}
	sortPlans(&projection)
	return projection, nil
}

func sortPlans(projection *planProjection) {
	for i := range projection.Plans {
		projection.Plans[i].Outputs = uniqueSortedStrings(projection.Plans[i].Outputs)
	}
	sort.Slice(projection.Plans, func(i, j int) bool {
		return comparePlanIdentity(projection.Plans[i].planIdentity, projection.Plans[j].planIdentity) < 0
	})
	projection.PlanOutputs = projection.PlanOutputs[:0]
	for _, plan := range projection.Plans {
		for _, output := range plan.Outputs {
			projection.PlanOutputs = append(projection.PlanOutputs, planOutput{Plan: plan.planIdentity, Output: output})
		}
	}
}

func uniqueSortedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	sort.Strings(values)
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}

func catalogInternallyValid(inspection catalogInspection) bool {
	h := inspection.Header
	if h.PayerID == "" || h.CollectionMonth.Day() != 1 || h.PublicationGeneration <= 0 ||
		(h.Status != "ready" && h.Status != "published") || !validFingerprint(h.OutputFingerprint) || h.OutputCount <= 0 ||
		int64(len(inspection.Outputs)) != h.OutputCount || h.CreatedAt.IsZero() || h.CompletedAt == nil ||
		h.CompletedAt.Before(h.CreatedAt) || h.StandardFactCount == nil || *h.StandardFactCount <= 0 ||
		h.ProviderSchemaVersion == nil || *h.ProviderSchemaVersion <= 0 || h.ProviderReleaseMonth == nil || h.ProviderReleaseMonth.Day() != 1 ||
		h.BillingCodeCount == nil || h.CodeFilterValueCount == nil || h.PlanCount == nil || h.PlanOutputCount == nil ||
		h.OutputCodeNetworkCount == nil || h.ProviderFilterValueCount == nil || *h.BillingCodeCount <= 0 || *h.PlanCount <= 0 || *h.PlanOutputCount <= 0 ||
		*h.CodeFilterValueCount < 0 || *h.OutputCodeNetworkCount < 0 || *h.ProviderFilterValueCount < 0 {
		return false
	}
	if h.Status == "ready" {
		if h.FailureCode != nil || h.PublishedAt != nil {
			return false
		}
	} else if h.FailureCode != nil || h.PublishedAt == nil || h.PublishedAt.Before(*h.CompletedAt) {
		return false
	}
	if !inspection.ReferencesValid {
		return false
	}
	if int64(len(inspection.Projection.Plans)) != inspection.Counts.Plans || int64(len(inspection.Projection.PlanOutputs)) != inspection.Counts.PlanOut {
		return false
	}
	seenIDs := make(map[int64]struct{}, len(inspection.Outputs))
	seenOutputs := make(map[string]struct{}, len(inspection.Outputs))
	targets := make([]release.Target, 0, len(inspection.Outputs))
	for _, output := range inspection.Outputs {
		if output.PayerID != h.PayerID || output.CollectionMonth.Format("2006-01") != h.CollectionMonth.Format("2006-01") ||
			output.SnapshotID <= 0 || output.OutputID != fmt.Sprintf("mrf-%d", output.SnapshotID) {
			return false
		}
		if _, ok := seenIDs[output.SnapshotID]; ok {
			return false
		}
		if _, ok := seenOutputs[output.OutputID]; ok {
			return false
		}
		seenIDs[output.SnapshotID] = struct{}{}
		seenOutputs[output.OutputID] = struct{}{}
		targets = append(targets, release.Target{PayerID: h.PayerID, CollectionMonth: h.CollectionMonth.Format("2006-01"), OutputID: output.OutputID, SnapshotID: output.SnapshotID})
	}
	if release.OutputFingerprint(targets) != h.OutputFingerprint {
		return false
	}
	if inspection.Counts.Billing != *h.BillingCodeCount || inspection.Counts.Code != *h.CodeFilterValueCount ||
		inspection.Counts.Plans != *h.PlanCount || inspection.Counts.PlanOut != *h.PlanOutputCount ||
		inspection.Counts.Networks != *h.OutputCodeNetworkCount || inspection.Counts.Providers != *h.ProviderFilterValueCount {
		return false
	}
	outputIDs := make(map[string]struct{}, len(inspection.Outputs))
	for _, output := range inspection.Outputs {
		outputIDs[output.OutputID] = struct{}{}
	}
	planOutputs := make(map[string]struct{}, len(inspection.Projection.PlanOutputs))
	for _, planOutput := range inspection.Projection.PlanOutputs {
		if _, ok := outputIDs[planOutput.Output]; !ok {
			return false
		}
		planOutputs[planOutput.Output] = struct{}{}
	}
	if len(planOutputs) != len(outputIDs) {
		return false
	}
	for _, plan := range inspection.Projection.Plans {
		if len(plan.Outputs) == 0 {
			return false
		}
	}
	return true
}

func catalogMatchesCandidate(inspection catalogInspection, candidate release.CatalogCandidate, snapshot planSnapshot, identity warehouseIdentity) bool {
	if !catalogInternallyValid(inspection) {
		return false
	}
	h := inspection.Header
	if h.PayerID != candidate.PayerID || h.CollectionMonth.Format("2006-01") != candidate.CollectionMonth || h.PublicationGeneration != candidate.TargetPublicationGeneration || h.OutputFingerprint != candidate.OutputFingerprint || h.OutputCount != int64(len(candidate.Targets)) || h.ProviderSchemaVersion == nil || *h.ProviderSchemaVersion != identity.SchemaVersion || h.ProviderReleaseMonth == nil || !h.ProviderReleaseMonth.Equal(identity.ReleaseMonth) {
		return false
	}
	actual := make(map[string]int64, len(inspection.Outputs))
	for _, output := range inspection.Outputs {
		actual[output.OutputID] = output.SnapshotID
	}
	for _, target := range candidate.Targets {
		if actual[target.OutputID] != target.SnapshotID {
			return false
		}
	}
	projection, err := projectPlans(snapshot)
	if err != nil {
		return false
	}
	return reflect.DeepEqual(inspection.Projection, projection)
}

func createBuildingCatalog(ctx context.Context, conn *pgxpool.Conn, candidate release.CatalogCandidate, replace bool, oldID int64) (int64, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, buildDatabaseError(ctx)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if replace {
		tag, err := tx.Exec(ctx, `DELETE FROM mrfweb.release_catalogs WHERE id = $1 AND status <> 'published'`, oldID)
		if err != nil {
			return 0, buildDatabaseError(ctx)
		}
		if tag.RowsAffected() != 1 {
			return 0, jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
		}
	}
	var catalogID int64
	if err := tx.QueryRow(ctx, `
INSERT INTO mrfweb.release_catalogs
    (payer_id, collection_month, publication_generation, status, output_fingerprint, output_count)
VALUES ($1, $2, $3, 'building', $4, $5)
RETURNING id`, candidate.PayerID, candidate.CollectionMonth+"-01", candidate.TargetPublicationGeneration, candidate.OutputFingerprint, len(candidate.Targets)).Scan(&catalogID); err != nil {
		return 0, buildDatabaseError(ctx)
	}
	outputCount, err := tx.CopyFrom(ctx, pgx.Identifier{"mrfweb", "release_outputs"}, []string{"catalog_id", "mrf_snapshot_id", "output_id"}, pgx.CopyFromSlice(len(candidate.Targets), func(i int) ([]any, error) {
		target := candidate.Targets[i]
		return []any{catalogID, target.SnapshotID, target.OutputID}, nil
	}))
	if err != nil || outputCount != int64(len(candidate.Targets)) {
		return 0, buildDatabaseError(ctx)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, buildDatabaseError(ctx)
	}
	return catalogID, nil
}

func populateCatalog(ctx context.Context, conn *pgxpool.Conn, catalogID int64, candidate release.CatalogCandidate, identity warehouseIdentity, extracted Result, projection planProjection) (BuildResult, error) {
	if extracted.WarehouseSchemaVersion != "2.0.0" || extracted.ProviderCatalogSchemaVersion != identity.SchemaVersion || !extracted.ProviderCatalogReleaseMonth.Equal(identity.ReleaseMonth) || extracted.OutputFingerprint != candidate.OutputFingerprint || extracted.StandardFactCount <= 0 || len(extracted.BillingCodes) == 0 || len(projection.Plans) == 0 {
		return BuildResult{}, jobs.Failure(jobs.FailureFilterCatalogPopulationFailed)
	}
	codeSet := make(map[codeKey]struct{}, len(extracted.BillingCodes))
	for _, row := range extracted.BillingCodes {
		key := codeKey{billingCodeType: row.BillingCodeType, billingCode: row.BillingCode}
		if _, exists := codeSet[key]; exists {
			return BuildResult{}, jobs.Failure(jobs.FailureFilterCatalogPopulationFailed)
		}
		codeSet[key] = struct{}{}
	}
	outputSet := make(map[string]struct{}, len(candidate.Targets))
	for _, target := range candidate.Targets {
		outputSet[target.OutputID] = struct{}{}
	}
	for _, row := range extracted.CodeFilterValues {
		if _, ok := codeSet[codeKey{billingCodeType: row.BillingCodeType, billingCode: row.BillingCode}]; !ok {
			return BuildResult{}, jobs.Failure(jobs.FailureFilterCatalogPopulationFailed)
		}
	}
	for _, row := range extracted.OutputCodeNetworks {
		if _, ok := codeSet[codeKey{billingCodeType: row.BillingCodeType, billingCode: row.BillingCode}]; !ok {
			return BuildResult{}, jobs.Failure(jobs.FailureFilterCatalogPopulationFailed)
		}
		if _, ok := outputSet[row.OutputID]; !ok {
			return BuildResult{}, jobs.Failure(jobs.FailureFilterCatalogPopulationFailed)
		}
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return BuildResult{}, buildDatabaseError(ctx)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var payer, status, fingerprint string
	var month time.Time
	var generation, outputCount int64
	if err := tx.QueryRow(ctx, `
SELECT payer_id, collection_month, publication_generation, status, output_fingerprint, output_count
FROM mrfweb.release_catalogs WHERE id = $1 FOR UPDATE`, catalogID).Scan(&payer, &month, &generation, &status, &fingerprint, &outputCount); err != nil {
		return BuildResult{}, buildDatabaseError(ctx)
	}
	if payer != candidate.PayerID || month.Format("2006-01") != candidate.CollectionMonth || generation != candidate.TargetPublicationGeneration || status != "building" || fingerprint != candidate.OutputFingerprint || outputCount != int64(len(candidate.Targets)) {
		return BuildResult{}, jobs.Failure(jobs.FailureFilterCatalogPopulationFailed)
	}
	actualOutputs, err := readCatalogOutputs(ctx, tx, catalogID)
	if err != nil || !sameCatalogOutputs(actualOutputs, candidate.Targets) {
		if err != nil {
			return BuildResult{}, err
		}
		return BuildResult{}, jobs.Failure(jobs.FailureFilterCatalogPopulationFailed)
	}
	billingCount, err := tx.CopyFrom(ctx, pgx.Identifier{"mrfweb", "release_billing_codes"}, []string{"catalog_id", "billing_code_type", "billing_code", "billing_code_type_version", "warehouse_service_name", "warehouse_service_description", "observation_count", "unmodified_observation_count"}, pgx.CopyFromSlice(len(extracted.BillingCodes), func(i int) ([]any, error) {
		row := extracted.BillingCodes[i]
		return []any{catalogID, row.BillingCodeType, row.BillingCode, row.BillingCodeTypeVersion, row.WarehouseServiceName, row.WarehouseServiceDescription, row.ObservationCount, row.UnmodifiedObservationCount}, nil
	}))
	if err != nil || billingCount != int64(len(extracted.BillingCodes)) {
		return BuildResult{}, buildDatabaseError(ctx)
	}
	codeCount, err := tx.CopyFrom(ctx, pgx.Identifier{"mrfweb", "release_code_filter_values"}, []string{"catalog_id", "billing_code_type", "billing_code", "filter_kind", "filter_value", "display_label", "observation_count"}, pgx.CopyFromSlice(len(extracted.CodeFilterValues), func(i int) ([]any, error) {
		row := extracted.CodeFilterValues[i]
		return []any{catalogID, row.BillingCodeType, row.BillingCode, row.FilterKind, row.FilterValue, row.DisplayLabel, row.ObservationCount}, nil
	}))
	if err != nil || codeCount != int64(len(extracted.CodeFilterValues)) {
		return BuildResult{}, buildDatabaseError(ctx)
	}
	planIDs := make(map[planIdentity]int64, len(projection.Plans))
	for _, plan := range projection.Plans {
		search, err := searchText(plan.planIdentity)
		if err != nil {
			return BuildResult{}, err
		}
		var planID int64
		if err := tx.QueryRow(ctx, `
INSERT INTO mrfweb.release_plans
    (catalog_id, plan_name, issuer_name, plan_id_type, plan_id, plan_market_type, search_text)
VALUES ($1,$2,$3,$4,$5,$6,$7)
RETURNING id`, catalogID, plan.PlanName, plan.IssuerName, plan.PlanIDType, plan.PlanID, plan.PlanMarketType, search).Scan(&planID); err != nil {
			return BuildResult{}, buildDatabaseError(ctx)
		}
		planIDs[plan.planIdentity] = planID
	}
	planOutputCount, err := tx.CopyFrom(ctx, pgx.Identifier{"mrfweb", "release_plan_outputs"}, []string{"catalog_id", "plan_id", "output_id"}, pgx.CopyFromSlice(len(projection.PlanOutputs), func(i int) ([]any, error) {
		row := projection.PlanOutputs[i]
		planID, ok := planIDs[row.Plan]
		if !ok {
			return nil, jobs.Failure(jobs.FailureFilterCatalogPopulationFailed)
		}
		return []any{catalogID, planID, row.Output}, nil
	}))
	if err != nil || planOutputCount != int64(len(projection.PlanOutputs)) {
		return BuildResult{}, buildDatabaseError(ctx)
	}
	networkCount, err := tx.CopyFrom(ctx, pgx.Identifier{"mrfweb", "release_output_code_networks"}, []string{"catalog_id", "output_id", "billing_code_type", "billing_code", "network_name", "observation_count"}, pgx.CopyFromSlice(len(extracted.OutputCodeNetworks), func(i int) ([]any, error) {
		row := extracted.OutputCodeNetworks[i]
		return []any{catalogID, row.OutputID, row.BillingCodeType, row.BillingCode, row.NetworkName, row.ObservationCount}, nil
	}))
	if err != nil || networkCount != int64(len(extracted.OutputCodeNetworks)) {
		return BuildResult{}, buildDatabaseError(ctx)
	}
	providerCount, err := tx.CopyFrom(ctx, pgx.Identifier{"mrfweb", "release_provider_filter_values"}, []string{"catalog_id", "filter_kind", "parent_value", "filter_value", "provider_count"}, pgx.CopyFromSlice(len(extracted.ProviderFilterValues), func(i int) ([]any, error) {
		row := extracted.ProviderFilterValues[i]
		return []any{catalogID, row.FilterKind, row.ParentValue, row.FilterValue, row.ProviderCount}, nil
	}))
	if err != nil || providerCount != int64(len(extracted.ProviderFilterValues)) {
		return BuildResult{}, buildDatabaseError(ctx)
	}

	counts, err := readCatalogCounts(ctx, tx, catalogID)
	if err != nil {
		return BuildResult{}, err
	}
	if counts.Billing != int64(len(extracted.BillingCodes)) || counts.Code != int64(len(extracted.CodeFilterValues)) || counts.Plans != int64(len(projection.Plans)) || counts.PlanOut != int64(len(projection.PlanOutputs)) || counts.Networks != int64(len(extracted.OutputCodeNetworks)) || counts.Providers != int64(len(extracted.ProviderFilterValues)) || counts.Billing <= 0 || counts.Plans <= 0 {
		return BuildResult{}, jobs.Failure(jobs.FailureFilterCatalogPopulationFailed)
	}
	var missingPlans, missingCodes, missingNetworks, missingOutputs int64
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM mrfweb.release_outputs o WHERE o.catalog_id = $1 AND NOT EXISTS (SELECT 1 FROM mrfweb.release_plan_outputs p WHERE p.catalog_id = o.catalog_id AND p.output_id = o.output_id)`, catalogID).Scan(&missingPlans); err != nil {
		return BuildResult{}, buildDatabaseError(ctx)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM mrfweb.release_code_filter_values v WHERE v.catalog_id = $1 AND NOT EXISTS (SELECT 1 FROM mrfweb.release_billing_codes b WHERE b.catalog_id = v.catalog_id AND b.billing_code_type = v.billing_code_type AND b.billing_code = v.billing_code)`, catalogID).Scan(&missingCodes); err != nil {
		return BuildResult{}, buildDatabaseError(ctx)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM mrfweb.release_output_code_networks n WHERE n.catalog_id = $1 AND NOT EXISTS (SELECT 1 FROM mrfweb.release_billing_codes b WHERE b.catalog_id = n.catalog_id AND b.billing_code_type = n.billing_code_type AND b.billing_code = n.billing_code)`, catalogID).Scan(&missingNetworks); err != nil {
		return BuildResult{}, buildDatabaseError(ctx)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM mrfweb.release_output_code_networks n WHERE n.catalog_id = $1 AND NOT EXISTS (SELECT 1 FROM mrfweb.release_outputs o WHERE o.catalog_id = n.catalog_id AND o.output_id = n.output_id)`, catalogID).Scan(&missingOutputs); err != nil {
		return BuildResult{}, buildDatabaseError(ctx)
	}
	if missingPlans != 0 || missingCodes != 0 || missingNetworks != 0 || missingOutputs != 0 {
		return BuildResult{}, jobs.Failure(jobs.FailureFilterCatalogPopulationFailed)
	}
	if _, err := tx.Exec(ctx, `
UPDATE mrfweb.release_catalogs
SET standard_fact_count = $2,
    provider_catalog_schema_version = $3,
    provider_catalog_release_month = $4,
    billing_code_count = $5,
    code_filter_value_count = $6,
    plan_count = $7,
    plan_output_count = $8,
    output_code_network_count = $9,
    provider_filter_value_count = $10,
    status = 'ready', completed_at = transaction_timestamp(), failure_code = NULL
WHERE id = $1`, catalogID, extracted.StandardFactCount, extracted.ProviderCatalogSchemaVersion, extracted.ProviderCatalogReleaseMonth, counts.Billing, counts.Code, counts.Plans, counts.PlanOut, counts.Networks, counts.Providers); err != nil {
		return BuildResult{}, buildDatabaseError(ctx)
	}
	if err := tx.Commit(ctx); err != nil {
		return BuildResult{}, buildDatabaseError(ctx)
	}
	return BuildResult{
		PayerID: candidate.PayerID, CollectionMonth: candidate.CollectionMonth,
		PublicationGeneration: candidate.TargetPublicationGeneration, CatalogID: catalogID,
		CatalogStatus: "ready", OutputCount: int64(len(candidate.Targets)),
		StandardFactCount: extracted.StandardFactCount, BillingCodeCount: counts.Billing,
		CodeFilterValueCount: counts.Code, PlanCount: counts.Plans, PlanOutputCount: counts.PlanOut,
		OutputCodeNetworkCount: counts.Networks, ProviderFilterValueCount: counts.Providers,
	}, nil
}

func sameCatalogOutputs(outputs []catalogOutput, targets []release.Target) bool {
	if len(outputs) != len(targets) {
		return false
	}
	seen := make(map[string]int64, len(outputs))
	for _, output := range outputs {
		if _, ok := seen[output.OutputID]; ok {
			return false
		}
		seen[output.OutputID] = output.SnapshotID
	}
	for _, target := range targets {
		if seen[target.OutputID] != target.SnapshotID {
			return false
		}
	}
	return true
}

func rereadPlanSnapshot(ctx context.Context, conn *pgxpool.Conn, targets []release.Target) (planSnapshot, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return planSnapshot{}, buildDatabaseError(ctx)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	snapshot, err := capturePlanSnapshot(ctx, tx, targets)
	if err != nil {
		return planSnapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return planSnapshot{}, buildDatabaseError(ctx)
	}
	return snapshot, nil
}
func finishFailedBuild(ctx context.Context, conn *pgxpool.Conn, catalogID int64, original error) error {
	public := original
	if ctx != nil && ctx.Err() != nil {
		public = jobs.Failure(jobs.FailureFilterCatalogCancelled)
	} else if errors.Is(original, database.ErrDatabase) {
		public = jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
	}
	code := cleanupFailureCode(public)
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = markCatalogFailed(cleanupCtx, conn, catalogID, code)
	return public
}
func cleanupFailureCode(err error) string {
	for _, code := range []string{
		jobs.FailureFilterCatalogConfigInvalid, jobs.FailureFilterCatalogWarehouseInvalid,
		jobs.FailureFilterCatalogDuckDBUnavailable, jobs.FailureFilterCatalogDuckDBVersionInvalid,
		jobs.FailureFilterCatalogQueryFailed, jobs.FailureFilterCatalogValueInvalid,
		jobs.FailureFilterCatalogProtocolInvalid, jobs.FailureFilterCatalogCancelled,
	} {
		if jobs.IsFailure(err, code) {
			return code
		}
	}
	if jobs.IsFailure(err, jobs.FailureFilterCatalogPlanInvalid) {
		return jobs.FailureFilterCatalogPlanInvalid
	}
	if jobs.IsFailure(err, jobs.FailureFilterCatalogPopulationFailed) {
		return jobs.FailureFilterCatalogPopulationFailed
	}
	return jobs.FailureFilterCatalogDatabaseFailed
}

func markCatalogFailed(ctx context.Context, conn *pgxpool.Conn, catalogID int64, code string) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM mrfweb.release_catalogs WHERE id = $1 FOR UPDATE`, catalogID).Scan(&status); err != nil {
		return err
	}
	if status != "building" {
		return errors.New("catalog is not building")
	}
	for _, table := range []string{
		"release_plan_outputs", "release_output_code_networks", "release_code_filter_values",
		"release_provider_filter_values", "release_plans", "release_billing_codes", "release_outputs",
	} {
		if _, err := tx.Exec(ctx, "DELETE FROM mrfweb."+table+" WHERE catalog_id = $1", catalogID); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `
UPDATE mrfweb.release_catalogs
SET status = 'failed', failure_code = $2, completed_at = transaction_timestamp(), published_at = NULL
WHERE id = $1`, catalogID, code); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func resultFromHeader(header catalogHeader, unchanged bool) (BuildResult, error) {
	if header.StandardFactCount == nil || header.BillingCodeCount == nil || header.CodeFilterValueCount == nil || header.PlanCount == nil || header.PlanOutputCount == nil || header.OutputCodeNetworkCount == nil || header.ProviderFilterValueCount == nil {
		return BuildResult{}, jobs.Failure(jobs.FailureFilterCatalogInconsistent)
	}
	return BuildResult{
		PayerID: header.PayerID, CollectionMonth: header.CollectionMonth.Format("2006-01"),
		PublicationGeneration: header.PublicationGeneration, CatalogID: header.ID,
		CatalogStatus: header.Status, OutputCount: header.OutputCount,
		StandardFactCount: *header.StandardFactCount, BillingCodeCount: *header.BillingCodeCount,
		CodeFilterValueCount: *header.CodeFilterValueCount, PlanCount: *header.PlanCount,
		PlanOutputCount: *header.PlanOutputCount, OutputCodeNetworkCount: *header.OutputCodeNetworkCount,
		ProviderFilterValueCount: *header.ProviderFilterValueCount, Unchanged: unchanged,
	}, nil
}

func buildDatabaseError(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return database.ErrDatabase
}

func mapBuildError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, database.ErrDatabase) {
		return jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
	}
	if jobs.IsFailure(err, jobs.FailureReleaseNotReady) || jobs.IsFailure(err, jobs.FailureDomainInvariant) || jobs.IsFailure(err, jobs.FailureSealedReleaseInconsistent) {
		return jobs.Failure(jobs.FailureFilterCatalogReleaseNotReady)
	}
	return err
}
