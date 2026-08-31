package filtercatalog

import (
	"context"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5"
)

type planQueryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type planIdentity struct {
	PlanName       string
	IssuerName     string
	PlanIDType     string
	PlanID         string
	PlanMarketType string
}

type planSnapshotRow struct {
	OutputID string
	planIdentity
}

type planReadinessFact struct {
	OutputID        string
	PlanCount       int64
	UnassignedCount int64
	PositiveBatch   bool
	IncompleteBatch bool
}

type planSnapshot struct {
	Rows  []planSnapshotRow
	Facts []planReadinessFact
}

type canonicalPlan struct {
	planIdentity
	Outputs []string
}

type planOutput struct {
	Plan   planIdentity
	Output string
}

type planProjection struct {
	Plans       []canonicalPlan
	PlanOutputs []planOutput
}

func capturePlanSnapshot(ctx context.Context, q planQueryer, targets []release.Target) (planSnapshot, error) {
	if q == nil || len(targets) == 0 {
		return planSnapshot{}, jobs.Failure(jobs.FailureFilterCatalogPlanInvalid)
	}
	ids := make([]int64, 0, len(targets))
	for _, target := range targets {
		ids = append(ids, target.SnapshotID)
	}
	wantOutputs := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		wantOutputs[target.OutputID] = struct{}{}
	}
	var snapshot planSnapshot
	rows, err := q.Query(ctx, `
SELECT 'mrf-' || p.mrf_snapshot_id,
       p.plan_name, p.issuer_name, p.plan_id_type, p.plan_id, p.plan_market_type
FROM mrfpipeline.mrf_plans p
WHERE p.mrf_snapshot_id = ANY($1::bigint[])`, ids)
	if err != nil {
		return planSnapshot{}, database.ErrDatabase
	}
	for rows.Next() {
		var row planSnapshotRow
		if err := rows.Scan(&row.OutputID, &row.PlanName, &row.IssuerName, &row.PlanIDType, &row.PlanID, &row.PlanMarketType); err != nil {
			rows.Close()
			return planSnapshot{}, database.ErrDatabase
		}
		if _, ok := wantOutputs[row.OutputID]; !ok {
			rows.Close()
			return planSnapshot{}, jobs.Failure(jobs.FailureFilterCatalogPlanInvalid)
		}
		if err := validatePlanIdentity(row.planIdentity); err != nil {
			rows.Close()
			return planSnapshot{}, err
		}
		snapshot.Rows = append(snapshot.Rows, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return planSnapshot{}, database.ErrDatabase
	}
	rows.Close()

	rows, err = q.Query(ctx, `
SELECT 'mrf-' || s.id,
       (SELECT count(*) FROM mrfpipeline.mrf_plans p WHERE p.mrf_snapshot_id = s.id),
       (SELECT count(*)
        FROM mrfpipeline.mrf_plans p
        WHERE p.mrf_snapshot_id = s.id
          AND NOT EXISTS (
              SELECT 1
              FROM mrfpipeline.plan_attachment_batch_items i
              JOIN mrfpipeline.plan_attachment_batches b ON b.id = i.plan_attachment_batch_id
              WHERE i.mrf_plan_id = p.id
                AND b.mrf_snapshot_id = s.id
                AND b.status = 'succeeded')),
       EXISTS (
           SELECT 1 FROM mrfpipeline.plan_attachment_batches b
           WHERE b.mrf_snapshot_id = s.id AND b.status = 'succeeded' AND b.added_plan_count > 0),
       EXISTS (
           SELECT 1 FROM mrfpipeline.plan_attachment_batches b
           WHERE b.mrf_snapshot_id = s.id AND b.status <> 'succeeded')
FROM mrfpipeline.mrf_snapshots s
WHERE s.id = ANY($1::bigint[])
ORDER BY s.id`, ids)
	if err != nil {
		return planSnapshot{}, database.ErrDatabase
	}
	for rows.Next() {
		var fact planReadinessFact
		if err := rows.Scan(&fact.OutputID, &fact.PlanCount, &fact.UnassignedCount, &fact.PositiveBatch, &fact.IncompleteBatch); err != nil {
			rows.Close()
			return planSnapshot{}, database.ErrDatabase
		}
		snapshot.Facts = append(snapshot.Facts, fact)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return planSnapshot{}, database.ErrDatabase
	}
	rows.Close()

	if len(snapshot.Facts) != len(targets) {
		return planSnapshot{}, jobs.Failure(jobs.FailureFilterCatalogPlanInvalid)
	}
	for _, fact := range snapshot.Facts {
		if _, ok := wantOutputForTargets(fact.OutputID, targets); !ok || fact.PlanCount <= 0 || fact.UnassignedCount != 0 || !fact.PositiveBatch || fact.IncompleteBatch {
			return planSnapshot{}, jobs.Failure(jobs.FailureFilterCatalogPlanInvalid)
		}
	}
	for _, row := range snapshot.Rows {
		if _, ok := wantOutputForTargets(row.OutputID, targets); !ok {
			return planSnapshot{}, jobs.Failure(jobs.FailureFilterCatalogPlanInvalid)
		}
	}
	sort.Slice(snapshot.Rows, func(i, j int) bool {
		if cmp := strings.Compare(snapshot.Rows[i].OutputID, snapshot.Rows[j].OutputID); cmp != 0 {
			return cmp < 0
		}
		return comparePlanIdentity(snapshot.Rows[i].planIdentity, snapshot.Rows[j].planIdentity) < 0
	})
	sort.Slice(snapshot.Facts, func(i, j int) bool {
		return strings.Compare(snapshot.Facts[i].OutputID, snapshot.Facts[j].OutputID) < 0
	})
	return snapshot, nil
}

func wantOutputForTargets(output string, targets []release.Target) (release.Target, bool) {
	for _, target := range targets {
		if target.OutputID == output {
			return target, true
		}
	}
	return release.Target{}, false
}

func validatePlanIdentity(identity planIdentity) error {
	for _, value := range []string{identity.PlanName, identity.IssuerName, identity.PlanIDType, identity.PlanID, identity.PlanMarketType} {
		if value == "" || !utf8.ValidString(value) || strings.ContainsAny(value, "\r\n") {
			return jobs.Failure(jobs.FailureFilterCatalogPlanInvalid)
		}
	}
	if identity.PlanIDType != "ein" && identity.PlanIDType != "hios" {
		return jobs.Failure(jobs.FailureFilterCatalogPlanInvalid)
	}
	if identity.PlanMarketType != "group" && identity.PlanMarketType != "individual" {
		return jobs.Failure(jobs.FailureFilterCatalogPlanInvalid)
	}
	return nil
}

func equalPlanSnapshots(left, right planSnapshot) bool {
	if len(left.Rows) != len(right.Rows) || len(left.Facts) != len(right.Facts) {
		return false
	}
	for i := range left.Rows {
		if left.Rows[i] != right.Rows[i] {
			return false
		}
	}
	for i := range left.Facts {
		if left.Facts[i] != right.Facts[i] {
			return false
		}
	}
	return true
}

func projectPlans(snapshot planSnapshot) (planProjection, error) {
	plans := make(map[planIdentity]*canonicalPlan)
	for _, row := range snapshot.Rows {
		if err := validatePlanIdentity(row.planIdentity); err != nil {
			return planProjection{}, err
		}
		plan := plans[row.planIdentity]
		if plan == nil {
			plan = &canonicalPlan{planIdentity: row.planIdentity}
			plans[row.planIdentity] = plan
		}
		if len(plan.Outputs) == 0 || plan.Outputs[len(plan.Outputs)-1] != row.OutputID {
			seen := false
			for _, output := range plan.Outputs {
				if output == row.OutputID {
					seen = true
					break
				}
			}
			if !seen {
				plan.Outputs = append(plan.Outputs, row.OutputID)
			}
		}
	}
	projection := planProjection{Plans: make([]canonicalPlan, 0, len(plans))}
	for _, plan := range plans {
		sort.Slice(plan.Outputs, func(i, j int) bool { return strings.Compare(plan.Outputs[i], plan.Outputs[j]) < 0 })
		projection.Plans = append(projection.Plans, *plan)
	}
	sort.Slice(projection.Plans, func(i, j int) bool {
		return comparePlanIdentity(projection.Plans[i].planIdentity, projection.Plans[j].planIdentity) < 0
	})
	for _, plan := range projection.Plans {
		for _, output := range plan.Outputs {
			projection.PlanOutputs = append(projection.PlanOutputs, planOutput{Plan: plan.planIdentity, Output: output})
		}
	}
	sort.Slice(projection.PlanOutputs, func(i, j int) bool {
		if cmp := comparePlanIdentity(projection.PlanOutputs[i].Plan, projection.PlanOutputs[j].Plan); cmp != 0 {
			return cmp < 0
		}
		return strings.Compare(projection.PlanOutputs[i].Output, projection.PlanOutputs[j].Output) < 0
	})
	if len(projection.Plans) == 0 || len(projection.PlanOutputs) == 0 {
		return planProjection{}, jobs.Failure(jobs.FailureFilterCatalogPlanInvalid)
	}
	return projection, nil
}

func searchText(identity planIdentity) (string, error) {
	values := []string{
		strings.Trim(identity.PlanName, " \t\r\n\v\f"),
		strings.Trim(identity.IssuerName, " \t\r\n\v\f"),
		strings.Trim(identity.PlanIDType, " \t\r\n\v\f"),
		strings.Trim(identity.PlanID, " \t\r\n\v\f"),
		strings.Trim(identity.PlanMarketType, " \t\r\n\v\f"),
	}
	result := strings.ToLower(strings.Join(values, " "))
	if result == "" || !utf8.ValidString(result) || strings.ContainsAny(result, "\r\n") {
		return "", jobs.Failure(jobs.FailureFilterCatalogPlanInvalid)
	}
	return result, nil
}
