package release

import (
	"context"
	"errors"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"strings"
	"time"
)

type catalogQueryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}
type activationCatalog struct {
	ID                                                                                                                    int64
	PayerID, Status, OutputFingerprint                                                                                    string
	CollectionMonth                                                                                                       time.Time
	OutputCount                                                                                                           int64
	StandardFacts, ProviderSchema, BillingCount, CodeFilterCount, PlanCount, PlanOutputCount, NetworkCount, ProviderCount *int64
	ProviderMonth                                                                                                         *time.Time
	FailureCode                                                                                                           *string
	CreatedAt                                                                                                             time.Time
	CompletedAt, PublishedAt                                                                                              *time.Time
	Outputs                                                                                                               []Target
}
type activationPlanRow struct{ OutputID, PlanName, IssuerName, PlanIDType, PlanID, PlanMarketType string }

type CatalogAudit struct {
	ID                    int64
	Status                string
	ProviderSchemaVersion *int64
	ProviderReleaseMonth  *time.Time
}

func readActivationHeader(ctx context.Context, q catalogQueryer, payer, month string, generation int64, lock bool) (activationCatalog, error) {
	var c activationCatalog
	clause := ""
	if lock {
		clause = " FOR UPDATE NOWAIT"
	}
	err := q.QueryRow(ctx, `SELECT id,payer_id,collection_month,status,output_fingerprint,output_count,standard_fact_count,provider_catalog_schema_version,provider_catalog_release_month,billing_code_count,code_filter_value_count,plan_count,plan_output_count,output_code_network_count,provider_filter_value_count,failure_code,created_at,completed_at,published_at FROM mrfweb.release_catalogs WHERE payer_id=$1 AND collection_month=$2 AND publication_generation=$3`+clause, payer, month+"-01", generation).Scan(&c.ID, &c.PayerID, &c.CollectionMonth, &c.Status, &c.OutputFingerprint, &c.OutputCount, &c.StandardFacts, &c.ProviderSchema, &c.ProviderMonth, &c.BillingCount, &c.CodeFilterCount, &c.PlanCount, &c.PlanOutputCount, &c.NetworkCount, &c.ProviderCount, &c.FailureCode, &c.CreatedAt, &c.CompletedAt, &c.PublishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, jobs.Failure(jobs.FailureFilterCatalogMissing)
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "55P03" {
		return c, jobs.Failure(jobs.FailureFilterCatalogNotReady)
	}
	if err != nil {
		return c, jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
	}
	return c, nil
}
func readActivationCatalogOutputs(ctx context.Context, q catalogQueryer, c *activationCatalog) error {
	rows, err := q.Query(ctx, `SELECT o.output_id,s.payer_id,s.collection_month,s.id FROM mrfweb.release_outputs o JOIN mrfpipeline.mrf_snapshots s ON s.id=o.mrf_snapshot_id WHERE o.catalog_id=$1 ORDER BY s.id`, c.ID)
	if err != nil {
		return jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
	}
	defer rows.Close()
	seen := map[Target]bool{}
	for rows.Next() {
		var stored, payer string
		var month time.Time
		var id int64
		if err := rows.Scan(&stored, &payer, &month, &id); err != nil {
			return jobs.Failure(jobs.FailureFilterCatalogInconsistent)
		}
		target := Target{PayerID: payer, CollectionMonth: formatMonth(month), OutputID: stored, SnapshotID: id}
		if payer != c.PayerID || target.CollectionMonth != formatMonth(c.CollectionMonth) || stored != formatOutputID(id) || seen[target] {
			return jobs.Failure(jobs.FailureFilterCatalogInconsistent)
		}
		seen[target] = true
		c.Outputs = append(c.Outputs, target)
	}
	if rows.Err() != nil {
		return jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
	}
	return nil
}
func sameTargetSet(a, b []Target) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[Target]bool{}
	for _, x := range a {
		if seen[x] {
			return false
		}
		seen[x] = true
	}
	seenB := map[Target]bool{}
	for _, x := range b {
		if seenB[x] || !seen[x] {
			return false
		}
		seenB[x] = true
	}
	return true
}
func validateActivationCatalog(ctx context.Context, q catalogQueryer, c activationCatalog) error {
	if c.PayerID == "" || c.CollectionMonth.Day() != 1 || c.OutputCount <= 0 || c.StandardFacts == nil || *c.StandardFacts <= 0 || c.ProviderSchema == nil || *c.ProviderSchema <= 0 || c.ProviderMonth == nil || c.ProviderMonth.Day() != 1 || c.CompletedAt == nil || c.CreatedAt.IsZero() || c.CompletedAt.Before(c.CreatedAt) || c.FailureCode != nil {
		return jobs.Failure(jobs.FailureFilterCatalogInconsistent)
	}
	if c.Status == "ready" && c.PublishedAt != nil || c.Status == "published" && (c.PublishedAt == nil || c.PublishedAt.Before(*c.CompletedAt)) {
		return jobs.Failure(jobs.FailureFilterCatalogInconsistent)
	}
	if c.Status != "ready" && c.Status != "published" {
		return jobs.Failure(jobs.FailureFilterCatalogInconsistent)
	}
	for _, x := range []struct {
		n        string
		p        *int64
		positive bool
	}{
		{"release_billing_codes", c.BillingCount, true},
		{"release_code_filter_values", c.CodeFilterCount, false},
		{"release_plans", c.PlanCount, true},
		{"release_plan_outputs", c.PlanOutputCount, true},
		{"release_output_code_networks", c.NetworkCount, false},
		{"release_provider_filter_values", c.ProviderCount, false},
	} {
		if x.p == nil || (x.positive && *x.p <= 0) || (!x.positive && *x.p < 0) {
			return jobs.Failure(jobs.FailureFilterCatalogInconsistent)
		}
		var n int64
		if err := q.QueryRow(ctx, "SELECT count(*) FROM mrfweb."+x.n+" WHERE catalog_id=$1", c.ID).Scan(&n); err != nil {
			return jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
		}
		if n != *x.p {
			return jobs.Failure(jobs.FailureFilterCatalogInconsistent)
		}
	}
	if err := validateCatalogReferences(ctx, q, c.ID); err != nil {
		return err
	}
	return validateActivationPlans(ctx, q, c)
}

func validateCatalogReferences(ctx context.Context, q catalogQueryer, id int64) error {
	for _, query := range []string{
		`SELECT count(*) FROM mrfweb.release_code_filter_values v WHERE v.catalog_id=$1 AND NOT EXISTS (SELECT 1 FROM mrfweb.release_billing_codes b WHERE b.catalog_id=v.catalog_id AND b.billing_code_type=v.billing_code_type AND b.billing_code=v.billing_code)`,
		`SELECT count(*) FROM mrfweb.release_output_code_networks n WHERE n.catalog_id=$1 AND (NOT EXISTS (SELECT 1 FROM mrfweb.release_billing_codes b WHERE b.catalog_id=n.catalog_id AND b.billing_code_type=n.billing_code_type AND b.billing_code=n.billing_code) OR NOT EXISTS (SELECT 1 FROM mrfweb.release_outputs o WHERE o.catalog_id=n.catalog_id AND o.output_id=n.output_id))`,
		`SELECT count(*) FROM mrfweb.release_plan_outputs p WHERE p.catalog_id=$1 AND (NOT EXISTS (SELECT 1 FROM mrfweb.release_plans x WHERE x.catalog_id=p.catalog_id AND x.id=p.plan_id) OR NOT EXISTS (SELECT 1 FROM mrfweb.release_outputs o WHERE o.catalog_id=p.catalog_id AND o.output_id=p.output_id))`,
	} {
		var missing int64
		if err := q.QueryRow(ctx, query, id).Scan(&missing); err != nil {
			return jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
		}
		if missing != 0 {
			return jobs.Failure(jobs.FailureFilterCatalogInconsistent)
		}
	}
	return nil
}

func validateActivationPlans(ctx context.Context, q catalogQueryer, c activationCatalog) error {
	rows, err := q.Query(ctx, `SELECT o.output_id,p.plan_name,p.issuer_name,p.plan_id_type,p.plan_id,p.plan_market_type,p.search_text FROM mrfweb.release_plan_outputs o JOIN mrfweb.release_plans p ON p.catalog_id=o.catalog_id AND p.id=o.plan_id WHERE o.catalog_id=$1 ORDER BY o.output_id COLLATE "C",p.plan_name COLLATE "C",p.issuer_name COLLATE "C",p.plan_id_type COLLATE "C",p.plan_id COLLATE "C",p.plan_market_type COLLATE "C"`, c.ID)
	if err != nil {
		return jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
	}
	defer rows.Close()
	for rows.Next() {
		var r activationPlanRow
		var search string
		if err := rows.Scan(&r.OutputID, &r.PlanName, &r.IssuerName, &r.PlanIDType, &r.PlanID, &r.PlanMarketType, &search); err != nil {
			return jobs.Failure(jobs.FailureFilterCatalogInconsistent)
		}
		if r.OutputID == "" || r.PlanName == "" || r.IssuerName == "" || r.PlanID == "" || strings.ContainsAny(r.PlanName+r.IssuerName+r.PlanID, "\r\n") || (r.PlanIDType != "ein" && r.PlanIDType != "hios") || (r.PlanMarketType != "group" && r.PlanMarketType != "individual") {
			return jobs.Failure(jobs.FailureFilterCatalogInconsistent)
		}
		want := strings.ToLower(strings.Join([]string{strings.Trim(r.PlanName, " \t\r\n\v\f"), strings.Trim(r.IssuerName, " \t\r\n\v\f"), strings.Trim(r.PlanIDType, " \t\r\n\v\f"), strings.Trim(r.PlanID, " \t\r\n\v\f"), strings.Trim(r.PlanMarketType, " \t\r\n\v\f")}, " "))
		if search != want {
			return jobs.Failure(jobs.FailureFilterCatalogInconsistent)
		}
	}
	if rows.Err() != nil {
		return jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
	}
	return nil
}
func validateAuthoritativeCatalogPlans(ctx context.Context, q catalogQueryer, c activationCatalog, targets []Target) error {
	ids := make([]int64, 0, len(targets))
	for _, t := range targets {
		ids = append(ids, t.SnapshotID)
	}
	auth, err := q.Query(ctx, `SELECT 'mrf-'||mrf_snapshot_id,plan_name,issuer_name,plan_id_type,plan_id,plan_market_type FROM mrfpipeline.mrf_plans WHERE mrf_snapshot_id=ANY($1::bigint[]) ORDER BY ('mrf-'||mrf_snapshot_id) COLLATE "C",plan_name COLLATE "C",issuer_name COLLATE "C",plan_id_type COLLATE "C",plan_id COLLATE "C",plan_market_type COLLATE "C"`, ids)
	if err != nil {
		return jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
	}
	var authoritative []activationPlanRow
	for auth.Next() {
		var r activationPlanRow
		if err := auth.Scan(&r.OutputID, &r.PlanName, &r.IssuerName, &r.PlanIDType, &r.PlanID, &r.PlanMarketType); err != nil {
			auth.Close()
			return jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
		}
		authoritative = append(authoritative, r)
	}
	if auth.Err() != nil {
		auth.Close()
		return jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
	}
	auth.Close()
	actual, err := q.Query(ctx, `SELECT o.output_id,p.plan_name,p.issuer_name,p.plan_id_type,p.plan_id,p.plan_market_type FROM mrfweb.release_plan_outputs o JOIN mrfweb.release_plans p ON p.catalog_id=o.catalog_id AND p.id=o.plan_id WHERE o.catalog_id=$1 ORDER BY o.output_id COLLATE "C",p.plan_name COLLATE "C",p.issuer_name COLLATE "C",p.plan_id_type COLLATE "C",p.plan_id COLLATE "C",p.plan_market_type COLLATE "C"`, c.ID)
	if err != nil {
		return jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
	}
	defer actual.Close()
	var rows []activationPlanRow
	for actual.Next() {
		var r activationPlanRow
		if err := actual.Scan(&r.OutputID, &r.PlanName, &r.IssuerName, &r.PlanIDType, &r.PlanID, &r.PlanMarketType); err != nil {
			return jobs.Failure(jobs.FailureFilterCatalogInconsistent)
		}
		rows = append(rows, r)
	}
	if actual.Err() != nil {
		return jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
	}
	if len(rows) != len(authoritative) {
		if c.Status == "published" {
			return jobs.Failure(jobs.FailureFilterCatalogInconsistent)
		}
		return jobs.Failure(jobs.FailureFilterCatalogStale)
	}
	for i := range rows {
		if rows[i] != authoritative[i] {
			if c.Status == "published" {
				return jobs.Failure(jobs.FailureFilterCatalogInconsistent)
			}
			return jobs.Failure(jobs.FailureFilterCatalogStale)
		}
	}
	return nil
}
func inspectActivationCatalog(ctx context.Context, q catalogQueryer, payer, month string, generation int64, targets []Target) (activationCatalog, error) {
	c, err := readActivationHeader(ctx, q, payer, month, generation, false)
	if err != nil {
		return c, err
	}
	if c.Status == "building" || c.Status == "failed" {
		return c, nil
	}
	if err := readActivationCatalogOutputs(ctx, q, &c); err != nil {
		return c, err
	}
	if c.OutputCount != int64(len(c.Outputs)) || c.OutputFingerprint != OutputFingerprint(c.Outputs) {
		return c, jobs.Failure(jobs.FailureFilterCatalogInconsistent)
	}
	if err := validateActivationCatalog(ctx, q, c); err != nil {
		return c, err
	}
	if c.OutputCount != int64(len(targets)) || c.OutputFingerprint != OutputFingerprint(targets) || !sameTargetSet(c.Outputs, targets) {
		if c.Status == "published" {
			return c, jobs.Failure(jobs.FailureFilterCatalogInconsistent)
		}
		return c, jobs.Failure(jobs.FailureFilterCatalogStale)
	}
	if err := validateAuthoritativeReadiness(ctx, q, c, targets); err != nil {
		return c, err
	}
	return c, nil
}
func lockActivationCatalog(ctx context.Context, tx pgx.Tx, payer, month string, generation int64, targets []Target, allowPublished bool) (activationCatalog, error) {
	c, err := readActivationHeader(ctx, tx, payer, month, generation, true)
	if err != nil {
		return c, err
	}
	if c.Status != "ready" && !(allowPublished && c.Status == "published") {
		return c, jobs.Failure(jobs.FailureFilterCatalogNotReady)
	}
	if err := readActivationCatalogOutputs(ctx, tx, &c); err != nil {
		return c, err
	}
	if c.OutputCount != int64(len(c.Outputs)) || c.OutputFingerprint != OutputFingerprint(c.Outputs) {
		return c, jobs.Failure(jobs.FailureFilterCatalogInconsistent)
	}
	if err := validateActivationCatalog(ctx, tx, c); err != nil {
		return c, err
	}
	if err := validateAuthoritativeCatalogPlans(ctx, tx, c, targets); err != nil {
		return c, err
	}
	if c.OutputCount != int64(len(targets)) || c.OutputFingerprint != OutputFingerprint(targets) || !sameTargetSet(c.Outputs, targets) {
		if c.Status == "published" {
			return c, jobs.Failure(jobs.FailureFilterCatalogInconsistent)
		}
		return c, jobs.Failure(jobs.FailureFilterCatalogStale)
	}
	if err := validateAuthoritativeReadiness(ctx, tx, c, targets); err != nil {
		return c, err
	}
	return c, nil
}
func AuditPublishedCatalog(ctx context.Context, q rowsQueryer, payer string, month time.Time, generation int64, targets []Target) (CatalogAudit, error) {
	c, err := inspectActivationCatalog(ctx, q, payer, formatMonth(month), generation, targets)
	if err != nil {
		return CatalogAudit{}, err
	}
	if c.Status != "published" {
		return CatalogAudit{}, jobs.Failure(jobs.FailureFilterCatalogNotReady)
	}
	return CatalogAudit{ID: c.ID, Status: c.Status, ProviderSchemaVersion: c.ProviderSchema, ProviderReleaseMonth: c.ProviderMonth}, nil
}
func validateAuthoritativeReadiness(ctx context.Context, q catalogQueryer, c activationCatalog, targets []Target) error {
	for _, t := range targets {
		var plans, unassigned int64
		var positive, unfinished bool
		err := q.QueryRow(ctx, `SELECT (SELECT count(*) FROM mrfpipeline.mrf_plans WHERE mrf_snapshot_id=$1),(SELECT count(*) FROM mrfpipeline.mrf_plans p WHERE p.mrf_snapshot_id=$1 AND NOT EXISTS(SELECT 1 FROM mrfpipeline.plan_attachment_batch_items i JOIN mrfpipeline.plan_attachment_batches b ON b.id=i.plan_attachment_batch_id WHERE i.mrf_plan_id=p.id AND b.mrf_snapshot_id=p.mrf_snapshot_id AND b.status='succeeded')),EXISTS(SELECT 1 FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id=$1 AND status='succeeded' AND added_plan_count>0),EXISTS(SELECT 1 FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id=$1 AND status<>'succeeded')`, t.SnapshotID).Scan(&plans, &unassigned, &positive, &unfinished)
		if err != nil {
			return jobs.Failure(jobs.FailureFilterCatalogDatabaseFailed)
		}
		if plans <= 0 || unassigned != 0 || !positive || unfinished {
			if c.Status == "published" {
				return jobs.Failure(jobs.FailureFilterCatalogInconsistent)
			}
			return jobs.Failure(jobs.FailureFilterCatalogStale)
		}
	}
	return nil
}
