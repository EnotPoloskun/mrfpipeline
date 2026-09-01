package release

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestIntegrationBuildingActivationPublishesReadyCatalog(t *testing.T) {
	pool := releaseTestDB(t)
	ctx := context.Background()
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	seedReadyRelease(t, pool, "uhc", "2026-08", "activation-catalog")
	candidate, err := SelectCatalogCandidate(ctx, pool, "uhc", month)
	if err != nil {
		t.Fatal(err)
	}
	catalogID := seedActivationCatalog(t, pool, "uhc", month, 1, "ready", candidate.Targets)
	got, err := Activate(ctx, pool, "uhc", month, func(targets []Target, schema int64, releaseMonth time.Time) error {
		if schema != 1 || !releaseMonth.Equal(month) {
			t.Fatal("provider identity")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.FilterCatalogID != catalogID || got.PublicationGeneration != 1 {
		t.Fatalf("result=%+v", got)
	}
}

func seedActivationCatalog(t *testing.T, pool *pgxpool.Pool, payer string, month time.Time, generation int64, status string, targets []Target) int64 {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var id int64
	if err := tx.QueryRow(ctx, `INSERT INTO mrfweb.release_catalogs(payer_id,collection_month,publication_generation,status,output_fingerprint,output_count) VALUES($1,$2,$3,'building',$4,$5) RETURNING id`, payer, month, generation, OutputFingerprint(targets), len(targets)).Scan(&id); err != nil {
		t.Fatal(err)
	}
	for _, target := range targets {
		if _, err := tx.Exec(ctx, `INSERT INTO mrfweb.release_outputs(catalog_id,mrf_snapshot_id,output_id) VALUES($1,$2,$3)`, id, target.SnapshotID, target.OutputID); err != nil {
			t.Fatal(err)
		}
	}
	type key struct{ name, issuer, typ, id, market string }
	plans := map[key]int64{}
	for _, target := range targets {
		rows, err := tx.Query(ctx, `SELECT plan_name,issuer_name,plan_id_type,plan_id,plan_market_type FROM mrfpipeline.mrf_plans WHERE mrf_snapshot_id=$1 ORDER BY plan_name,issuer_name,plan_id_type,plan_id,plan_market_type`, target.SnapshotID)
		if err != nil {
			t.Fatal(err)
		}
		var keys []key
		for rows.Next() {
			var k key
			if err := rows.Scan(&k.name, &k.issuer, &k.typ, &k.id, &k.market); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			keys = append(keys, k)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
		for _, k := range keys {
			pid := plans[k]
			if pid == 0 {
				search := strings.ToLower(strings.Join([]string{k.name, k.issuer, k.typ, k.id, k.market}, " "))
				if err := tx.QueryRow(ctx, `INSERT INTO mrfweb.release_plans(catalog_id,plan_name,issuer_name,plan_id_type,plan_id,plan_market_type,search_text) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id`, id, k.name, k.issuer, k.typ, k.id, k.market, search).Scan(&pid); err != nil {
					t.Fatal(err)
				}
				plans[k] = pid
			}
			if _, err := tx.Exec(ctx, `INSERT INTO mrfweb.release_plan_outputs(catalog_id,plan_id,output_id) VALUES($1,$2,$3)`, id, pid, target.OutputID); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO mrfweb.release_billing_codes(catalog_id,billing_code_type,billing_code,observation_count,unmodified_observation_count) VALUES($1,'CPT','1',1,1)`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE mrfweb.release_catalogs SET standard_fact_count=1,provider_catalog_schema_version=1,provider_catalog_release_month=$2,billing_code_count=1,code_filter_value_count=0,plan_count=$3,plan_output_count=(SELECT count(*) FROM mrfweb.release_plan_outputs WHERE catalog_id=$1),output_code_network_count=0,provider_filter_value_count=0,status=$4,completed_at=transaction_timestamp(),published_at=CASE WHEN $4='published' THEN transaction_timestamp() END WHERE id=$1`, id, month, len(plans), status); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return id
}
func TestIntegrationActiveAppendPublishesNextCatalog(t *testing.T) {
	pool := releaseTestDB(t)
	ctx := context.Background()
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	seedReadyRelease(t, pool, "uhc", "2026-08", "append-first")
	first, err := SelectCatalogCandidate(ctx, pool, "uhc", month)
	if err != nil {
		t.Fatal(err)
	}
	cat1 := seedActivationCatalog(t, pool, "uhc", month, 1, "ready", first.Targets)
	if _, err := Activate(ctx, pool, "uhc", month, func([]Target, int64, time.Time) error { return nil }); err != nil {
		t.Fatal(err)
	}
	source, snap := seedSnapshotState(t, pool, "uhc", "2026-08", "append-new", jobs.StatusSucceeded, jobs.StatusSucceeded, jobs.StatusSucceeded, true)
	markSnapshotReady(t, pool, source, snap, "append-new")
	next, err := SelectCatalogCandidate(ctx, pool, "uhc", month)
	if err != nil {
		t.Fatal(err)
	}
	cat2 := seedActivationCatalog(t, pool, "uhc", month, 2, "ready", next.Targets)
	got, err := Activate(ctx, pool, "uhc", month, func([]Target, int64, time.Time) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if got.FilterCatalogID != cat2 || got.PublicationGeneration != 2 || got.AddedOutputCount != 1 {
		t.Fatalf("result=%+v", got)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM mrfweb.release_catalogs WHERE id=$1`, cat1).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "published" {
		t.Fatalf("catalog1=%s", status)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM mrfweb.release_catalogs WHERE id=$1`, cat2).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "published" {
		t.Fatalf("catalog2=%s", status)
	}
}
func TestIntegrationActiveNoNewPublishedIsIdempotent(t *testing.T) {
	pool := releaseTestDB(t)
	ctx := context.Background()
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	seedReadyRelease(t, pool, "uhc", "2026-08", "idempotent")
	candidate, err := SelectCatalogCandidate(ctx, pool, "uhc", month)
	if err != nil {
		t.Fatal(err)
	}
	cat := seedActivationCatalog(t, pool, "uhc", month, 1, "ready", candidate.Targets)
	if _, err := Activate(ctx, pool, "uhc", month, func([]Target, int64, time.Time) error { return nil }); err != nil {
		t.Fatal(err)
	}
	var before, after time.Time
	if err := pool.QueryRow(ctx, `SELECT last_activated_at FROM mrfpipeline.monthly_releases WHERE payer_id='uhc' AND collection_month=$1`, month).Scan(&before); err != nil {
		t.Fatal(err)
	}
	got, err := Activate(ctx, pool, "uhc", month, func([]Target, int64, time.Time) error { return nil })
	if err != nil || got.FilterCatalogID != cat {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if err := pool.QueryRow(ctx, `SELECT last_activated_at FROM mrfpipeline.monthly_releases WHERE payer_id='uhc' AND collection_month=$1`, month).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !before.Equal(after) {
		t.Fatal("timestamp changed")
	}
}

func TestIntegrationActiveReadyBackfillPreservesRelease(t *testing.T) {
	pool := releaseTestDB(t)
	ctx := context.Background()
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	seedReadyRelease(t, pool, "uhc", "2026-08", "backfill")
	candidate, err := SelectCatalogCandidate(ctx, pool, "uhc", month)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE mrfpipeline.monthly_releases SET status='active',sealed_at=transaction_timestamp(),last_activated_at=transaction_timestamp(),publication_generation=1 WHERE payer_id='uhc' AND collection_month=$1`, month); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.monthly_release_outputs(payer_id,collection_month,mrf_snapshot_id,published_generation) VALUES('uhc',$1,$2,1)`, month, candidate.Targets[0].SnapshotID); err != nil {
		t.Fatal(err)
	}
	cat := seedActivationCatalog(t, pool, "uhc", month, 1, "ready", candidate.Targets)
	got, err := Activate(ctx, pool, "uhc", month, func([]Target, int64, time.Time) error { return nil })
	if err != nil || got.FilterCatalogID != cat {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM mrfweb.release_catalogs WHERE id=$1`, cat).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "published" {
		t.Fatal(status)
	}
}
func TestIntegrationInactivePublishedRollback(t *testing.T) {
	pool := releaseTestDB(t)
	ctx := context.Background()
	a := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	b := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	seedReadyRelease(t, pool, "uhc", "2026-08", "rollback-a")
	seedReadyRelease(t, pool, "uhc", "2026-09", "rollback-b")
	ca, err := SelectCatalogCandidate(ctx, pool, "uhc", a)
	if err != nil {
		t.Fatal(err)
	}
	cida := seedActivationCatalog(t, pool, "uhc", a, 1, "ready", ca.Targets)
	if _, err := Activate(ctx, pool, "uhc", a, func([]Target, int64, time.Time) error { return nil }); err != nil {
		t.Fatal(err)
	}
	cb, err := SelectCatalogCandidate(ctx, pool, "uhc", b)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE mrfpipeline.monthly_releases SET status='inactive',sealed_at=transaction_timestamp(),last_activated_at=transaction_timestamp(),publication_generation=1 WHERE payer_id='uhc' AND collection_month=$1`, b); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.monthly_release_outputs(payer_id,collection_month,mrf_snapshot_id,published_generation) VALUES('uhc',$1,$2,1)`, b, cb.Targets[0].SnapshotID); err != nil {
		t.Fatal(err)
	}
	cidb := seedActivationCatalog(t, pool, "uhc", b, 1, "published", cb.Targets)
	got, err := Activate(ctx, pool, "uhc", b, func([]Target, int64, time.Time) error { return nil })
	if err != nil || got.FilterCatalogID != cidb || got.PreviousCollectionMonth == nil || *got.PreviousCollectionMonth != "2026-08" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	_ = cida
}

func TestIntegrationInactiveReadyRollbackPromotesCatalog(t *testing.T) {
	pool := releaseTestDB(t)
	ctx := context.Background()
	a := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	b := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	seedReadyRelease(t, pool, "uhc", "2026-08", "ready-a")
	seedReadyRelease(t, pool, "uhc", "2026-09", "ready-b")
	ca, _ := SelectCatalogCandidate(ctx, pool, "uhc", a)
	cida := seedActivationCatalog(t, pool, "uhc", a, 1, "ready", ca.Targets)
	if _, err := Activate(ctx, pool, "uhc", a, func([]Target, int64, time.Time) error { return nil }); err != nil {
		t.Fatal(err)
	}
	cb, err := SelectCatalogCandidate(ctx, pool, "uhc", b)
	if err != nil {
		t.Fatal(err)
	}
	pool.Exec(ctx, `UPDATE mrfpipeline.monthly_releases SET status='inactive',sealed_at=transaction_timestamp(),last_activated_at=transaction_timestamp(),publication_generation=1 WHERE payer_id='uhc' AND collection_month=$1`, b)
	pool.Exec(ctx, `INSERT INTO mrfpipeline.monthly_release_outputs(payer_id,collection_month,mrf_snapshot_id,published_generation) VALUES('uhc',$1,$2,1)`, b, cb.Targets[0].SnapshotID)
	cidb := seedActivationCatalog(t, pool, "uhc", b, 1, "ready", cb.Targets)
	got, err := Activate(ctx, pool, "uhc", b, func([]Target, int64, time.Time) error { return nil })
	if err != nil || got.FilterCatalogID != cidb || got.PreviousCollectionMonth == nil || *got.PreviousCollectionMonth != "2026-08" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	_ = cida
}
func TestIntegrationActivationMissingCatalogPrecedence(t *testing.T) {
	pool := releaseTestDB(t)
	ctx := context.Background()
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	seedReadyRelease(t, pool, "uhc", "2026-08", "missing-catalog")
	_, err := Activate(ctx, pool, "uhc", month, func([]Target, int64, time.Time) error { return nil })
	if !jobs.IsFailure(err, jobs.FailureFilterCatalogMissing) {
		t.Fatalf("err=%v", err)
	}
	var status string
	var generation int64
	if err := pool.QueryRow(ctx, `SELECT status,publication_generation FROM mrfpipeline.monthly_releases WHERE payer_id='uhc' AND collection_month=$1`, month).Scan(&status, &generation); err != nil {
		t.Fatal(err)
	}
	if status != Building || generation != 0 {
		t.Fatalf("release=%s/%d", status, generation)
	}
}
func TestIntegrationActivationBuildingOrFailedCatalogNotReady(t *testing.T) {
	for _, status := range []string{"building", "failed"} {
		t.Run(status, func(t *testing.T) {
			pool := releaseTestDB(t)
			ctx := context.Background()
			month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
			seedReadyRelease(t, pool, "uhc", "2026-08", status)
			candidate, err := SelectCatalogCandidate(ctx, pool, "uhc", month)
			if err != nil {
				t.Fatal(err)
			}
			var id int64
			if status == "building" {
				err = pool.QueryRow(ctx, `INSERT INTO mrfweb.release_catalogs(payer_id,collection_month,publication_generation,status,output_fingerprint,output_count) VALUES('uhc',$1,1,'building',$2,1) RETURNING id`, month, candidate.OutputFingerprint).Scan(&id)
			} else {
				err = pool.QueryRow(ctx, `INSERT INTO mrfweb.release_catalogs(payer_id,collection_month,publication_generation,status,output_fingerprint,output_count,failure_code,completed_at) VALUES('uhc',$1,1,'failed',$2,1,'filter_catalog_population_failed',transaction_timestamp()) RETURNING id`, month, candidate.OutputFingerprint).Scan(&id)
			}
			if err != nil {
				t.Fatal(err)
			}
			_, err = Activate(ctx, pool, "uhc", month, func([]Target, int64, time.Time) error { return nil })
			if !jobs.IsFailure(err, jobs.FailureFilterCatalogNotReady) {
				t.Fatalf("err=%v", err)
			}
			var got string
			if err := pool.QueryRow(ctx, `SELECT status FROM mrfweb.release_catalogs WHERE id=$1`, id).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != status {
				t.Fatalf("status=%s", got)
			}
		})
	}
}
func TestIntegrationActivationCatalogRowLockContention(t *testing.T) {
	pool := releaseTestDB(t)
	ctx := context.Background()
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	seedReadyRelease(t, pool, "uhc", "2026-08", "lock-contention")
	candidate, err := SelectCatalogCandidate(ctx, pool, "uhc", month)
	if err != nil {
		t.Fatal(err)
	}
	id := seedActivationCatalog(t, pool, "uhc", month, 1, "ready", candidate.Targets)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	lockTx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lockTx.Rollback(ctx)
	if err := lockTx.QueryRow(ctx, `SELECT id FROM mrfweb.release_catalogs WHERE id=$1 FOR UPDATE`, id).Scan(new(int64)); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_, err = Activate(short, pool, "uhc", month, func([]Target, int64, time.Time) error { return nil })
	if !jobs.IsFailure(err, jobs.FailureFilterCatalogNotReady) {
		t.Fatalf("err=%v", err)
	}
}

func TestIntegrationActivationCatalogCorruptionPrecedence(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*testing.T, *pgxpool.Pool, int64)
		provider bool
	}{
		{"fingerprint", func(t *testing.T, p *pgxpool.Pool, id int64) {
			if _, e := p.Exec(context.Background(), `UPDATE mrfweb.release_catalogs SET output_fingerprint=repeat('0',64) WHERE id=$1`, id); e != nil {
				t.Fatal(e)
			}
		}, false},
		{"count", func(t *testing.T, p *pgxpool.Pool, id int64) {
			if _, e := p.Exec(context.Background(), `UPDATE mrfweb.release_catalogs SET plan_count=plan_count+1 WHERE id=$1`, id); e != nil {
				t.Fatal(e)
			}
		}, false},
		{"search", func(t *testing.T, p *pgxpool.Pool, id int64) {
			if _, e := p.Exec(context.Background(), `UPDATE mrfweb.release_plans SET search_text='wrong' WHERE catalog_id=$1`, id); e != nil {
				t.Fatal(e)
			}
		}, false},
		{"provider", func(*testing.T, *pgxpool.Pool, int64) {}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := releaseTestDB(t)
			ctx := context.Background()
			m := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
			seedReadyRelease(t, p, "uhc", "2026-08", tc.name)
			c, e := SelectCatalogCandidate(ctx, p, "uhc", m)
			if e != nil {
				t.Fatal(e)
			}
			id := seedActivationCatalog(t, p, "uhc", m, 1, "ready", c.Targets)
			tc.mutate(t, p, id)
			cb := func([]Target, int64, time.Time) error { return nil }
			if tc.provider {
				cb = func([]Target, int64, time.Time) error { return jobs.Failure(jobs.FailureFilterCatalogInconsistent) }
			}
			_, e = Activate(ctx, p, "uhc", m, cb)
			if !jobs.IsFailure(e, jobs.FailureFilterCatalogInconsistent) {
				t.Fatalf("err=%v", e)
			}
		})
	}
}
func TestIntegrationActivationReadyStaleOutput(t *testing.T) {
	pool := releaseTestDB(t)
	ctx := context.Background()
	m := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	seedReadyRelease(t, pool, "uhc", "2026-08", "stale-first")
	c, err := SelectCatalogCandidate(ctx, pool, "uhc", m)
	if err != nil {
		t.Fatal(err)
	}
	id := seedActivationCatalog(t, pool, "uhc", m, 1, "ready", c.Targets)
	source, snap := seedSnapshotState(t, pool, "uhc", "2026-08", "stale-new", jobs.StatusSucceeded, jobs.StatusSucceeded, jobs.StatusSucceeded, true)
	markSnapshotReady(t, pool, source, snap, "stale-new")
	_, err = Activate(ctx, pool, "uhc", m, func([]Target, int64, time.Time) error { return nil })
	if !jobs.IsFailure(err, jobs.FailureFilterCatalogStale) {
		t.Fatalf("err=%v", err)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM mrfweb.release_catalogs WHERE id=$1`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "ready" {
		t.Fatal(status)
	}
}
func TestIntegrationActivationPlanMismatchClassification(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "ready", true: "published"}[published], func(t *testing.T) {
			pool := releaseTestDB(t)
			ctx := context.Background()
			m := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
			seedReadyRelease(t, pool, "uhc", "2026-08", "plan-mismatch")
			c, err := SelectCatalogCandidate(ctx, pool, "uhc", m)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `UPDATE mrfpipeline.monthly_releases SET status='active',sealed_at=transaction_timestamp(),last_activated_at=transaction_timestamp(),publication_generation=1 WHERE payer_id='uhc' AND collection_month=$1`, m); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.monthly_release_outputs(payer_id,collection_month,mrf_snapshot_id,published_generation) VALUES('uhc',$1,$2,1)`, m, c.Targets[0].SnapshotID); err != nil {
				t.Fatal(err)
			}
			status := "ready"
			if published {
				status = "published"
			}
			id := seedActivationCatalog(t, pool, "uhc", m, 1, status, c.Targets)
			var plan, batch int64
			if err := pool.QueryRow(ctx, `INSERT INTO mrfpipeline.mrf_plans(mrf_snapshot_id,plan_name,issuer_name,plan_id_type,plan_id,plan_market_type) VALUES($1,'new','issuer','hios','new','group') RETURNING id`, c.Targets[0].SnapshotID).Scan(&plan); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(ctx, `INSERT INTO mrfpipeline.plan_attachment_batches(mrf_snapshot_id,status,requested_plan_count,added_plan_count,completed_at) VALUES($1,'succeeded',1,1,transaction_timestamp()) RETURNING id`, c.Targets[0].SnapshotID).Scan(&batch); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.plan_attachment_batch_items(plan_attachment_batch_id,mrf_plan_id) VALUES($1,$2)`, batch, plan); err != nil {
				t.Fatal(err)
			}
			_, err = Activate(ctx, pool, "uhc", m, func([]Target, int64, time.Time) error { return nil })
			want := jobs.FailureFilterCatalogStale
			if published {
				want = jobs.FailureFilterCatalogInconsistent
			}
			if !jobs.IsFailure(err, want) {
				t.Fatalf("err=%v want=%s", err, want)
			}
			var got string
			if err := pool.QueryRow(ctx, `SELECT status FROM mrfweb.release_catalogs WHERE id=$1`, id).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != status {
				t.Fatalf("catalog status=%s", got)
			}
		})
	}
}
