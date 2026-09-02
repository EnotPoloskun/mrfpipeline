package release

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestIntegrationCatalogCandidateStateMatrix(t *testing.T) {
	pool := releaseTestDB(t)
	ctx := context.Background()
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	seedReadyRelease(t, pool, "uhc", "2026-08", "candidate-first")
	building, err := SelectCatalogCandidate(ctx, pool, "uhc", month)
	if err != nil || building.ReleaseStatus != Building || building.TargetPublicationGeneration != 1 || !building.HasNewOutputs || len(building.Targets) != 1 {
		t.Fatalf("building candidate=%+v err=%v", building, err)
	}
	if building.OutputFingerprint != OutputFingerprint(building.Targets) {
		t.Fatal("building fingerprint mismatch")
	}
	if _, err := pool.Exec(ctx, `UPDATE mrfpipeline.monthly_releases SET status='active', sealed_at=transaction_timestamp(), last_activated_at=transaction_timestamp(), publication_generation=1 WHERE payer_id='uhc' AND collection_month=$1`, month); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.monthly_release_outputs(payer_id,collection_month,mrf_snapshot_id,published_generation) VALUES ('uhc',$1,$2,1)`, month, building.Targets[0].SnapshotID); err != nil {
		t.Fatal(err)
	}
	unchanged, err := SelectCatalogCandidate(ctx, pool, "uhc", month)
	if err != nil || unchanged.TargetPublicationGeneration != 1 || unchanged.HasNewOutputs || len(unchanged.Targets) != 1 {
		t.Fatalf("active unchanged=%+v err=%v", unchanged, err)
	}
	var source, snap, plan, batch int64
	if err := pool.QueryRow(ctx, `INSERT INTO mrfpipeline.mrf_sources(source_url,collection_month,download_status,parse_status) VALUES ('https://example.invalid/mrf-new',$1,'succeeded','succeeded') RETURNING id`, month).Scan(&source); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO mrfpipeline.mrf_snapshots(mrf_source_id,payer_id,collection_month,consume_status) VALUES ($1,'uhc',$2,'succeeded') RETURNING id`, source, month).Scan(&snap); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.monthly_release_mrf_sources(payer_id,collection_month,mrf_source_id) VALUES ('uhc',$1,$2)`, month, source); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO mrfpipeline.mrf_plans(mrf_snapshot_id,plan_name,issuer_name,plan_id_type,plan_id,plan_market_type) VALUES ($1,'new-plan','issuer','hios','new','group') RETURNING id`, snap).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO mrfpipeline.plan_attachment_batches(mrf_snapshot_id,status,requested_plan_count,added_plan_count,completed_at) VALUES ($1,'succeeded',1,1,transaction_timestamp()) RETURNING id`, snap).Scan(&batch); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.plan_attachment_batch_items(plan_attachment_batch_id,mrf_plan_id) VALUES ($1,$2)`, batch, plan); err != nil {
		t.Fatal(err)
	}
	next, err := SelectCatalogCandidate(ctx, pool, "uhc", month)
	if err != nil || !next.HasNewOutputs || next.TargetPublicationGeneration != 2 || len(next.Targets) != 2 || next.Targets[0].SnapshotID > next.Targets[1].SnapshotID {
		t.Fatalf("active new=%+v err=%v", next, err)
	}
	repeat, err := SelectCatalogCandidate(ctx, pool, "uhc", month)
	if err != nil || !reflect.DeepEqual(next.Targets, repeat.Targets) || next.OutputFingerprint != repeat.OutputFingerprint {
		t.Fatalf("nondeterministic next=%+v repeat=%+v err=%v", next, repeat, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE mrfpipeline.monthly_releases SET status='inactive' WHERE payer_id='uhc' AND collection_month=$1`, month); err != nil {
		t.Fatal(err)
	}
	inactive, err := SelectCatalogCandidate(ctx, pool, "uhc", month)
	if err != nil || inactive.ReleaseStatus != Inactive || inactive.TargetPublicationGeneration != 1 || len(inactive.Targets) != 1 {
		t.Fatalf("inactive=%+v err=%v", inactive, err)
	}
}

func TestIntegrationCatalogCandidateRejectsUnreadyDatabaseState(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *pgxpool.Pool, int64)
	}{
		{"consume failed", func(t *testing.T, pool *pgxpool.Pool, snapshot int64) {
			_, err := pool.Exec(context.Background(), `UPDATE mrfpipeline.mrf_snapshots SET consume_status='failed', failure_code='test' WHERE id=$1`, snapshot)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"planless", func(t *testing.T, pool *pgxpool.Pool, snapshot int64) {
			if _, err := pool.Exec(context.Background(), `DELETE FROM mrfpipeline.plan_attachment_batch_items WHERE mrf_plan_id IN (SELECT id FROM mrfpipeline.mrf_plans WHERE mrf_snapshot_id=$1)`, snapshot); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(context.Background(), `DELETE FROM mrfpipeline.plan_attachment_batches WHERE mrf_snapshot_id=$1`, snapshot); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(context.Background(), `DELETE FROM mrfpipeline.mrf_plans WHERE mrf_snapshot_id=$1`, snapshot); err != nil {
				t.Fatal(err)
			}
		}},
		{"attachment pending", func(t *testing.T, pool *pgxpool.Pool, snapshot int64) {
			if _, err := pool.Exec(context.Background(), `UPDATE mrfpipeline.plan_attachment_batches SET status='pending', added_plan_count=NULL, completed_at=NULL WHERE mrf_snapshot_id=$1`, snapshot); err != nil {
				t.Fatal(err)
			}
		}},
		{"attachment failed", func(t *testing.T, pool *pgxpool.Pool, snapshot int64) {
			if _, err := pool.Exec(context.Background(), `UPDATE mrfpipeline.plan_attachment_batches SET status='failed', added_plan_count=NULL, completed_at=transaction_timestamp(), failure_code='test' WHERE mrf_snapshot_id=$1`, snapshot); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := releaseTestDB(t)
			month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
			snapshot := seedReadyRelease(t, pool, "uhc", "2026-08", tc.name)
			tc.mutate(t, pool, snapshot)
			_, err := SelectCatalogCandidate(context.Background(), pool, "uhc", month)
			if !jobs.IsFailure(err, jobs.FailureReleaseNotReady) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestIntegrationCatalogCandidateRejectsActiveOrInactiveGenerationZero(t *testing.T) {
	for _, status := range []string{"active", "inactive"} {
		t.Run(status, func(t *testing.T) {
			pool := releaseTestDB(t)
			ctx := context.Background()
			month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
			seedReadyRelease(t, pool, "uhc", "2026-08", "generation-zero-"+status)
			if _, err := pool.Exec(ctx, `UPDATE mrfpipeline.monthly_releases SET status=$1, sealed_at=transaction_timestamp(), last_activated_at=transaction_timestamp(), publication_generation=0 WHERE payer_id='uhc' AND collection_month=$2`, status, month); err != nil {
				t.Fatal(err)
			}
			_, err := SelectCatalogCandidate(ctx, pool, "uhc", month)
			if !jobs.IsFailure(err, jobs.FailureDomainInvariant) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}
