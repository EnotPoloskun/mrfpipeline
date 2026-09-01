package release

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

func TestIntegrationStory31ReadinessCatalogFieldsAndBlockers(t *testing.T) {
	for _, tc := range []struct {
		name          string
		catalogStatus string
		wantReady     bool
		wantBlocker   string
	}{
		{"ready", "ready", true, ""},
		{"missing", "", false, "filter_catalog_missing"},
		{"published", "published", true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := releaseTestDB(t)
			ctx := context.Background()
			month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
			seedReadyRelease(t, pool, "uhc", "2026-08", tc.name)
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
			if tc.catalogStatus != "" {
				seedActivationCatalog(t, pool, "uhc", month, 1, tc.catalogStatus, candidate.Targets)
			}
			got, err := Readiness(ctx, pool, "uhc", month)
			if err != nil {
				t.Fatal(err)
			}
			if got.FilterCatalogReady != tc.wantReady || got.ProspectiveCandidateValid != true {
				t.Fatalf("readiness=%+v", got)
			}
			if tc.wantBlocker != "" && !hasBlocker(got.Blockers, tc.wantBlocker) {
				t.Fatalf("blockers=%v", got.Blockers)
			}
		})
	}
}

func hasBlocker(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestIntegrationStory31ReadinessInvalidCandidateNullsCatalog(t *testing.T) {
	pool := releaseTestDB(t)
	ctx := context.Background()
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	seedReadyRelease(t, pool, "uhc", "2026-08", "invalid-candidate")
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
	seedActivationCatalog(t, pool, "uhc", month, 1, "ready", candidate.Targets)
	if _, err := pool.Exec(ctx, `UPDATE mrfpipeline.plan_attachment_batches SET status='failed',added_plan_count=NULL,completed_at=transaction_timestamp(),failure_code='test' WHERE mrf_snapshot_id=$1`, candidate.Targets[0].SnapshotID); err != nil {
		t.Fatal(err)
	}
	got, err := Readiness(ctx, pool, "uhc", month)
	if err != nil {
		t.Fatal(err)
	}
	if got.ProspectiveCandidateValid || got.FilterCatalogReady || got.FilterCatalogID != nil || got.FilterCatalogGeneration != nil || got.FilterCatalogStatus != nil || got.FilterCatalogOutputCount != nil {
		t.Fatalf("readiness=%+v", got)
	}
	for _, b := range got.Blockers {
		if strings.HasPrefix(b, "filter_catalog_") {
			t.Fatalf("catalog blocker=%v", b)
		}
	}
}
func TestIntegrationStory31ReadinessBuildingFailedCatalog(t *testing.T) {
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
			if _, err := pool.Exec(ctx, `UPDATE mrfpipeline.monthly_releases SET status='active',sealed_at=transaction_timestamp(),last_activated_at=transaction_timestamp(),publication_generation=1 WHERE payer_id='uhc' AND collection_month=$1`, month); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.monthly_release_outputs(payer_id,collection_month,mrf_snapshot_id,published_generation) VALUES('uhc',$1,$2,1)`, month, candidate.Targets[0].SnapshotID); err != nil {
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
			got, err := Readiness(ctx, pool, "uhc", month)
			if err != nil {
				t.Fatal(err)
			}
			want := "filter_catalog_" + status
			if !got.ProspectiveCandidateValid || got.FilterCatalogReady || got.FilterCatalogID == nil || got.FilterCatalogStatus == nil || *got.FilterCatalogStatus != status || !hasBlocker(got.Blockers, want) || got.DatabaseReady {
				t.Fatalf("readiness=%+v", got)
			}
		})
	}
}
func TestIntegrationStory31ReadinessInconsistentCatalog(t *testing.T) {
	pool := releaseTestDB(t)
	ctx := context.Background()
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	seedReadyRelease(t, pool, "uhc", "2026-08", "status-inconsistent")
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
	id := seedActivationCatalog(t, pool, "uhc", month, 1, "ready", candidate.Targets)
	if _, err := pool.Exec(ctx, `UPDATE mrfweb.release_catalogs SET plan_count=plan_count+1 WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	got, err := Readiness(ctx, pool, "uhc", month)
	if err != nil {
		t.Fatal(err)
	}
	if !got.ProspectiveCandidateValid || got.FilterCatalogReady || got.DatabaseReady || !hasBlocker(got.Blockers, "filter_catalog_inconsistent") {
		t.Fatalf("readiness=%+v", got)
	}
}
func TestIntegrationStory31ReadinessStaleCatalog(t *testing.T) {
	pool := releaseTestDB(t)
	ctx := context.Background()
	month := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	seedReadyRelease(t, pool, "uhc", "2026-08", "status-stale")
	old, err := SelectCatalogCandidate(ctx, pool, "uhc", month)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE mrfpipeline.monthly_releases SET status='active',sealed_at=transaction_timestamp(),last_activated_at=transaction_timestamp(),publication_generation=1 WHERE payer_id='uhc' AND collection_month=$1`, month); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mrfpipeline.monthly_release_outputs(payer_id,collection_month,mrf_snapshot_id,published_generation) VALUES('uhc',$1,$2,1)`, month, old.Targets[0].SnapshotID); err != nil {
		t.Fatal(err)
	}
	id := seedActivationCatalog(t, pool, "uhc", month, 2, "ready", old.Targets)
	source, snap := seedSnapshotState(t, pool, "uhc", "2026-08", "status-stale-new", jobs.StatusSucceeded, jobs.StatusSucceeded, jobs.StatusSucceeded, true)
	markSnapshotReady(t, pool, source, snap, "status-stale-new")
	got, err := Readiness(ctx, pool, "uhc", month)
	if err != nil {
		t.Fatal(err)
	}
	if !got.ProspectiveCandidateValid || got.FilterCatalogReady || got.DatabaseReady || got.FilterCatalogID == nil || *got.FilterCatalogID != id || !hasBlocker(got.Blockers, "filter_catalog_stale") {
		t.Fatalf("readiness=%+v", got)
	}
}
