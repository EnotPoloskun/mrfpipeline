package release

import (
	"context"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"testing"
	"time"
)

func TestIntegrationStory31ActiveHandoffZeroAndMissing(t *testing.T) {
	t.Run("zero active", func(t *testing.T) {
		p := releaseTestDB(t)
		out, err := ListActiveOutputs(context.Background(), p)
		if err != nil || len(out) != 0 {
			t.Fatalf("out=%+v err=%v", out, err)
		}
	})
	t.Run("missing catalog fails closed", func(t *testing.T) {
		p := releaseTestDB(t)
		m := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
		if _, err := p.Exec(context.Background(), `INSERT INTO mrfpipeline.monthly_releases(payer_id,collection_month,status,sealed_at,last_activated_at,publication_generation) VALUES('uhc',$1,'active',transaction_timestamp(),transaction_timestamp(),1)`, m); err != nil {
			t.Fatal(err)
		}
		_, err := ListActiveOutputs(context.Background(), p)
		if !jobs.IsFailure(err, jobs.FailureFilterCatalogInconsistent) {
			t.Fatalf("err=%v", err)
		}
	})
}
func TestIntegrationStory31ActiveHandoffPublishedAndMismatch(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		t.Run(map[bool]string{false: "exact", true: "mismatch"}[mismatch], func(t *testing.T) {
			p := releaseTestDB(t)
			ctx := context.Background()
			m := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
			seedReadyRelease(t, p, "uhc", "2026-08", "handoff-exact")
			c, err := SelectCatalogCandidate(ctx, p, "uhc", m)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.Exec(ctx, `UPDATE mrfpipeline.monthly_releases SET status='active',sealed_at=transaction_timestamp(),last_activated_at=transaction_timestamp(),publication_generation=1 WHERE payer_id='uhc' AND collection_month=$1`, m); err != nil {
				t.Fatal(err)
			}
			if _, err := p.Exec(ctx, `INSERT INTO mrfpipeline.monthly_release_outputs(payer_id,collection_month,mrf_snapshot_id,published_generation) VALUES('uhc',$1,$2,1)`, m, c.Targets[0].SnapshotID); err != nil {
				t.Fatal(err)
			}
			id := seedActivationCatalog(t, p, "uhc", m, 1, "published", c.Targets)
			if mismatch {
				if _, err := p.Exec(ctx, `DELETE FROM mrfweb.release_outputs WHERE catalog_id=$1`, id); err != nil {
					t.Fatal(err)
				}
			}
			out, err := ListActiveOutputs(ctx, p)
			if mismatch {
				if !jobs.IsFailure(err, jobs.FailureFilterCatalogInconsistent) {
					t.Fatalf("err=%v", err)
				}
			} else if err != nil || len(out) != 1 || out[0].SnapshotID != c.Targets[0].SnapshotID {
				t.Fatalf("out=%+v err=%v", out, err)
			}
		})
	}
}
func TestIntegrationStory31MultiPayerHandoffFailClosed(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "independent", true: "global-fail-closed"}[corrupt], func(t *testing.T) {
			p := releaseTestDB(t)
			ctx := context.Background()
			m := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
			for _, payer := range []string{"uhc", "aetna"} {
				seedReadyRelease(t, p, payer, "2026-08", payer)
				c, err := SelectCatalogCandidate(ctx, p, payer, m)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := p.Exec(ctx, `UPDATE mrfpipeline.monthly_releases SET status='active',sealed_at=transaction_timestamp(),last_activated_at=transaction_timestamp(),publication_generation=1 WHERE payer_id=$1 AND collection_month=$2`, payer, m); err != nil {
					t.Fatal(err)
				}
				if _, err := p.Exec(ctx, `INSERT INTO mrfpipeline.monthly_release_outputs(payer_id,collection_month,mrf_snapshot_id,published_generation) VALUES($1,$2,$3,1)`, payer, m, c.Targets[0].SnapshotID); err != nil {
					t.Fatal(err)
				}
				id := seedActivationCatalog(t, p, payer, m, 1, "published", c.Targets)
				if corrupt && payer == "aetna" {
					if _, err := p.Exec(ctx, `DELETE FROM mrfweb.release_outputs WHERE catalog_id=$1`, id); err != nil {
						t.Fatal(err)
					}
				}
			}
			out, err := ListActiveOutputs(ctx, p)
			if corrupt {
				if !jobs.IsFailure(err, jobs.FailureFilterCatalogInconsistent) || len(out) != 0 {
					t.Fatalf("out=%+v err=%v", out, err)
				}
			} else if err != nil || len(out) != 2 || out[0].PayerID != "aetna" || out[1].PayerID != "uhc" {
				t.Fatalf("out=%+v err=%v", out, err)
			}
		})
	}
}
