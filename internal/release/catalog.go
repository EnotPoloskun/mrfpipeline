package release

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/jackc/pgx/v5"
)

// CatalogCandidate is the exact database publication target used by a filter
// catalog build. Targets retain the release's numeric ordering; the
// fingerprint independently uses ASCII output ordering.
type CatalogCandidate struct {
	PayerID                      string
	CollectionMonth              string
	ReleaseStatus                string
	CurrentPublicationGeneration int64
	TargetPublicationGeneration  int64
	Targets                      []Target
	OutputFingerprint            string
	HasNewOutputs                bool
}

// SelectCatalogCandidate computes one complete database-only catalog target.
// The queryer may be a pool, a dedicated connection, or a short transaction.
// It deliberately does not inspect warehouse files.
func SelectCatalogCandidate(ctx context.Context, q rowsQueryer, payer string, month time.Time) (CatalogCandidate, error) {
	if q == nil {
		return CatalogCandidate{}, jobs.Failure(jobs.FailureInvalidArguments)
	}
	var candidate CatalogCandidate
	var generation int64
	if err := q.QueryRow(ctx, `
SELECT status, publication_generation
FROM mrfpipeline.monthly_releases
WHERE payer_id = $1 AND collection_month = $2`, payer, month).Scan(&candidate.ReleaseStatus, &generation); errors.Is(err, pgx.ErrNoRows) {
		return CatalogCandidate{}, jobs.Failure(jobs.FailureReleaseNotFound)
	} else if err != nil {
		return CatalogCandidate{}, dbFailure(ctx)
	}
	if candidate.ReleaseStatus != Building && candidate.ReleaseStatus != Active && candidate.ReleaseStatus != Inactive {
		return CatalogCandidate{}, jobs.Failure(jobs.FailureDomainInvariant)
	}
	if (candidate.ReleaseStatus == Active || candidate.ReleaseStatus == Inactive) && generation == 0 {
		return CatalogCandidate{}, jobs.Failure(jobs.FailureDomainInvariant)
	}
	candidate.PayerID = payer
	candidate.CollectionMonth = formatMonth(month)
	candidate.CurrentPublicationGeneration = generation

	published, err := listPublishedTargets(ctx, q, payer, month)
	if err != nil {
		return CatalogCandidate{}, err
	}
	if err := validateTargetRelation(payer, candidate.CollectionMonth, published); err != nil {
		return CatalogCandidate{}, err
	}
	if err := validateDatabaseTargetState(ctx, q, published); err != nil {
		return CatalogCandidate{}, err
	}
	if candidate.ReleaseStatus == Building && len(published) != 0 {
		return CatalogCandidate{}, jobs.Failure(jobs.FailureReleaseNotReady)
	}

	var targets []Target
	switch candidate.ReleaseStatus {
	case Building:
		if _, err := inspectPublicationCheckpoint(ctx, q, payer, month); err != nil {
			return CatalogCandidate{}, err
		}
		publishable, err := listPublishableTargets(ctx, q, payer, month)
		if err != nil {
			return CatalogCandidate{}, err
		}
		if err := validateTargetRelation(payer, candidate.CollectionMonth, publishable); err != nil {
			return CatalogCandidate{}, err
		}
		targets = publishable
		candidate.HasNewOutputs = true
	case Active:
		if _, err := inspectPublicationCheckpoint(ctx, q, payer, month); err != nil {
			return CatalogCandidate{}, err
		}
		publishable, err := listPublishableTargets(ctx, q, payer, month)
		if err != nil {
			return CatalogCandidate{}, err
		}
		if err := validateTargetRelation(payer, candidate.CollectionMonth, publishable); err != nil {
			return CatalogCandidate{}, err
		}
		publishedIDs := make(map[int64]struct{}, len(published))
		for _, target := range published {
			publishedIDs[target.SnapshotID] = struct{}{}
		}
		var newCount int
		for _, target := range publishable {
			if _, ok := publishedIDs[target.SnapshotID]; !ok {
				newCount++
			}
		}
		if newCount > 0 {
			targets = mergeTargets(published, publishable)
			candidate.HasNewOutputs = true
		} else {
			targets = published
		}
	case Inactive:
		targets = published
	}
	if len(targets) == 0 {
		return CatalogCandidate{}, jobs.Failure(jobs.FailureReleaseNotReady)
	}
	if err := validateTargetRelation(payer, candidate.CollectionMonth, targets); err != nil {
		return CatalogCandidate{}, err
	}
	if err := validateDatabaseTargetState(ctx, q, targets); err != nil {
		return CatalogCandidate{}, err
	}
	candidate.Targets = append([]Target(nil), targets...)
	if candidate.HasNewOutputs {
		if generation == math.MaxInt64 {
			return CatalogCandidate{}, jobs.Failure(jobs.FailureReleaseNotReady)
		}
		candidate.TargetPublicationGeneration = generation + 1
	} else {
		candidate.TargetPublicationGeneration = generation
	}
	candidate.OutputFingerprint = OutputFingerprint(candidate.Targets)
	return candidate, nil
}

// OutputFingerprint hashes a separate output-ID-sorted copy of targets.
func OutputFingerprint(targets []Target) string {
	copyTargets := append([]Target(nil), targets...)
	sort.Slice(copyTargets, func(i, j int) bool {
		return strings.Compare(copyTargets[i].OutputID, copyTargets[j].OutputID) < 0
	})
	hash := sha256.New()
	for _, target := range copyTargets {
		_, _ = hash.Write([]byte(target.PayerID))
		_, _ = hash.Write([]byte{0x1f})
		_, _ = hash.Write([]byte(target.CollectionMonth))
		_, _ = hash.Write([]byte{0x1f})
		_, _ = hash.Write([]byte(target.OutputID))
		_, _ = hash.Write([]byte{'\n'})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// FilterCatalogLockKey returns the signed 64-bit session-lock key required for
// one exact payer/month. It is intentionally separate from worker lock keys.
func FilterCatalogLockKey(payer, month string) int64 {
	hash := sha256.New()
	_, _ = hash.Write([]byte("mrfpipeline/filter-catalog"))
	_, _ = hash.Write([]byte{0x1f})
	_, _ = hash.Write([]byte(payer))
	_, _ = hash.Write([]byte{0x1f})
	_, _ = hash.Write([]byte(month))
	return int64(binary.BigEndian.Uint64(hash.Sum(nil)[:8]))
}

func validateTargetRelation(payer, month string, targets []Target) error {
	seenSnapshots := make(map[int64]struct{}, len(targets))
	seenOutputs := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		if target.PayerID != payer || target.CollectionMonth != month || target.SnapshotID <= 0 || target.OutputID != formatOutputID(target.SnapshotID) {
			return jobs.Failure(jobs.FailureDomainInvariant)
		}
		if _, ok := seenSnapshots[target.SnapshotID]; ok {
			return jobs.Failure(jobs.FailureDomainInvariant)
		}
		if _, ok := seenOutputs[target.OutputID]; ok {
			return jobs.Failure(jobs.FailureDomainInvariant)
		}
		seenSnapshots[target.SnapshotID] = struct{}{}
		seenOutputs[target.OutputID] = struct{}{}
	}
	return nil
}

func validateDatabaseTargetState(ctx context.Context, q rowsQueryer, targets []Target) error {
	for _, target := range targets {
		var payer string
		var month time.Time
		var consumeStatus string
		var planCount, unassignedCount int64
		var positiveBatch, incompleteBatch bool
		err := q.QueryRow(ctx, `
SELECT s.payer_id,
       s.collection_month,
       s.consume_status,
       (SELECT count(*) FROM mrfpipeline.mrf_plans p WHERE p.mrf_snapshot_id = s.id),
       EXISTS (
           SELECT 1 FROM mrfpipeline.plan_attachment_batches b
           WHERE b.mrf_snapshot_id = s.id AND b.status = 'succeeded' AND b.added_plan_count > 0),
       EXISTS (
           SELECT 1 FROM mrfpipeline.plan_attachment_batches b
           WHERE b.mrf_snapshot_id = s.id AND b.status <> 'succeeded'),
       (SELECT count(*)
        FROM mrfpipeline.mrf_plans p
        WHERE p.mrf_snapshot_id = s.id
          AND NOT EXISTS (
              SELECT 1
              FROM mrfpipeline.plan_attachment_batch_items i
              JOIN mrfpipeline.plan_attachment_batches b ON b.id = i.plan_attachment_batch_id
              WHERE i.mrf_plan_id = p.id
                AND b.mrf_snapshot_id = s.id
                AND b.status = 'succeeded'))
FROM mrfpipeline.mrf_snapshots s
WHERE s.id = $1`, target.SnapshotID).Scan(
			&payer, &month, &consumeStatus, &planCount, &positiveBatch, &incompleteBatch, &unassignedCount)
		if errors.Is(err, pgx.ErrNoRows) {
			return jobs.Failure(jobs.FailureDomainInvariant)
		}
		if err != nil {
			return dbFailure(ctx)
		}
		if payer != target.PayerID || formatMonth(month) != target.CollectionMonth || consumeStatus != "succeeded" ||
			planCount <= 0 || !positiveBatch || incompleteBatch || unassignedCount != 0 {
			return jobs.Failure(jobs.FailureReleaseNotReady)
		}
	}
	return nil
}
