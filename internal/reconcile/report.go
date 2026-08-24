package reconcile

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

// Report is the compact reconcile success object. Mutation counts describe
// work committed by this invocation; sealed inconsistency is detected state.
type Report struct {
	RepairedJobCount                int `json:"repaired_job_count"`
	UnblockedStageCount             int `json:"unblocked_stage_count"`
	ScheduledPlanBatchCount         int `json:"scheduled_plan_batch_count"`
	CleanedArtifactCount            int `json:"cleaned_artifact_count"`
	SealedReleaseInconsistencyCount int `json:"sealed_release_inconsistency_count"`

	logger     *slog.Logger
	sealedSeen map[sealedDomainKey]struct{}
}

type sealedDomainKey struct {
	table string
	id    int64
}

func (r *Report) recordSealed(logger *slog.Logger, kind string, domainID int64) {
	r.recordSealedDomain(logger, sealedTable(kind), kind, domainID)
}

func (r *Report) recordSealedSnapshot(logger *slog.Logger, kind string, snapshotID int64) {
	r.recordSealedDomain(logger, "mrf_snapshots", kind, snapshotID)
}

func (r *Report) recordSealedDomain(logger *slog.Logger, table, kind string, domainID int64) {
	if r == nil || domainID <= 0 {
		return
	}
	key := sealedDomainKey{table: table, id: domainID}
	if r.sealedSeen == nil {
		r.sealedSeen = make(map[sealedDomainKey]struct{})
	}
	if _, ok := r.sealedSeen[key]; ok {
		return
	}
	r.sealedSeen[key] = struct{}{}
	r.SealedReleaseInconsistencyCount++
	if logger == nil {
		logger = r.logger
	}
	if logger != nil {
		logger.LogAttrs(context.Background(), slog.LevelWarn, "sealed_release_inconsistent",
			slog.String("kind", kind),
			slog.Int64("domain_id", domainID),
			slog.String("failure", jobs.FailureSealedReleaseInconsistent),
		)
	}
}

func sealedTable(kind string) string {
	switch kind {
	case jobs.KindDiscoveryRun:
		return "discovery_runs"
	case jobs.KindTOCDownload, jobs.KindTOCParse, jobs.KindTOCImport:
		return "toc_files"
	case jobs.KindMRFDownload, jobs.KindMRFParse:
		return "mrf_sources"
	case jobs.KindConsumerIngest:
		return "mrf_snapshots"
	case jobs.KindConsumerAttachPlans:
		return "plan_attachment_batches"
	default:
		return kind
	}
}

// RetryResult is the compact retry success object.
type RetryResult struct {
	Stage      string `json:"stage"`
	DomainID   int64  `json:"domain_id"`
	RiverJobID int64  `json:"river_job_id"`
}

// FormatReport returns compact JSON plus a trailing newline.
func FormatReport(r Report) (string, error) {
	raw, err := json.Marshal(r)
	if err != nil {
		return "", jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	return string(raw) + "\n", nil
}

// FormatRetry returns compact JSON plus a trailing newline.
func FormatRetry(r RetryResult) (string, error) {
	raw, err := json.Marshal(r)
	if err != nil {
		return "", jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	return string(raw) + "\n", nil
}

func dbFail(ctxErr error) error {
	if ctxErr != nil {
		return ctxErr
	}
	return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
}

func artFail() error {
	return jobs.Failure(jobs.FailureArtifactReconciliationFailed)
}

func leaseLost() error {
	return jobs.Failure(jobs.FailureWorkerLeaseLost)
}
