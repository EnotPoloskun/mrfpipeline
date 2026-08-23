package reconcile

import (
	"encoding/json"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

// Report is the compact reconcile success object. Counts are mutations
// committed by this invocation.
type Report struct {
	RepairedJobCount         int `json:"repaired_job_count"`
	UnblockedStageCount      int `json:"unblocked_stage_count"`
	ScheduledPlanBatchCount  int `json:"scheduled_plan_batch_count"`
	CleanedArtifactCount     int `json:"cleaned_artifact_count"`
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
