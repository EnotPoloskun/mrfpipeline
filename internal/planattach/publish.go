package planattach

import (
	"errors"
	"path/filepath"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/planbatch"
)

const filePlans = "plans.json"

func plansDir(ws *artifact.Workspace, batchID int64) (string, error) {
	if ws == nil {
		return "", jobs.Failure(jobs.FailurePlanAttachInvariant)
	}
	return ws.PlanBatchDir(batchID)
}

func plansPath(ws *artifact.Workspace, batchID int64) (string, error) {
	dir, err := plansDir(ws, batchID)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, filePlans), nil
}

func publishPlansJSON(ws *artifact.Workspace, batchID int64, canonical []byte) (string, error) {
	if ws == nil {
		return "", jobs.Failure(jobs.FailurePlanAttachInvariant)
	}
	path, err := ws.PublishPlanBatch(batchID, canonical)
	if err != nil {
		return "", mapPublishError(err)
	}
	return path, nil
}

func mapPublishError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, artifact.ErrPlanBatchInput) {
		return jobs.Failure(jobs.FailurePlanAttachInputInvalid)
	}
	if errors.Is(err, artifact.ErrPlanBatchOutput) {
		return jobs.Failure(jobs.FailurePlanAttachOutputInvalid)
	}
	if errors.Is(err, artifact.ErrArtifact) {
		return jobs.Failure(jobs.FailurePlanAttachInvariant)
	}
	return jobs.Failure(jobs.FailurePlanAttachOutputInvalid)
}

func formatBatchID(id int64) string {
	return planbatch.FormatPlanBatchID(id)
}
