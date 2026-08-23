package planattach

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/planbatch"
)

const filePlans = "plans.json"

func plansDir(ws *artifact.Workspace, batchID int64) (string, error) {
	if ws == nil {
		return "", jobs.Failure(jobs.FailurePlanAttachInvariant)
	}
	name, err := artifact.RecordDirName(artifact.KindPlanBatch, batchID)
	if err != nil {
		return "", jobs.Failure(jobs.FailurePlanAttachInvariant)
	}
	return filepath.Join(ws.Root, "plan-batches", name), nil
}

func plansPath(ws *artifact.Workspace, batchID int64) (string, error) {
	dir, err := plansDir(ws, batchID)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, filePlans), nil
}

func publishPlansJSON(ws *artifact.Workspace, batchID int64, canonical []byte) (string, error) {
	dir, err := plansDir(ws, batchID)
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, filePlans)
	info, err := os.Lstat(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", jobs.Failure(jobs.FailurePlanAttachInputInvalid)
		}
		if err := writeBatchDir(ws, dir, batchID, canonical); err != nil {
			return "", err
		}
		return path, nil
	}
	if isSymlink(info) || !info.IsDir() {
		return "", jobs.Failure(jobs.FailurePlanAttachInputInvalid)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", jobs.Failure(jobs.FailurePlanAttachInputInvalid)
	}
	hasPlans := false
	for _, e := range entries {
		if e.Name() == filePlans {
			hasPlans = true
			break
		}
	}
	if len(entries) == 0 || !hasPlans {
		if err := os.RemoveAll(dir); err != nil {
			return "", jobs.Failure(jobs.FailurePlanAttachInputInvalid)
		}
		if err := writeBatchDir(ws, dir, batchID, canonical); err != nil {
			return "", err
		}
		return path, nil
	}
	if len(entries) != 1 {
		return "", jobs.Failure(jobs.FailurePlanAttachInputInvalid)
	}
	man, err := os.Lstat(path)
	if err != nil || isSymlink(man) || !man.Mode().IsRegular() {
		return "", jobs.Failure(jobs.FailurePlanAttachInputInvalid)
	}
	existing, err := os.ReadFile(path)
	if err != nil {
		return "", jobs.Failure(jobs.FailurePlanAttachInputInvalid)
	}
	if !bytes.Equal(existing, canonical) {
		return "", jobs.Failure(jobs.FailurePlanAttachInputInvalid)
	}
	return path, nil
}

func writeBatchDir(ws *artifact.Workspace, final string, batchID int64, canonical []byte) error {
	if ws == nil {
		return jobs.Failure(jobs.FailurePlanAttachInvariant)
	}
	tmp, err := os.MkdirTemp(ws.StagingDir(), "plan-batch-"+strconv.FormatInt(batchID, 10)+"-")
	if err != nil {
		return jobs.Failure(jobs.FailurePlanAttachOutputInvalid)
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(tmp)
		}
	}()
	if err := os.Chmod(tmp, 0700); err != nil {
		return jobs.Failure(jobs.FailurePlanAttachOutputInvalid)
	}
	f, err := os.OpenFile(filepath.Join(tmp, filePlans), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return jobs.Failure(jobs.FailurePlanAttachOutputInvalid)
	}
	if _, err := f.Write(canonical); err != nil {
		_ = f.Close()
		return jobs.Failure(jobs.FailurePlanAttachOutputInvalid)
	}
	if err := f.Close(); err != nil {
		return jobs.Failure(jobs.FailurePlanAttachOutputInvalid)
	}
	if err := os.Rename(tmp, final); err != nil {
		return jobs.Failure(jobs.FailurePlanAttachOutputInvalid)
	}
	published = true
	return nil
}

func isSymlink(info os.FileInfo) bool {
	return info.Mode()&os.ModeSymlink != 0
}

func formatBatchID(id int64) string {
	return planbatch.FormatPlanBatchID(id)
}
