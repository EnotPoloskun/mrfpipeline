package artifact

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
)

// PublishPlanBatch atomically publishes or reuses
// plan-batches/plan-batch-<id>/plans.json using workspace path guards.
func (w *Workspace) PublishPlanBatch(id int64, canonical []byte) (string, error) {
	if w == nil {
		return "", artErr("workspace")
	}
	dir, err := w.PlanBatchDir(id)
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, filePlans)
	top, err := topDir(KindPlanBatch)
	if err != nil {
		return "", err
	}
	if err := w.verifyChain(filepath.Join(w.Root, top), true); err != nil {
		return "", err
	}
	if err := w.verifyChain(dir, false); err != nil {
		return "", err
	}

	info, err := os.Lstat(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", fmtPlanBatchInput()
		}
		if err := w.writePlanBatchDir(dir, id, canonical); err != nil {
			return "", err
		}
		return path, nil
	}
	if isSymlink(info) || !info.IsDir() {
		return "", fmtPlanBatchInput()
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", fmtPlanBatchInput()
	}
	hasPlans := false
	for _, e := range entries {
		if e.Name() == filePlans {
			hasPlans = true
			break
		}
	}
	if len(entries) == 0 || !hasPlans {
		if err := w.RemovePlanBatch(id); err != nil {
			return "", fmtPlanBatchInput()
		}
		if err := w.writePlanBatchDir(dir, id, canonical); err != nil {
			return "", err
		}
		return path, nil
	}
	if len(entries) != 1 {
		return "", fmtPlanBatchInput()
	}
	man, err := os.Lstat(path)
	if err != nil || isSymlink(man) || !man.Mode().IsRegular() {
		return "", fmtPlanBatchInput()
	}
	existing, err := os.ReadFile(path)
	if err != nil {
		return "", fmtPlanBatchInput()
	}
	if !bytes.Equal(existing, canonical) {
		return "", fmtPlanBatchInput()
	}
	return path, nil
}

func (w *Workspace) writePlanBatchDir(final string, id int64, canonical []byte) error {
	prefix := stagingBatchPrefix + strconv.FormatInt(id, 10) + "-"
	tmp, err := w.mkdirStagingTemp(prefix)
	if err != nil {
		return fmtPlanBatchOutput()
	}
	published := false
	defer func() {
		if !published {
			_ = removeExactDir(tmp)
		}
	}()
	if err := os.Chmod(tmp, dirMode); err != nil {
		return fmtPlanBatchOutput()
	}
	if err := writeExclusive(filepath.Join(tmp, filePlans), canonical); err != nil {
		return fmtPlanBatchOutput()
	}
	if err := w.verifyChain(final, false); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmtPlanBatchOutput()
	}
	published = true
	return nil
}

func (w *Workspace) mkdirStagingTemp(prefix string) (string, error) {
	dir := w.StagingDir()
	if err := w.verifyChain(dir, true); err != nil {
		return "", err
	}
	tmp, err := os.MkdirTemp(dir, prefix)
	if err != nil {
		return "", artErr("create")
	}
	if err := w.verifyChain(tmp, true); err != nil {
		_ = removeExactDir(tmp)
		return "", err
	}
	return tmp, nil
}

func fmtPlanBatchInput() error {
	return wrapPlanBatch(ErrPlanBatchInput)
}

func fmtPlanBatchOutput() error {
	return wrapPlanBatch(ErrPlanBatchOutput)
}

func wrapPlanBatch(err error) error {
	return errorsJoinArtifact(err)
}

func errorsJoinArtifact(err error) error {
	return joinErr(ErrArtifact, err)
}
