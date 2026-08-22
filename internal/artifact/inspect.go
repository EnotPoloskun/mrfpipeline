package artifact

import (
	"os"
	"path/filepath"
)

const (
	ParsedAbsent          = "absent"
	ParsedEmpty           = "empty"
	ParsedIncomplete      = "incomplete"
	ParsedManifestPresent = "manifest_present"
)

func (w *Workspace) inspectDownloadDir(dir string) (complete bool, n int64, err error) {
	if err := w.verifyChain(dir, false); err != nil {
		return false, 0, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, 0, nil
		}
		return false, 0, artErr("stat")
	}
	if isSymlink(info) {
		return false, 0, artErr("symlink")
	}
	if !info.IsDir() {
		return false, 0, artErr("type")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, 0, artErr("read")
	}
	if len(entries) != 2 {
		return false, 0, nil
	}
	var haveData, haveMan bool
	for _, e := range entries {
		switch e.Name() {
		case fileData:
			haveData = true
		case fileManifest:
			haveMan = true
		default:
			return false, 0, nil
		}
	}
	if !haveData || !haveMan {
		return false, 0, nil
	}
	dataPath := filepath.Join(dir, fileData)
	manPath := filepath.Join(dir, fileManifest)
	di, err := os.Lstat(dataPath)
	if err != nil || requireRegular(di) != nil {
		return false, 0, nil
	}
	mi, err := os.Lstat(manPath)
	if err != nil || requireRegular(mi) != nil {
		return false, 0, nil
	}
	raw, err := readFileLimited(manPath, 4096)
	if err != nil {
		return false, 0, nil
	}
	want, err := parseDownloadManifest(raw)
	if err != nil {
		return false, 0, nil
	}
	if di.Size() != want {
		return false, 0, nil
	}
	return true, want, nil
}

// InspectParsed reports absent, empty, incomplete, or manifest_present.
func (w *Workspace) InspectParsed(kind string, id int64) (string, error) {
	dir, err := w.parsedPath(kind, id)
	if err != nil {
		return "", err
	}
	if err := w.verifyChain(dir, false); err != nil {
		return "", err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return ParsedAbsent, nil
		}
		return "", artErr("stat")
	}
	if isSymlink(info) {
		return "", artErr("symlink")
	}
	if !info.IsDir() {
		return "", artErr("type")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", artErr("read")
	}
	if len(entries) == 0 {
		return ParsedEmpty, nil
	}
	for _, e := range entries {
		if e.Name() != fileManifest {
			continue
		}
		mi, err := os.Lstat(filepath.Join(dir, fileManifest))
		if err != nil {
			return "", artErr("stat")
		}
		if err := requireRegular(mi); err != nil {
			return "", err
		}
		return ParsedManifestPresent, nil
	}
	return ParsedIncomplete, nil
}

// ResetParsed creates or empties an incomplete parsed leaf and refuses
// manifest_present output.
func (w *Workspace) ResetParsed(kind string, id int64) error {
	dir, err := w.parsedPath(kind, id)
	if err != nil {
		return err
	}
	state, err := w.InspectParsed(kind, id)
	if err != nil {
		return err
	}
	switch state {
	case ParsedManifestPresent:
		return artErr("preserve")
	case ParsedEmpty:
		return nil
	case ParsedIncomplete:
		if err := w.verifyChain(dir, true); err != nil {
			return err
		}
		if err := removeExactDir(dir); err != nil {
			return err
		}
	case ParsedAbsent:
	default:
		return artErr("parsed")
	}
	rec, err := w.recordPath(kind, id)
	if err != nil {
		return err
	}
	if err := w.verifyChain(rec, false); err != nil {
		return err
	}
	if err := mkdirReal(rec); err != nil {
		return err
	}
	return mkdirReal(dir)
}

// RemoveDownload removes one exact download leaf. Absent is success.
func (w *Workspace) RemoveDownload(kind string, id int64) error {
	dir, err := w.downloadPath(kind, id)
	if err != nil {
		return err
	}
	if err := w.verifyChain(dir, false); err != nil {
		return err
	}
	return removeExactDir(dir)
}

func (w *Workspace) ensureRecord(kind string, id int64) (string, error) {
	rec, err := w.recordPath(kind, id)
	if err != nil {
		return "", err
	}
	top, err := topDir(kind)
	if err != nil {
		return "", err
	}
	topPath := filepath.Join(w.Root, top)
	if err := w.verifyChain(topPath, true); err != nil {
		return "", err
	}
	if err := mkdirReal(rec); err != nil {
		return "", err
	}
	if err := w.verifyChain(rec, true); err != nil {
		return "", err
	}
	return rec, nil
}
