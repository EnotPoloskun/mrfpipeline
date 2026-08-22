package artifact

import (
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
)

// StagingDir returns <root>/.staging. Story 05 sets TMPDIR to this path once
// before River starts. This story does not set TMPDIR.
func StagingDir(root string) string {
	return filepath.Join(filepath.Clean(root), stagingName)
}

func formatID(id int64) (string, error) {
	if id <= 0 {
		return "", artErr("id")
	}
	return strconv.FormatInt(id, 10), nil
}

// RecordDirName is the exact record directory name for a kind and positive ID.
func RecordDirName(kind string, id int64) (string, error) {
	s, err := formatID(id)
	if err != nil {
		return "", err
	}
	switch kind {
	case KindTOC:
		return tocPrefix + s, nil
	case KindMRF:
		return mrfPrefix + s, nil
	case KindPlanBatch:
		return batPrefix + s, nil
	default:
		return "", artErr("kind")
	}
}

func topDir(kind string) (string, error) {
	switch kind {
	case KindTOC:
		return dirTOC, nil
	case KindMRF:
		return dirMRF, nil
	case KindPlanBatch:
		return dirPlanBatch, nil
	default:
		return "", artErr("kind")
	}
}

func (w *Workspace) recordPath(kind string, id int64) (string, error) {
	if w == nil || w.Root == "" {
		return "", artErr("workspace")
	}
	top, err := topDir(kind)
	if err != nil {
		return "", err
	}
	name, err := RecordDirName(kind, id)
	if err != nil {
		return "", err
	}
	return filepath.Join(w.Root, top, name), nil
}

func (w *Workspace) downloadPath(kind string, id int64) (string, error) {
	rec, err := w.recordPath(kind, id)
	if err != nil {
		return "", err
	}
	if kind != KindTOC && kind != KindMRF {
		return "", artErr("kind")
	}
	return filepath.Join(rec, dirDownload), nil
}

func (w *Workspace) parsedPath(kind string, id int64) (string, error) {
	rec, err := w.recordPath(kind, id)
	if err != nil {
		return "", err
	}
	if kind != KindTOC && kind != KindMRF {
		return "", artErr("kind")
	}
	return filepath.Join(rec, dirParsed), nil
}

func stagingPrefix(kind string, id int64) (string, error) {
	s, err := formatID(id)
	if err != nil {
		return "", err
	}
	switch kind {
	case KindTOC:
		return stagingTOCPrefix + s + "-", nil
	case KindMRF:
		return stagingMRFPrefix + s + "-", nil
	default:
		return "", artErr("kind")
	}
}

func parseStagingName(name string) (kind string, id int64, ok bool) {
	var rest string
	switch {
	case strings.HasPrefix(name, stagingTOCPrefix):
		kind = KindTOC
		rest = name[len(stagingTOCPrefix):]
	case strings.HasPrefix(name, stagingMRFPrefix):
		kind = KindMRF
		rest = name[len(stagingMRFPrefix):]
	default:
		return "", 0, false
	}
	i := 0
	for i < len(rest) && unicode.IsDigit(rune(rest[i])) {
		i++
	}
	if i == 0 || i >= len(rest) || rest[i] != '-' {
		return "", 0, false
	}
	n, err := strconv.ParseInt(rest[:i], 10, 64)
	if err != nil || n <= 0 {
		return "", 0, false
	}
	return kind, n, true
}
