package consumeringest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func inspectCompletedSnapshot(warehouse, payer, feedID, month, outputID string, want catalogIdentity) error {
	final, err := expectedFinalPath(warehouse, payer, month, outputID)
	if err != nil {
		return errOutputInvalid
	}
	root, err := normalizePath(warehouse)
	if err != nil {
		return errOutputInvalid
	}
	info, err := os.Lstat(final)
	if errors.Is(err, os.ErrNotExist) {
		return errTargetAbsent
	}
	if err != nil {
		return errOutputInvalid
	}
	if isSymlink(info) || !info.IsDir() {
		return errOutputInvalid
	}
	if err := requireRealAncestors(root, final); err != nil {
		return err
	}
	whIdent, err := readWarehouseIdentity(filepath.Join(root, fileWarehouse))
	if err != nil {
		return errOutputInvalid
	}
	if !whIdent.equal(want) {
		return errOutputInvalid
	}
	raw, err := readRegularFile(filepath.Join(final, fileManifest))
	if err != nil {
		return errOutputInvalid
	}
	m, err := decodeSnapshotManifest(raw)
	if err != nil {
		return err
	}
	if m.OutputID != outputID || m.PayerID != payer || m.FeedID != feedID || m.CollectionMonth != month {
		return errOutputInvalid
	}
	if !m.Catalog.equal(whIdent) {
		return errOutputInvalid
	}
	if err := validateSnapshotLayout(final, outputID, m); err != nil {
		return err
	}
	return nil
}

var errTargetAbsent = errors.New("consumer snapshot target absent")

func requireRealAncestors(warehouse, target string) error {
	cur := target
	for {
		info, err := os.Lstat(cur)
		if err != nil || isSymlink(info) || !info.IsDir() {
			return errOutputInvalid
		}
		if cur == warehouse {
			return nil
		}
		next := filepath.Dir(cur)
		if next == cur {
			return errOutputInvalid
		}
		cur = next
	}
}

func validateSnapshotLayout(dir, outputID string, m snapshotManifest) error {
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != len(snapshotDatasets)+1 {
		return errOutputInvalid
	}
	seen := map[string]bool{}
	for _, e := range entries {
		seen[e.Name()] = true
		p := filepath.Join(dir, e.Name())
		fi, err := os.Lstat(p)
		if err != nil || isSymlink(fi) {
			return errOutputInvalid
		}
		if e.Name() == fileManifest {
			if !fi.Mode().IsRegular() {
				return errOutputInvalid
			}
			continue
		}
		if !fi.IsDir() {
			return errOutputInvalid
		}
		count, ok := m.Datasets[e.Name()]
		if !ok {
			return errOutputInvalid
		}
		if err := validateParts(p, outputID, count.PartCount); err != nil {
			return err
		}
	}
	if !seen[fileManifest] {
		return errOutputInvalid
	}
	for _, name := range snapshotDatasets {
		if !seen[name] {
			return errOutputInvalid
		}
	}
	return nil
}

func validateParts(dir, outputID string, partCount int64) error {
	entries, err := os.ReadDir(dir)
	if err != nil || int64(len(entries)) != partCount {
		return errOutputInvalid
	}
	named := map[string]bool{}
	for _, e := range entries {
		fi, err := os.Lstat(filepath.Join(dir, e.Name()))
		if err != nil || isSymlink(fi) || !fi.Mode().IsRegular() {
			return errOutputInvalid
		}
		named[e.Name()] = true
	}
	for i := int64(0); i < partCount; i++ {
		want := fmt.Sprintf("%s-part-%05d.parquet", outputID, i)
		if !named[want] {
			return errOutputInvalid
		}
	}
	return nil
}
