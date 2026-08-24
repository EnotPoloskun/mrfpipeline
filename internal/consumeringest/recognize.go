package consumeringest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// InspectCompletedSnapshot requires a recognized 2.0.0 warehouse and one
// completed rate snapshot for the expected output ID.
func InspectCompletedSnapshot(warehouse, payer, month, outputID string) error {
	wh, err := InspectWarehouse(warehouse)
	if err != nil {
		return err
	}
	if wh.Kind != warehouseRecognized {
		return errOutputInvalid
	}
	_, err = inspectCompletedSnapshotCounts(warehouse, payer, month, outputID, wh.Catalog)
	return err
}

// InspectPlanAssociations validates the shallow, durable inventory expected
// for one completed snapshot. It deliberately does not inspect Parquet rows.
func InspectPlanAssociations(warehouse, outputID string, batchIDs []int64) error {
	root, err := normalizePath(warehouse)
	if err != nil {
		return errOutputInvalid
	}
	dir := filepath.Join(root, "plan_associations", "output_id="+outputID)
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		if len(batchIDs) == 0 {
			return nil
		}
		return errOutputInvalid
	}
	if err != nil {
		return errOutputUnreadable
	}
	if isSymlink(info) || !info.IsDir() {
		return errOutputInvalid
	}
	if err := requireRealAncestors(root, dir); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return errOutputUnreadable
	}
	want := make(map[string]struct{}, len(batchIDs))
	for _, batchID := range batchIDs {
		if batchID <= 0 {
			return errOutputInvalid
		}
		want[fmt.Sprintf("plan-batch-%d-part-00000.parquet", batchID)] = struct{}{}
	}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		fi, err := os.Lstat(path)
		if err != nil {
			return errOutputUnreadable
		}
		if isSymlink(fi) || !fi.Mode().IsRegular() {
			return errOutputInvalid
		}
		if _, ok := want[entry.Name()]; !ok {
			return errOutputInvalid
		}
		delete(want, entry.Name())
	}
	if len(want) != 0 {
		return errOutputInvalid
	}
	return nil
}

// IsPublicationUnreadable distinguishes filesystem failures from a readable
// publication that simply fails its shallow contract.
func IsPublicationUnreadable(err error) bool {
	return errors.Is(err, errOutputUnreadable)
}

func inspectCompletedSnapshot(warehouse, payer, month, outputID string, want catalogIdentity) error {
	_, err := inspectCompletedSnapshotCounts(warehouse, payer, month, outputID, want)
	return err
}

func inspectCompletedSnapshotCounts(warehouse, payer, month, outputID string, want catalogIdentity) (map[string]datasetCount, error) {
	final, err := expectedFinalPath(warehouse, payer, month, outputID)
	if err != nil {
		return nil, errOutputInvalid
	}
	root, err := normalizePath(warehouse)
	if err != nil {
		return nil, errOutputInvalid
	}
	info, err := os.Lstat(final)
	if errors.Is(err, os.ErrNotExist) {
		return nil, errTargetAbsent
	}
	if err != nil {
		return nil, errOutputUnreadable
	}
	if isSymlink(info) || !info.IsDir() {
		return nil, errOutputInvalid
	}
	if err := requireRealAncestors(root, final); err != nil {
		return nil, err
	}
	whIdent, err := readWarehouseIdentity(filepath.Join(root, fileWarehouse))
	if err != nil {
		return nil, err
	}
	if !whIdent.equal(want) {
		return nil, errOutputInvalid
	}
	raw, err := readRegularFile(filepath.Join(final, fileManifest))
	if err != nil {
		return nil, err
	}
	m, err := decodeSnapshotManifest(raw)
	if err != nil {
		return nil, err
	}
	if m.OutputID != outputID || m.PayerID != payer || m.CollectionMonth != month {
		return nil, errOutputInvalid
	}
	if !m.Catalog.equal(whIdent) {
		return nil, errOutputInvalid
	}
	if err := validateSnapshotLayout(final, outputID, m); err != nil {
		return nil, err
	}
	return m.Datasets, nil
}

var errTargetAbsent = errors.New("consumer snapshot target absent")

func requireRealAncestors(warehouse, target string) error {
	cur := target
	for {
		info, err := os.Lstat(cur)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return errOutputInvalid
			}
			return errOutputUnreadable
		}
		if isSymlink(info) || !info.IsDir() {
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
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errOutputInvalid
		}
		return errOutputUnreadable
	}
	if len(entries) != len(snapshotDatasets)+1 {
		return errOutputInvalid
	}
	seen := map[string]bool{}
	for _, e := range entries {
		seen[e.Name()] = true
		p := filepath.Join(dir, e.Name())
		fi, err := os.Lstat(p)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return errOutputInvalid
			}
			return errOutputUnreadable
		}
		if isSymlink(fi) {
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
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errOutputInvalid
		}
		return errOutputUnreadable
	}
	if int64(len(entries)) != partCount {
		return errOutputInvalid
	}
	named := map[string]bool{}
	for _, e := range entries {
		fi, err := os.Lstat(filepath.Join(dir, e.Name()))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return errOutputInvalid
			}
			return errOutputUnreadable
		}
		if isSymlink(fi) || !fi.Mode().IsRegular() {
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
