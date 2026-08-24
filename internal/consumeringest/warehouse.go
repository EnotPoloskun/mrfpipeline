package consumeringest

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

const (
	warehouseAbsent     = "absent"
	warehouseEmpty      = "empty"
	warehouseRecognized = "recognized"
	fileWarehouse       = "warehouse.json"
	warehouseVersion    = "2.0.0"
	catalogSchema       = int64(1)
)

// WarehouseState is the shallow startup view of the configured warehouse.
type WarehouseState struct {
	Path    string
	Kind    string
	Catalog catalogIdentity
}

type catalogIdentity struct {
	SchemaVersion int64
	ReleaseMonth  string
}

func (c catalogIdentity) equal(other catalogIdentity) bool {
	return c.SchemaVersion == other.SchemaVersion && c.ReleaseMonth == other.ReleaseMonth
}

// InspectWarehouse accepts absent, empty real dir, or a real dir whose
// warehouse.json is exact consumer 2.0.0. It does not require catalog copy or seed.
func InspectWarehouse(path string) (WarehouseState, error) {
	var zero WarehouseState
	if path == "" {
		return zero, jobs.Failure("runtime")
	}
	clean, err := normalizePath(path)
	if err != nil {
		return zero, jobs.Failure("runtime")
	}
	info, err := os.Lstat(clean)
	if errors.Is(err, os.ErrNotExist) {
		return WarehouseState{Path: clean, Kind: warehouseAbsent}, nil
	}
	if err != nil {
		return zero, errOutputUnreadable
	}
	if isSymlink(info) || !info.IsDir() {
		return zero, jobs.Failure("runtime")
	}
	entries, err := os.ReadDir(clean)
	if err != nil {
		return zero, errOutputUnreadable
	}
	if len(entries) == 0 {
		return WarehouseState{Path: clean, Kind: warehouseEmpty}, nil
	}
	ident, err := readWarehouseIdentity(filepath.Join(clean, fileWarehouse))
	if err != nil {
		if errors.Is(err, errOutputUnreadable) {
			return zero, errOutputUnreadable
		}
		return zero, jobs.Failure("runtime")
	}
	return WarehouseState{Path: clean, Kind: warehouseRecognized, Catalog: ident}, nil
}

func CheckWarehouseCatalog(ws WarehouseState, catalog CatalogID, artifactRoot, servicesPath string) error {
	if !catalog.ok {
		return jobs.Failure("runtime")
	}
	art, err := normalizePath(artifactRoot)
	if err != nil {
		return jobs.Failure("runtime")
	}
	svc, err := normalizePath(servicesPath)
	if err != nil {
		return jobs.Failure("runtime")
	}
	if err := artifact.CheckPairOverlap(catalog.Path, art); err != nil {
		return jobs.Failure("runtime")
	}
	if err := artifact.CheckPairOverlap(catalog.Path, svc); err != nil {
		return jobs.Failure("runtime")
	}
	if err := artifact.CheckPairOverlap(ws.Path, svc); err != nil {
		return jobs.Failure("runtime")
	}
	owned := filepath.Join(ws.Path, "provider_catalog")
	if catalog.Path == owned {
		if ws.Kind != warehouseRecognized {
			return jobs.Failure("runtime")
		}
		return nil
	}
	if err := artifact.CheckPairOverlap(catalog.Path, ws.Path); err != nil {
		return jobs.Failure("runtime")
	}
	return nil
}

func readWarehouseIdentity(path string) (catalogIdentity, error) {
	data, err := readRegularFile(path)
	if err != nil {
		return catalogIdentity{}, err
	}
	return decodeWarehouseJSON(data)
}

func readRegularFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errOutputInvalid
		}
		return nil, errOutputUnreadable
	}
	if isSymlink(info) || !info.Mode().IsRegular() {
		return nil, errOutputInvalid
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errOutputUnreadable
	}
	return data, nil
}
