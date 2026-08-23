package consumeringest

import (
	"os"
	"path/filepath"
	"syscall"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

const fileManifest = "manifest.json"

// CatalogID is process-local configured-path metadata. It is not catalog identity.
type CatalogID struct {
	Path          string
	Dev           uint64
	Ino           uint64
	Size          int64
	Mtime         int64
	ManifestDev   uint64
	ManifestIno   uint64
	ManifestSize  int64
	ManifestMtime int64
	ok            bool
}

// InspectCatalog requires a real, non-symlink, readable catalog directory and
// records root plus manifest.json filesystem metadata.
func InspectCatalog(path string) (CatalogID, error) {
	var zero CatalogID
	if path == "" {
		return zero, jobs.Failure("runtime")
	}
	clean, err := normalizePath(path)
	if err != nil {
		return zero, jobs.Failure("runtime")
	}
	info, err := os.Lstat(clean)
	if err != nil || isSymlink(info) || !info.IsDir() {
		return zero, jobs.Failure("runtime")
	}
	dir, err := os.Open(clean)
	if err != nil {
		return zero, jobs.Failure("runtime")
	}
	_ = dir.Close()
	man := filepath.Join(clean, fileManifest)
	manInfo, err := os.Lstat(man)
	if err != nil || isSymlink(manInfo) || !manInfo.Mode().IsRegular() {
		return zero, jobs.Failure("runtime")
	}
	id := CatalogID{
		Path: clean, Size: info.Size(), Mtime: info.ModTime().UnixNano(),
		ManifestSize: manInfo.Size(), ManifestMtime: manInfo.ModTime().UnixNano(),
		ok: true,
	}
	if sys, ok := info.Sys().(*syscall.Stat_t); ok {
		id.Dev = uint64(sys.Dev)
		id.Ino = uint64(sys.Ino)
	}
	if sys, ok := manInfo.Sys().(*syscall.Stat_t); ok {
		id.ManifestDev = uint64(sys.Dev)
		id.ManifestIno = uint64(sys.Ino)
	}
	return id, nil
}

func (c CatalogID) same(other CatalogID) bool {
	if !c.ok || !other.ok || c.Path != other.Path {
		return false
	}
	return c.Dev == other.Dev && c.Ino == other.Ino && c.Size == other.Size && c.Mtime == other.Mtime &&
		c.ManifestDev == other.ManifestDev && c.ManifestIno == other.ManifestIno &&
		c.ManifestSize == other.ManifestSize && c.ManifestMtime == other.ManifestMtime
}

func isSymlink(info os.FileInfo) bool {
	return info.Mode()&os.ModeSymlink != 0
}
