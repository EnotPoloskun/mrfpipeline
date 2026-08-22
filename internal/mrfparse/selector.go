package mrfparse

import (
	"os"
	"syscall"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

const maxServicesBytes = 16 << 20

// ServicesID is process-local selector immutability metadata. It is not a digest.
type ServicesID struct {
	Path  string
	Dev   uint64
	Ino   uint64
	Size  int64
	Mtime int64
	ok    bool
}

// InspectServices requires a real, regular, non-symlink, readable, nonempty
// selector file at most 16 MiB and records device/inode/size/mtime-nsec.
func InspectServices(path string) (ServicesID, error) {
	var zero ServicesID
	if path == "" {
		return zero, jobs.Failure("runtime")
	}
	clean, err := normalizePath(path)
	if err != nil {
		return zero, jobs.Failure("runtime")
	}
	info, err := os.Lstat(clean)
	if err != nil || isSymlink(info) || !info.Mode().IsRegular() {
		return zero, jobs.Failure("runtime")
	}
	if info.Size() <= 0 || info.Size() > maxServicesBytes {
		return zero, jobs.Failure("runtime")
	}
	f, err := os.Open(clean)
	if err != nil {
		return zero, jobs.Failure("runtime")
	}
	_ = f.Close()
	id := ServicesID{Path: clean, Size: info.Size(), Mtime: info.ModTime().UnixNano(), ok: true}
	if sys, ok := info.Sys().(*syscall.Stat_t); ok {
		id.Dev = uint64(sys.Dev)
		id.Ino = uint64(sys.Ino)
	}
	return id, nil
}

func (s ServicesID) same(other ServicesID) bool {
	if !s.ok || !other.ok || s.Path != other.Path {
		return false
	}
	return s.Dev == other.Dev && s.Ino == other.Ino && s.Size == other.Size && s.Mtime == other.Mtime
}
