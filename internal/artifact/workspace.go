package artifact

import (
	"context"
	"os"
	"path/filepath"
)

// Workspace is a recognized artifact root. Root is the retained physical path.
type Workspace struct {
	Root string
}

// StagingDir is the process TMPDIR Story 05 will set. It is not assigned here.
func (w *Workspace) StagingDir() string {
	if w == nil {
		return ""
	}
	return StagingDir(w.Root)
}

var allowedRootNames = map[string]bool{
	markerName:   true,
	stagingName:  true,
	dirTOC:       true,
	dirMRF:       true,
	dirPlanBatch: true,
}

var fixedDirs = []string{stagingName, dirTOC, dirMRF, dirPlanBatch}

// Init creates or verifies the artifact workspace. It does not set TMPDIR,
// start River, or compare overlap with warehouse paths.
func Init(ctx context.Context, artifactRoot string) (*Workspace, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root := filepath.Clean(artifactRoot)
	if root == "" || !filepath.IsAbs(root) {
		return nil, artErr("root")
	}
	if err := ensureRoot(ctx, root); err != nil {
		return nil, err
	}
	if err := requireRealDir(root); err != nil {
		return nil, err
	}
	phys, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, artErr("root")
	}
	if err := requireRealDir(phys); err != nil {
		return nil, err
	}
	if err := ensureFixedDirs(phys); err != nil {
		return nil, err
	}
	return &Workspace{Root: phys}, nil
}

func ensureRoot(ctx context.Context, root string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := os.Lstat(root)
	if err == nil {
		if isSymlink(info) {
			return artErr("symlink")
		}
		if !info.IsDir() {
			return artErr("type")
		}
		return prepareExisting(root)
	}
	if !os.IsNotExist(err) {
		return artErr("stat")
	}
	if err := createMissingParents(root); err != nil {
		return err
	}
	return publishNewRoot(root)
}

func createMissingParents(root string) error {
	parent := filepath.Dir(root)
	if parent == root {
		return artErr("ancestor")
	}
	info, err := os.Lstat(parent)
	if err == nil {
		if isSymlink(info) || !info.IsDir() {
			return artErr("ancestor")
		}
		if err := openDir(parent); err != nil {
			return err
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return artErr("ancestor")
	}
	if err := createMissingParents(parent); err != nil {
		return err
	}
	return mkdirReal(parent)
}

func openDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return artErr("ancestor")
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.IsDir() {
		return artErr("ancestor")
	}
	return nil
}

func publishNewRoot(root string) error {
	parent := filepath.Dir(root)
	tmp, err := os.MkdirTemp(parent, ".mrfpipeline-init-")
	if err != nil {
		return artErr("create")
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.RemoveAll(tmp)
		}
	}()
	if err := writeExclusive(filepath.Join(tmp, markerName), markerJSON()); err != nil {
		return err
	}
	for _, name := range fixedDirs {
		if err := mkdirReal(filepath.Join(tmp, name)); err != nil {
			return err
		}
	}
	if err := os.Rename(tmp, root); err != nil {
		return artErr("rename")
	}
	ok = true
	return nil
}

func prepareExisting(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return artErr("read")
	}
	if onlyMarkerTmp(entries) {
		if err := removeMarkerTmp(root); err != nil {
			return err
		}
		entries, err = os.ReadDir(root)
		if err != nil {
			return artErr("read")
		}
	}
	if len(entries) == 0 {
		if err := publishMarker(root); err != nil {
			return err
		}
		return nil
	}
	if err := requireValidMarker(root); err != nil {
		return err
	}
	for _, e := range entries {
		if !allowedRootNames[e.Name()] {
			return artErr("unrecognized")
		}
	}
	return nil
}

func onlyMarkerTmp(entries []os.DirEntry) bool {
	return len(entries) == 1 && entries[0].Name() == markerTmpName
}

func removeMarkerTmp(root string) error {
	p := filepath.Join(root, markerTmpName)
	info, err := os.Lstat(p)
	if err != nil {
		return artErr("marker")
	}
	if isSymlink(info) || !info.Mode().IsRegular() {
		return artErr("marker")
	}
	if err := os.Remove(p); err != nil {
		return artErr("marker")
	}
	return nil
}

func publishMarker(root string) error {
	tmp := filepath.Join(root, markerTmpName)
	if err := writeExclusive(tmp, markerJSON()); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(root, markerName)); err != nil {
		_ = os.Remove(tmp)
		return artErr("rename")
	}
	return nil
}

func requireValidMarker(root string) error {
	p := filepath.Join(root, markerName)
	info, err := os.Lstat(p)
	if err != nil {
		return artErr("marker")
	}
	if err := requireRegular(info); err != nil {
		return err
	}
	data, err := readFileLimited(p, 4096)
	if err != nil {
		return err
	}
	return parseWorkspaceMarker(data)
}

func ensureFixedDirs(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return artErr("read")
	}
	for _, e := range entries {
		if !allowedRootNames[e.Name()] {
			return artErr("unrecognized")
		}
	}
	for _, name := range fixedDirs {
		p := filepath.Join(root, name)
		info, err := os.Lstat(p)
		if err != nil {
			if os.IsNotExist(err) {
				if err := mkdirReal(p); err != nil {
					return err
				}
				continue
			}
			return artErr("stat")
		}
		if isSymlink(info) {
			return artErr("symlink")
		}
		if !info.IsDir() {
			return artErr("type")
		}
	}
	return nil
}
