package artifact

import (
	"io"
	"os"
	"path/filepath"
	"strings"
)

func isSymlink(info os.FileInfo) bool {
	return info.Mode()&os.ModeSymlink != 0
}

func requireRegular(info os.FileInfo) error {
	if isSymlink(info) || !info.Mode().IsRegular() {
		return artErr("type")
	}
	return nil
}

func requireRealDir(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return classifyArt(nil, "stat", err)
	}
	if isSymlink(info) {
		return artErr("symlink")
	}
	if !info.IsDir() {
		return artErr("type")
	}
	return nil
}

func mkdirReal(path string) error {
	if err := os.Mkdir(path, dirMode); err != nil && !os.IsExist(err) {
		return artErr("create")
	}
	return requireRealDir(path)
}

func writeExclusive(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fileMode)
	if err != nil {
		return artErr("create")
	}
	_, werr := f.Write(data)
	cerr := f.Close()
	if werr != nil || cerr != nil {
		_ = os.Remove(path)
		return artErr("write")
	}
	return nil
}

func underRoot(root, target string) error {
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return artErr("path")
	}
	return nil
}

func (w *Workspace) verifyChain(target string, lastMustExist bool) error {
	if w == nil {
		return artErr("workspace")
	}
	if err := underRoot(w.Root, target); err != nil {
		return err
	}
	if err := requireRealDir(w.Root); err != nil {
		return err
	}
	rel, err := filepath.Rel(w.Root, target)
	if err != nil {
		return artErr("path")
	}
	if rel == "." {
		return nil
	}
	cur := w.Root
	parts := strings.Split(rel, string(filepath.Separator))
	for i, part := range parts {
		if part == "" || part == "." {
			continue
		}
		cur = filepath.Join(cur, part)
		info, err := os.Lstat(cur)
		if err != nil {
			if os.IsNotExist(err) && !lastMustExist {
				return nil
			}
			return classifyArt(nil, "stat", err)
		}
		if isSymlink(info) {
			return artErr("symlink")
		}
		if i < len(parts)-1 && !info.IsDir() {
			return artErr("type")
		}
	}
	return nil
}

func removeExactDir(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return artErr("stat")
	}
	if isSymlink(info) {
		return artErr("symlink")
	}
	if !info.IsDir() {
		return artErr("type")
	}
	again, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return artErr("stat")
	}
	if isSymlink(again) || !again.IsDir() {
		return artErr("type")
	}
	if err := os.RemoveAll(path); err != nil {
		return artErr("remove")
	}
	return nil
}

func readFileLimited(path string, max int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, artErr("read")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, artErr("stat")
	}
	if !info.Mode().IsRegular() {
		return nil, artErr("type")
	}
	if info.Size() > int64(max) {
		return nil, artErr("manifest")
	}
	data, err := io.ReadAll(io.LimitReader(f, int64(max)+1))
	if err != nil {
		return nil, artErr("read")
	}
	if len(data) > max {
		return nil, artErr("manifest")
	}
	return data, nil
}
