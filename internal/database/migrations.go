package database

import (
	"embed"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"
	"unicode/utf8"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

var migrationName = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

type migrationFile struct {
	Version int
	Name    string
	SQL     string
}

func loadEmbeddedMigrations() ([]migrationFile, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, dbErr("load migrations")
	}
	files := make([]migrationFile, 0, len(entries))
	seen := make(map[int]string, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		file, err := parseMigrationFile(entry.Name())
		if err != nil {
			return nil, err
		}
		if _, ok := seen[file.Version]; ok {
			return nil, dbErr("load migrations")
		}
		body, err := fs.ReadFile(migrationFS, path.Join("migrations", entry.Name()))
		if err != nil {
			return nil, dbErr("load migrations")
		}
		if !utf8.Valid(body) {
			return nil, dbErr("load migrations")
		}
		file.SQL = string(body)
		seen[file.Version] = file.Name
		files = append(files, file)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Version < files[j].Version })
	if err := validateMigrationSet(files); err != nil {
		return nil, err
	}
	return files, nil
}

func parseMigrationFile(name string) (migrationFile, error) {
	m := migrationName.FindStringSubmatch(name)
	if m == nil {
		return migrationFile{}, dbErr("load migrations")
	}
	version, err := strconv.Atoi(m[1])
	if err != nil || version < 1 {
		return migrationFile{}, dbErr("load migrations")
	}
	return migrationFile{Version: version, Name: name}, nil
}

func validateMigrationSet(files []migrationFile) error {
	if len(files) == 0 {
		return dbErr("load migrations")
	}
	for i, file := range files {
		if file.Version != i+1 {
			return dbErr("load migrations")
		}
	}
	return nil
}
