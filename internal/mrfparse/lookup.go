package mrfparse

import (
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/enotpoloskun/mrfpipeline/internal/config"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

var (
	resolvedParserMu sync.Mutex
	resolvedParser   string
)

// SetResolvedParser stores the executable path chosen at mrf-role start.
// DefaultParse execs this path instead of searching PATH again. Tests may
// clear it with an empty string.
func SetResolvedParser(path string) {
	resolvedParserMu.Lock()
	defer resolvedParserMu.Unlock()
	resolvedParser = path
}

func parserExecutable() (string, error) {
	resolvedParserMu.Lock()
	path := resolvedParser
	resolvedParserMu.Unlock()
	if path != "" {
		return path, nil
	}
	return LookupParser(os.Getenv(config.EnvMRFParserPath))
}

// LookupParser resolves the mrfparser executable. explicit is an operator
// path (typically MRFPIPELINE_MRFPARSER_PATH); empty means LookPath("mrfparser").
// The returned error never includes the path.
func LookupParser(explicit string) (string, error) {
	var path string
	var err error
	if explicit != "" {
		path, err = filepath.Abs(explicit)
		if err != nil {
			return "", jobs.Failure("runtime")
		}
		path = filepath.Clean(path)
	} else {
		path, err = exec.LookPath("mrfparser")
		if err != nil {
			return "", jobs.Failure("runtime")
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", jobs.Failure("runtime")
	}
	if info.IsDir() || info.Mode()&0111 == 0 {
		return "", jobs.Failure("runtime")
	}
	return path, nil
}
