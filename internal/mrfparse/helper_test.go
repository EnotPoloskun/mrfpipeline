//go:build unix

package mrfparse

import (
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

const helperEnv = "MRFPARSE_HELPER"

func TestMain(m *testing.M) {
	if os.Getenv(helperEnv) != "" {
		os.Exit(runParseHelper())
	}
	os.Exit(m.Run())
}

func runParseHelper() int {
	mode := os.Getenv(helperEnv)
	args := os.Args[1:]
	cfg := map[string]string{}
	skipValue := false
	for i, arg := range args {
		if skipValue {
			skipValue = false
			continue
		}
		switch arg {
		case "parse":
			continue
		case "--input", "--output", "--services", "--temp-dir":
			if i+1 >= len(args) {
				return 2
			}
			cfg[arg] = args[i+1]
			skipValue = true
		}
	}
	switch mode {
	case "exit0":
		if err := writeHelperLayout(cfg["--output"], cfg["--input"], cfg["--services"]); err != nil {
			return 1
		}
		return 0
	case "exit4":
		return 4
	case "exit7":
		return 7
	case "trapterm":
		writeHelperReady()
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM)
		<-c
		return 7
	case "ignoreterm":
		signal.Ignore(syscall.SIGTERM)
		writeHelperReady()
		select {}
	case "overlap":
		dir := os.Getenv("MRFPARSE_HELPER_OVERLAP")
		if dir == "" {
			return 1
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return 1
		}
		if len(entries) > 0 {
			_ = os.WriteFile(filepath.Join(dir, "overlap"), []byte("1"), 0600)
		}
		marker := filepath.Join(dir, strconv.Itoa(os.Getpid()))
		if err := os.WriteFile(marker, []byte("1"), 0600); err != nil {
			return 1
		}
		time.Sleep(150 * time.Millisecond)
		_ = os.Remove(marker)
		if err := writeHelperLayout(cfg["--output"], cfg["--input"], cfg["--services"]); err != nil {
			return 1
		}
		return 0
	default:
		return 1
	}
}

func writeHelperReady() {
	if path := os.Getenv("MRFPARSE_HELPER_READY"); path != "" {
		_ = os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0600)
	}
}

func writeHelperLayout(output, input, services string) error {
	return writeValidLayout(output, expectedSourceURI(input), services)
}
