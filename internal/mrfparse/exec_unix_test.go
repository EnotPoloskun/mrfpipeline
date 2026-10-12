//go:build unix

package mrfparse

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/EnotPoloskun/mrfparser"
	"github.com/enotpoloskun/mrfpipeline/internal/config"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

func TestMapParserWaitRedactsUnknownWait(t *testing.T) {
	t.Parallel()
	err := mapParserWait(context.Background(), errors.New("https://secret.example/mrf.json"))
	if !jobs.IsFailure(err, jobs.FailureMRFParseExecutionFailed) {
		t.Fatalf("unknown wait: %v", err)
	}
	if strings.Contains(err.Error(), "https://") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("leaked %v", err)
	}
}

func TestMapParserWaitCancellationWins(t *testing.T) {
	t.Parallel()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mapParserWait(canceled, errors.New("exit 4")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait: %v", err)
	}
}

func TestCLIExit0Validates(t *testing.T) {
	useParserHelper(t, "exit0")
	ws := mustWorkspace(t)
	svc := mustServices(t)
	id := int64(21)
	writeDownload(t, ws, id, []byte("x"))
	w := &Worker{Workspace: ws, Services: svc}
	if err := w.parse(context.Background(), parseJob(21, id)); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultParseTrapSIGTERMReturnsCanceled(t *testing.T) {
	useParserHelper(t, "trapterm")
	cfg, ready := helperParseConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- DefaultParse(ctx, cfg) }()
	waitHelperReady(t, ready)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err %v", err)
		}
		if jobs.IsFailure(err, jobs.FailureMRFParseInputInvalid) {
			t.Fatal("mapped input invalid")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

func TestDefaultParseIgnoreSIGTERMThenSIGKILL(t *testing.T) {
	useParserHelper(t, "ignoreterm")
	old := parserKillWait
	parserKillWait = 50 * time.Millisecond
	t.Cleanup(func() { parserKillWait = old })
	cfg, ready := helperParseConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- DefaultParse(ctx, cfg) }()
	pid := waitHelperReady(t, ready)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err %v", err)
		}
		if time.Since(start) < 50*time.Millisecond {
			t.Fatalf("returned before kill wait: %s", time.Since(start))
		}
		proc, err := os.FindProcess(pid)
		if err != nil {
			t.Fatal(err)
		}
		if err := proc.Signal(syscall.Signal(0)); err == nil {
			t.Fatal("helper still running")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

func TestDefaultParseSerializesChildren(t *testing.T) {
	useParserHelper(t, "overlap")
	overlap := t.TempDir()
	t.Setenv("MRFPARSE_HELPER_OVERLAP", overlap)
	cfg1, _ := helperParseConfig(t)
	cfg2, _ := helperParseConfig(t)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); errs <- DefaultParse(context.Background(), cfg1) }()
	go func() { defer wg.Done(); errs <- DefaultParse(context.Background(), cfg2) }()
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(overlap, "overlap")); !os.IsNotExist(err) {
		t.Fatal("children overlapped")
	}
}

func TestDefaultParseUsesResolvedParser(t *testing.T) {
	useParserHelper(t, "exit0")
	t.Setenv(config.EnvMRFParserPath, filepath.Join(t.TempDir(), "missing-mrfparser"))
	t.Setenv("PATH", t.TempDir())
	cfg, _ := helperParseConfig(t)
	if err := DefaultParse(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
}

func useParserHelper(t *testing.T, mode string) {
	t.Helper()
	t.Setenv(helperEnv, mode)
	path, err := LookupParser(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	SetResolvedParser(path)
	t.Cleanup(func() { SetResolvedParser("") })
}

func helperParseConfig(t *testing.T) (mrfparser.Config, string) {
	t.Helper()
	dir := t.TempDir()
	in := filepath.Join(dir, "data")
	if err := os.WriteFile(in, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	svc := filepath.Join(dir, "services.csv")
	if err := os.WriteFile(svc, []byte("billing_code_type,billing_code\nCPT,99213\n"), 0600); err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(dir, "tmp")
	if err := os.Mkdir(tmp, 0700); err != nil {
		t.Fatal(err)
	}
	ready := filepath.Join(dir, "ready")
	t.Setenv("MRFPARSE_HELPER_READY", ready)
	cfg := mrfparser.DefaultConfig()
	cfg.Input = in
	cfg.Output = filepath.Join(dir, "parsed")
	cfg.Services = svc
	cfg.TempDir = tmp
	if err := os.Mkdir(cfg.Output, 0700); err != nil {
		t.Fatal(err)
	}
	return cfg, ready
}

func waitHelperReady(t *testing.T, ready string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(ready)
		if err == nil {
			pid, convErr := strconv.Atoi(strings.TrimSpace(string(raw)))
			if convErr == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("helper was not ready")
	return 0
}
