//go:build unix

package mrfparse

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/EnotPoloskun/mrfparser"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

// parserKillWait is the SIGTERM-to-SIGKILL interval. Production uses
// jobs.GracefulStop. Tests set a short duration; it is not an environment knob.
var parserKillWait = jobs.GracefulStop

func runParserCLI(ctx context.Context, cfg mrfparser.Config) error {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := parserExecutable()
	if err != nil {
		return jobs.Failure(jobs.FailureMRFParseExecutionFailed)
	}
	cmd := parserCommand(path, cfg)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := cmd.Start(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return jobs.Failure(jobs.FailureMRFParseExecutionFailed)
	}
	stop := make(chan struct{})
	var signalDone sync.WaitGroup
	signalDone.Add(1)
	go func() {
		defer signalDone.Done()
		signalParserGroup(ctx, cmd.Process.Pid, stop)
	}()
	waitErr := cmd.Wait()
	close(stop)
	signalDone.Wait()
	return mapParserWait(ctx, waitErr)
}

func parserCommand(path string, cfg mrfparser.Config) *exec.Cmd {
	cmd := exec.Command(path, parserCLIArgs(cfg)...)
	cmd.SysProcAttr = sysProcAttr()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd
}

func signalParserGroup(ctx context.Context, pid int, stop <-chan struct{}) {
	select {
	case <-ctx.Done():
		_ = syscall.Kill(-pid, syscall.SIGTERM)
		timer := time.NewTimer(parserKillWait)
		defer timer.Stop()
		select {
		case <-timer.C:
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		case <-stop:
		}
	case <-stop:
	}
}

func mapParserWait(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if err == nil {
		return nil
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return jobs.Failure(jobs.FailureMRFParseExecutionFailed)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok {
		return jobs.Failure(jobs.FailureMRFParseExecutionFailed)
	}
	if status.Signaled() {
		return jobs.Failure(jobs.FailureMRFParseExecutionFailed)
	}
	return mapParserExit(ctx, status.ExitStatus())
}
