package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/config"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/jackc/pgx/v5/pgxpool"
)

const acceptancePoll = 5 * time.Second

type liveReport struct {
	TOCLimit           int64 `json:"toc_limit"`
	DurationMS         int64 `json:"duration_ms"`
	TOCRows            int64 `json:"toc_row_count"`
	TOCImportSucceeded int64 `json:"toc_import_succeeded"`
	TOCImportFailed    int64 `json:"toc_import_failed"`
	Sources            int64 `json:"mrf_source_count"`
	SnapshotsSucceeded int64 `json:"snapshot_consume_succeeded"`
	BatchesSucceeded   int64 `json:"plan_batch_succeeded"`
	WarehouseFiles     int64 `json:"warehouse_file_count"`
	PeakArtifactBytes  int64 `json:"peak_artifact_bytes"`
	PeakWarehouseBytes int64 `json:"peak_warehouse_bytes"`
	ReconcileRepaired  int64 `json:"reconcile_repaired_job_count"`
	RestartSources     int64 `json:"restart_mrf_source_count"`
	RestartSnapshots   int64 `json:"restart_snapshot_count"`
	RestartWarehouse   int64 `json:"restart_warehouse_file_count"`
}

type activeRelation struct {
	ActiveOutputs []activeOutput `json:"active_outputs"`
}

type activeOutput struct {
	PayerID string `json:"payer_id"`
	Month   string `json:"collection_month"`
	Output  string `json:"output_id"`
}

type workProc struct {
	cmd      *exec.Cmd
	errc     chan error
	exited   atomic.Bool
	stopOnce sync.Once
	stopErr  error
}

func runLiveAcceptance(ctx context.Context, getenv func(string) string, bin string) (liveReport, error) {
	var zero liveReport
	if getenv == nil {
		getenv = os.Getenv
	}
	limit, err := CheckAcceptanceGuards(getenv)
	if err != nil {
		return zero, err
	}
	month := getenv(EnvRealCollectionMonth)
	wait := DefaultAcceptanceWait
	if raw := getenv(EnvRealTimeout); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return zero, AcceptanceGuardError{Reason: "timeout"}
		}
		wait = d
	}
	dbURL := getenv(EnvTestDatabase)
	env := liveEnv(getenv, dbURL)
	start := time.Now()
	if err := runBin(ctx, bin, env, "migrate"); err != nil {
		return zero, err
	}

	workCtx, stopWork := context.WithCancel(ctx)
	defer stopWork()
	proc, err := startWork(workCtx, bin, env)
	if err != nil {
		return zero, err
	}
	defer func() { _ = stopProc(proc, stopWork) }()

	readyWait, cancelReady := context.WithTimeout(ctx, wait)
	defer cancelReady()
	if err := waitRiverClient(readyWait, dbURL, proc); err != nil {
		return zero, err
	}
	if err := runBin(ctx, bin, env, "discover",
		"--payer", "uhc",
		"--collection-month", month,
		"--limit", fmt.Sprintf("%d", limit),
	); err != nil {
		return zero, err
	}

	idleWait, cancelIdle := context.WithTimeout(ctx, wait)
	defer cancelIdle()
	peaks, err := waitIdle(idleWait, dbURL, getenv(config.EnvArtifactRoot), getenv(config.EnvWarehousePath), proc)
	if err != nil {
		return zero, err
	}
	if err := stopProc(proc, stopWork); err != nil {
		return zero, err
	}
	goneWait, cancelGone := context.WithTimeout(ctx, wait)
	defer cancelGone()
	if err := waitNoRiverClient(goneWait, dbURL); err != nil {
		return zero, err
	}
	statusOut, err := runBinOutput(ctx, bin, env, "month", "status", "--payer", "uhc", "--collection-month", month)
	if err != nil {
		return zero, err
	}
	var status release.StatusReport
	if err := json.Unmarshal(bytes.TrimSpace(statusOut), &status); err != nil || !status.DatabaseReady {
		return zero, jobs.Failure(jobs.FailureReleaseNotReady)
	}
	activationOut, err := runBinOutput(ctx, bin, env, "month", "activate", "--payer", "uhc", "--collection-month", month)
	if err != nil {
		return zero, err
	}
	var activation release.ActivationResult
	if err := json.Unmarshal(bytes.TrimSpace(activationOut), &activation); err != nil || activation.OutputCount == 0 {
		return zero, jobs.Failure(jobs.FailureDomainInvariant)
	}
	if err := runBin(ctx, bin, env, "month", "activate", "--payer", "uhc", "--collection-month", month); err != nil {
		return zero, err
	}
	activeOut, err := runBinOutput(ctx, bin, env, "month", "status")
	if err != nil {
		return zero, err
	}
	var active activeRelation
	if err := json.Unmarshal(bytes.TrimSpace(activeOut), &active); err != nil || len(active.ActiveOutputs) == 0 {
		return zero, jobs.Failure(jobs.FailureDomainInvariant)
	}

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return zero, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	defer pool.Close()
	rep, err := snapshotLive(ctx, pool, getenv(config.EnvWarehousePath), limit, start)
	if err != nil {
		return zero, err
	}
	rep.PeakArtifactBytes = peaks[0]
	rep.PeakWarehouseBytes = peaks[1]

	out, err := runBinOutput(ctx, bin, env, "reconcile")
	if err != nil {
		return zero, err
	}
	var rec Report
	if err := json.Unmarshal(bytes.TrimSpace(out), &rec); err != nil {
		return zero, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	if rec.SealedReleaseInconsistencyCount != 0 {
		return zero, jobs.Failure(jobs.FailureSealedReleaseInconsistent)
	}
	rep.ReconcileRepaired = int64(rec.RepairedJobCount)

	work2Ctx, stop2 := context.WithCancel(ctx)
	defer stop2()
	proc2, err := startWork(work2Ctx, bin, env)
	if err != nil {
		return zero, err
	}
	defer func() { _ = stopProc(proc2, stop2) }()
	ready2, cancelReady2 := context.WithTimeout(ctx, wait)
	defer cancelReady2()
	if err := waitRiverClient(ready2, dbURL, proc2); err != nil {
		return zero, err
	}
	idle2, cancelIdle2 := context.WithTimeout(ctx, wait)
	defer cancelIdle2()
	if _, err := waitIdle(idle2, dbURL, getenv(config.EnvArtifactRoot), getenv(config.EnvWarehousePath), proc2); err != nil {
		return zero, err
	}
	if err := stopProc(proc2, stop2); err != nil {
		return zero, err
	}

	out, err = runBinOutput(ctx, bin, env, "reconcile")
	if err != nil {
		return zero, err
	}
	if err := json.Unmarshal(bytes.TrimSpace(out), &rec); err != nil {
		return zero, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	if rec.RepairedJobCount != 0 || rec.UnblockedStageCount != 0 || rec.ScheduledPlanBatchCount != 0 || rec.SealedReleaseInconsistencyCount != 0 {
		return zero, jobs.Failure(jobs.FailureDomainInvariant)
	}
	activeAfterRestart, err := runBinOutput(ctx, bin, env, "month", "status")
	if err != nil {
		return zero, err
	}
	var activeAfter activeRelation
	if err := json.Unmarshal(bytes.TrimSpace(activeAfterRestart), &activeAfter); err != nil || len(activeAfter.ActiveOutputs) == 0 {
		return zero, jobs.Failure(jobs.FailureDomainInvariant)
	}
	if !sameActiveRelation(active, activeAfter) {
		return zero, jobs.Failure(jobs.FailureDomainInvariant)
	}
	foundActiveMonth := false
	for _, output := range activeAfter.ActiveOutputs {
		if output.PayerID == "uhc" && output.Month == month && output.Output != "" {
			foundActiveMonth = true
			break
		}
	}
	if !foundActiveMonth {
		return zero, jobs.Failure(jobs.FailureDomainInvariant)
	}

	after, err := snapshotLive(ctx, pool, getenv(config.EnvWarehousePath), limit, start)
	if err != nil {
		return zero, err
	}
	if after.Sources != rep.Sources || after.SnapshotsSucceeded != rep.SnapshotsSucceeded || after.WarehouseFiles != rep.WarehouseFiles {
		return zero, jobs.Failure(jobs.FailureDomainInvariant)
	}
	rep.RestartSources = after.Sources
	rep.RestartSnapshots = after.SnapshotsSucceeded
	rep.RestartWarehouse = after.WarehouseFiles
	rep.DurationMS = time.Since(start).Milliseconds()
	if path := getenv(EnvRealReport); path != "" {
		raw, err := json.Marshal(rep)
		if err != nil {
			return zero, err
		}
		if err := os.WriteFile(path, append(raw, '\n'), 0600); err != nil {
			return zero, err
		}
	}
	return rep, nil
}

func sameActiveRelation(left, right activeRelation) bool {
	if len(left.ActiveOutputs) != len(right.ActiveOutputs) {
		return false
	}
	for i := range left.ActiveOutputs {
		if left.ActiveOutputs[i] != right.ActiveOutputs[i] {
			return false
		}
	}
	return true
}

func liveEnv(getenv func(string) string, dbURL string) []string {
	out := make([]string, 0, 16)
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "MRFPIPELINE_") {
			continue
		}
		out = append(out, kv)
	}
	out = append(out,
		config.EnvDatabaseURL+"="+dbURL,
		config.EnvArtifactRoot+"="+getenv(config.EnvArtifactRoot),
		config.EnvWarehousePath+"="+getenv(config.EnvWarehousePath),
		config.EnvProviderCatalogPath+"="+getenv(config.EnvProviderCatalogPath),
		config.EnvServicesPath+"="+getenv(config.EnvServicesPath),
	)
	return out
}

func runBin(ctx context.Context, bin string, env []string, args ...string) error {
	_, err := runBinOutput(ctx, bin, env, args...)
	return err
}

func runBinOutput(ctx context.Context, bin string, env []string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = env
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		name := "command"
		if len(args) > 0 {
			name = args[0]
		}
		return nil, fmt.Errorf("acceptance %s failed", name)
	}
	return stdout.Bytes(), nil
}

func startWork(ctx context.Context, bin string, env []string) (*workProc, error) {
	cmd := exec.Command(bin, "work")
	cmd.Env = env
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("acceptance work failed")
	}
	p := &workProc{cmd: cmd, errc: make(chan error, 1)}
	go func() {
		err := cmd.Wait()
		p.exited.Store(true)
		p.errc <- err
	}()
	go func() {
		<-ctx.Done()
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}
	}()
	return p, nil
}

func stopProc(p *workProc, cancel context.CancelFunc) error {
	if p == nil {
		return nil
	}
	p.stopOnce.Do(func() {
		cancel()
		select {
		case err := <-p.errc:
			p.stopErr = err
		case <-time.After(jobs.GracefulStop + 5*time.Second):
			if p.cmd.Process != nil {
				_ = p.cmd.Process.Kill()
			}
			p.stopErr = <-p.errc
			if p.stopErr == nil {
				p.stopErr = fmt.Errorf("acceptance work stop timed out")
			}
		}
	})
	return p.stopErr
}

func waitRiverClient(ctx context.Context, dbURL string, proc *workProc) error {
	return waitRiverCount(ctx, dbURL, proc, func(n int64) bool { return n > 0 }, true)
}

func waitNoRiverClient(ctx context.Context, dbURL string) error {
	return waitRiverCount(ctx, dbURL, nil, func(n int64) bool { return n == 0 }, false)
}

func waitRiverCount(ctx context.Context, dbURL string, proc *workProc, ready func(int64) bool, failIfExited bool) error {
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	defer pool.Close()
	ticker := time.NewTicker(acceptancePoll)
	defer ticker.Stop()
	for {
		var n int64
		qerr := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline_river.river_client`).Scan(&n)
		if qerr == nil && ready(n) {
			return nil
		}
		if failIfExited && proc != nil && proc.exited.Load() {
			return fmt.Errorf("acceptance work exited before ready")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitIdle(ctx context.Context, dbURL, artifactRoot, warehouse string, proc *workProc) ([2]int64, error) {
	var peaks [2]int64
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return peaks, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	defer pool.Close()
	ticker := time.NewTicker(acceptancePoll)
	defer ticker.Stop()
	idleStreak := 0
	for {
		samplePeaks(&peaks, artifactRoot, warehouse)
		idle, err := domainIdle(ctx, pool)
		if err != nil {
			return peaks, err
		}
		if idle {
			idleStreak++
			if idleStreak >= 3 {
				return peaks, nil
			}
		} else {
			idleStreak = 0
		}
		if proc.exited.Load() {
			return peaks, fmt.Errorf("acceptance work exited while waiting")
		}
		select {
		case <-ctx.Done():
			return peaks, ctx.Err()
		case <-ticker.C:
		}
	}
}

func domainIdle(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	var n int64
	err := pool.QueryRow(ctx, `
SELECT
    (SELECT count(*) FROM mrfpipeline.discovery_runs WHERE status IN ('pending', 'running'))
  + (SELECT count(*) FROM mrfpipeline.toc_files
        WHERE download_status IN ('pending', 'running')
           OR parse_status IN ('pending', 'running')
           OR import_status IN ('pending', 'running'))
  + (SELECT count(*) FROM mrfpipeline.mrf_sources
        WHERE download_status IN ('pending', 'running')
           OR parse_status IN ('pending', 'running'))
  + (SELECT count(*) FROM mrfpipeline.mrf_snapshots WHERE consume_status IN ('pending', 'running'))
  + (SELECT count(*) FROM mrfpipeline.plan_attachment_batches WHERE status IN ('pending', 'running'))`).Scan(&n)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	return n == 0, nil
}

func snapshotLive(ctx context.Context, pool *pgxpool.Pool, warehouse string, limit int64, start time.Time) (liveReport, error) {
	var r liveReport
	r.TOCLimit = limit
	r.DurationMS = time.Since(start).Milliseconds()
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.toc_files`).Scan(&r.TOCRows); err != nil {
		return r, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.toc_files WHERE import_status = 'succeeded'`).Scan(&r.TOCImportSucceeded); err != nil {
		return r, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.toc_files WHERE import_status = 'failed'`).Scan(&r.TOCImportFailed); err != nil {
		return r, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	var unfinished int64
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM mrfpipeline.toc_files
WHERE import_status NOT IN ('succeeded', 'failed')
  AND download_status <> 'failed'
  AND parse_status <> 'failed'`).Scan(&unfinished); err != nil {
		return r, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	if r.TOCRows == 0 || unfinished != 0 {
		return r, jobs.Failure(jobs.FailureDomainInvariant)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.mrf_sources`).Scan(&r.Sources); err != nil {
		return r, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.mrf_snapshots WHERE consume_status = 'succeeded'`).Scan(&r.SnapshotsSucceeded); err != nil {
		return r, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.plan_attachment_batches WHERE status = 'succeeded'`).Scan(&r.BatchesSucceeded); err != nil {
		return r, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	n, err := countWarehouseFiles(warehouse)
	if err != nil {
		return r, err
	}
	r.WarehouseFiles = n
	return r, nil
}

func countWarehouseFiles(root string) (int64, error) {
	var n int64
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.IsDir() && info.Name() == ".staging" {
			return filepath.SkipDir
		}
		if !info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			n++
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return 0, jobs.Failure(jobs.FailureArtifactReconciliationFailed)
	}
	return n, nil
}

func samplePeaks(peaks *[2]int64, artifactRoot, warehouse string) {
	if n := dirBytes(artifactRoot); n > peaks[0] {
		peaks[0] = n
	}
	if n := dirBytes(warehouse); n > peaks[1] {
		peaks[1] = n
	}
}

func dirBytes(root string) int64 {
	var n int64
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		n += info.Size()
		return nil
	})
	return n
}
