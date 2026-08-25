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

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/config"
	"github.com/enotpoloskun/mrfpipeline/internal/consumeringest"
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
	PeakResidentHeld   int64 `json:"peak_mrf_resident_held"`
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
	bounded := getenv(EnvRealBounded) == "1"
	residentCapacity := acceptanceCapacity(getenv)
	mrfTarget := "all"
	if bounded {
		mrfTarget = getenv(EnvRealMRFSourceLimit)
		if mrfTarget == "" {
			mrfTarget = "1"
		}
	}
	start := time.Now()
	if err := runBin(ctx, bin, env, "migrate"); err != nil {
		return zero, err
	}

	workCtx, stopWork := context.WithCancel(ctx)
	defer stopWork()
	control, err := startWork(workCtx, bin, env, "control", residentCapacity)
	if err != nil {
		return zero, err
	}
	mrf, err := startWork(workCtx, bin, env, "mrf", residentCapacity)
	if err != nil {
		_ = stopProc(control, stopWork)
		return zero, err
	}
	mrfSecond, err := startWork(workCtx, bin, env, "mrf", residentCapacity)
	if err != nil {
		_ = stopProc(mrf, stopWork)
		_ = stopProc(control, stopWork)
		return zero, err
	}
	mrfWorkers := []*workProc{mrf, mrfSecond}
	consumer, err := startWork(workCtx, bin, env, "consumer", residentCapacity)
	if err != nil {
		for _, p := range mrfWorkers {
			_ = stopProc(p, stopWork)
		}
		_ = stopProc(control, stopWork)
		return zero, err
	}
	procs := append([]*workProc{control}, mrfWorkers...)
	procs = append(procs, consumer)
	defer func() {
		for _, p := range procs {
			_ = stopProc(p, stopWork)
		}
	}()

	readyWait, cancelReady := context.WithTimeout(ctx, wait)
	defer cancelReady()
	if err := waitRiverClient(readyWait, dbURL, control); err != nil {
		return zero, err
	}
	if err := runBin(ctx, bin, env, "discover",
		"--payer", "uhc",
		"--collection-month", month,
		"--limit", fmt.Sprintf("%d", limit),
		"--mrf-source-limit", mrfTarget,
	); err != nil {
		return zero, err
	}

	idleWait, cancelIdle := context.WithTimeout(ctx, wait)
	defer cancelIdle()
	peaks, err := waitIdle(idleWait, dbURL, getenv(config.EnvArtifactRoot), getenv(config.EnvWarehousePath), control)
	if err != nil {
		return zero, err
	}
	for _, p := range procs {
		if err := stopProc(p, stopWork); err != nil {
			return zero, err
		}
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
	if err := json.Unmarshal(bytes.TrimSpace(statusOut), &status); err != nil {
		return zero, jobs.Failure(jobs.FailureReleaseNotReady)
	}
	if bounded {
		if !boundedReleaseStatus(status) || status.DatabaseReady || status.MRFResidentHeld != 0 {
			return zero, jobs.Failure(jobs.FailureDomainInvariant)
		}
		if _, err := runBinOutput(ctx, bin, env, "month", "activate", "--payer", "uhc", "--collection-month", month); err == nil {
			return zero, jobs.Failure(jobs.FailureDomainInvariant)
		}
		pool, err := pgxpool.New(ctx, dbURL)
		if err != nil {
			return zero, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
		}
		defer pool.Close()
		var held int64
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM mrfpipeline.mrf_materialization_slots`).Scan(&held); err != nil || held > 0 {
			return zero, jobs.Failure(jobs.FailureDomainInvariant)
		}
		ws, err := artifact.Open(ctx, getenv(config.EnvArtifactRoot))
		if err != nil {
			return zero, jobs.Failure(jobs.FailureArtifactReconciliationFailed)
		}
		rows, err := pool.Query(ctx, `
SELECT a.mrf_source_id
FROM mrfpipeline.monthly_release_mrf_sources a
WHERE a.payer_id = 'uhc' AND a.collection_month = $1
ORDER BY a.mrf_source_id`, month+"-01")
		if err != nil {
			return zero, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
		}
		for rows.Next() {
			var sourceID int64
			if err := rows.Scan(&sourceID); err != nil {
				rows.Close()
				return zero, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
			}
			rawState, rawErr := ws.InspectDownloadState(artifact.KindMRF, sourceID)
			parsedState, parsedErr := ws.InspectParsed(artifact.KindMRF, sourceID)
			if rawErr != nil || parsedErr != nil || rawState != artifact.DownloadAbsent || parsedState != artifact.ParsedManifestPresent {
				rows.Close()
				return zero, jobs.Failure(jobs.FailureDomainInvariant)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return zero, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
		}
		rows.Close()
		if err := validateBoundedConsumerOutputs(ctx, pool, getenv(config.EnvWarehousePath), month, status); err != nil {
			return zero, err
		}
		rep, err := snapshotLive(ctx, pool, getenv(config.EnvWarehousePath), limit, start)
		if err != nil {
			return zero, err
		}
		if rep.SnapshotsSucceeded == 0 || rep.WarehouseFiles == 0 {
			return zero, jobs.Failure(jobs.FailureDomainInvariant)
		}
		rep.PeakArtifactBytes = peaks[0]
		rep.PeakWarehouseBytes = peaks[1]
		rep.PeakResidentHeld = peaks[2]
		return boundedRestartAcceptance(ctx, bin, env, dbURL, pool, getenv, month,
			residentCapacity, wait, limit, status, rep, start)
	}
	if !status.DatabaseReady {
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
	rep.PeakResidentHeld = peaks[2]

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
	control2, err := startWork(work2Ctx, bin, env, "control", residentCapacity)
	if err != nil {
		return zero, err
	}
	mrf2, err := startWork(work2Ctx, bin, env, "mrf", residentCapacity)
	if err != nil {
		_ = stopProc(control2, stop2)
		return zero, err
	}
	mrf3, err := startWork(work2Ctx, bin, env, "mrf", residentCapacity)
	if err != nil {
		_ = stopProc(mrf2, stop2)
		_ = stopProc(control2, stop2)
		return zero, err
	}
	consumer2, err := startWork(work2Ctx, bin, env, "consumer", residentCapacity)
	if err != nil {
		_ = stopProc(mrf3, stop2)
		_ = stopProc(mrf2, stop2)
		_ = stopProc(control2, stop2)
		return zero, err
	}
	procs2 := []*workProc{control2, mrf2, mrf3, consumer2}
	defer func() {
		for _, p := range procs2 {
			_ = stopProc(p, stop2)
		}
	}()
	ready2, cancelReady2 := context.WithTimeout(ctx, wait)
	defer cancelReady2()
	if err := waitRiverClient(ready2, dbURL, control2); err != nil {
		return zero, err
	}
	idle2, cancelIdle2 := context.WithTimeout(ctx, wait)
	defer cancelIdle2()
	if _, err := waitIdle(idle2, dbURL, getenv(config.EnvArtifactRoot), getenv(config.EnvWarehousePath), control2); err != nil {
		return zero, err
	}
	for _, p := range procs2 {
		if err := stopProc(p, stop2); err != nil {
			return zero, err
		}
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
	if err := writeLiveReport(getenv, rep); err != nil {
		return zero, err
	}
	return rep, nil
}

func boundedRestartAcceptance(ctx context.Context, bin string, env []string, dbURL string, pool *pgxpool.Pool,
	getenv func(string) string, month, residentCapacity string, wait time.Duration,
	limit int64, before release.StatusReport, rep liveReport, start time.Time) (liveReport, error) {
	if _, err := runBinOutput(ctx, bin, env, "reconcile"); err != nil {
		return liveReport{}, err
	}
	workCtx, stopWork := context.WithCancel(ctx)
	defer stopWork()
	control, err := startWork(workCtx, bin, env, "control", residentCapacity)
	if err != nil {
		return liveReport{}, err
	}
	mrfA, err := startWork(workCtx, bin, env, "mrf", residentCapacity)
	if err != nil {
		_ = stopProc(control, stopWork)
		return liveReport{}, err
	}
	mrfB, err := startWork(workCtx, bin, env, "mrf", residentCapacity)
	if err != nil {
		_ = stopProc(mrfA, stopWork)
		_ = stopProc(control, stopWork)
		return liveReport{}, err
	}
	consumer, err := startWork(workCtx, bin, env, "consumer", residentCapacity)
	if err != nil {
		_ = stopProc(mrfB, stopWork)
		_ = stopProc(mrfA, stopWork)
		_ = stopProc(control, stopWork)
		return liveReport{}, err
	}
	procs := []*workProc{control, mrfA, mrfB, consumer}
	defer func() {
		for _, p := range procs {
			_ = stopProc(p, stopWork)
		}
	}()
	ready, cancelReady := context.WithTimeout(ctx, wait)
	defer cancelReady()
	if err := waitRiverClient(ready, dbURL, control); err != nil {
		return liveReport{}, err
	}
	idle, cancelIdle := context.WithTimeout(ctx, wait)
	defer cancelIdle()
	if _, err := waitIdle(idle, dbURL, getenv(config.EnvArtifactRoot), getenv(config.EnvWarehousePath), control); err != nil {
		return liveReport{}, err
	}
	for _, p := range procs {
		if err := stopProc(p, stopWork); err != nil {
			return liveReport{}, err
		}
	}
	noRiver, cancelNoRiver := context.WithTimeout(ctx, wait)
	defer cancelNoRiver()
	if err := waitNoRiverClient(noRiver, dbURL); err != nil {
		return liveReport{}, err
	}
	if _, err := runBinOutput(ctx, bin, env, "reconcile"); err != nil {
		return liveReport{}, err
	}
	statusOut, err := runBinOutput(ctx, bin, env, "month", "status", "--payer", "uhc", "--collection-month", month)
	if err != nil {
		return liveReport{}, err
	}
	var afterStatus release.StatusReport
	if err := json.Unmarshal(bytes.TrimSpace(statusOut), &afterStatus); err != nil {
		return liveReport{}, jobs.Failure(jobs.FailureReleaseNotReady)
	}
	if !boundedReleaseStatus(afterStatus) || afterStatus.MRFResidentHeld != 0 || afterStatus.DatabaseReady || len(afterStatus.Blockers) != len(before.Blockers) || afterStatus.Blockers[0] != before.Blockers[0] {
		return liveReport{}, jobs.Failure(jobs.FailureDomainInvariant)
	}
	after, err := snapshotLive(ctx, pool, getenv(config.EnvWarehousePath), limit, start)
	if err != nil {
		return liveReport{}, err
	}
	if after.Sources != rep.Sources || after.SnapshotsSucceeded != rep.SnapshotsSucceeded || after.WarehouseFiles != rep.WarehouseFiles {
		return liveReport{}, jobs.Failure(jobs.FailureDomainInvariant)
	}
	rep.RestartSources = after.Sources
	rep.RestartSnapshots = after.SnapshotsSucceeded
	rep.RestartWarehouse = after.WarehouseFiles
	rep.DurationMS = time.Since(start).Milliseconds()
	if err := writeLiveReport(getenv, rep); err != nil {
		return liveReport{}, err
	}
	return rep, nil
}

func boundedReleaseStatus(status release.StatusReport) bool {
	return status.Partial && !status.DatabaseReady && len(status.Blockers) == 1 &&
		status.Blockers[0] == "mrf_source_target_partial" &&
		status.MRFSourcesSelected > 0 && status.ConsumerFailed == 0 &&
		status.ConsumerPending == 0 && status.ConsumerRunning == 0 &&
		status.ConsumerRetrying == 0 && status.ConsumerSucceeded > 0
}

func validateBoundedConsumerOutputs(ctx context.Context, pool *pgxpool.Pool, warehouse, month string, status release.StatusReport) error {
	if status.ConsumerFailed != 0 || status.ConsumerPending != 0 || status.ConsumerRunning != 0 || status.ConsumerRetrying != 0 {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	rows, err := pool.Query(ctx, `
SELECT s.id, s.payer_id, to_char(s.collection_month, 'YYYY-MM'), s.consume_status
FROM mrfpipeline.mrf_snapshots s
JOIN mrfpipeline.monthly_release_mrf_sources a
  ON a.payer_id = s.payer_id AND a.collection_month = s.collection_month
 AND a.mrf_source_id = s.mrf_source_id
WHERE a.payer_id = 'uhc' AND a.collection_month = $1::date
ORDER BY s.id`, month+"-01")
	if err != nil {
		return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	defer rows.Close()
	var total, succeeded int64
	for rows.Next() {
		var snapshotID int64
		var payer, collectionMonth, consumeStatus string
		if err := rows.Scan(&snapshotID, &payer, &collectionMonth, &consumeStatus); err != nil {
			return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
		}
		total++
		if consumeStatus != "succeeded" {
			return jobs.Failure(jobs.FailureDomainInvariant)
		}
		if err := consumeringest.InspectCompletedSnapshot(warehouse, payer, collectionMonth, consumeringest.FormatSnapshotOutputID(snapshotID)); err != nil {
			return jobs.Failure(jobs.FailureDomainInvariant)
		}
		batchIDs, err := release.ListPlanBatchIDs(ctx, pool, snapshotID)
		if err != nil {
			return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
		}
		if err := consumeringest.InspectPlanAssociations(warehouse, consumeringest.FormatSnapshotOutputID(snapshotID), batchIDs); err != nil {
			return jobs.Failure(jobs.FailureDomainInvariant)
		}
		succeeded++
	}
	if err := rows.Err(); err != nil {
		return jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
	}
	if total == 0 || succeeded != total {
		return jobs.Failure(jobs.FailureDomainInvariant)
	}
	return nil
}

func writeLiveReport(getenv func(string) string, report liveReport) error {
	path := getenv(EnvRealReport)
	if path == "" {
		return nil
	}
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0600)
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

func acceptanceCapacity(getenv func(string) string) string {
	if raw := getenv(EnvRealResidentCapacity); raw != "" {
		return raw
	}
	return "4"
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

func startWork(ctx context.Context, bin string, env []string, role, residentCapacity string) (*workProc, error) {
	cmd := exec.Command(bin, "work", "--role", role)
	cmd.Env = env
	if role == "control" {
		cmd.Env = append(append([]string(nil), env...), config.EnvMRFResidentCapacity+"="+residentCapacity)
	}
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

func waitIdle(ctx context.Context, dbURL, artifactRoot, warehouse string, proc *workProc) ([3]int64, error) {
	var peaks [3]int64
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
		var held, capacity int64
		if err := pool.QueryRow(ctx, `
SELECT (SELECT count(*) FROM mrfpipeline.mrf_materialization_slots),
       COALESCE((SELECT resident_capacity FROM mrfpipeline.pipeline_runtime WHERE id = true), 0)`).Scan(&held, &capacity); err != nil {
			return peaks, jobs.Failure(jobs.FailureReconciliationDatabaseFailed)
		}
		if held > peaks[2] {
			peaks[2] = held
		}
		if capacity <= 0 || held > capacity {
			return peaks, jobs.Failure(jobs.FailureCapacityBelowHeld)
		}
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

func samplePeaks(peaks *[3]int64, artifactRoot, warehouse string) {
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
