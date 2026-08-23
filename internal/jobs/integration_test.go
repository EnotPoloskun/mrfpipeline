package jobs

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

type testArgs struct {
	StageID int64 `json:"stage_id"`
}

func (testArgs) Kind() string { return "test.stage" }
func (testArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: "test"}
}
func (a *testArgs) UnmarshalJSON(data []byte) error {
	return unmarshalOnePositiveInt64(data, "stage_id", &a.StageID)
}

type successorArgs struct {
	StageID int64 `json:"stage_id"`
}

func (successorArgs) Kind() string { return "test.successor" }
func (successorArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: "test"}
}
func (a *successorArgs) UnmarshalJSON(data []byte) error {
	return unmarshalOnePositiveInt64(data, "stage_id", &a.StageID)
}

func testStageSpec() StageSpec {
	return StageSpec{
		Table:             "mrfpipeline_test.stages",
		IDColumn:          "id",
		StatusColumn:      "status",
		JobIDColumn:       "river_job_id",
		StartedAtColumn:   "started_at",
		CompletedAtColumn: "completed_at",
		FailureCodeColumn: "failure_code",
		UpdatedAtColumn:   "updated_at",
	}
}

func successorSpec() StageSpec {
	s := testStageSpec()
	s.Table = "mrfpipeline_test.successors"
	return s
}

func testDB(t *testing.T) (string, *pgxpool.Pool) {
	t.Helper()
	raw := os.Getenv("MRFPIPELINE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("MRFPIPELINE_TEST_DATABASE_URL is not set")
	}
	cfg, err := pgxpool.ParseConfig(raw)
	if err != nil {
		t.Fatal("invalid test database url")
	}
	if !strings.HasPrefix(cfg.ConnConfig.Database, "mrfpipeline_test_") {
		t.Fatal("test database name must start with mrfpipeline_test_")
	}
	database.TestDBMu.Lock()
	pool, err := pgxpool.New(context.Background(), raw)
	if err != nil {
		database.TestDBMu.Unlock()
		t.Fatal("connect test database")
	}
	if err := resetPipelineSchemas(context.Background(), pool); err != nil {
		pool.Close()
		database.TestDBMu.Unlock()
		t.Fatal("reset schema")
	}
	t.Cleanup(func() {
		_ = resetPipelineSchemas(context.Background(), pool)
		pool.Close()
		database.TestDBMu.Unlock()
	})
	return raw, pool
}

func resetPipelineSchemas(ctx context.Context, pool *pgxpool.Pool) error {
	for _, schema := range []string{database.RiverSchema, database.ApplicationSchema, "mrfpipeline_test"} {
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			return err
		}
	}
	return nil
}

func migrateAndFixtures(t *testing.T, url string, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := database.Migrate(context.Background(), url); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_, err := pool.Exec(context.Background(), `
CREATE SCHEMA mrfpipeline_test;
CREATE TABLE mrfpipeline_test.stages (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    status text NOT NULL DEFAULT 'pending',
    river_job_id bigint,
    failure_code text,
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);
CREATE TABLE mrfpipeline_test.successors (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    status text NOT NULL DEFAULT 'blocked',
    river_job_id bigint,
    failure_code text,
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);`)
	if err != nil {
		t.Fatal(err)
	}
}

func insertStage(t *testing.T, pool *pgxpool.Pool, status string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(context.Background(), `INSERT INTO mrfpipeline_test.stages (status) VALUES ($1) RETURNING id`, status).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func insertSuccessor(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(context.Background(), `INSERT INTO mrfpipeline_test.successors DEFAULT VALUES RETURNING id`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func testLogger() *slog.Logger { return NewLogger(io.Discard) }

func TestIntegrationInsertTxVisibility(t *testing.T) {
	url, pool := testDB(t)
	migrateAndFixtures(t, url, pool)
	client, err := NewInsertClient(context.Background(), pool, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := InsertTx(context.Background(), client, tx, &testArgs{StageID: 1})
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE id = $1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("visible before commit")
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE id = $1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("present after rollback")
	}

	tx, err = pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err = InsertTx(context.Background(), client, tx, &testArgs{StageID: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job WHERE id = $1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("missing after commit")
	}
}

func TestIntegrationScheduleAtomicWithDomain(t *testing.T) {
	url, pool := testDB(t)
	migrateAndFixtures(t, url, pool)
	client, err := NewInsertClient(context.Background(), pool, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	stageID := insertStage(t, pool, StatusPending)
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	res, err := Schedule(context.Background(), tx, client, testStageSpec(), stageID, &testArgs{StageID: stageID})
	if err != nil || res.Outcome != ScheduleInserted {
		t.Fatalf("%+v %v", res, err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	var status string
	var jobID *int64
	if err := pool.QueryRow(context.Background(), `SELECT status, river_job_id FROM mrfpipeline_test.stages WHERE id = $1`, stageID).Scan(&status, &jobID); err != nil {
		t.Fatal(err)
	}
	if status != StatusPending || jobID != nil {
		t.Fatalf("domain changed after rollback: %s %v", status, jobID)
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("job survived rollback")
	}

	tx, err = pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	res, err = Schedule(context.Background(), tx, client, testStageSpec(), stageID, &testArgs{StageID: stageID})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT status, river_job_id FROM mrfpipeline_test.stages WHERE id = $1`, stageID).Scan(&status, &jobID); err != nil {
		t.Fatal(err)
	}
	if status != StatusPending || jobID == nil || *jobID != res.JobID {
		t.Fatalf("after commit %s %v want %d", status, jobID, res.JobID)
	}
}

func TestIntegrationConcurrentSchedule(t *testing.T) {
	url, pool := testDB(t)
	migrateAndFixtures(t, url, pool)
	client, err := NewInsertClient(context.Background(), pool, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	stageID := insertStage(t, pool, StatusPending)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	results := make([]ScheduleResult, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			defer wg.Done()
			tx, err := pool.Begin(context.Background())
			if err != nil {
				errs[i] = err
				return
			}
			results[i], errs[i] = Schedule(context.Background(), tx, client, testStageSpec(), stageID, &testArgs{StageID: stageID})
			if errs[i] != nil {
				_ = tx.Rollback(context.Background())
				return
			}
			errs[i] = tx.Commit(context.Background())
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("sched %d: %v", i, err)
		}
	}
	var stored int64
	if err := pool.QueryRow(context.Background(), `SELECT river_job_id FROM mrfpipeline_test.stages WHERE id = $1`, stageID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != results[0].JobID && stored != results[1].JobID {
		t.Fatalf("stored %d results %+v", stored, results)
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM mrfpipeline_river.river_job`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("jobs %d", n)
	}
}

type immediateRetry struct{}

func (immediateRetry) NextRetry(job *rivertype.JobRow) time.Time { return time.Now() }

type testWorker struct {
	river.WorkerDefaults[testArgs]
	pool   *pgxpool.Pool
	spec   StageSpec
	work   func(context.Context, int64) error
	succID int64
}

func (w *testWorker) Work(ctx context.Context, job *river.Job[testArgs]) error {
	var succ *Successor
	if w.succID > 0 {
		succ = &Successor{Spec: successorSpec(), DomainID: w.succID, Args: &successorArgs{StageID: w.succID}}
	}
	return Run(ctx, RunParams{
		Pool:        w.pool,
		Client:      river.ClientFromContext[pgx.Tx](ctx),
		Spec:        w.spec,
		DomainID:    job.Args.StageID,
		RiverJobID:  job.ID,
		Attempt:     job.Attempt,
		MaxAttempts: job.MaxAttempts,
		Work:        func(ctx context.Context) error { return w.work(ctx, job.Args.StageID) },
		Successor:   succ,
	})
}

type successorWorker struct {
	river.WorkerDefaults[successorArgs]
	pool *pgxpool.Pool
}

func (w *successorWorker) Work(ctx context.Context, job *river.Job[successorArgs]) error {
	return Run(ctx, RunParams{
		Pool:        w.pool,
		Client:      river.ClientFromContext[pgx.Tx](ctx),
		Spec:        successorSpec(),
		DomainID:    job.Args.StageID,
		RiverJobID:  job.ID,
		Attempt:     job.Attempt,
		MaxAttempts: job.MaxAttempts,
		Work:        func(context.Context) error { return nil },
	})
}

func startTestRuntime(t *testing.T, pool *pgxpool.Pool, workers *river.Workers, handler river.ErrorHandler, maxAttempts int) *river.Client[pgx.Tx] {
	t.Helper()
	if err := database.ValidateCurrent(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	cfg := ClientConfig(workers, map[string]river.QueueConfig{"test": {MaxWorkers: 1}}, handler, testLogger())
	cfg.MaxAttempts = maxAttempts
	cfg.RetryPolicy = immediateRetry{}
	cfg.FetchPollInterval = 50 * time.Millisecond
	client, err := river.NewClient(riverpgxv5.New(pool), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Shutdown(context.Background(), client) })
	return client
}

func waitStatus(t *testing.T, pool *pgxpool.Pool, table string, id int64, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		err := pool.QueryRow(context.Background(), `SELECT status FROM `+table+` WHERE id = $1`, id).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s id=%d status %s", table, id, want)
}

func TestIntegrationWorkerSuccessAndPrune(t *testing.T) {
	url, pool := testDB(t)
	migrateAndFixtures(t, url, pool)
	stageID := insertStage(t, pool, StatusPending)
	succID := insertSuccessor(t, pool)
	workers := river.NewWorkers()
	river.AddWorker(workers, &testWorker{pool: pool, spec: testStageSpec(), succID: succID, work: func(context.Context, int64) error { return nil }})
	river.AddWorker(workers, &successorWorker{pool: pool})
	client := startTestRuntime(t, pool, workers, NewDomainErrorHandler(pool, []KindBinding{{
		Kind: "test.stage", Spec: testStageSpec(), ArgField: "stage_id",
	}}, testLogger()), 3)

	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	res, err := Schedule(context.Background(), tx, client, testStageSpec(), stageID, &testArgs{StageID: stageID})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, pool, "mrfpipeline_test.stages", stageID, StatusSucceeded)
	waitStatus(t, pool, "mrfpipeline_test.successors", succID, StatusSucceeded)

	if _, err := pool.Exec(context.Background(), `DELETE FROM mrfpipeline_river.river_job WHERE id = $1`, res.JobID); err != nil {
		t.Fatal(err)
	}
	var status string
	var jobID int64
	if err := pool.QueryRow(context.Background(), `SELECT status, river_job_id FROM mrfpipeline_test.stages WHERE id = $1`, stageID).Scan(&status, &jobID); err != nil {
		t.Fatal(err)
	}
	if status != StatusSucceeded || jobID != res.JobID {
		t.Fatalf("prune changed domain %s %d", status, jobID)
	}
}

func TestIntegrationWorkerRetriesAndPanic(t *testing.T) {
	url, pool := testDB(t)
	migrateAndFixtures(t, url, pool)
	failID := insertStage(t, pool, StatusPending)
	panicID := insertStage(t, pool, StatusPending)
	workers := river.NewWorkers()
	river.AddWorker(workers, &testWorker{pool: pool, spec: testStageSpec(), work: func(_ context.Context, id int64) error {
		if id == panicID {
			panic("unexpected")
		}
		return jobErr("work")
	}})
	handler := NewDomainErrorHandler(pool, []KindBinding{{
		Kind: "test.stage", Spec: testStageSpec(), ArgField: "stage_id",
	}}, testLogger())
	client := startTestRuntime(t, pool, workers, handler, 2)

	for _, id := range []int64{failID, panicID} {
		tx, err := pool.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Schedule(context.Background(), tx, client, testStageSpec(), id, &testArgs{StageID: id}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	waitStatus(t, pool, "mrfpipeline_test.stages", failID, StatusFailed)
	waitStatus(t, pool, "mrfpipeline_test.stages", panicID, StatusFailed)
	var code string
	if err := pool.QueryRow(context.Background(), `SELECT failure_code FROM mrfpipeline_test.stages WHERE id = $1`, failID).Scan(&code); err != nil {
		t.Fatal(err)
	}
	if code != FailureAttemptsExhausted {
		t.Fatalf("code %s", code)
	}
}

func TestIntegrationShutdown(t *testing.T) {
	url, pool := testDB(t)
	migrateAndFixtures(t, url, pool)
	coop := insertStage(t, pool, StatusPending)
	block := insertStage(t, pool, StatusPending)
	started := make(chan struct{}, 2)
	workers := river.NewWorkers()
	river.AddWorker(workers, &testWorker{pool: pool, spec: testStageSpec(), work: func(ctx context.Context, id int64) error {
		if id == coop {
			return nil
		}
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}})
	client := startTestRuntime(t, pool, workers, nil, 8)

	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Schedule(context.Background(), tx, client, testStageSpec(), coop, &testArgs{StageID: coop}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, pool, "mrfpipeline_test.stages", coop, StatusSucceeded)

	tx, err = pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Schedule(context.Background(), tx, client, testStageSpec(), block, &testArgs{StageID: block}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked job did not start")
	}
	soft := time.Now()
	if err := Shutdown(context.Background(), client); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if time.Since(soft) < GracefulStop {
		t.Fatalf("expected grace period, lasted %s", time.Since(soft))
	}
}

func TestIntegrationRuntimeRejectsStaleSchema(t *testing.T) {
	url, pool := testDB(t)
	if _, err := NewInsertClient(context.Background(), pool, testLogger()); err == nil || !errors.Is(err, database.ErrDatabase) {
		t.Fatalf("insert want ErrDatabase, got %v", err)
	} else if !strings.Contains(err.Error(), "mrfpipeline migrate") {
		t.Fatalf("insert missing migrate instruction: %v", err)
	} else if strings.Contains(err.Error(), url) {
		t.Fatalf("insert leaked url: %v", err)
	}
	if _, err := NewRuntime(context.Background(), pool, river.NewWorkers(), nil, nil, testLogger()); err == nil || !errors.Is(err, database.ErrDatabase) {
		t.Fatalf("want ErrDatabase, got %v", err)
	} else if !strings.Contains(err.Error(), "mrfpipeline migrate") {
		t.Fatalf("missing migrate instruction: %v", err)
	} else if strings.Contains(err.Error(), url) {
		t.Fatalf("leaked url: %v", err)
	}
	if _, err := database.Migrate(context.Background(), url); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `DELETE FROM mrfpipeline.schema_migrations`); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRuntime(context.Background(), pool, river.NewWorkers(), map[string]river.QueueConfig{"test": {MaxWorkers: 1}}, nil, testLogger()); err == nil || !errors.Is(err, database.ErrDatabase) {
		t.Fatalf("stale app: %v", err)
	}
	resetSchema := func() {
		if err := resetPipelineSchemas(context.Background(), pool); err != nil {
			t.Fatal(err)
		}
		if _, err := database.Migrate(context.Background(), url); err != nil {
			t.Fatal(err)
		}
	}
	resetSchema()
	if _, err := pool.Exec(context.Background(), `DELETE FROM mrfpipeline_river.river_migration WHERE version = $1`, database.ExpectedRiverVersion); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRuntime(context.Background(), pool, river.NewWorkers(), map[string]river.QueueConfig{"test": {MaxWorkers: 1}}, nil, testLogger()); err == nil || !errors.Is(err, database.ErrDatabase) {
		t.Fatalf("stale river: %v", err)
	} else if !strings.Contains(err.Error(), "mrfpipeline migrate") {
		t.Fatalf("missing migrate instruction: %v", err)
	} else if strings.Contains(err.Error(), url) {
		t.Fatalf("leaked url: %v", err)
	}
}

func TestIntegrationClaimNoops(t *testing.T) {
	url, pool := testDB(t)
	migrateAndFixtures(t, url, pool)
	blocked := insertStage(t, pool, StatusBlocked)
	if _, err := Claim(context.Background(), pool, testStageSpec(), blocked, 99); !isFailure(err, FailureDomainInvariant) {
		t.Fatalf("blocked: %v", err)
	}
	done := insertStage(t, pool, StatusSucceeded)
	if _, err := pool.Exec(context.Background(), `UPDATE mrfpipeline_test.stages SET river_job_id = 5 WHERE id = $1`, done); err != nil {
		t.Fatal(err)
	}
	res, err := Claim(context.Background(), pool, testStageSpec(), done, 5)
	if err != nil || res.Action != ClaimNoop {
		t.Fatalf("succeeded: %+v %v", res, err)
	}
	stale := insertStage(t, pool, StatusPending)
	if _, err := pool.Exec(context.Background(), `UPDATE mrfpipeline_test.stages SET river_job_id = 8 WHERE id = $1`, stale); err != nil {
		t.Fatal(err)
	}
	res, err = Claim(context.Background(), pool, testStageSpec(), stale, 9)
	if err != nil || res.Action != ClaimNoop {
		t.Fatalf("stale: %+v %v", res, err)
	}
	failed := insertStage(t, pool, StatusFailed)
	if _, err := pool.Exec(context.Background(), `UPDATE mrfpipeline_test.stages SET river_job_id = 11 WHERE id = $1`, failed); err != nil {
		t.Fatal(err)
	}
	res, err = Claim(context.Background(), pool, testStageSpec(), failed, 11)
	if err != nil || res.Action != ClaimNoop {
		t.Fatalf("failed: %+v %v", res, err)
	}
	missing := insertStage(t, pool, StatusPending)
	res, err = Claim(context.Background(), pool, testStageSpec(), missing, 99)
	if err != nil || res.Action != ClaimNoop {
		t.Fatalf("nil job id: %+v %v", res, err)
	}
	var status string
	var jobID *int64
	if err := pool.QueryRow(context.Background(), `SELECT status, river_job_id FROM mrfpipeline_test.stages WHERE id = $1`, missing).Scan(&status, &jobID); err != nil {
		t.Fatal(err)
	}
	if status != StatusPending || jobID != nil {
		t.Fatalf("nil job id claim mutated row: %s %v", status, jobID)
	}
}

func TestIntegrationRunLeaseLostLeavesRunning(t *testing.T) {
	url, pool := testDB(t)
	migrateAndFixtures(t, url, pool)
	client, err := NewInsertClient(context.Background(), pool, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	stageID := insertStage(t, pool, StatusPending)
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	res, err := Schedule(context.Background(), tx, client, testStageSpec(), stageID, &testArgs{StageID: stageID})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	err = Run(context.Background(), RunParams{
		Pool:        pool,
		Client:      client,
		Spec:        testStageSpec(),
		DomainID:    stageID,
		RiverJobID:  res.JobID,
		Attempt:     8,
		MaxAttempts: 8,
		Work: func(context.Context) error {
			return Failure(FailureWorkerLeaseLost)
		},
	})
	if err != nil {
		t.Fatalf("want nil to River, got %v", err)
	}
	var status string
	var code *string
	var jobID int64
	if err := pool.QueryRow(context.Background(), `
SELECT status, failure_code, river_job_id FROM mrfpipeline_test.stages WHERE id = $1`, stageID).Scan(&status, &code, &jobID); err != nil {
		t.Fatal(err)
	}
	if status != StatusRunning || code != nil || jobID != res.JobID {
		t.Fatalf("domain mutated: %s %v %d", status, code, jobID)
	}
}
