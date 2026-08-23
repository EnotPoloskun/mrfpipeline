package database

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestWorkerLeaseKeys(t *testing.T) {
	t.Parallel()
	if WorkerLeaseClass != 7319 || WorkerLeaseObject != 1 {
		t.Fatal("worker lease keys must stay frozen")
	}
	if MigrationLockClass != 7319 || MigrationLockObject != 2 {
		t.Fatal("migration lock must stay distinct")
	}
}

func TestAcquireWorkerLeaseContention(t *testing.T) {
	raw := os.Getenv("MRFPIPELINE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("MRFPIPELINE_TEST_DATABASE_URL is not set")
	}
	cfg, err := pgxpool.ParseConfig(raw)
	if err != nil || !strings.HasPrefix(cfg.ConnConfig.Database, "mrfpipeline_test_") {
		t.Fatal("test database name must start with mrfpipeline_test_")
	}
	TestDBMu.Lock()
	defer TestDBMu.Unlock()
	pool, err := pgxpool.New(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	first, err := AcquireWorkerLease(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Release(context.Background()) }()
	if err := first.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := AcquireWorkerLease(context.Background(), pool)
	if err == nil {
		_ = second.Release(context.Background())
		t.Fatal("second lease succeeded")
	}
	if !IsLeaseUnavailable(err) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), "7319") {
		t.Fatal("logged lease keys")
	}
}

func TestReleaseUnlockFailureClosesSession(t *testing.T) {
	raw := os.Getenv("MRFPIPELINE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("MRFPIPELINE_TEST_DATABASE_URL is not set")
	}
	cfg, err := pgxpool.ParseConfig(raw)
	if err != nil || !strings.HasPrefix(cfg.ConnConfig.Database, "mrfpipeline_test_") {
		t.Fatal("test database name must start with mrfpipeline_test_")
	}
	TestDBMu.Lock()
	defer TestDBMu.Unlock()
	pool, err := pgxpool.New(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	lease, err := AcquireWorkerLease(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	err = lease.Release(canceled)
	if err == nil {
		t.Fatal("expected unlock failure")
	}
	if strings.Contains(err.Error(), "7319") {
		t.Fatal("logged lease keys")
	}
	second, err := AcquireWorkerLease(context.Background(), pool)
	if err != nil {
		t.Fatalf("lock still held after hijack close: %v", err)
	}
	if err := second.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}
