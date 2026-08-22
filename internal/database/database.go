package database

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrDatabase is the sentinel wrapped by driver, connection, version, and
// schema-state failures. Public errors include only a safe operation name.
var ErrDatabase = errors.New("database operation failed")

// TestDBMu serializes disposable-database integration tests that share one URL.
var TestDBMu sync.Mutex

// Application migration advisory lock. Distinct from the worker lease (7319, 1).
const (
	MigrationLockClass  = 7319
	MigrationLockObject = 2

	ApplicationSchema = "mrfpipeline"
	RiverSchema       = "mrfpipeline_river"

	// ExpectedRiverVersion is the current River main line for the pinned
	// github.com/riverqueue/river v0.39.0 driver. Do not fake this value.
	ExpectedRiverVersion = 6
)

const (
	minServerVersion = 150000
	// One connection holds the pipeline advisory lock; River's migrator uses
	// the remaining pool slots.
	maxMigrateConns = 3
)

func dbErr(op string) error {
	return fmt.Errorf("%w: %s", ErrDatabase, op)
}

func classify(ctx context.Context, op string, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return dbErr(op)
}

func requirePostgres15(versionNum int) error {
	if versionNum < minServerVersion {
		return dbErr("unsupported postgresql version")
	}
	return nil
}
