package database

import (
	"context"
	"errors"
	"fmt"
)

// ErrDatabase is the sentinel wrapped by driver, connection, version, and
// schema-state failures. Public errors include only a safe operation name.
var ErrDatabase = errors.New("database operation failed")

// Application migration advisory lock. Distinct from the worker lease (7319, 1).
const (
	MigrationLockClass  = 7319
	MigrationLockObject = 2
)

const (
	minServerVersion = 150000
	maxMigrateConns  = 2
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
