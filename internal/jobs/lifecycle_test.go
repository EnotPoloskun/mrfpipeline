package jobs

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestLockForSucceedRunsPreLockFirst(t *testing.T) {
	t.Parallel()
	var order []string
	_, err := lockForSucceed(context.Background(), nil, StageSpec{}, 1, func(context.Context, pgx.Tx) error {
		order = append(order, "prelock")
		return Failure(FailureMissingRecord)
	})
	if !IsFailure(err, FailureMissingRecord) {
		t.Fatalf("got %v", err)
	}
	if len(order) != 1 || order[0] != "prelock" {
		t.Fatalf("order %v", order)
	}
}

func TestRunLeaseLostDoesNotMutate(t *testing.T) {
	t.Parallel()
	var confirmed int
	err := Run(context.Background(), RunParams{
		DomainID:    1,
		RiverJobID:  2,
		Attempt:     8,
		MaxAttempts: 8,
		Claim: func(context.Context) (ClaimResult, error) {
			return ClaimResult{Action: ClaimWork}, nil
		},
		Work: func(context.Context) error {
			return Failure(FailureWorkerLeaseLost)
		},
		Confirm: func(context.Context, pgx.Tx) error {
			confirmed++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("want nil to River, got %v", err)
	}
	if confirmed != 0 {
		t.Fatal("succeeded after lease loss")
	}
	if strings.Contains(Failure(FailureWorkerLeaseLost).Error(), "7319") {
		t.Fatal("logged lease keys")
	}
}

func TestRunCanceledWorkDoesNotMutate(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	err := Run(ctx, RunParams{
		DomainID:   1,
		RiverJobID: 2,
		Attempt:    1,
		Claim: func(context.Context) (ClaimResult, error) {
			return ClaimResult{Action: ClaimWork}, nil
		},
		Work: func(context.Context) error {
			cancel()
			return context.Canceled
		},
		Confirm: func(context.Context, pgx.Tx) error {
			t.Fatal("confirm")
			return nil
		},
	})
	if err != nil {
		t.Fatalf("want nil to River, got %v", err)
	}
}
