package jobs

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func TestRunForcedRescueSerializesWork(t *testing.T) {
	t.Parallel()
	var overlapping atomic.Bool
	var inWork atomic.Int32
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	work := func(context.Context) error {
		if inWork.Add(1) != 1 {
			overlapping.Store(true)
		}
		defer inWork.Add(-1)
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return Failure(FailureWorkerLeaseLost)
	}
	params := func() RunParams {
		return RunParams{
			Spec:       TOCDownloadStage,
			DomainID:   42,
			RiverJobID: 7,
			Claim: func(context.Context) (ClaimResult, error) {
				return ClaimResult{Action: ClaimWork}, nil
			},
			Work: work,
		}
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := Run(context.Background(), params()); err != nil {
			t.Errorf("first: %v", err)
		}
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first work did not start")
	}
	go func() {
		defer wg.Done()
		if err := Run(context.Background(), params()); err != nil {
			t.Errorf("rescued: %v", err)
		}
	}()
	time.Sleep(50 * time.Millisecond)
	if overlapping.Load() {
		close(release)
		wg.Wait()
		t.Fatal("rescued delivery overlapped in-process work")
	}
	close(release)
	wg.Wait()
	if overlapping.Load() {
		t.Fatal("rescued delivery overlapped in-process work")
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
