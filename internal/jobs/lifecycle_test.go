package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
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

func TestRunCanceledDownloadDoesNotMutate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		kind string
		spec StageSpec
	}{
		{kind: KindMRFDownload, spec: MRFDownloadStage},
		{kind: KindTOCDownload, spec: TOCDownloadStage},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			var terminal int
			err := Run(ctx, RunParams{
				Kind:       tc.kind,
				Spec:       tc.spec,
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
				Terminal: func(context.Context) error {
					terminal++
					return nil
				},
			})
			if err != nil {
				t.Fatalf("want nil to River, got %v", err)
			}
			if terminal != 0 {
				t.Fatal("plain cancel ran terminal cleanup")
			}
		})
	}
}

func TestRunShutdownCancelDoesNotFailMRFOccupancy(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	var terminal int
	err := Run(ctx, RunParams{
		Kind:       KindMRFParse,
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
		Terminal: func(context.Context) error {
			terminal++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("want nil to River, got %v", err)
	}
	if terminal != 0 {
		t.Fatal("shutdown cancel ran terminal occupancy cleanup")
	}
}

func TestRunRemoteCancelFailsMRFOccupancy(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(context.Background())
	var terminal int
	var failed string
	err := Run(ctx, RunParams{
		Kind:       KindMRFParse,
		Spec:       MRFParseStage,
		DomainID:   1,
		RiverJobID: 2,
		Attempt:    1,
		Claim: func(context.Context) (ClaimResult, error) {
			return ClaimResult{Action: ClaimWork}, nil
		},
		Work: func(context.Context) error {
			cancel(river.ErrJobCancelledRemotely)
			return context.Canceled
		},
		Fail: func(_ context.Context, code string) error {
			failed = code
			return nil
		},
		Terminal: func(context.Context) error {
			terminal++
			return nil
		},
	})
	if err == nil {
		t.Fatal("remote MRF cancel must not look like interruption")
	}
	if failed != FailureRiverTerminalWithoutResult {
		t.Fatalf("failure %q", failed)
	}
	if terminal != 1 {
		t.Fatal("remote cancel skipped terminal occupancy cleanup")
	}
}

func TestRunRemoteCancelRunsTerminal(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(context.Background())
	var terminal int
	var failed string
	err := Run(ctx, RunParams{
		Kind:       KindMRFDownload,
		Spec:       MRFDownloadStage,
		DomainID:   1,
		RiverJobID: 2,
		Attempt:    1,
		Claim: func(context.Context) (ClaimResult, error) {
			return ClaimResult{Action: ClaimWork}, nil
		},
		Work: func(context.Context) error {
			cancel(river.ErrJobCancelledRemotely)
			return context.Canceled
		},
		Fail: func(_ context.Context, code string) error {
			failed = code
			return nil
		},
		Terminal: func(context.Context) error {
			terminal++
			return nil
		},
	})
	if err == nil {
		t.Fatal("remote MRF cancel must not look like interruption")
	}
	if failed != FailureRiverTerminalWithoutResult {
		t.Fatalf("failure %q", failed)
	}
	if terminal != 1 {
		t.Fatal("remote cancel skipped terminal cleanup")
	}
}

func TestRunRemoteCancelFailsTOCDownload(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(context.Background())
	var terminal int
	var failed string
	err := Run(ctx, RunParams{
		Kind:       KindTOCDownload,
		Spec:       TOCDownloadStage,
		DomainID:   1,
		RiverJobID: 2,
		Attempt:    1,
		Claim: func(context.Context) (ClaimResult, error) {
			return ClaimResult{Action: ClaimWork}, nil
		},
		Work: func(context.Context) error {
			cancel(river.ErrJobCancelledRemotely)
			return context.Canceled
		},
		Fail: func(_ context.Context, code string) error {
			failed = code
			return nil
		},
		Terminal: func(context.Context) error {
			terminal++
			return nil
		},
	})
	if err == nil {
		t.Fatal("remote TOC cancel must not look like interruption")
	}
	if failed != FailureRiverTerminalWithoutResult {
		t.Fatalf("failure %q", failed)
	}
	if terminal != 1 {
		t.Fatal("remote cancel skipped terminal cleanup")
	}
}

func TestRunRemoteCancelLeavesTOCParseAsInterruption(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(context.Background())
	var terminal int
	err := Run(ctx, RunParams{
		Kind:       KindTOCParse,
		Spec:       TOCParseStage,
		DomainID:   1,
		RiverJobID: 2,
		Attempt:    1,
		Claim: func(context.Context) (ClaimResult, error) {
			return ClaimResult{Action: ClaimWork}, nil
		},
		Work: func(context.Context) error {
			cancel(river.ErrJobCancelledRemotely)
			return context.Canceled
		},
		Terminal: func(context.Context) error {
			terminal++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("want nil to River, got %v", err)
	}
	if terminal != 0 {
		t.Fatal("TOC parse remote cancel ran terminal cleanup")
	}
}

func TestRunDownloadTerminalRunsOnLastAttempt(t *testing.T) {
	t.Parallel()
	var terminal, failed int
	err := Run(context.Background(), RunParams{
		Kind:        KindTOCDownload,
		Spec:        TOCDownloadStage,
		DomainID:    1,
		RiverJobID:  2,
		Attempt:     4,
		MaxAttempts: 4,
		Claim: func(context.Context) (ClaimResult, error) {
			return ClaimResult{Action: ClaimWork}, nil
		},
		Work: func(context.Context) error {
			return Failure(FailureTOCDownload)
		},
		Fail: func(context.Context, string) error {
			failed++
			return nil
		},
		Terminal: func(context.Context) error {
			terminal++
			return nil
		},
	})
	if err == nil || failed != 1 || terminal != 1 {
		t.Fatalf("err=%v failed=%d terminal=%d", err, failed, terminal)
	}
}

func TestRunDownloadTerminalRunsOnImmediateFailure(t *testing.T) {
	t.Parallel()
	var terminal, failed int
	err := Run(context.Background(), RunParams{
		Kind:        KindMRFDownload,
		Spec:        MRFDownloadStage,
		DomainID:    1,
		RiverJobID:  2,
		Attempt:     1,
		MaxAttempts: 4,
		Claim: func(context.Context) (ClaimResult, error) {
			return ClaimResult{Action: ClaimWork}, nil
		},
		Work: func(context.Context) error {
			return Failure(FailureMRFDownloadNotFound)
		},
		Fail: func(context.Context, string) error {
			failed++
			return nil
		},
		Terminal: func(context.Context) error {
			terminal++
			return nil
		},
	})
	if err == nil || failed != 1 || terminal != 1 {
		t.Fatalf("err=%v failed=%d terminal=%d", err, failed, terminal)
	}
}

func TestRemoteJobCancelCause(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(river.ErrJobCancelledRemotely)
	if !isRemoteJobCancel(ctx) {
		t.Fatal("remote cancel cause")
	}
	plain, stop := context.WithCancel(context.Background())
	stop()
	if isRemoteJobCancel(plain) {
		t.Fatal("plain cancel is not remote job cancel")
	}
	if isResidentSlotKind(KindTOCDownload) || !isResidentSlotKind(KindMRFDownload) || !isResidentSlotKind(KindMRFParse) {
		t.Fatal("resident slot kinds")
	}
	if !isRemoteCancelTerminalKind(KindTOCDownload) || !isRemoteCancelTerminalKind(KindMRFDownload) || !isRemoteCancelTerminalKind(KindMRFParse) || isRemoteCancelTerminalKind(KindTOCParse) {
		t.Fatal("remote cancel terminal kinds")
	}
}

func TestRemoteJobCancelCausePropagatesThroughWithCancel(t *testing.T) {
	t.Parallel()
	parent, cancel := context.WithCancelCause(context.Background())
	child, stop := context.WithCancel(parent)
	defer stop()
	cancel(river.ErrJobCancelledRemotely)
	if !isRemoteJobCancel(child) {
		t.Fatal("execution lock WithCancel must keep remote cancel cause")
	}
}

func TestRunSealedDeliverySuppressesAllProductionKinds(t *testing.T) {
	t.Parallel()
	for _, kind := range ProductionKinds() {
		t.Run(kind, func(t *testing.T) {
			var buf bytes.Buffer
			called := false
			err := Run(context.Background(), RunParams{
				Spec:       DiscoveryRunStage,
				Kind:       kind,
				Queue:      QueueDiscovery,
				Logger:     NewLogger(&buf),
				DomainID:   7,
				RiverJobID: 9,
				Claim: func(context.Context) (ClaimResult, error) {
					return ClaimResult{}, Failure(FailureSealedReleaseInconsistent)
				},
				Work: func(context.Context) error {
					called = true
					return nil
				},
			})
			if err == nil || called || !strings.Contains(err.Error(), FailureSealedReleaseInconsistent) {
				t.Fatalf("err=%v called=%v", err, called)
			}
			var record map[string]any
			if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record); err != nil {
				t.Fatal(err)
			}
			if record["msg"] != "sealed_delivery_suppressed" || record["failure"] != FailureSealedReleaseInconsistent {
				t.Fatalf("record %v", record)
			}
			if fmt.Sprint(record["domain_id"]) != "7" {
				t.Fatalf("domain id %v", record["domain_id"])
			}
		})
	}
}

func TestLifecycleFailureCodeKeepsInfrastructureDistinct(t *testing.T) {
	t.Parallel()
	if got := lifecycleFailureCode(Failure(FailureTOCParseOutputInvalid)); got != FailureTOCParseOutputInvalid {
		t.Fatalf("coded failure %q", got)
	}
	if got := lifecycleFailureCode(fmt.Errorf("database unavailable")); got != FailureJobLifecycle {
		t.Fatalf("infrastructure failure %q", got)
	}
}
