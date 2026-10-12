package mrfparse

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

func TestMapParserExitCodes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	if err := mapParserExit(ctx, 0); err != nil {
		t.Fatalf("exit 0: %v", err)
	}
	if err := mapParserExit(ctx, 4); !jobs.IsFailure(err, jobs.FailureMRFParseInputInvalid) {
		t.Fatalf("exit 4: %v", err)
	}
	if err := mapParserExit(ctx, 4); err != nil && (strings.Contains(err.Error(), "/") || strings.Contains(err.Error(), "http")) {
		t.Fatalf("exit 4 leaked %v", err)
	}
	for _, code := range []int{1, 2, 3, 5, 6, 7, 99} {
		if err := mapParserExit(ctx, code); !jobs.IsFailure(err, jobs.FailureMRFParseExecutionFailed) {
			t.Fatalf("exit %d: %v", code, err)
		}
	}
}

func TestMapParserExitCancellationWins(t *testing.T) {
	t.Parallel()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mapParserExit(canceled, 4); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled exit 4: %v", err)
	}
	if err := mapParserExit(canceled, 7); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled exit 7: %v", err)
	}
	deadline, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	if err := mapParserExit(deadline, 7); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline exit 7: %v", err)
	}
}

func TestMapParserExitLiveContextExit7(t *testing.T) {
	t.Parallel()
	if err := mapParserExit(context.Background(), 7); !jobs.IsFailure(err, jobs.FailureMRFParseExecutionFailed) {
		t.Fatalf("live exit 7: %v", err)
	}
}
