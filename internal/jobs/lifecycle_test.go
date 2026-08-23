package jobs

import (
	"context"
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
