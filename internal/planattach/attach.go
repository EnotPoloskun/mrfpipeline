package planattach

import (
	"context"

	"github.com/enotpoloskun/mrfconsumer"
)

// AttachFunc matches mrfconsumer.AttachPlans.
type AttachFunc func(ctx context.Context, cfg mrfconsumer.AttachPlansConfig) (mrfconsumer.AttachPlansReport, error)

// DefaultAttach calls the pinned public consumer.
func DefaultAttach(ctx context.Context, cfg mrfconsumer.AttachPlansConfig) (mrfconsumer.AttachPlansReport, error) {
	return mrfconsumer.AttachPlans(ctx, cfg)
}

func resolveAttach(fn AttachFunc) AttachFunc {
	if fn != nil {
		return fn
	}
	return DefaultAttach
}
