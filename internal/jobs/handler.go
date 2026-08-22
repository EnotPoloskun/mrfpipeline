package jobs

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

// DomainErrorHandler attempts a terminal domain transition when River is
// discarding an unexpected error or panic. Unknown kinds and undecodable
// arguments do not trigger a broad update.
type DomainErrorHandler struct {
	pool     *pgxpool.Pool
	bindings map[string]KindBinding
	logger   *slog.Logger
}

func NewDomainErrorHandler(pool *pgxpool.Pool, bindings []KindBinding, logger *slog.Logger) *DomainErrorHandler {
	m := make(map[string]KindBinding, len(bindings))
	for _, b := range bindings {
		m[b.Kind] = b
	}
	return &DomainErrorHandler{pool: pool, bindings: m, logger: logger}
}

func (h *DomainErrorHandler) HandleError(ctx context.Context, job *rivertype.JobRow, err error) *river.ErrorHandlerResult {
	if job != nil && job.Attempt >= job.MaxAttempts {
		h.failKnown(ctx, job)
	}
	return nil
}

func (h *DomainErrorHandler) HandlePanic(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *river.ErrorHandlerResult {
	if job != nil && job.Attempt >= job.MaxAttempts {
		h.failKnown(ctx, job)
	}
	return nil
}

func (h *DomainErrorHandler) failKnown(ctx context.Context, job *rivertype.JobRow) {
	if h == nil || h.pool == nil || job == nil {
		return
	}
	binding, ok := h.bindings[job.Kind]
	if !ok {
		return
	}
	id, err := decodePositiveID(job.EncodedArgs, binding.ArgField)
	if err != nil {
		if h.logger != nil {
			h.logger.LogAttrs(ctx, slog.LevelInfo, "job_error",
				slog.String("kind", job.Kind),
				slog.Int64("job_id", job.ID),
				slog.Int("attempt", job.Attempt),
				slog.String("failure", FailureInvalidArguments),
			)
		}
		return
	}
	if ferr := MarkFailed(ctx, h.pool, binding.Spec, id, job.ID, FailureAttemptsExhausted); ferr != nil && h.logger != nil {
		h.logger.LogAttrs(ctx, slog.LevelInfo, "job_error",
			slog.String("kind", job.Kind),
			slog.Int64("job_id", job.ID),
			slog.Int("attempt", job.Attempt),
			slog.String("failure", FailureAttemptsExhausted),
		)
	}
}
