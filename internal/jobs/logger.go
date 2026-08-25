package jobs

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"
)

var allowedLogKeys = map[string]bool{
	"kind":         true,
	"role":         true,
	"queue":        true,
	"job_id":       true,
	"attempt":      true,
	"failure":      true,
	"outcome":      true,
	"duration_ms":  true,
	"count":        true,
	"domain_id":    true,
	"phase":        true,
	"percent":      true,
	"copied_bytes": true,
	"total_bytes":  true,
}

var allowedPhases = map[string]bool{
	"download":                 true,
	"toc_destination_prepared": true,
	"toc_input_parsed":         true,
	"toc_parquet_closed":       true,
	"mrf_parse":                true,
	"validating_input":         true,
	"provider_relationships":   true,
	"rate_facts":               true,
	"publishing":               true,
}

type safeHandler struct {
	inner slog.Handler
}

func (h *safeHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *safeHandler) Handle(ctx context.Context, rec slog.Record) error {
	var attrs []slog.Attr
	rec.Attrs(func(a slog.Attr) bool {
		if !allowedLogKeys[a.Key] {
			return true
		}
		if a.Key == "failure" && a.Value.Kind() != slog.KindString {
			return true
		}
		attrs = append(attrs, a)
		return true
	})
	out := slog.NewRecord(rec.Time, rec.Level, rec.Message, rec.PC)
	out.AddAttrs(attrs...)
	return h.inner.Handle(ctx, out)
}

func (h *safeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	var kept []slog.Attr
	for _, a := range attrs {
		if allowedLogKeys[a.Key] {
			kept = append(kept, a)
		}
	}
	return &safeHandler{inner: h.inner.WithAttrs(kept)}
}

func (h *safeHandler) WithGroup(name string) slog.Handler {
	return &safeHandler{inner: h.inner.WithGroup(name)}
}

// NewLogger returns a slog JSON logger on w (typically stderr) that emits
// only allowlisted fields. River library error attributes are dropped.
func NewLogger(w io.Writer) *slog.Logger {
	if w == nil {
		w = io.Discard
	}
	return slog.New(&safeHandler{inner: slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.MessageKey || a.Key == slog.LevelKey || a.Key == slog.TimeKey {
				return a
			}
			if a.Key == "err" || a.Key == "error" || a.Key == slog.SourceKey {
				return slog.Attr{}
			}
			return a
		},
	})})
}

// Progress records throttled liveness for one executing River job. Progress
// is never stored in PostgreSQL, River metadata, or manifests.
type Progress struct {
	logger *slog.Logger
	mu     sync.Mutex
	last   map[int64]time.Time
	begun  map[int64]bool
	phase  map[int64]string
}

func NewProgress(logger *slog.Logger) *Progress {
	if logger == nil {
		logger = NewLogger(io.Discard)
	}
	return &Progress{logger: logger, last: map[int64]time.Time{}, begun: map[int64]bool{}, phase: map[int64]string{}}
}

// ProgressParams is one progress observation.
type ProgressParams struct {
	JobID       int64
	Kind        string
	Queue       string
	Phase       string
	CopiedBytes int64
	TotalBytes  int64
	Percent     *int
	Done        bool
	now         time.Time
}

func (p *Progress) Log(params ProgressParams) error {
	if !allowedPhases[params.Phase] {
		return jobErr("progress")
	}
	if params.JobID <= 0 || params.CopiedBytes < 0 || params.TotalBytes < 0 {
		return jobErr("progress")
	}
	if params.Percent != nil && (*params.Percent < 0 || *params.Percent > 100) {
		return jobErr("progress")
	}
	now := params.now
	if now.IsZero() {
		now = time.Now()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	start := !p.begun[params.JobID]
	if start {
		p.begun[params.JobID] = true
	}
	samePhase := p.phase[params.JobID] == params.Phase
	if !start && !params.Done && samePhase {
		if prev, ok := p.last[params.JobID]; ok && now.Sub(prev) < ProgressEvery {
			return nil
		}
	}
	p.last[params.JobID] = now
	p.phase[params.JobID] = params.Phase
	if params.Done {
		delete(p.last, params.JobID)
		delete(p.begun, params.JobID)
		delete(p.phase, params.JobID)
	}
	attrs := []slog.Attr{
		slog.String("kind", params.Kind),
		slog.String("queue", params.Queue),
		slog.Int64("job_id", params.JobID),
		slog.String("phase", params.Phase),
	}
	if params.Percent != nil {
		attrs = append(attrs, slog.Int("percent", *params.Percent))
	} else if params.TotalBytes > 0 {
		percent := 100 * params.CopiedBytes / params.TotalBytes
		if percent > 100 {
			percent = 100
		}
		attrs = append(attrs,
			slog.Int64("percent", percent),
			slog.Int64("copied_bytes", params.CopiedBytes),
			slog.Int64("total_bytes", params.TotalBytes),
		)
	} else {
		attrs = append(attrs, slog.Int64("copied_bytes", params.CopiedBytes))
	}
	p.logger.LogAttrs(context.Background(), slog.LevelInfo, "progress", attrs...)
	return nil
}
