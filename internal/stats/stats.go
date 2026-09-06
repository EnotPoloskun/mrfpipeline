package stats

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Row is one stage/payer/month count line.
type Row struct {
	Stage           string `json:"stage"`
	PayerID         string `json:"payer_id"`
	CollectionMonth string `json:"collection_month"`
	Blocked         int64  `json:"blocked"`
	Pending         int64  `json:"pending"`
	Running         int64  `json:"running"`
	Succeeded       int64  `json:"succeeded"`
	Failed          int64  `json:"failed"`
	Total           int64  `json:"total"`
}

// Options optionally restricts stats to one payer and/or collection month.
type Options struct {
	Payer string
	Month time.Time
}

var stageOrder = []string{
	"toc.download",
	"toc.parse",
	"toc.import",
	"mrf.download",
	"mrf.parse",
	"consumer.ingest",
	"consumer.attach_plans",
}

var stageQueries = []struct {
	stage string
	sql   string
}{
	{"toc.download", sqlTOCDownload},
	{"toc.parse", sqlTOCParse},
	{"toc.import", sqlTOCImport},
	{"mrf.download", sqlMRFDownload},
	{"mrf.parse", sqlMRFParse},
	{"consumer.ingest", sqlConsumerIngest},
	{"consumer.attach_plans", sqlAttachPlans},
}

// Collect returns pipeline stage counts from PostgreSQL.
func Collect(ctx context.Context, pool *pgxpool.Pool, opts Options) ([]Row, error) {
	if ctx == nil {
		panic("stats: nil context")
	}
	if pool == nil {
		return nil, dbFailure(ctx)
	}
	var payer any
	if opts.Payer != "" {
		payer = opts.Payer
	}
	var month any
	if !opts.Month.IsZero() {
		month = opts.Month
	}

	rows := make([]Row, 0)
	for _, spec := range stageQueries {
		stageRows, err := queryStage(ctx, pool, spec.stage, spec.sql, payer, month)
		if err != nil {
			return nil, err
		}
		rows = append(rows, stageRows...)
	}
	sortRows(rows)
	return rows, nil
}

func queryStage(ctx context.Context, pool *pgxpool.Pool, stage, sql string, payer, month any) ([]Row, error) {
	qrows, err := pool.Query(ctx, sql, payer, month)
	if err != nil {
		return nil, dbFailure(ctx)
	}
	defer qrows.Close()

	out := make([]Row, 0)
	for qrows.Next() {
		var payerID string
		var collectionMonth time.Time
		var blocked, pending, running, succeeded, failed, total int64
		if err := qrows.Scan(&payerID, &collectionMonth, &blocked, &pending, &running, &succeeded, &failed, &total); err != nil {
			return nil, dbFailure(ctx)
		}
		out = append(out, Row{
			Stage:           stage,
			PayerID:         payerID,
			CollectionMonth: formatMonth(collectionMonth),
			Blocked:         blocked,
			Pending:         pending,
			Running:         running,
			Succeeded:       succeeded,
			Failed:          failed,
			Total:           total,
		})
	}
	if err := qrows.Err(); err != nil {
		return nil, dbFailure(ctx)
	}
	return out, nil
}

func formatMonth(month time.Time) string {
	return fmt.Sprintf("%04d-%02d", month.Year(), int(month.Month()))
}

func stageIndex(stage string) int {
	for i, name := range stageOrder {
		if name == stage {
			return i
		}
	}
	return len(stageOrder)
}

func sortRows(rows []Row) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].PayerID != rows[j].PayerID {
			return rows[i].PayerID < rows[j].PayerID
		}
		if rows[i].CollectionMonth != rows[j].CollectionMonth {
			return rows[i].CollectionMonth < rows[j].CollectionMonth
		}
		return stageIndex(rows[i].Stage) < stageIndex(rows[j].Stage)
	})
}

// FormatTable renders the default aligned text table.
func FormatTable(rows []Row) string {
	headers := []string{"stage", "payer", "month", "blocked", "pending", "running", "succeeded", "failed", "total"}
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, row := range rows {
		texts := []string{
			row.Stage,
			row.PayerID,
			row.CollectionMonth,
			fmt.Sprintf("%d", row.Blocked),
			fmt.Sprintf("%d", row.Pending),
			fmt.Sprintf("%d", row.Running),
			fmt.Sprintf("%d", row.Succeeded),
			fmt.Sprintf("%d", row.Failed),
			fmt.Sprintf("%d", row.Total),
		}
		for i, text := range texts {
			if len(text) > widths[i] {
				widths[i] = len(text)
			}
		}
	}

	var b strings.Builder
	writeTableLine(&b, headers, widths, 3)
	for _, row := range rows {
		writeTableLine(&b, []string{
			row.Stage,
			row.PayerID,
			row.CollectionMonth,
			fmt.Sprintf("%d", row.Blocked),
			fmt.Sprintf("%d", row.Pending),
			fmt.Sprintf("%d", row.Running),
			fmt.Sprintf("%d", row.Succeeded),
			fmt.Sprintf("%d", row.Failed),
			fmt.Sprintf("%d", row.Total),
		}, widths, 3)
	}
	return b.String()
}

func writeTableLine(b *strings.Builder, cols []string, widths []int, numericFrom int) {
	for i, col := range cols {
		if i > 0 {
			b.WriteByte(' ')
		}
		if i >= numericFrom {
			fmt.Fprintf(b, "%*s", widths[i], col)
		} else {
			fmt.Fprintf(b, "%-*s", widths[i], col)
		}
	}
	b.WriteByte('\n')
}

// FormatJSON renders a compact JSON array with a trailing newline.
func FormatJSON(rows []Row) (string, error) {
	if rows == nil {
		rows = []Row{}
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		return "", err
	}
	return string(raw) + "\n", nil
}

func dbFailure(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return database.ErrDatabase
}
