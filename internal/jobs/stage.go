package jobs

import (
	"fmt"
	"regexp"

	"github.com/enotpoloskun/mrfpipeline/internal/database"
)

const (
	StatusBlocked   = "blocked"
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
)

// StageSpec names the columns of one domain stage. Later stories pass
// production tables; Story 03 proves the protocol on test-only tables.
type StageSpec struct {
	Table             string
	IDColumn          string
	StatusColumn      string
	JobIDColumn       string
	StartedAtColumn   string
	CompletedAtColumn string
	FailureCodeColumn string
	UpdatedAtColumn   string
}

// KindBinding maps a River job kind to its domain stage and argument field.
type KindBinding struct {
	Kind     string
	Spec     StageSpec
	ArgField string
}

// TOCDownloadStage is the download stage on toc_files.
var TOCDownloadStage = StageSpec{
	Table:             "mrfpipeline.toc_files",
	IDColumn:          "id",
	StatusColumn:      "download_status",
	JobIDColumn:       "download_river_job_id",
	FailureCodeColumn: "failure_code",
	UpdatedAtColumn:   "updated_at",
}

// TOCParseStage is the parse stage on toc_files.
var TOCParseStage = StageSpec{
	Table:             "mrfpipeline.toc_files",
	IDColumn:          "id",
	StatusColumn:      "parse_status",
	JobIDColumn:       "parse_river_job_id",
	FailureCodeColumn: "failure_code",
	UpdatedAtColumn:   "updated_at",
}

// TOCImportStage is the import stage on toc_files.
var TOCImportStage = StageSpec{
	Table:             "mrfpipeline.toc_files",
	IDColumn:          "id",
	StatusColumn:      "import_status",
	JobIDColumn:       "import_river_job_id",
	FailureCodeColumn: "failure_code",
	UpdatedAtColumn:   "updated_at",
}

// MRFDownloadStage is the download stage on mrf_sources.
var MRFDownloadStage = StageSpec{
	Table:             "mrfpipeline.mrf_sources",
	IDColumn:          "id",
	StatusColumn:      "download_status",
	JobIDColumn:       "download_river_job_id",
	FailureCodeColumn: "failure_code",
	UpdatedAtColumn:   "updated_at",
}

// MRFParseStage is the parse stage on mrf_sources.
var MRFParseStage = StageSpec{
	Table:             "mrfpipeline.mrf_sources",
	IDColumn:          "id",
	StatusColumn:      "parse_status",
	JobIDColumn:       "parse_river_job_id",
	FailureCodeColumn: "failure_code",
	UpdatedAtColumn:   "updated_at",
}

// DiscoveryRunStage is the domain stage for discovery.run.
var DiscoveryRunStage = StageSpec{
	Table:             "mrfpipeline.discovery_runs",
	IDColumn:          "id",
	StatusColumn:      "status",
	JobIDColumn:       "river_job_id",
	StartedAtColumn:   "started_at",
	CompletedAtColumn: "completed_at",
	FailureCodeColumn: "failure_code",
	UpdatedAtColumn:   "updated_at",
}

var identRE = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
var tableRE = regexp.MustCompile(`^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$`)

func (s StageSpec) validate() error {
	if !tableRE.MatchString(s.Table) {
		return fmt.Errorf("%w: %w: stage", ErrJob, database.ErrDatabase)
	}
	for _, col := range []string{s.IDColumn, s.StatusColumn, s.JobIDColumn, s.UpdatedAtColumn} {
		if !identRE.MatchString(col) {
			return fmt.Errorf("%w: %w: stage", ErrJob, database.ErrDatabase)
		}
	}
	for _, col := range []string{s.StartedAtColumn, s.CompletedAtColumn, s.FailureCodeColumn} {
		if col != "" && !identRE.MatchString(col) {
			return fmt.Errorf("%w: %w: stage", ErrJob, database.ErrDatabase)
		}
	}
	return nil
}

type stageRow struct {
	id     int64
	status string
	jobID  *int64
}
