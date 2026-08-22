package jobs

import (
	"encoding/json"

	"github.com/riverqueue/river"
)

const (
	KindDiscoveryRun        = "discovery.run"
	KindTOCDownload         = "toc.download"
	KindTOCParse            = "toc.parse"
	KindTOCImport           = "toc.import"
	KindMRFDownload         = "mrf.download"
	KindMRFParse            = "mrf.parse"
	KindConsumerIngest      = "consumer.ingest"
	KindConsumerAttachPlans = "consumer.attach_plans"

	QueueDiscovery   = "discovery"
	QueueTOCDownload = "toc_download"
	QueueTOCParse    = "toc_parse"
	QueueTOCImport   = "toc_import"
	QueueMRFDownload = "mrf_download"
	QueueMRFParse    = "mrf_parse"
	QueueConsumer    = "consumer"

	FieldDiscoveryRunID        = "discovery_run_id"
	FieldTOCFileID             = "toc_file_id"
	FieldMRFSourceID           = "mrf_source_id"
	FieldMRFSnapshotID         = "mrf_snapshot_id"
	FieldPlanAttachmentBatchID = "plan_attachment_batch_id"
)

// DiscoveryRunArgs is the durable payload for discovery.run.
type DiscoveryRunArgs struct {
	DiscoveryRunID int64 `json:"discovery_run_id"`
}

func (DiscoveryRunArgs) Kind() string { return KindDiscoveryRun }
func (DiscoveryRunArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueueDiscovery}
}
func (a *DiscoveryRunArgs) UnmarshalJSON(data []byte) error {
	return unmarshalOnePositiveInt64(data, FieldDiscoveryRunID, &a.DiscoveryRunID)
}

// TOCDownloadArgs is the durable payload for toc.download.
type TOCDownloadArgs struct {
	TOCFileID int64 `json:"toc_file_id"`
}

func (TOCDownloadArgs) Kind() string { return KindTOCDownload }
func (TOCDownloadArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueueTOCDownload}
}
func (a *TOCDownloadArgs) UnmarshalJSON(data []byte) error {
	return unmarshalOnePositiveInt64(data, FieldTOCFileID, &a.TOCFileID)
}

// TOCParseArgs is the durable payload for toc.parse.
type TOCParseArgs struct {
	TOCFileID int64 `json:"toc_file_id"`
}

func (TOCParseArgs) Kind() string { return KindTOCParse }
func (TOCParseArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueueTOCParse}
}
func (a *TOCParseArgs) UnmarshalJSON(data []byte) error {
	return unmarshalOnePositiveInt64(data, FieldTOCFileID, &a.TOCFileID)
}

// TOCImportArgs is the durable payload for toc.import.
type TOCImportArgs struct {
	TOCFileID int64 `json:"toc_file_id"`
}

func (TOCImportArgs) Kind() string { return KindTOCImport }
func (TOCImportArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueueTOCImport}
}
func (a *TOCImportArgs) UnmarshalJSON(data []byte) error {
	return unmarshalOnePositiveInt64(data, FieldTOCFileID, &a.TOCFileID)
}

// MRFDownloadArgs is the durable payload for mrf.download.
type MRFDownloadArgs struct {
	MRFSourceID int64 `json:"mrf_source_id"`
}

func (MRFDownloadArgs) Kind() string { return KindMRFDownload }
func (MRFDownloadArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueueMRFDownload}
}
func (a *MRFDownloadArgs) UnmarshalJSON(data []byte) error {
	return unmarshalOnePositiveInt64(data, FieldMRFSourceID, &a.MRFSourceID)
}

// MRFParseArgs is the durable payload for mrf.parse.
//
// Story 10 must wrap every mrfparser.Parse call with one process-level mutex.
// Queue concurrency 1 on mrf_parse bounds resource use; it is not that
// contract. River 0.39 rescue does not terminate the existing Go invocation.
type MRFParseArgs struct {
	MRFSourceID int64 `json:"mrf_source_id"`
}

func (MRFParseArgs) Kind() string { return KindMRFParse }
func (MRFParseArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueueMRFParse}
}
func (a *MRFParseArgs) UnmarshalJSON(data []byte) error {
	return unmarshalOnePositiveInt64(data, FieldMRFSourceID, &a.MRFSourceID)
}

// ConsumerIngestArgs is the durable payload for consumer.ingest.
type ConsumerIngestArgs struct {
	MRFSnapshotID int64 `json:"mrf_snapshot_id"`
}

func (ConsumerIngestArgs) Kind() string { return KindConsumerIngest }
func (ConsumerIngestArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueueConsumer}
}
func (a *ConsumerIngestArgs) UnmarshalJSON(data []byte) error {
	return unmarshalOnePositiveInt64(data, FieldMRFSnapshotID, &a.MRFSnapshotID)
}

// ConsumerAttachPlansArgs is the durable payload for consumer.attach_plans.
type ConsumerAttachPlansArgs struct {
	PlanAttachmentBatchID int64 `json:"plan_attachment_batch_id"`
}

func (ConsumerAttachPlansArgs) Kind() string { return KindConsumerAttachPlans }
func (ConsumerAttachPlansArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueueConsumer}
}
func (a *ConsumerAttachPlansArgs) UnmarshalJSON(data []byte) error {
	return unmarshalOnePositiveInt64(data, FieldPlanAttachmentBatchID, &a.PlanAttachmentBatchID)
}

// ProductionKinds is the version 1 job catalog in table order.
func ProductionKinds() []string {
	return []string{
		KindDiscoveryRun,
		KindTOCDownload,
		KindTOCParse,
		KindTOCImport,
		KindMRFDownload,
		KindMRFParse,
		KindConsumerIngest,
		KindConsumerAttachPlans,
	}
}

var (
	_ river.JobArgs               = DiscoveryRunArgs{}
	_ river.JobArgsWithInsertOpts = DiscoveryRunArgs{}
	_ json.Unmarshaler            = (*DiscoveryRunArgs)(nil)
)
