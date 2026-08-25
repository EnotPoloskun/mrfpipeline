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
	KindControlSchedule     = "control.schedule"

	QueueDiscovery   = "discovery"
	QueueTOCDownload = "toc_download"
	QueueTOCParse    = "toc_parse"
	QueueTOCImport   = "toc_import"
	QueueMRFDownload = "mrf_download"
	QueueMRFParse    = "mrf_parse"
	QueueConsumer    = "consumer"
	QueueControl     = "control"

	FieldDiscoveryRunID         = "discovery_run_id"
	FieldTOCFileID              = "toc_file_id"
	FieldMRFSourceID            = "mrf_source_id"
	FieldMRFSnapshotID          = "mrf_snapshot_id"
	FieldPlanAttachmentBatchID  = "plan_attachment_batch_id"
	FieldControlScheduleEventID = "control_schedule_event_id"
)

// ControlScheduleArgs carries a durable control event.  The event identity is
// intentionally unique per wake so a wake committed during an active scan is
// not lost to River's ByArgs uniqueness.
type ControlScheduleArgs struct {
	EventID int64 `json:"control_schedule_event_id"`
}

func (ControlScheduleArgs) Kind() string { return KindControlSchedule }
func (ControlScheduleArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueueControl, UniqueOpts: river.UniqueOpts{ByArgs: true}}
}
func (a *ControlScheduleArgs) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return jobErr("control schedule args")
	}
	// Accept the pre-Story-21 empty wake for upgrade compatibility. New wakes
	// always carry an event identity and therefore cannot be lost to ByArgs.
	if len(fields) == 0 {
		a.EventID = 0
		return nil
	}
	if len(fields) != 1 {
		return jobErr("control schedule args")
	}
	var decoded struct {
		EventID int64 `json:"control_schedule_event_id"`
	}
	if _, ok := fields[FieldControlScheduleEventID]; !ok {
		return jobErr("control schedule args")
	}
	if err := json.Unmarshal(data, &decoded); err != nil || decoded.EventID <= 0 {
		return jobErr("control schedule args")
	}
	a.EventID = decoded.EventID
	return nil
}

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

// ProductionBindings maps each production kind to its domain stage and argument field.
func ProductionBindings() []KindBinding {
	return []KindBinding{
		{Kind: KindDiscoveryRun, Spec: DiscoveryRunStage, ArgField: FieldDiscoveryRunID},
		{Kind: KindTOCDownload, Spec: TOCDownloadStage, ArgField: FieldTOCFileID},
		{Kind: KindTOCParse, Spec: TOCParseStage, ArgField: FieldTOCFileID},
		{Kind: KindTOCImport, Spec: TOCImportStage, ArgField: FieldTOCFileID},
		{Kind: KindMRFDownload, Spec: MRFDownloadStage, ArgField: FieldMRFSourceID},
		{Kind: KindMRFParse, Spec: MRFParseStage, ArgField: FieldMRFSourceID},
		{Kind: KindConsumerIngest, Spec: ConsumerIngestStage, ArgField: FieldMRFSnapshotID},
		{Kind: KindConsumerAttachPlans, Spec: ConsumerAttachPlansStage, ArgField: FieldPlanAttachmentBatchID},
	}
}

// ArgsFor constructs typed River args for one production kind and domain ID.
func ArgsFor(kind string, domainID int64) (river.JobArgs, error) {
	if kind == KindControlSchedule {
		return &ControlScheduleArgs{EventID: domainID}, nil
	}
	if domainID <= 0 {
		return nil, Failure(FailureInvalidArguments)
	}
	switch kind {
	case KindDiscoveryRun:
		return &DiscoveryRunArgs{DiscoveryRunID: domainID}, nil
	case KindTOCDownload:
		return &TOCDownloadArgs{TOCFileID: domainID}, nil
	case KindTOCParse:
		return &TOCParseArgs{TOCFileID: domainID}, nil
	case KindTOCImport:
		return &TOCImportArgs{TOCFileID: domainID}, nil
	case KindMRFDownload:
		return &MRFDownloadArgs{MRFSourceID: domainID}, nil
	case KindMRFParse:
		return &MRFParseArgs{MRFSourceID: domainID}, nil
	case KindConsumerIngest:
		return &ConsumerIngestArgs{MRFSnapshotID: domainID}, nil
	case KindConsumerAttachPlans:
		return &ConsumerAttachPlansArgs{PlanAttachmentBatchID: domainID}, nil
	default:
		return nil, Failure(FailureInvalidArguments)
	}
}

// DomainIDFromEncodedArgs reads the single numeric argument from River JSON.
func DomainIDFromEncodedArgs(kind string, raw []byte) (int64, error) {
	args, err := ArgsFor(kind, 1)
	if err != nil {
		return 0, err
	}
	switch a := args.(type) {
	case *DiscoveryRunArgs:
		if err := a.UnmarshalJSON(raw); err != nil {
			return 0, err
		}
		return a.DiscoveryRunID, nil
	case *TOCDownloadArgs:
		if err := a.UnmarshalJSON(raw); err != nil {
			return 0, err
		}
		return a.TOCFileID, nil
	case *TOCParseArgs:
		if err := a.UnmarshalJSON(raw); err != nil {
			return 0, err
		}
		return a.TOCFileID, nil
	case *TOCImportArgs:
		if err := a.UnmarshalJSON(raw); err != nil {
			return 0, err
		}
		return a.TOCFileID, nil
	case *MRFDownloadArgs:
		if err := a.UnmarshalJSON(raw); err != nil {
			return 0, err
		}
		return a.MRFSourceID, nil
	case *MRFParseArgs:
		if err := a.UnmarshalJSON(raw); err != nil {
			return 0, err
		}
		return a.MRFSourceID, nil
	case *ConsumerIngestArgs:
		if err := a.UnmarshalJSON(raw); err != nil {
			return 0, err
		}
		return a.MRFSnapshotID, nil
	case *ConsumerAttachPlansArgs:
		if err := a.UnmarshalJSON(raw); err != nil {
			return 0, err
		}
		return a.PlanAttachmentBatchID, nil
	case *ControlScheduleArgs:
		if err := a.UnmarshalJSON(raw); err != nil {
			return 0, err
		}
		return a.EventID, nil
	default:
		return 0, Failure(FailureInvalidArguments)
	}
}

var (
	_ river.JobArgs               = DiscoveryRunArgs{}
	_ river.JobArgsWithInsertOpts = DiscoveryRunArgs{}
	_ json.Unmarshaler            = (*DiscoveryRunArgs)(nil)
)
