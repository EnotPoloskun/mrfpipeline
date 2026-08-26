package jobs

import (
	"log/slog"

	"github.com/riverqueue/river"
)

// productionQueues is the default policy map used while constructing a
// client. Long-lived production workers replace it with QueuesForRole before
// starting, so callers cannot accidentally start the obsolete all-in-one
// topology through an exported queue helper.
func productionQueues() map[string]river.QueueConfig {
	return map[string]river.QueueConfig{
		QueueControl:     {MaxWorkers: 1},
		QueueDiscovery:   {MaxWorkers: 1},
		QueueTOCDownload: {MaxWorkers: 4},
		QueueTOCParse:    {MaxWorkers: 2},
		QueueTOCImport:   {MaxWorkers: 2},
		QueueMRFDownload: {MaxWorkers: 2},
		// mrf_parse concurrency 1 bounds CPU/memory/disk. It is not the
		// Story 10 process-level mrfparser.Parse mutex.
		QueueMRFParse: {MaxWorkers: 1},
		QueueConsumer: {MaxWorkers: 1},
	}
}

// ProductionPolicy returns the fixed base River client policy. Long-lived
// roles replace its queue map before starting. Retention, fetch
// cooldown, and retry use River defaults (zero / nil here). JobTimeoutNone is
// River's infinite timeout (-1); zero would become River's one-minute default.
func ProductionPolicy() river.Config {
	return river.Config{
		JobTimeout:           JobTimeoutNone,
		MaxAttempts:          MaxAttempts,
		Queues:               productionQueues(),
		RescueStuckJobsAfter: RescueAfter,
		Schema:               RiverSchema,
		SoftStopTimeout:      GracefulStop,
	}
}

// ClientConfig builds a client config from the production policy, optionally
// replacing the queue map for private test workers.
func ClientConfig(workers *river.Workers, queues map[string]river.QueueConfig, handler river.ErrorHandler, logger *slog.Logger) *river.Config {
	cfg := ProductionPolicy()
	cfg.Workers = workers
	if queues != nil {
		cfg.Queues = queues
	}
	cfg.ErrorHandler = handler
	cfg.Logger = logger
	return &cfg
}
