package jobs

import (
	"strconv"
	"sync"
)

type flightSlot struct {
	mu   sync.Mutex
	refs int
}

var (
	flightMu sync.Mutex
	flights  = map[string]*flightSlot{}
)

func flightKey(spec StageSpec, domainID int64) string {
	return spec.Table + "\x1f" + spec.StatusColumn + "\x1f" + strconv.FormatInt(domainID, 10)
}

// lockStageFlight serializes the entire jobs.Run invocation for one domain
// stage in this process. River rescue can deliver the same job ID while the
// original invocation is still running; claim allows that duplicate. The lock
// keeps artifact-mutating work from overlapping.
func lockStageFlight(spec StageSpec, domainID int64) func() {
	key := flightKey(spec, domainID)
	flightMu.Lock()
	slot := flights[key]
	if slot == nil {
		slot = &flightSlot{}
		flights[key] = slot
	}
	slot.refs++
	flightMu.Unlock()
	slot.mu.Lock()
	return func() {
		slot.mu.Unlock()
		flightMu.Lock()
		slot.refs--
		if slot.refs == 0 {
			delete(flights, key)
		}
		flightMu.Unlock()
	}
}
