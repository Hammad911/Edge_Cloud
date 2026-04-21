package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// Phase labels a regime of a fault-injection run so metrics can be
// reported per-regime: before/during a fault window and after heal.
type Phase int

const (
	PhaseBefore Phase = iota
	PhaseDuring
	PhaseAfter
)

// String returns a human-friendly phase name for JSON output.
func (p Phase) String() string {
	switch p {
	case PhaseBefore:
		return "before"
	case PhaseDuring:
		return "during"
	case PhaseAfter:
		return "after"
	default:
		return "unknown"
	}
}

// PhaseTracker switches the current phase on demand and exposes per-phase
// counters and latency histograms. Goroutine-safe.
type PhaseTracker struct {
	mu      sync.RWMutex
	current Phase

	transitions map[Phase]time.Time

	local  [3]*LatencyHistogram
	lag    [3]*LatencyHistogram
	opsOK  [3]atomic.Int64
	opsErr [3]atomic.Int64

	// convergeAt records the first time after PhaseAfter when the lag
	// tracker reported zero outstanding replications. 0 means "never".
	convergeAt    atomic.Int64
	phaseAfterAt  atomic.Int64
}

// NewPhaseTracker starts in PhaseBefore.
func NewPhaseTracker() *PhaseTracker {
	t := &PhaseTracker{
		transitions: map[Phase]time.Time{PhaseBefore: time.Now()},
	}
	for i := range t.local {
		t.local[i] = NewLatencyHistogram(1024)
		t.lag[i] = NewLatencyHistogram(1024)
	}
	return t
}

// Set switches the phase. Calling with the same phase is a no-op.
func (t *PhaseTracker) Set(p Phase) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if p == t.current {
		return
	}
	t.current = p
	t.transitions[p] = time.Now()
	if p == PhaseAfter {
		t.phaseAfterAt.Store(time.Now().UnixNano())
	}
}

// Current reports the active phase.
func (t *PhaseTracker) Current() Phase {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.current
}

// RecordOp adds a (latency, success) sample to the current phase.
func (t *PhaseTracker) RecordOp(d time.Duration, ok bool) {
	p := t.Current()
	t.local[p].Record(d)
	if ok {
		t.opsOK[p].Add(1)
	} else {
		t.opsErr[p].Add(1)
	}
}

// RecordLag records a replication-lag sample in the current phase.
func (t *PhaseTracker) RecordLag(d time.Duration) {
	p := t.Current()
	t.lag[p].Record(d)
}

// NoteConverged is called by the driver once a post-heal settling
// condition is observed (e.g. replication-lag tracker reports zero
// pending). The first such call in PhaseAfter wins.
func (t *PhaseTracker) NoteConverged() {
	if t.Current() != PhaseAfter {
		return
	}
	if t.convergeAt.Load() == 0 {
		t.convergeAt.Store(time.Now().UnixNano())
	}
}

// Snapshot returns a JSON-ready view of each phase's stats.
func (t *PhaseTracker) Snapshot() PhaseSnapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := PhaseSnapshot{
		Phases: make([]PhaseStats, 0, 3),
	}
	for i := PhaseBefore; i <= PhaseAfter; i++ {
		entered := t.transitions[i]
		out.Phases = append(out.Phases, PhaseStats{
			Phase:          i.String(),
			EnteredAt:      entered,
			OpsSucceeded:   t.opsOK[i].Load(),
			OpsFailed:      t.opsErr[i].Load(),
			LocalLatency:   t.local[i].Snapshot(),
			ReplicationLag: t.lag[i].Snapshot(),
		})
	}
	if phaseAfter := t.phaseAfterAt.Load(); phaseAfter != 0 {
		if c := t.convergeAt.Load(); c != 0 {
			out.ConvergenceTime = time.Duration(c - phaseAfter)
		}
	}
	return out
}

// PhaseSnapshot is the JSON-ready summary.
type PhaseSnapshot struct {
	Phases          []PhaseStats  `json:"phases"`
	ConvergenceTime time.Duration `json:"convergence_time_after_heal"`
}

// PhaseStats is a single phase's numbers.
type PhaseStats struct {
	Phase          string       `json:"phase"`
	EnteredAt      time.Time    `json:"entered_at"`
	OpsSucceeded   int64        `json:"ops_succeeded"`
	OpsFailed      int64        `json:"ops_failed"`
	LocalLatency   LatencyStats `json:"local_latency"`
	ReplicationLag LatencyStats `json:"replication_lag"`
}
