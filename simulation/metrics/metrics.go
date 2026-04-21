// Package metrics records simulator-wide observations: per-op latency,
// throughput, and replication lag. The data structures are designed to
// be cheap on the hot path (one mutex per metric, no allocation per
// sample) and to render summary statistics suitable for paper figures.
package metrics

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// LatencyHistogram is an unbounded slice-backed reservoir. It is fine
// for a single simulator run (millions of samples max); for production
// use we'd swap in HDR-histogram.
type LatencyHistogram struct {
	mu      sync.Mutex
	samples []time.Duration
}

// NewLatencyHistogram returns an empty histogram with capacity hint.
func NewLatencyHistogram(capHint int) *LatencyHistogram {
	if capHint < 0 {
		capHint = 0
	}
	return &LatencyHistogram{samples: make([]time.Duration, 0, capHint)}
}

// Record adds a latency sample. Goroutine-safe.
func (h *LatencyHistogram) Record(d time.Duration) {
	h.mu.Lock()
	h.samples = append(h.samples, d)
	h.mu.Unlock()
}

// Snapshot computes summary statistics in milliseconds. Returns zero if
// the histogram is empty.
func (h *LatencyHistogram) Snapshot() LatencyStats {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := len(h.samples)
	if n == 0 {
		return LatencyStats{}
	}
	cp := make([]time.Duration, n)
	copy(cp, h.samples)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })

	var sum time.Duration
	for _, s := range cp {
		sum += s
	}
	mean := time.Duration(int64(sum) / int64(n))

	pct := func(p float64) time.Duration {
		idx := int(math.Ceil(p*float64(n))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		return cp[idx]
	}
	return LatencyStats{
		Count: n,
		Min:   cp[0],
		P50:   pct(0.50),
		P95:   pct(0.95),
		P99:   pct(0.99),
		Max:   cp[n-1],
		Mean:  mean,
	}
}

// LatencyStats is a JSON-friendly summary of a histogram.
type LatencyStats struct {
	Count int           `json:"count"`
	Min   time.Duration `json:"min"`
	Mean  time.Duration `json:"mean"`
	P50   time.Duration `json:"p50"`
	P95   time.Duration `json:"p95"`
	P99   time.Duration `json:"p99"`
	Max   time.Duration `json:"max"`
}

// ReplicationLagTracker records, per (origin, key, value), the wall-time
// delta between the moment the value was committed at its origin site
// and the moment it became visible at every other site.
//
// The tracker is intentionally simple: it samples a subset of writes
// (every Nth) so 500-site runs don't exhaust memory.
type ReplicationLagTracker struct {
	hist       *LatencyHistogram
	sampleRate int

	mu      sync.Mutex
	pending map[lagKey]time.Time
	count   atomic.Int64
}

type lagKey struct {
	key   string
	value string // hex/string of value bytes; we use len + first 8 bytes
}

// NewReplicationLagTracker returns a tracker that samples 1 in
// sampleRate writes (sampleRate <= 1 disables sampling, recording all).
func NewReplicationLagTracker(sampleRate int) *ReplicationLagTracker {
	if sampleRate < 1 {
		sampleRate = 1
	}
	return &ReplicationLagTracker{
		hist:       NewLatencyHistogram(1024),
		sampleRate: sampleRate,
		pending:    make(map[lagKey]time.Time),
	}
}

// RecordWrite is called when an origin site commits (key, value) at t.
// The first call for a given (key, value) wins; later commits with the
// same value are ignored (acceptable because tests use unique values).
func (t *ReplicationLagTracker) RecordWrite(key string, value []byte, at time.Time) {
	if t == nil {
		return
	}
	if t.sampleRate > 1 && t.count.Add(1)%int64(t.sampleRate) != 0 {
		return
	}
	k := lagKey{key: key, value: shortValue(value)}
	t.mu.Lock()
	if _, ok := t.pending[k]; !ok {
		t.pending[k] = at
	}
	t.mu.Unlock()
}

// RecordObservation is called when a non-origin site applies (key,
// value). If we have a matching pending write the lag is recorded.
func (t *ReplicationLagTracker) RecordObservation(key string, value []byte, at time.Time) {
	if t == nil {
		return
	}
	k := lagKey{key: key, value: shortValue(value)}
	t.mu.Lock()
	written, ok := t.pending[k]
	t.mu.Unlock()
	if !ok {
		return
	}
	t.hist.Record(at.Sub(written))
}

// Stats returns the lag distribution.
func (t *ReplicationLagTracker) Stats() LatencyStats {
	if t == nil {
		return LatencyStats{}
	}
	return t.hist.Snapshot()
}

// PendingCount reports outstanding writes that have not been observed
// at any other site yet (useful as a coverage indicator).
func (t *ReplicationLagTracker) PendingCount() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}

func shortValue(v []byte) string {
	if len(v) == 0 {
		return ""
	}
	if len(v) <= 8 {
		return string(v)
	}
	return string(v[:8])
}
