package ycsb

import (
	"encoding/json"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Histogram is a small, lock-free-ish, log-linear latency histogram.
// It is accurate to roughly ±3% across six orders of magnitude (1µs → 1s)
// and is deliberately header-only so reports can be reproduced without
// pulling in a full HDR-histogram dependency.
type Histogram struct {
	mu      sync.Mutex
	buckets map[int64]int64 // key = integer microseconds (snapped to bucket)
	count   int64
	sum     int64 // microseconds
	max     int64
	min     int64
}

// NewHistogram constructs an empty histogram.
func NewHistogram() *Histogram {
	return &Histogram{buckets: make(map[int64]int64), min: math.MaxInt64}
}

// Observe records a single latency sample.
func (h *Histogram) Observe(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	bucket := snap(us)

	h.mu.Lock()
	h.buckets[bucket]++
	h.count++
	h.sum += us
	if us > h.max {
		h.max = us
	}
	if us < h.min {
		h.min = us
	}
	h.mu.Unlock()
}

// snap rounds a microsecond value to the nearest logarithmic bucket.
// The bucket granularity is 2^-5 of the value's order of magnitude which
// bounds the relative error to <~3%.
func snap(us int64) int64 {
	if us <= 0 {
		return 0
	}
	const subBuckets = 32
	exp := 0
	v := us
	for v >= subBuckets*2 {
		v >>= 1
		exp++
	}
	return int64(v) << exp
}

// Summary flattens the histogram into a stable, serialisable snapshot.
type Summary struct {
	Count int64              `json:"count"`
	MeanUs float64           `json:"mean_us"`
	MinUs  int64             `json:"min_us"`
	MaxUs  int64             `json:"max_us"`
	P50Us  int64             `json:"p50_us"`
	P95Us  int64             `json:"p95_us"`
	P99Us  int64             `json:"p99_us"`
	P999Us int64             `json:"p999_us"`
}

// Snapshot returns a percentile summary.
func (h *Histogram) Snapshot() Summary {
	h.mu.Lock()
	defer h.mu.Unlock()

	s := Summary{Count: h.count, MaxUs: h.max}
	if h.count == 0 {
		return s
	}
	s.MinUs = h.min
	s.MeanUs = float64(h.sum) / float64(h.count)

	keys := make([]int64, 0, len(h.buckets))
	for k := range h.buckets {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	s.P50Us = percentile(keys, h.buckets, h.count, 0.50)
	s.P95Us = percentile(keys, h.buckets, h.count, 0.95)
	s.P99Us = percentile(keys, h.buckets, h.count, 0.99)
	s.P999Us = percentile(keys, h.buckets, h.count, 0.999)
	return s
}

func percentile(keys []int64, buckets map[int64]int64, total int64, q float64) int64 {
	target := int64(math.Ceil(float64(total) * q))
	if target <= 0 {
		target = 1
	}
	var running int64
	for _, k := range keys {
		running += buckets[k]
		if running >= target {
			return k
		}
	}
	return keys[len(keys)-1]
}

// OpStats tracks counters + latency per operation type.
type OpStats struct {
	Success int64 `json:"success"`
	Error   int64 `json:"error"`
	Latency *Histogram `json:"-"`
}

// MarshalJSON emits counters + percentile summary.
func (s *OpStats) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Success int64   `json:"success"`
		Error   int64   `json:"error"`
		Latency Summary `json:"latency"`
	}{
		Success: s.Success,
		Error:   s.Error,
		Latency: s.Latency.Snapshot(),
	})
}

// Observe records a latency sample for this op.
func (s *OpStats) Observe(d time.Duration, err error) {
	if err != nil {
		atomic.AddInt64(&s.Error, 1)
		return
	}
	atomic.AddInt64(&s.Success, 1)
	s.Latency.Observe(d)
}

// Metrics is the aggregate view of a run.
type Metrics struct {
	mu    sync.Mutex
	ops   map[OpKind]*OpStats
	start time.Time
	end   time.Time
}

// NewMetrics creates an empty metrics aggregator.
func NewMetrics() *Metrics {
	return &Metrics{ops: make(map[OpKind]*OpStats)}
}

// Start records the run start wall-clock time.
func (m *Metrics) Start() {
	m.mu.Lock()
	m.start = time.Now()
	m.mu.Unlock()
}

// Stop records the run end wall-clock time.
func (m *Metrics) Stop() {
	m.mu.Lock()
	m.end = time.Now()
	m.mu.Unlock()
}

// Observe records one operation result.
func (m *Metrics) Observe(op OpKind, d time.Duration, err error) {
	m.mu.Lock()
	s, ok := m.ops[op]
	if !ok {
		s = &OpStats{Latency: NewHistogram()}
		m.ops[op] = s
	}
	m.mu.Unlock()
	s.Observe(d, err)
}

// Report is a JSON-friendly snapshot of a completed run.
type Report struct {
	Workload    string              `json:"workload"`
	Target      string              `json:"target"`
	Concurrency int                 `json:"concurrency"`
	Operations  int64               `json:"operations"`
	Errors      int64               `json:"errors"`
	DurationMs  int64               `json:"duration_ms"`
	Throughput  float64             `json:"throughput_ops_per_sec"`
	Ops         map[string]*OpStats `json:"ops"`
}

// Report renders the current state into a flat JSON-safe struct.
func (m *Metrics) Report(workload, target string, concurrency int) Report {
	m.mu.Lock()
	defer m.mu.Unlock()

	end := m.end
	if end.IsZero() {
		end = time.Now()
	}
	dur := end.Sub(m.start)
	if dur <= 0 {
		dur = time.Nanosecond
	}

	var total, errs int64
	ops := make(map[string]*OpStats, len(m.ops))
	for k, v := range m.ops {
		ops[string(k)] = v
		total += atomic.LoadInt64(&v.Success) + atomic.LoadInt64(&v.Error)
		errs += atomic.LoadInt64(&v.Error)
	}

	return Report{
		Workload:    workload,
		Target:      target,
		Concurrency: concurrency,
		Operations:  total,
		Errors:      errs,
		DurationMs:  dur.Milliseconds(),
		Throughput:  float64(total) / dur.Seconds(),
		Ops:         ops,
	}
}
