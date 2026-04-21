package ycsb

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	mrand "math/rand"
	"sync"
	"sync/atomic"
	"time"

	kvv1 "edge-cloud-replication/gen/proto/edgecloud/kv/v1"
	"edge-cloud-replication/evaluation/checker"
	"edge-cloud-replication/pkg/hlc"
)

// Config captures everything a benchmark run needs. Zero values are
// filled in with sane defaults by (*Runner).normalise.
type Config struct {
	Targets       []string
	Spec          Spec
	Concurrency   int
	Operations    int           // 0 = use Duration
	Duration      time.Duration // 0 = use Operations
	OpTimeout     time.Duration
	Stickiness    bool
	Seed          int64
	KeyPrefix     string
	ValueSize     int
	LoadBatch     int
	HistoryOut    string // optional JSONL path for evaluation/checker
	Logger        *slog.Logger
	ClientFactory func(addr string) (Client, error) // optional override (tests)
}

// Runner drives a Config against the target cluster.
type Runner struct {
	cfg      Config
	pool     *Pool
	metrics  *Metrics
	history  checker.Recorder
	startSeq int64
}

// NewRunner validates the config, dials the pool, and opens the optional
// history file.
func NewRunner(cfg Config) (*Runner, error) {
	cfg, err := normalise(cfg)
	if err != nil {
		return nil, err
	}
	pool, err := NewPool(cfg.Targets, cfg.ClientFactory)
	if err != nil {
		return nil, err
	}
	r := &Runner{cfg: cfg, pool: pool, metrics: NewMetrics()}
	if cfg.HistoryOut != "" {
		rec, err := checker.NewJSONLRecorder(cfg.HistoryOut)
		if err != nil {
			_ = pool.Close()
			return nil, fmt.Errorf("history recorder: %w", err)
		}
		r.history = rec
	}
	return r, nil
}

// Close releases the client pool and flushes the history recorder.
func (r *Runner) Close() error {
	var firstErr error
	if r.history != nil {
		if err := r.history.Close(); err != nil {
			firstErr = err
		}
	}
	if err := r.pool.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// Metrics exposes the aggregate metrics (useful between Load and Run).
func (r *Runner) Metrics() *Metrics { return r.metrics }

// Load populates the keyspace with `record_count` keys spread across the
// targets. It always runs with stickiness=true so the insert load is
// evenly distributed. If LoadBatch > 0, a worker pool is used.
func (r *Runner) Load(ctx context.Context) error {
	if r.cfg.Spec.RecordCount <= 0 {
		return nil
	}

	concurrency := r.cfg.LoadBatch
	if concurrency <= 0 {
		concurrency = r.cfg.Concurrency
	}
	if concurrency <= 0 {
		concurrency = 8
	}

	jobs := make(chan int, concurrency*2)
	var wg sync.WaitGroup
	var failed atomic.Int64
	errCh := make(chan error, 1)

	for w := 0; w < concurrency; w++ {
		addr, client, err := r.pool.Pick(w, true)
		if err != nil {
			return err
		}
		wg.Add(1)
		go func(worker int, addr string, client Client) {
			defer wg.Done()
			rng := mrand.New(mrand.NewSource(r.cfg.Seed + int64(worker*7919)))
			session := fmt.Sprintf("load-%d", worker)
			var token *kvv1.CausalToken
			for idx := range jobs {
				key := FormatKey(r.cfg.KeyPrefix, idx)
				val := randomValue(rng, r.cfg.ValueSize)
				cctx, cancel := context.WithTimeout(ctx, r.cfg.OpTimeout)
				start := time.Now()
				tok, err := client.Put(cctx, key, val, token)
				cancel()
				lat := time.Since(start)
				r.metrics.Observe(OpInsert, lat, err)
				r.recordEvent(session, addr, checker.OpPut, key, val, false, tok, start, lat, err)
				if err != nil {
					failed.Add(1)
					select {
					case errCh <- fmt.Errorf("load put %s: %w", key, err):
					default:
					}
					continue
				}
				token = mergeToken(token, tok)
			}
		}(w, addr, client)
	}

	r.metrics.Start()
	for i := 0; i < r.cfg.Spec.RecordCount; i++ {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		case jobs <- i:
		}
	}
	close(jobs)
	wg.Wait()
	r.metrics.Stop()

	if failed.Load() > 0 {
		select {
		case err := <-errCh:
			return err
		default:
			return fmt.Errorf("load finished with %d failures", failed.Load())
		}
	}
	return nil
}

// Run executes the operation mix in closed loop. It respects either a
// total op budget (Operations > 0) or a wall-clock duration.
func (r *Runner) Run(ctx context.Context) error {
	probs, _, err := r.cfg.Spec.Mix.Normalised()
	if err != nil {
		return err
	}
	ops := expandMix(probs)
	if len(ops) == 0 {
		return errors.New("ycsb: empty operation mix")
	}

	var deadline time.Time
	if r.cfg.Duration > 0 {
		deadline = time.Now().Add(r.cfg.Duration)
	}
	var budget *atomic.Int64
	if r.cfg.Operations > 0 {
		budget = &atomic.Int64{}
		budget.Store(int64(r.cfg.Operations))
	}

	var wg sync.WaitGroup
	r.metrics.Start()
	for w := 0; w < r.cfg.Concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			r.workerLoop(ctx, worker, ops, deadline, budget)
		}(w)
	}
	wg.Wait()
	r.metrics.Stop()
	return nil
}

// Report returns a JSON-ready snapshot of current metrics.
func (r *Runner) Report() Report {
	target := "multi"
	if tgts := r.pool.Targets(); len(tgts) == 1 {
		target = tgts[0]
	}
	return r.metrics.Report(string(r.cfg.Spec.Workload), target, r.cfg.Concurrency)
}

// workerLoop is the closed-loop body executed by each worker.
func (r *Runner) workerLoop(ctx context.Context, worker int, ops []OpKind, deadline time.Time, budget *atomic.Int64) {
	addr, client, err := r.pool.Pick(worker, r.cfg.Stickiness)
	if err != nil {
		r.cfg.Logger.Error("ycsb pick client failed", "worker", worker, "error", err)
		return
	}

	rng := mrand.New(mrand.NewSource(r.cfg.Seed + int64(worker*104729)))
	keygen := NewKeyGen(r.cfg.Spec.Distribution, r.cfg.Spec.RecordCount)
	session := fmt.Sprintf("run-%04d", worker)
	var token *kvv1.CausalToken

	for {
		if ctx.Err() != nil {
			return
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return
		}
		if budget != nil {
			if budget.Add(-1) < 0 {
				return
			}
		}

		op := ops[rng.Intn(len(ops))]
		key, value := r.pickOp(op, keygen, rng)
		cctx, cancel := context.WithTimeout(ctx, r.cfg.OpTimeout)

		start := time.Now()
		var tok *kvv1.CausalToken
		var deleted bool
		var readVal []byte
		var opErr error

		switch op {
		case OpRead:
			readVal, _, tok, opErr = client.Get(cctx, key, token)
		case OpUpdate:
			tok, opErr = client.Put(cctx, key, value, token)
		case OpInsert:
			if lg, ok := keygen.(*LatestGen); ok {
				// Grow the keyspace past the current tail so `latest`
				// biases correctly on subsequent reads.
				idx := lg.Tail()
				key = FormatKey(r.cfg.KeyPrefix, idx)
				lg.Advance()
			}
			tok, opErr = client.Put(cctx, key, value, token)
		case OpDelete:
			tok, opErr = client.Delete(cctx, key, token)
			deleted = opErr == nil
		case OpReadModifyWrite:
			var rtok *kvv1.CausalToken
			readVal, _, rtok, opErr = client.Get(cctx, key, token)
			if opErr == nil {
				tok, opErr = client.Put(cctx, key, value, rtok)
			}
		}
		cancel()

		lat := time.Since(start)
		r.metrics.Observe(op, lat, opErr)

		switch op {
		case OpRead, OpReadModifyWrite:
			r.recordEvent(session, addr, checker.OpGet, key, readVal, false, tok, start, lat, opErr)
			if op == OpReadModifyWrite && opErr == nil {
				r.recordEvent(session, addr, checker.OpPut, key, value, false, tok, start, lat, nil)
			}
		case OpUpdate, OpInsert:
			r.recordEvent(session, addr, checker.OpPut, key, value, false, tok, start, lat, opErr)
		case OpDelete:
			r.recordEvent(session, addr, checker.OpDelete, key, nil, deleted, tok, start, lat, opErr)
		}

		if opErr == nil {
			token = mergeToken(token, tok)
		}
	}
}

// pickOp returns the (key, value) tuple for one op. value is only
// populated for write-class ops; the caller is free to discard it.
func (r *Runner) pickOp(op OpKind, keygen KeyGen, rng *mrand.Rand) (string, []byte) {
	switch op {
	case OpInsert:
		return "", nil // caller picks a tail key
	default:
		idx := keygen.Next(rng)
		key := FormatKey(r.cfg.KeyPrefix, idx)
		if op == OpRead {
			return key, nil
		}
		return key, randomValue(rng, r.cfg.ValueSize)
	}
}

// recordEvent writes an evaluation/checker event when history capture is
// enabled.
func (r *Runner) recordEvent(session, site string, kind checker.OpKind, key string, value []byte, deleted bool, tok *kvv1.CausalToken, issued time.Time, lat time.Duration, err error) {
	if r.history == nil {
		return
	}
	seq := atomic.AddInt64(&r.startSeq, 1)
	ev := checker.Event{
		Seq:       seq,
		SessionID: session,
		Site:      site,
		Kind:      kind,
		Key:       key,
		Value:     append([]byte(nil), value...),
		Deleted:   deleted,
		WriteTS:   tokenToHLC(tok),
		IssuedAt:  issued,
		Latency:   lat,
	}
	if err != nil {
		ev.Err = err.Error()
	}
	r.history.Record(ev)
}

func tokenToHLC(t *kvv1.CausalToken) hlc.Timestamp {
	if t == nil {
		return hlc.Timestamp{}
	}
	return hlc.Timestamp{Physical: t.GetPhysical(), Logical: t.GetLogical()}
}

// expandMix turns a normalised probability map into a 100-slot lookup
// table so workers can sample O(1) without recomputing CDFs.
func expandMix(probs map[OpKind]float64) []OpKind {
	const slots = 100
	out := make([]OpKind, 0, slots)
	kinds := []OpKind{OpRead, OpUpdate, OpInsert, OpReadModifyWrite, OpDelete}
	for _, k := range kinds {
		p, ok := probs[k]
		if !ok {
			continue
		}
		n := int(p*slots + 0.5)
		for i := 0; i < n; i++ {
			out = append(out, k)
		}
	}
	if len(out) == 0 {
		for k := range probs {
			out = append(out, k)
		}
	}
	return out
}

// randomValue returns a fresh buffer of `size` bytes filled with
// pseudorandom data. Values are drawn from crypto/rand so different
// records are extremely unlikely to collide, which helps diagnose read
// misrouting in post-hoc analysis.
func randomValue(r *mrand.Rand, size int) []byte {
	if size <= 0 {
		size = 128
	}
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err == nil {
		return buf
	}
	// Fallback to math/rand if entropy source unavailable.
	for i := range buf {
		buf[i] = byte(r.Intn(256))
	}
	return buf
}

func normalise(cfg Config) (Config, error) {
	if len(cfg.Targets) == 0 {
		return cfg, errors.New("ycsb: at least one target required")
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 16
	}
	if cfg.OpTimeout <= 0 {
		cfg.OpTimeout = 2 * time.Second
	}
	if cfg.ValueSize <= 0 {
		cfg.ValueSize = 128
	}
	if cfg.Seed == 0 {
		cfg.Seed = time.Now().UnixNano()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Spec.RecordCount <= 0 {
		cfg.Spec.RecordCount = 1000
	}
	if cfg.Spec.Distribution == "" {
		cfg.Spec.Distribution = DistZipfian
	}
	if cfg.Duration == 0 && cfg.Operations == 0 {
		cfg.Duration = 10 * time.Second
	}
	return cfg, nil
}
