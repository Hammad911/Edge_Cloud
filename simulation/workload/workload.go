// Package workload generates synthetic operations against simulator
// sites. The two distributions implemented here cover the bulk of
// edge-cloud replication papers: uniform (each key equally likely) and
// Zipfian (a few hot keys absorb most traffic).
package workload

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"edge-cloud-replication/pkg/hlc"
	"edge-cloud-replication/simulation/site"
)

// OpKind enumerates the operations the workload may issue.
type OpKind int

const (
	OpPut OpKind = iota
	OpGet
	OpDelete
)

// Op is a single generated operation. Sites returned by Generator pick
// values per call; the workload runner is responsible for picking which
// site issues it.
type Op struct {
	Kind  OpKind
	Key   string
	Value []byte
}

// Generator returns the next Op to run. Implementations must be
// goroutine-safe.
type Generator interface {
	Next() Op
}

// Spec configures the high-level workload mix.
type Spec struct {
	NumKeys      int
	WriteRatio   float64 // fraction of ops that are Put
	DeleteRatio  float64 // fraction of ops that are Delete (subset of writes? no: independent)
	ValueSize    int
	Distribution Distribution
	Seed         int64
}

// Distribution controls how keys are picked.
type Distribution int

const (
	Uniform Distribution = iota
	Zipfian
)

// NewGenerator constructs a Generator from a Spec.
func NewGenerator(spec Spec) Generator {
	if spec.NumKeys <= 0 {
		spec.NumKeys = 1000
	}
	if spec.ValueSize <= 0 {
		spec.ValueSize = 32
	}
	rng := rand.New(rand.NewSource(spec.Seed))
	g := &generator{
		spec: spec,
		rng:  rng,
	}
	if spec.Distribution == Zipfian {
		// Zipf with s=1.1, v=1 over [0, NumKeys-1]. The std lib's
		// Zipf is parameterised slightly differently; we use it as-is.
		g.zipf = rand.NewZipf(rng, 1.1, 1.0, uint64(spec.NumKeys-1))
	}
	return g
}

type generator struct {
	spec Spec
	mu   sync.Mutex
	rng  *rand.Rand
	zipf *rand.Zipf
}

func (g *generator) Next() Op {
	g.mu.Lock()
	defer g.mu.Unlock()

	var keyIdx uint64
	if g.spec.Distribution == Zipfian && g.zipf != nil {
		keyIdx = g.zipf.Uint64()
	} else {
		keyIdx = uint64(g.rng.Intn(g.spec.NumKeys))
	}
	key := fmt.Sprintf("k%08d", keyIdx)

	r := g.rng.Float64()
	switch {
	case r < g.spec.DeleteRatio:
		return Op{Kind: OpDelete, Key: key}
	case r < g.spec.DeleteRatio+g.spec.WriteRatio:
		val := make([]byte, g.spec.ValueSize)
		g.rng.Read(val)
		return Op{Kind: OpPut, Key: key, Value: val}
	default:
		return Op{Kind: OpGet, Key: key}
	}
}

// RunSpec parameterises Run.
type RunSpec struct {
	Sites       []*site.Site
	Generator   Generator
	Concurrency int
	Duration    time.Duration
	// QPSPerWorker caps each worker's issue rate. <=0 means uncapped.
	QPSPerWorker float64
	// Hook is called after every applied op; nil means no callback.
	// Hooks must be goroutine-safe.
	Hook OpCallback
	// Observer, if non-nil, receives a rich result for every op,
	// including the value+timestamp returned by Gets. Used by the
	// offline history recorder. Safe to pass nil.
	Observer OpObserver
	// PinSessions makes each worker goroutine talk to exactly one
	// site for the entire run (round-robin assignment). This models
	// the standard causal-consistency assumption where a logical
	// session sticks to a replica. Without pinning, workers randomly
	// hop between replicas, which is a legitimate anti-pattern that
	// breaks monotonic-reads / read-your-writes guarantees whenever
	// replication is still in flight.
	PinSessions bool
}

// OpCallback observes each completed op for metrics or causality
// tracking. Latency is wall-time of the local mutation (not replication).
type OpCallback func(siteIdx int, op Op, latency time.Duration, err error)

// OpResult captures everything the checker needs about a completed op:
// for writes the HLC commit ts; for reads the value and the ts of the
// observed version.
type OpResult struct {
	WriteTS  hlc.Timestamp
	ReadValue []byte
	ReadTS   hlc.Timestamp
	Err      error
}

// OpObserver is invoked after every op with the full outcome. Must be
// goroutine-safe; the workload runner calls it from many workers.
type OpObserver func(siteIdx int, session string, op Op, latency time.Duration, res OpResult)

// Result aggregates run-time statistics.
type Result struct {
	TotalOps  int64
	PutOps    int64
	GetOps    int64
	DeleteOps int64
	Errors    int64
	WallTime  time.Duration
}

// Run executes the workload for spec.Duration across spec.Concurrency
// goroutines. Returns when the deadline expires.
func Run(ctx context.Context, spec RunSpec) Result {
	if spec.Concurrency <= 0 {
		spec.Concurrency = 8
	}
	if len(spec.Sites) == 0 {
		return Result{}
	}
	ctx, cancel := context.WithTimeout(ctx, spec.Duration)
	defer cancel()

	var (
		total, puts, gets, dels, errs int64
		wg                            sync.WaitGroup
	)
	start := time.Now()

	pickSite := func(idx int) (int, *site.Site) {
		i := idx % len(spec.Sites)
		return i, spec.Sites[i]
	}

	for w := 0; w < spec.Concurrency; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			var ticker *time.Ticker
			if spec.QPSPerWorker > 0 {
				period := time.Duration(float64(time.Second) / spec.QPSPerWorker)
				ticker = time.NewTicker(period)
				defer ticker.Stop()
			}
			rng := rand.New(rand.NewSource(int64(w*7919 + 1)))
			session := fmt.Sprintf("w%04d", w)
			pinnedIdx := w % len(spec.Sites)
			for {
				if ctx.Err() != nil {
					return
				}
				if ticker != nil {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
					}
				}
				op := spec.Generator.Next()
				var (
					idx int
					st  *site.Site
				)
				if spec.PinSessions {
					idx, st = pickSite(pinnedIdx)
				} else {
					idx, st = pickSite(rng.Intn(len(spec.Sites)))
				}
				t0 := time.Now()
				var (
					err     error
					res     OpResult
					gotVal  []byte
					gotTS   hlc.Timestamp
					writeTS hlc.Timestamp
				)
				switch op.Kind {
				case OpPut:
					writeTS, err = st.Put(ctx, op.Key, op.Value)
					atomic.AddInt64(&puts, 1)
				case OpDelete:
					writeTS, err = st.Delete(ctx, op.Key)
					atomic.AddInt64(&dels, 1)
				case OpGet:
					gotVal, gotTS, err = st.Get(ctx, op.Key)
					if err == site.ErrNotFound {
						err = nil
					}
					atomic.AddInt64(&gets, 1)
				}
				lat := time.Since(t0)
				if err != nil && ctx.Err() == nil {
					atomic.AddInt64(&errs, 1)
				}
				atomic.AddInt64(&total, 1)
				res = OpResult{WriteTS: writeTS, ReadValue: gotVal, ReadTS: gotTS, Err: err}
				if spec.Hook != nil {
					spec.Hook(idx, op, lat, err)
				}
				if spec.Observer != nil {
					spec.Observer(idx, session, op, lat, res)
				}
			}
		}()
	}
	wg.Wait()
	return Result{
		TotalOps:  total,
		PutOps:    puts,
		GetOps:    gets,
		DeleteOps: dels,
		Errors:    errs,
		WallTime:  time.Since(start),
	}
}

// ZipfHotKeyShare is a sanity helper exposed for the metrics package: it
// returns the expected fraction of ops that hit the top-k keys under a
// Zipf(s=1.1) distribution over n keys.
func ZipfHotKeyShare(n, k int) float64 {
	if k <= 0 || n <= 0 {
		return 0
	}
	if k > n {
		k = n
	}
	const s = 1.1
	var num, den float64
	for i := 1; i <= n; i++ {
		w := math.Pow(float64(i), -s)
		den += w
		if i <= k {
			num += w
		}
	}
	return num / den
}
