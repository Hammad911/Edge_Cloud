// Package ycsb implements a closed-loop, YCSB-style workload driver that
// talks gRPC to the real edge-node binaries.
//
// It intentionally mirrors only the pieces of YCSB that are meaningful for
// a causally-consistent KV store (Workloads A/B/C/F), and adds native
// support for per-worker session stickiness + causal tokens so the
// read-your-writes / monotonic-reads guarantees of the server can be
// exercised end-to-end.
package ycsb

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
)

// KeyDistribution selects which statistical distribution is used to pick
// keys for read and update operations.
type KeyDistribution string

const (
	// DistUniform picks keys uniformly at random from the loaded range.
	DistUniform KeyDistribution = "uniform"
	// DistZipfian picks keys using a Zipfian distribution, concentrating
	// traffic on a small hot set (parameter `theta`, default 0.99).
	DistZipfian KeyDistribution = "zipfian"
	// DistLatest is Zipfian over the most recently inserted keys, as
	// defined by workload D in the original YCSB paper.
	DistLatest KeyDistribution = "latest"
)

// KeyGen returns the next key index in [0, n).
type KeyGen interface {
	Next(r *rand.Rand) int
}

// UniformGen selects keys uniformly.
type UniformGen struct{ N int }

// Next returns a uniformly random index.
func (u UniformGen) Next(r *rand.Rand) int {
	if u.N <= 0 {
		return 0
	}
	return r.Intn(u.N)
}

// ZipfianGen selects keys using a static Zipfian distribution over [0, N).
// It is a direct Go port of the textbook algorithm from
// "Quickly generating billion-record synthetic databases" (Gray et al. 1994)
// and matches the default theta=0.99 used by YCSB.
//
// ZipfianGen is safe for concurrent use because it holds no mutable state.
type ZipfianGen struct {
	N     int
	Theta float64

	// precomputed constants
	alpha  float64
	zeta2  float64
	zetaN  float64
	eta    float64
	cached bool
	cacheN int
}

// NewZipfian constructs a ZipfianGen with the given population size.
// If theta <= 0 a reasonable default of 0.99 is used.
func NewZipfian(n int, theta float64) *ZipfianGen {
	if n <= 0 {
		n = 1
	}
	if theta <= 0 {
		theta = 0.99
	}
	z := &ZipfianGen{N: n, Theta: theta}
	z.recompute()
	return z
}

func (z *ZipfianGen) recompute() {
	z.zeta2 = zeta(2, z.Theta)
	z.zetaN = zeta(z.N, z.Theta)
	z.alpha = 1.0 / (1.0 - z.Theta)
	z.eta = (1.0 - math.Pow(2.0/float64(z.N), 1.0-z.Theta)) /
		(1.0 - z.zeta2/z.zetaN)
	z.cached = true
	z.cacheN = z.N
}

// Next draws one key index from the Zipfian distribution.
func (z *ZipfianGen) Next(r *rand.Rand) int {
	if !z.cached || z.cacheN != z.N {
		z.recompute()
	}
	u := r.Float64()
	uz := u * z.zetaN
	switch {
	case uz < 1.0:
		return 0
	case uz < 1.0+math.Pow(0.5, z.Theta):
		return 1
	}
	ret := int(float64(z.N) * math.Pow(z.eta*u-z.eta+1.0, z.alpha))
	if ret < 0 {
		return 0
	}
	if ret >= z.N {
		return z.N - 1
	}
	return ret
}

// zeta computes the generalized harmonic number H(n, theta).
func zeta(n int, theta float64) float64 {
	var sum float64
	for i := 1; i <= n; i++ {
		sum += 1.0 / math.Pow(float64(i), theta)
	}
	return sum
}

// LatestGen wraps a Zipfian distribution over the most recently inserted
// keys. It is fed by a monotonically increasing tail provided by the
// runner (inserts bump `tail`, reads are biased toward the tail end).
type LatestGen struct {
	mu   sync.RWMutex
	tail int
	zipf *ZipfianGen
}

// NewLatest returns a LatestGen seeded with the initial population size.
func NewLatest(n int) *LatestGen {
	return &LatestGen{tail: n, zipf: NewZipfian(n, 0.99)}
}

// Advance bumps the population size. Called by the runner after each
// insert so subsequent reads can see the new key.
func (l *LatestGen) Advance() {
	l.mu.Lock()
	l.tail++
	l.zipf.N = l.tail
	l.mu.Unlock()
}

// Tail returns the current size of the population.
func (l *LatestGen) Tail() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.tail
}

// Next picks a hot (recent) key biased toward the tail.
func (l *LatestGen) Next(r *rand.Rand) int {
	l.mu.RLock()
	tail := l.tail
	offset := l.zipf.Next(r)
	l.mu.RUnlock()
	if tail == 0 {
		return 0
	}
	idx := tail - 1 - offset
	if idx < 0 {
		idx = 0
	}
	return idx
}

// NewKeyGen builds a KeyGen for the given distribution and population.
func NewKeyGen(dist KeyDistribution, n int) KeyGen {
	switch dist {
	case DistZipfian:
		return NewZipfian(n, 0.99)
	case DistLatest:
		return NewLatest(n)
	case DistUniform, "":
		return UniformGen{N: n}
	default:
		return UniformGen{N: n}
	}
}

// FormatKey renders the key-space index using the conventional YCSB layout
// (`user<12-digit hex>`). It is stable across runs and languages so
// offline tools can correlate traces.
func FormatKey(prefix string, idx int) string {
	if prefix == "" {
		prefix = "user"
	}
	return fmt.Sprintf("%s%012d", prefix, idx)
}
