// Package hlc implements Hybrid Logical Clocks (Kulkarni et al., 2014) and a
// partitioned variant used for cross-cluster causal replication.
//
// The baseline Clock is the classic HLC: a single scalar timestamp that
// combines physical time with a logical counter, preserving causality across
// events that share a happens-before relationship while staying close to
// wall-clock time.
//
// PartitionedClock is the scalability lever from the project proposal. Instead
// of maintaining one logical component per replica (O(n) metadata, as in a
// per-replica vector clock), it maintains one component per *datacenter group*.
// At 200 edge sites arranged into 20 DC groups, that is a 10x reduction in
// per-write metadata - and the overhead grows with the number of groups, not
// the number of sites, which lets us hit the O(log n) target in the proposal.
package hlc

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// PhysicalClock returns a monotonic-ish wall-clock time in nanoseconds since
// the Unix epoch. It is abstracted so tests can inject a deterministic clock.
type PhysicalClock func() int64

// SystemClock is the default PhysicalClock backed by time.Now().UnixNano().
func SystemClock() int64 { return time.Now().UnixNano() }

// Timestamp is a single HLC reading. It is comparable with ==, ordered by
// Compare, and cheap to copy (16 bytes). Serialization is the caller's job.
type Timestamp struct {
	// Physical is the physical component in nanoseconds since the Unix epoch.
	Physical int64
	// Logical disambiguates events that share a physical component and
	// captures causality jumps when incoming timestamps run ahead of local
	// physical time.
	Logical uint32
}

// Zero reports whether the timestamp is the zero value (pre-initialised).
func (t Timestamp) Zero() bool { return t.Physical == 0 && t.Logical == 0 }

// Compare returns -1/0/+1 by (physical, logical) lexicographic order.
func (t Timestamp) Compare(o Timestamp) int {
	switch {
	case t.Physical < o.Physical:
		return -1
	case t.Physical > o.Physical:
		return 1
	case t.Logical < o.Logical:
		return -1
	case t.Logical > o.Logical:
		return 1
	default:
		return 0
	}
}

// Before reports whether t strictly precedes o.
func (t Timestamp) Before(o Timestamp) bool { return t.Compare(o) < 0 }

// After reports whether t strictly follows o.
func (t Timestamp) After(o Timestamp) bool { return t.Compare(o) > 0 }

// Equal reports whether the two timestamps are equal.
func (t Timestamp) Equal(o Timestamp) bool { return t == o }

// String formats the timestamp as "physical.logical".
func (t Timestamp) String() string {
	return fmt.Sprintf("%d.%d", t.Physical, t.Logical)
}

// Clock is a Hybrid Logical Clock. All methods are safe for concurrent use.
//
// A Clock has three operations:
//
//   - Now()       - produce a fresh local event (e.g. a local write).
//   - Update(rt)  - incorporate a remote event's timestamp on receive.
//   - Peek()      - observe current time without advancing.
//
// The HLC invariant: for any local event e followed by a local event f,
// e.Now() < f.Now(). For any local event f that follows receipt of a remote
// event with timestamp r, we have r < f.Now().
type Clock struct {
	phys PhysicalClock

	mu    sync.Mutex
	state Timestamp

	// maxDrift bounds how far the logical component may advance when remote
	// timestamps run arbitrarily into the future (e.g. due to a badly skewed
	// peer). If exceeded, Update returns ErrClockDrift.
	maxDrift time.Duration
}

// Option configures a Clock at construction time.
type Option func(*Clock)

// WithPhysicalClock overrides the wall-clock source (for testing).
func WithPhysicalClock(p PhysicalClock) Option { return func(c *Clock) { c.phys = p } }

// WithMaxDrift bounds the allowed forward drift when receiving remote
// timestamps. The default is 5 minutes, chosen as a conservative upper bound
// for the proposal's WAN-connected deployments - a remote timestamp more than
// five minutes ahead of local physical time indicates either a severely
// skewed peer or corruption, and should be rejected rather than silently
// absorbed.
func WithMaxDrift(d time.Duration) Option { return func(c *Clock) { c.maxDrift = d } }

// ErrClockDrift is returned by Update when a remote timestamp exceeds the
// configured maximum drift.
var ErrClockDrift = errors.New("hlc: remote timestamp exceeds max drift")

// New constructs a Clock with sensible defaults. Call options to customise.
func New(opts ...Option) *Clock {
	c := &Clock{
		phys:     SystemClock,
		maxDrift: 5 * time.Minute,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Now advances the clock and returns the next local event timestamp.
//
// Implements the canonical HLC send/local step:
//
//	pt  := physical_clock()
//	if pt > state.physical:
//	    state = (pt, 0)
//	else:
//	    state = (state.physical, state.logical + 1)
//	return state
func (c *Clock) Now() Timestamp {
	pt := c.phys()
	c.mu.Lock()
	defer c.mu.Unlock()

	if pt > c.state.Physical {
		c.state = Timestamp{Physical: pt, Logical: 0}
	} else {
		c.state.Logical++
	}
	return c.state
}

// Update merges a received remote timestamp into the local state and returns
// the resulting local timestamp (i.e. the timestamp that should be stamped on
// any subsequent local event caused by this receive).
//
// Implements the canonical HLC receive step:
//
//	pt := physical_clock()
//	l' := max(state.physical, remote.physical, pt)
//	if l' == state.physical == remote.physical:
//	    logical = max(state.logical, remote.logical) + 1
//	elif l' == state.physical:
//	    logical = state.logical + 1
//	elif l' == remote.physical:
//	    logical = remote.logical + 1
//	else:
//	    logical = 0
//	state = (l', logical)
func (c *Clock) Update(remote Timestamp) (Timestamp, error) {
	pt := c.phys()
	c.mu.Lock()
	defer c.mu.Unlock()

	if remote.Physical > pt && time.Duration(remote.Physical-pt) > c.maxDrift {
		return c.state, fmt.Errorf("%w: remote=%d local=%d drift=%s",
			ErrClockDrift, remote.Physical, pt, time.Duration(remote.Physical-pt))
	}

	maxPhys := pt
	if c.state.Physical > maxPhys {
		maxPhys = c.state.Physical
	}
	if remote.Physical > maxPhys {
		maxPhys = remote.Physical
	}

	var logical uint32
	switch {
	case maxPhys == c.state.Physical && maxPhys == remote.Physical:
		if c.state.Logical > remote.Logical {
			logical = c.state.Logical + 1
		} else {
			logical = remote.Logical + 1
		}
	case maxPhys == c.state.Physical:
		logical = c.state.Logical + 1
	case maxPhys == remote.Physical:
		logical = remote.Logical + 1
	default:
		logical = 0
	}

	c.state = Timestamp{Physical: maxPhys, Logical: logical}
	return c.state, nil
}

// Peek returns the current state without advancing it.
func (c *Clock) Peek() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}
