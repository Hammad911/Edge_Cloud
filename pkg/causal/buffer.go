package causal

import (
	"sync"

	"edge-cloud-replication/pkg/hlc"
)

// Buffer holds Events whose causal dependencies have not yet been applied
// locally. It maintains a "delivered frontier": a partitioned timestamp that
// componentwise tracks the highest CommitTS we have applied for each origin
// group. An event becomes deliverable when, for every group g present in
// the event's Deps, frontier[g] >= deps[g].
//
// Buffer is the in-memory engine used by both edge and cloud receivers; the
// transport layer feeds it Admit() calls and consumes Deliver() results.
//
// Concurrency: safe for concurrent use.
type Buffer struct {
	mu       sync.Mutex
	pending  []*Event
	frontier map[hlc.GroupID]hlc.Timestamp
	maxSize  int

	// dedup is keyed by (origin, commit_ts) so the same event arriving
	// twice (e.g. cloud re-shipping after a transient network blip) does
	// not get applied twice. EventID is sender-scoped and therefore not
	// reliable for dedup across different paths.
	dedup map[dedupKey]struct{}
}

type dedupKey struct {
	Origin hlc.GroupID
	TS     hlc.Timestamp
}

// BufferOption configures a Buffer.
type BufferOption func(*Buffer)

// WithMaxBuffered bounds the number of events held pending delivery. When
// the bound is exceeded, Admit returns ErrBufferFull and the sender is
// expected to back off. 0 (default) is unbounded.
func WithMaxBuffered(n int) BufferOption {
	return func(b *Buffer) { b.maxSize = n }
}

// WithInitialFrontier seeds the delivered frontier; useful when a receiver
// is restored from a snapshot and already has state for some groups.
func WithInitialFrontier(f hlc.PartitionedTimestamp) BufferOption {
	return func(b *Buffer) {
		for g, ts := range f.Groups {
			b.frontier[g] = ts
		}
	}
}

// NewBuffer constructs an empty buffer.
func NewBuffer(opts ...BufferOption) *Buffer {
	b := &Buffer{
		frontier: make(map[hlc.GroupID]hlc.Timestamp),
		dedup:    make(map[dedupKey]struct{}),
	}
	for _, o := range opts {
		o(b)
	}
	return b
}

// ErrBufferFull is returned by Admit when the buffer is at capacity.
var ErrBufferFull = errBufferFull("causal: buffer full")

type errBufferFull string

func (e errBufferFull) Error() string { return string(e) }

// AdmitResult reports the disposition of an Admit call.
type AdmitResult int

const (
	// AdmitDelivered means the event was deliverable immediately and is
	// included in the next Deliver() result.
	AdmitDelivered AdmitResult = iota
	// AdmitBuffered means the event is held until its dependencies are
	// satisfied.
	AdmitBuffered
	// AdmitDuplicate means we have already seen and applied this event
	// (matched by origin + commit_ts).
	AdmitDuplicate
)

// Admit places an event into the buffer. It returns the disposition; on
// AdmitDelivered the caller should immediately invoke Deliver() to drain
// any newly-deliverable events (the just-admitted one plus any unblocked
// followers).
//
// Admit returns ErrBufferFull when the buffer is bounded and full.
func (b *Buffer) Admit(e *Event) (AdmitResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := dedupKey{Origin: e.Origin, TS: e.CommitTS}
	if _, ok := b.dedup[key]; ok {
		return AdmitDuplicate, nil
	}
	if cur, ok := b.frontier[e.Origin]; ok && !e.CommitTS.After(cur) {
		// Frontier already past this event's CommitTS: the originator's
		// own contribution to the partitioned vector covers it. Treat
		// as duplicate; nothing to apply.
		b.dedup[key] = struct{}{}
		return AdmitDuplicate, nil
	}

	if b.maxSize > 0 && len(b.pending) >= b.maxSize {
		return 0, ErrBufferFull
	}
	b.pending = append(b.pending, e)
	if b.depsSatisfiedLocked(e) {
		return AdmitDelivered, nil
	}
	return AdmitBuffered, nil
}

// Deliver returns all events whose dependencies are now satisfied, in
// causal order, and removes them from the buffer. It also advances the
// delivered frontier to reflect the returned events.
//
// Callers should call Deliver after every Admit that returned
// AdmitDelivered, and after any external advance of the frontier (e.g. a
// local write that may unblock a remote event awaiting our origin).
func (b *Buffer) Deliver() []*Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.drainLocked()
}

// AdvanceFrontier records that the local layer has applied a write at
// (origin, ts). This may unblock buffered events waiting on origin. It
// does NOT drain; the caller should follow up with Deliver() (the
// Replicator's delivery loop does this on every Notify()).
func (b *Buffer) AdvanceFrontier(origin hlc.GroupID, ts hlc.Timestamp) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cur := b.frontier[origin]
	if ts.After(cur) {
		b.frontier[origin] = ts
	}
}

// Frontier returns a snapshot of the delivered frontier.
func (b *Buffer) Frontier() hlc.PartitionedTimestamp {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := hlc.PartitionedTimestamp{Groups: make(map[hlc.GroupID]hlc.Timestamp, len(b.frontier))}
	for g, t := range b.frontier {
		out.Groups[g] = t
	}
	return out
}

// PendingCount reports the number of buffered events not yet deliverable.
func (b *Buffer) PendingCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending)
}

// drainLocked repeatedly walks pending and pulls out every event whose
// deps are satisfied. Each delivered event advances the frontier, which
// may unblock further events; we loop until a full pass yields nothing.
func (b *Buffer) drainLocked() []*Event {
	var out []*Event
	for {
		progressed := false
		kept := b.pending[:0]
		for _, ev := range b.pending {
			if !b.depsSatisfiedLocked(ev) {
				kept = append(kept, ev)
				continue
			}
			out = append(out, ev)
			b.dedup[dedupKey{Origin: ev.Origin, TS: ev.CommitTS}] = struct{}{}
			cur := b.frontier[ev.Origin]
			if ev.CommitTS.After(cur) {
				b.frontier[ev.Origin] = ev.CommitTS
			}
			progressed = true
		}
		b.pending = kept
		if !progressed {
			return out
		}
	}
}

// depsSatisfiedLocked checks every group entry in deps against the local
// frontier. The originator's own contribution is allowed to be exactly
// CommitTS - 1 in the strict sense, but since Deps always represents
// "happens-before this commit", an entry equal to or below the local
// frontier is sufficient.
func (b *Buffer) depsSatisfiedLocked(e *Event) bool {
	for g, dep := range e.Deps.Groups {
		if g == e.Origin {
			// The originator's own contribution to Deps is the prior
			// state; the event's CommitTS is by construction after it.
			cur := b.frontier[g]
			if dep.After(cur) {
				return false
			}
			continue
		}
		cur := b.frontier[g]
		if dep.After(cur) {
			return false
		}
	}
	return true
}
