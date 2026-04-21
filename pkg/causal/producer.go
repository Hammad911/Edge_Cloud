package causal

import (
	"context"
	"sync"
	"sync/atomic"

	"edge-cloud-replication/pkg/hlc"
)

// Producer is the bridge between the local KV layer and the causal
// replication transport. The kv.Service (or, in the raft world, the FSM
// after a successful Apply) calls Publish on every successful local
// mutation. The Producer stamps the event with a fresh partitioned-HLC
// frontier and enqueues it in the Outbox for shipping to peers.
//
// Producer is concurrency-safe.
type Producer struct {
	clock    *hlc.PartitionedClock
	outbox   *Outbox
	senderID string

	nextID atomic.Uint64
}

// NewProducer constructs a Producer. SenderID identifies this node in
// outbound events (it is distinct from origin, which always names the
// originator GROUP, not the individual node).
func NewProducer(clock *hlc.PartitionedClock, outbox *Outbox, senderID string) *Producer {
	if clock == nil {
		panic("causal.NewProducer: clock must not be nil")
	}
	if outbox == nil {
		panic("causal.NewProducer: outbox must not be nil")
	}
	return &Producer{
		clock:    clock,
		outbox:   outbox,
		senderID: senderID,
	}
}

// Publish records a successful local mutation as a replication event. The
// caller passes the HLC timestamp the local store assigned (so that the
// event's CommitTS matches the locally-stored version). The Producer
// stamps the event's Deps from the current partitioned-HLC frontier
// observed BEFORE this write; the receiver uses Deps to decide when this
// event is causally deliverable.
func (p *Producer) Publish(key string, value []byte, deleted bool, ts hlc.Timestamp) *Event {
	own := p.clock.OwnGroup()

	// Snapshot Deps as the frontier BEFORE this write. If we included the
	// new ts in Deps, the event would depend on itself: a receiver that
	// has never seen `own` would never be able to satisfy
	// frontier[own] >= ts because applying the event is what advances
	// the frontier. Pre-snapshot deps means receivers see "the event
	// depends on the originator already being at the previous ts", which
	// is satisfied by either applying the prior event or, for the very
	// first event, an empty deps map.
	deps := p.clock.Peek()
	deps.Origin = own

	// Now advance our ownGroup entry so SUBSEQUENT events from this
	// origin carry the new ts in their Deps. We use StampAt rather than
	// Merge because Merge intentionally does not touch the ownGroup
	// (see PartitionedClock.Merge for the rationale).
	p.clock.StampAt(ts)

	id := p.nextID.Add(1)
	ev := &Event{
		EventID:  id,
		SenderID: p.senderID,
		Origin:   own,
		Key:      key,
		Value:    cloneBytes(value),
		Deleted:  deleted,
		CommitTS: ts,
		Deps:     deps,
	}
	p.outbox.Enqueue(ev)
	return ev
}

// PublishRemote records that a remote event was applied locally, so it can
// be forwarded to other peers (this is what lets the cloud hub fan out
// edge-A's writes to edge-B). It does NOT re-stamp Deps - the original
// event's deps are propagated unchanged - but it does update the SenderID
// to this node so cycles can be broken.
func (p *Producer) PublishRemote(e *Event) *Event {
	id := p.nextID.Add(1)
	out := &Event{
		EventID:  id,
		SenderID: p.senderID,
		Origin:   e.Origin,
		Key:      e.Key,
		Value:    cloneBytes(e.Value),
		Deleted:  e.Deleted,
		CommitTS: e.CommitTS,
		Deps:     e.Deps.Clone(),
	}
	p.outbox.Enqueue(out)
	return out
}

// Logger is set lazily by Replicator.New to avoid plumbing it through the
// Producer constructor signature; it is purely for diagnostic logs and
// safe to leave nil in tests that construct a Producer by hand.
//
// Outbox is a bounded, multi-consumer FIFO of events awaiting shipment.
// Each Sender goroutine consumes from its own private slice (replenished
// from the central queue), so a slow peer cannot block the others.
//
// For Milestone 5 we take the simplest correct approach: a shared FIFO
// with per-subscriber checkpoints. Subscribers register a Cursor and read
// monotonically from it; the Outbox prunes events once every cursor has
// advanced past them.
type Outbox struct {
	mu      sync.Mutex
	cond    *sync.Cond
	events  []outboxEntry
	maxSize int
	closed  bool

	// minCursor across all live subscribers; entries with id <= minCursor
	// are eligible for pruning.
	subscribers map[string]uint64
}

type outboxEntry struct {
	id uint64
	ev *Event
}

// NewOutbox constructs an Outbox holding up to maxSize events. 0 is
// unbounded.
func NewOutbox(maxSize int) *Outbox {
	o := &Outbox{
		maxSize:     maxSize,
		subscribers: make(map[string]uint64),
	}
	o.cond = sync.NewCond(&o.mu)
	return o
}

// Enqueue appends an event. When the outbox is bounded and full the
// oldest event is dropped (with a future hook for backpressure). Drops
// are bad — they violate causal guarantees once a peer has missed an
// event — but for now the bound is purely a memory safety valve.
func (o *Outbox) Enqueue(e *Event) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	o.events = append(o.events, outboxEntry{id: e.EventID, ev: e})
	if o.maxSize > 0 && len(o.events) > o.maxSize {
		o.events = o.events[len(o.events)-o.maxSize:]
	}
	o.cond.Broadcast()
}

// Subscribe registers a named subscriber and returns its initial cursor
// (always 0, meaning "send me everything from the start"). Subscribers
// must call Unsubscribe when they shut down so the outbox can prune.
func (o *Outbox) Subscribe(name string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.subscribers[name]; !ok {
		o.subscribers[name] = 0
	}
}

// Unsubscribe removes a subscriber. Pending events are not pruned by
// this call (they may still be needed by other subscribers); the next
// Enqueue triggers prune.
func (o *Outbox) Unsubscribe(name string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.subscribers, name)
	o.cond.Broadcast()
}

// Pending returns the number of events in the outbox that this
// subscriber has not yet acked (i.e. have id > its cursor). Returns 0
// when the subscriber is unknown or no buffered events qualify.
func (o *Outbox) Pending(name string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	cursor, ok := o.subscribers[name]
	if !ok {
		return 0
	}
	var n int
	for _, e := range o.events {
		if e.id > cursor {
			n++
		}
	}
	return n
}

// TotalPending returns the aggregate number of (subscriber, event) pairs
// that have not yet been acked. Useful as a strong "everything is
// settled" signal for tests and offline checkers.
func (o *Outbox) TotalPending() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	var n int
	for _, cursor := range o.subscribers {
		for _, e := range o.events {
			if e.id > cursor {
				n++
			}
		}
	}
	return n
}

// Next blocks until an event with id > cursor is available for the named
// subscriber, then returns the next such event and the new cursor. Returns
// nil, 0, ctx.Err() when the context is cancelled or the outbox is closed.
//
// Next DOES NOT advance the subscriber's prune watermark - that happens in
// Ack once the caller confirms delivery. Driving prune from Next would mean
// a subscriber that observed an event but failed to ship it (partition,
// network error, crash mid-send) could see its event pruned before the
// retry finds it, silently losing the write. Callers that never retry
// can call Ack immediately after Next to preserve the old semantics.
func (o *Outbox) Next(ctx context.Context, name string, cursor uint64) (*Event, uint64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	stopCtx := contextWatch(ctx, o.cond)
	defer stopCtx()

	for {
		if o.closed {
			return nil, cursor, ErrOutboxClosed
		}
		if err := ctx.Err(); err != nil {
			return nil, cursor, err
		}
		for _, entry := range o.events {
			if entry.id > cursor {
				return entry.ev, entry.id, nil
			}
		}
		o.cond.Wait()
	}
}

// Ack advances the named subscriber's prune watermark to id. Callers
// should invoke Ack after a successful send so the outbox can garbage-
// collect events every subscriber has durably received.
func (o *Outbox) Ack(name string, id uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if cur, ok := o.subscribers[name]; ok && cur < id {
		o.subscribers[name] = id
		o.pruneLocked()
	}
}

// Close stops the outbox; all blocked Next calls return ErrOutboxClosed.
func (o *Outbox) Close() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed = true
	o.cond.Broadcast()
}

// pruneLocked drops events whose id <= every subscriber's cursor. With no
// subscribers, nothing is pruned (so a late-joining subscriber still sees
// history). This is the simplest correct policy; a production system would
// also expire by age or size.
func (o *Outbox) pruneLocked() {
	if len(o.subscribers) == 0 {
		return
	}
	min := uint64(0)
	first := true
	for _, c := range o.subscribers {
		if first || c < min {
			min = c
			first = false
		}
	}
	keep := o.events[:0]
	for _, e := range o.events {
		if e.id > min {
			keep = append(keep, e)
		}
	}
	o.events = keep
}

// ErrOutboxClosed is returned by Next after Close has been invoked.
var ErrOutboxClosed = errOutboxClosed("causal: outbox closed")

type errOutboxClosed string

func (e errOutboxClosed) Error() string { return string(e) }

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// contextWatch wires ctx.Done into a cond broadcast so Wait wakes up on
// cancellation. It returns a cleanup function the caller must defer.
func contextWatch(ctx context.Context, cond *sync.Cond) func() {
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			cond.L.Lock()
			cond.Broadcast()
			cond.L.Unlock()
		case <-stop:
		}
	}()
	return func() { close(stop) }
}
