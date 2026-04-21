// Package network is an in-memory message bus that simulates a wide-area
// network for the edge-cloud simulator. It models per-link latency (mean +
// jitter), packet loss, and partitions, while keeping the full simulation
// in a single process so we can scale to hundreds of sites without
// touching a real socket.
//
// Sites are addressed by short string IDs (e.g. "edge-0042", "cloud").
// Messages are delivered to the destination's inbox channel after the
// link's latency; partitioned links silently drop messages.
package network

import (
	"container/heap"
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

// Address is an opaque, simulator-scoped identifier for a site.
type Address string

// Message is the unit of delivery on the simulated network. Payload is
// opaque to the network layer; sites encode their own protocol bytes
// (e.g. a serialized causal.Event).
type Message struct {
	From    Address
	To      Address
	Payload any
	// SentAt is when the source called Send; the receiver can compute
	// observed latency as time.Since(SentAt). Useful for measuring
	// replication lag end-to-end.
	SentAt time.Time
}

// LinkProfile describes the propagation characteristics of a directed
// edge in the network. The profile is consulted on every Send; changing
// it at runtime takes effect for subsequent sends only.
type LinkProfile struct {
	// MeanLatency is the deterministic minimum delivery delay.
	MeanLatency time.Duration
	// Jitter is added uniformly in [-Jitter, +Jitter] (clipped at 0).
	Jitter time.Duration
	// LossRate is the probability in [0,1] that a message is silently
	// dropped instead of delivered.
	LossRate float64
}

// Default returns a sensible profile for a wide-area link.
func DefaultLinkProfile() LinkProfile {
	return LinkProfile{
		MeanLatency: 25 * time.Millisecond,
		Jitter:      5 * time.Millisecond,
		LossRate:    0,
	}
}

// Network connects a set of sites. It is goroutine-safe; multiple sites
// (and the simulator harness) call Send and Recv concurrently.
//
// Delivery scheduling is centralised in a single background goroutine
// that pops a min-heap of pending messages, instead of spawning one
// goroutine per Send. This keeps the goroutine count flat at O(N_sites)
// rather than O(in_flight_messages), which matters at 500-site scale.
type Network struct {
	mu        sync.RWMutex
	sites     map[Address]chan Message
	links     map[linkKey]LinkProfile
	defaultLP LinkProfile
	partition map[linkKey]bool
	closed    bool

	rng *rand.Rand

	inboxBuffer int

	schedMu   sync.Mutex
	schedCV   *sync.Cond
	pending   pendingHeap
	stopSched chan struct{}

	// metrics
	sent      int64
	delivered int64
	dropped   int64
}

type linkKey struct {
	from, to Address
}

// Option configures a Network at construction time.
type Option func(*Network)

// WithDefaultLink sets the link profile used for any (from, to) pair that
// does not have an explicit override.
func WithDefaultLink(p LinkProfile) Option {
	return func(n *Network) { n.defaultLP = p }
}

// WithSeed makes the network's RNG deterministic for reproducible runs.
func WithSeed(seed int64) Option {
	return func(n *Network) { n.rng = rand.New(rand.NewSource(seed)) }
}

// WithInboxBuffer overrides the default inbox channel buffer size. Larger
// buffers absorb bursts; senders block (rather than drop) when full.
func WithInboxBuffer(n int) Option {
	return func(net *Network) { net.inboxBuffer = n }
}

// New builds an empty Network and starts its delivery scheduler.
func New(opts ...Option) *Network {
	n := &Network{
		sites:       make(map[Address]chan Message),
		links:       make(map[linkKey]LinkProfile),
		defaultLP:   DefaultLinkProfile(),
		partition:   make(map[linkKey]bool),
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
		inboxBuffer: 65536,
		stopSched:   make(chan struct{}),
	}
	n.schedCV = sync.NewCond(&n.schedMu)
	for _, o := range opts {
		o(n)
	}
	go n.scheduler()
	return n
}

// Register attaches a site with the given address and returns its inbox
// channel. The caller is the only authorized reader.
func (n *Network) Register(addr Address) <-chan Message {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.sites[addr]; ok {
		panic("network: address " + addr + " already registered")
	}
	ch := make(chan Message, n.inboxBuffer)
	n.sites[addr] = ch
	return ch
}

// SetLink configures the profile for a specific directed link. Use Link
// (which calls SetLink in both directions) for symmetric WAN links.
func (n *Network) SetLink(from, to Address, p LinkProfile) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.links[linkKey{from: from, to: to}] = p
}

// Link configures both directions of a symmetric link.
func (n *Network) Link(a, b Address, p LinkProfile) {
	n.SetLink(a, b, p)
	n.SetLink(b, a, p)
}

// Partition removes the directed link from -> to. Sends along it are
// silently dropped until Heal restores the link.
func (n *Network) Partition(from, to Address) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partition[linkKey{from: from, to: to}] = true
}

// Heal restores a directed link previously Partitioned.
func (n *Network) Heal(from, to Address) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.partition, linkKey{from: from, to: to})
}

// PartitionAll partitions a site away from the network in both
// directions. Equivalent to a "node down".
func (n *Network) PartitionAll(addr Address) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for peer := range n.sites {
		if peer == addr {
			continue
		}
		n.partition[linkKey{from: addr, to: peer}] = true
		n.partition[linkKey{from: peer, to: addr}] = true
	}
}

// HealAll undoes PartitionAll.
func (n *Network) HealAll(addr Address) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for peer := range n.sites {
		delete(n.partition, linkKey{from: addr, to: peer})
		delete(n.partition, linkKey{from: peer, to: addr})
	}
}

// Send schedules delivery of a message from src to dst. Returns
// ErrUnknownAddress if dst is not registered or ErrClosed after Close.
// Once scheduled, delivery is asynchronous: the message is enqueued on a
// min-heap and dispatched to the destination's inbox when its deadline
// elapses. If the destination's inbox is full at delivery time, the
// scheduler blocks (rather than dropping) so producers feel realistic
// backpressure.
func (n *Network) Send(src, dst Address, payload any) error {
	n.mu.RLock()
	if n.closed {
		n.mu.RUnlock()
		return ErrClosed
	}
	if _, ok := n.sites[dst]; !ok {
		n.mu.RUnlock()
		return ErrUnknownAddress
	}
	if n.partition[linkKey{from: src, to: dst}] {
		n.mu.RUnlock()
		n.mu.Lock()
		n.dropped++
		n.mu.Unlock()
		return nil
	}
	lp, ok := n.links[linkKey{from: src, to: dst}]
	if !ok {
		lp = n.defaultLP
	}
	n.mu.RUnlock()

	n.mu.Lock()
	n.sent++
	if lp.LossRate > 0 && n.rng.Float64() < lp.LossRate {
		n.dropped++
		n.mu.Unlock()
		return nil
	}
	delay := computeDelay(lp, n.rng)
	n.mu.Unlock()

	msg := Message{From: src, To: dst, Payload: payload, SentAt: time.Now()}
	deadline := time.Now().Add(delay)

	n.schedMu.Lock()
	heap.Push(&n.pending, pendingMsg{deadline: deadline, msg: msg})
	n.schedCV.Broadcast()
	n.schedMu.Unlock()
	return nil
}

// SendBlocking is currently an alias for Send; the inbox is large
// enough and the scheduler retries on backpressure, so synchronous
// behaviour at the call site is unnecessary.
func (n *Network) SendBlocking(ctx context.Context, src, dst Address, payload any) error {
	_ = ctx
	return n.Send(src, dst, payload)
}

// scheduler is the single delivery worker. It pops the heap's earliest
// deadline, sleeps until it is due, then forwards the message to the
// destination's inbox. Blocking on a full inbox is intentional: it
// models TCP backpressure on a real WAN link.
func (n *Network) scheduler() {
	for {
		n.schedMu.Lock()
		for n.pending.Len() == 0 {
			select {
			case <-n.stopSched:
				n.schedMu.Unlock()
				return
			default:
			}
			n.schedCV.Wait()
		}
		next := n.pending[0]
		now := time.Now()
		if next.deadline.After(now) {
			wait := next.deadline.Sub(now)
			n.schedMu.Unlock()
			t := time.NewTimer(wait)
			select {
			case <-t.C:
			case <-n.stopSched:
				t.Stop()
				return
			}
			continue
		}
		heap.Pop(&n.pending)
		n.schedMu.Unlock()

		n.mu.RLock()
		closed := n.closed
		partitioned := n.partition[linkKey{from: next.msg.From, to: next.msg.To}]
		inbox := n.sites[next.msg.To]
		n.mu.RUnlock()

		if closed {
			return
		}
		if partitioned || inbox == nil {
			n.mu.Lock()
			n.dropped++
			n.mu.Unlock()
			continue
		}
		select {
		case inbox <- next.msg:
			n.mu.Lock()
			n.delivered++
			n.mu.Unlock()
		case <-n.stopSched:
			return
		}
	}
}

// Stats returns a snapshot of cumulative counters.
func (n *Network) Stats() Stats {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return Stats{Sent: n.sent, Delivered: n.delivered, Dropped: n.dropped}
}

// Close stops further deliveries; in-flight messages are dropped.
func (n *Network) Close() {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.closed = true
	n.mu.Unlock()

	close(n.stopSched)
	n.schedMu.Lock()
	n.schedCV.Broadcast()
	n.schedMu.Unlock()
}

// pendingMsg is a heap entry: a message and the wall-clock deadline at
// which it should be delivered to the destination inbox.
type pendingMsg struct {
	deadline time.Time
	msg      Message
}

type pendingHeap []pendingMsg

func (h pendingHeap) Len() int           { return len(h) }
func (h pendingHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }
func (h pendingHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *pendingHeap) Push(x any)        { *h = append(*h, x.(pendingMsg)) }
func (h *pendingHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// Stats reports cumulative network counters.
type Stats struct {
	Sent, Delivered, Dropped int64
}

// ErrUnknownAddress is returned by Send when dst is not registered.
var ErrUnknownAddress = errors.New("network: unknown address")

// ErrClosed is returned by Send after Close has been called.
var ErrClosed = errors.New("network: closed")

func computeDelay(lp LinkProfile, rng *rand.Rand) time.Duration {
	d := lp.MeanLatency
	if lp.Jitter > 0 {
		jitter := time.Duration(rng.Int63n(int64(2*lp.Jitter+1))) - lp.Jitter
		d += jitter
	}
	if d < 0 {
		return 0
	}
	return d
}
