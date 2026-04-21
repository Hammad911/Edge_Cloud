// Package site is the simulator's notion of a single edge or cloud node.
// It composes the production primitives (pkg/hlc, pkg/storage, pkg/causal)
// with a simulator transport (simulation/network) so the same code paths
// run end-to-end without the overhead of real gRPC.
//
// A Site exposes the surface a workload generator and a causality
// checker need: Put, Get, Delete (local), plus Stats. Cross-site
// replication is driven by background goroutines (sender, receiver,
// applier) that run for the lifetime of the simulation.
package site

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"edge-cloud-replication/pkg/causal"
	"edge-cloud-replication/pkg/hlc"
	"edge-cloud-replication/pkg/storage"
	"edge-cloud-replication/simulation/network"
)

// Role labels a site as a cloud hub or an edge replica. The role only
// affects defaults (e.g. which peers it ships to); both roles use the
// same causal stack.
type Role int

const (
	RoleEdge Role = iota
	RoleCloud
)

// Config bundles the inputs needed to build a Site.
type Config struct {
	Address network.Address
	GroupID hlc.GroupID
	Role    Role
	Peers   []network.Address // who to ship outgoing events to

	// Logger is optional. When nil the site logs to io.Discard, which
	// is the right default for large simulations where logs would
	// dwarf the actual work.
	Logger *slog.Logger

	// Observer, if non-nil, is invoked after every successful local
	// commit AND every successful remote apply. The simulator uses
	// this to drive metrics (replication lag) and the causality
	// checker. Must be goroutine-safe; called from background
	// goroutines as well as from Put/Delete callers.
	Observer ApplyObserver
}

// Apply is the kind of mutation that triggered the observation.
type ApplyKind int

const (
	ApplyLocal ApplyKind = iota
	ApplyRemote
)

// ApplyEvent is the observation handed to ApplyObserver after every
// commit on the local site (whether the source is local or remote).
type ApplyEvent struct {
	Site     network.Address
	Group    hlc.GroupID
	Kind     ApplyKind
	Origin   hlc.GroupID
	Key      string
	Value    []byte
	Deleted  bool
	CommitTS hlc.Timestamp
	Deps     hlc.PartitionedTimestamp
	At       time.Time
}

// ApplyObserver is invoked by Site after every successful commit.
type ApplyObserver func(ApplyEvent)

// Stats is a snapshot of a site's counters.
type Stats struct {
	LocalPuts       int64
	LocalDeletes    int64
	LocalGets       int64
	EventsShipped   int64
	EventsReceived  int64
	EventsApplied   int64
	EventsBuffered  int64
	EventsDuplicate int64
	// OutboxPending is the total number of (subscriber, event) pairs
	// waiting to be shipped. A non-zero value means at least one peer
	// has not yet received every local mutation.
	OutboxPending int64
	// InboxPending is the number of messages sitting in the receiver's
	// channel that have not yet been popped and admitted to the buffer.
	// Small (<= channel capacity) but non-zero during bursty traffic.
	InboxPending int64
}

// Site is one simulated node.
type Site struct {
	cfg Config
	net *network.Network

	clock  *hlc.Clock
	pclock *hlc.PartitionedClock
	store  storage.Store

	buffer   *causal.Buffer
	outbox   *causal.Outbox
	producer *causal.Producer
	applier  causal.Applier

	inbox <-chan network.Message

	// notifyCh wakes the delivery loop on receive.
	notifyCh chan struct{}

	logger *slog.Logger

	// counters
	puts, dels, gets atomic.Int64
	shipped          atomic.Int64
	received         atomic.Int64
	applied          atomic.Int64
	bufferedCount    atomic.Int64
	duplicates       atomic.Int64

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// New constructs a Site and registers it on the network.
func New(cfg Config, net *network.Network) *Site {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	clock := hlc.New()
	pclock := hlc.NewPartitionedClock(cfg.GroupID, clock)
	store := storage.NewMemStore()
	outbox := causal.NewOutbox(0)

	logger := cfg.Logger.With(slog.String("site", string(cfg.Address)))

	return &Site{
		cfg:      cfg,
		net:      net,
		clock:    clock,
		pclock:   pclock,
		store:    store,
		buffer:   causal.NewBuffer(),
		outbox:   outbox,
		producer: causal.NewProducer(pclock, outbox, string(cfg.Address)),
		applier:  causal.NewStoreApplier(store, pclock, logger),
		inbox:    net.Register(cfg.Address),
		notifyCh: make(chan struct{}, 1),
		logger:   logger,
	}
}

// Address returns the site's network address.
func (s *Site) Address() network.Address { return s.cfg.Address }

// Group returns the site's GroupID.
func (s *Site) Group() hlc.GroupID { return s.cfg.GroupID }

// Start launches the background goroutines: one shipper per peer, one
// receiver, and one delivery loop. Idempotent.
func (s *Site) Start(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.running = true

	for _, peer := range s.cfg.Peers {
		peer := peer
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.runShipper(ctx, peer)
		}()
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runReceiver(ctx)
	}()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runDeliverer(ctx)
	}()
}

// Stop cancels the background goroutines and waits for them to exit.
func (s *Site) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	cancel := s.cancel
	s.running = false
	s.mu.Unlock()

	cancel()
	s.outbox.Close()
	s.wg.Wait()
}

// --- KV API ------------------------------------------------------------

// Put writes key=value at the local site. Returns the HLC commit
// timestamp; cross-cluster propagation happens asynchronously.
func (s *Site) Put(ctx context.Context, key string, value []byte) (hlc.Timestamp, error) {
	if err := ctx.Err(); err != nil {
		return hlc.Timestamp{}, err
	}
	ts := s.clock.Now()
	if err := s.store.Put(key, value, ts); err != nil {
		return hlc.Timestamp{}, fmt.Errorf("local put: %w", err)
	}
	ev := s.producer.Publish(key, value, false, ts)
	s.buffer.MarkApplied(s.cfg.GroupID, ts)
	s.puts.Add(1)
	s.fireObserver(ApplyLocal, ev)
	return ts, nil
}

// Delete tombstones key locally and propagates the deletion.
func (s *Site) Delete(ctx context.Context, key string) (hlc.Timestamp, error) {
	if err := ctx.Err(); err != nil {
		return hlc.Timestamp{}, err
	}
	ts := s.clock.Now()
	if err := s.store.Delete(key, ts); err != nil {
		return hlc.Timestamp{}, fmt.Errorf("local delete: %w", err)
	}
	ev := s.producer.Publish(key, nil, true, ts)
	s.buffer.MarkApplied(s.cfg.GroupID, ts)
	s.dels.Add(1)
	s.fireObserver(ApplyLocal, ev)
	return ts, nil
}

// Get returns the most recent locally-visible value for key. The Get is
// purely local: causal ordering of remote writes is enforced at apply
// time, not at read time.
func (s *Site) Get(_ context.Context, key string) ([]byte, hlc.Timestamp, error) {
	v, err := s.store.Get(key)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			s.gets.Add(1)
			return nil, hlc.Timestamp{}, ErrNotFound
		}
		return nil, hlc.Timestamp{}, err
	}
	s.gets.Add(1)
	return v.Value, v.Timestamp, nil
}

// Stats returns a snapshot of counters.
func (s *Site) Stats() Stats {
	return Stats{
		LocalPuts:       s.puts.Load(),
		LocalDeletes:    s.dels.Load(),
		LocalGets:       s.gets.Load(),
		EventsShipped:   s.shipped.Load(),
		EventsReceived:  s.received.Load(),
		EventsApplied:   s.applied.Load(),
		EventsBuffered:  s.bufferedCount.Load(),
		EventsDuplicate: s.duplicates.Load(),
		OutboxPending:   int64(s.outbox.TotalPending()),
		InboxPending:    int64(len(s.inbox)),
	}
}

// ErrNotFound is returned by Get when the key is absent or tombstoned.
var ErrNotFound = errors.New("site: not found")

// --- background loops --------------------------------------------------

// runShipper pulls events from the local outbox and Sends them across
// the simulated network to a single peer.
func (s *Site) runShipper(ctx context.Context, peer network.Address) {
	subName := "peer/" + string(peer)
	s.outbox.Subscribe(subName)
	defer s.outbox.Unsubscribe(subName)

	cursor := uint64(0)
	backoff := 10 * time.Millisecond
	const maxBackoff = 500 * time.Millisecond
	for {
		ev, newCursor, err := s.outbox.Next(ctx, subName, cursor)
		if err != nil {
			return
		}
		if err := s.net.Send(s.cfg.Address, peer, ev); err != nil {
			// Send failed - typically the link to `peer` is
			// partitioned or the network dropped the packet. Do
			// NOT advance the cursor; back off and retry the same
			// event until the link heals or the context dies.
			// Dropping here silently was the old behaviour and
			// caused permanent data loss across partitions.
			s.logger.Debug("network send failed; will retry",
				slog.String("peer", string(peer)),
				slog.Any("err", err),
			)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			continue
		}
		s.shipped.Add(1)
		cursor = newCursor
		s.outbox.Ack(subName, newCursor)
		backoff = 10 * time.Millisecond
	}
}

// runReceiver pulls messages off the inbox and admits them to the
// causal buffer.
func (s *Site) runReceiver(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-s.inbox:
			if !ok {
				return
			}
			ev, ok := msg.Payload.(*causal.Event)
			if !ok {
				s.logger.Warn("non-event payload received",
					slog.String("from", string(msg.From)))
				continue
			}
			s.received.Add(1)
			res, err := s.buffer.Admit(ev)
			if err != nil {
				s.logger.Warn("buffer admit failed",
					slog.Any("err", err))
				continue
			}
			switch res {
			case causal.AdmitDuplicate:
				s.duplicates.Add(1)
			case causal.AdmitDelivered, causal.AdmitBuffered:
				s.notify()
			}
		}
	}
}

// runDeliverer drains the buffer and applies events. The cloud also
// re-publishes remote events so they fan out to other edges.
func (s *Site) runDeliverer(ctx context.Context) {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		ready := s.buffer.Deliver()
		s.bufferedCount.Store(int64(s.buffer.PendingCount()))
		for _, ev := range ready {
			if err := s.applier.Apply(ctx, ev); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				s.logger.Warn("apply failed",
					slog.String("key", ev.Key),
					slog.Any("err", err))
				continue
			}
			s.applied.Add(1)
			s.fireObserver(ApplyRemote, ev)
			// Cloud is the hub: re-publish so other edges receive it.
			// Edges don't fan out (no peers other than cloud anyway).
			if s.cfg.Role == RoleCloud {
				s.producer.PublishRemote(ev)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-s.notifyCh:
		}
	}
}

func (s *Site) notify() {
	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}

func (s *Site) fireObserver(kind ApplyKind, ev *causal.Event) {
	if s.cfg.Observer == nil || ev == nil {
		return
	}
	s.cfg.Observer(ApplyEvent{
		Site:     s.cfg.Address,
		Group:    s.cfg.GroupID,
		Kind:     kind,
		Origin:   ev.Origin,
		Key:      ev.Key,
		Value:    ev.Value,
		Deleted:  ev.Deleted,
		CommitTS: ev.CommitTS,
		Deps:     ev.Deps,
		At:       time.Now(),
	})
}
