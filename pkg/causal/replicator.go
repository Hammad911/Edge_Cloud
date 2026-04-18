package causal

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"edge-cloud-replication/pkg/hlc"
)

// Replicator is the top-level causal-replication orchestrator for a single
// node. It owns:
//
//   - the local PartitionedClock (one per node, shared with the KV layer)
//   - the Outbox (FIFO of events awaiting shipment)
//   - one Sender per configured peer
//   - the Delivery loop that drains Buffer.Deliver() into the Applier
//
// Receivers are NOT owned here: they are constructed by the gRPC server in
// internal/server/replication.go and call back into Admit/AdvanceFrontier
// on the Buffer this Replicator exposes.
type Replicator struct {
	NodeID  string
	GroupID hlc.GroupID

	Clock    *hlc.PartitionedClock
	Outbox   *Outbox
	Buffer   *Buffer
	Producer *Producer
	Applier  Applier

	peers   []PeerSpec
	logger  *slog.Logger
	started time.Time

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	group   *errgroup.Group

	// notify is closed each time the buffer is poked (admit, advance,
	// or external nudge). The delivery loop wakes on it.
	notifyMu sync.Mutex
	notifyCh chan struct{}
}

// Config bundles the inputs the Replicator needs.
type Config struct {
	NodeID         string
	GroupID        hlc.GroupID
	Peers          []PeerSpec
	OutboxCapacity int
}

// New constructs a Replicator. The caller supplies the local clock, the
// applier (StoreApplier or RaftApplier), and a logger.
func New(cfg Config, clock *hlc.PartitionedClock, applier Applier, logger *slog.Logger) *Replicator {
	if clock == nil {
		panic("causal.New: clock must not be nil")
	}
	if applier == nil {
		panic("causal.New: applier must not be nil")
	}
	outbox := NewOutbox(cfg.OutboxCapacity)
	r := &Replicator{
		NodeID:   cfg.NodeID,
		GroupID:  cfg.GroupID,
		Clock:    clock,
		Outbox:   outbox,
		Buffer:   NewBuffer(),
		Applier:  applier,
		peers:    append([]PeerSpec(nil), cfg.Peers...),
		logger:   logger.With(slog.String("component", "causal_replicator"), slog.String("node", cfg.NodeID)),
		notifyCh: make(chan struct{}, 1),
	}
	r.Producer = NewProducer(clock, outbox, cfg.NodeID)
	return r
}

// Start launches the senders and the delivery loop. Returns immediately.
// Stop must be called for orderly shutdown.
func (r *Replicator) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running {
		return errors.New("causal: replicator already started")
	}

	ctx, cancel := context.WithCancel(ctx)
	g, gctx := errgroup.WithContext(ctx)

	for _, p := range r.peers {
		p := p
		s := NewSender(p, r.NodeID, string(r.GroupID), r.Outbox, r.logger)
		g.Go(func() error {
			r.logger.Info("sender starting", slog.String("peer", p.Name), slog.String("addr", p.Addr))
			err := s.Run(gctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				r.logger.Warn("sender exited", slog.String("peer", p.Name), slog.Any("err", err))
			}
			return err
		})
	}

	g.Go(func() error { return r.deliveryLoop(gctx) })

	r.running = true
	r.cancel = cancel
	r.group = g
	r.started = time.Now()
	r.logger.Info("causal replicator started",
		slog.String("group", string(r.GroupID)),
		slog.Int("peers", len(r.peers)),
	)
	return nil
}

// Stop cancels the senders and delivery loop, waits for them to exit, and
// closes the outbox. Safe to call multiple times.
func (r *Replicator) Stop() error {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return nil
	}
	cancel := r.cancel
	g := r.group
	r.running = false
	r.mu.Unlock()

	cancel()
	r.Outbox.Close()
	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	r.logger.Info("causal replicator stopped")
	return nil
}

// Notify wakes the delivery loop. The receiver gRPC server calls this
// after every Admit or AdvanceFrontier.
func (r *Replicator) Notify() {
	r.notifyMu.Lock()
	defer r.notifyMu.Unlock()
	select {
	case r.notifyCh <- struct{}{}:
	default:
	}
}

func (r *Replicator) deliveryLoop(ctx context.Context) error {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()

	for {
		ready := r.Buffer.Deliver()
		for _, ev := range ready {
			if err := r.Applier.Apply(ctx, ev); err != nil {
				if errors.Is(err, ErrNotLeader) {
					// Drop and let leader receive again on its own
					// stream. The buffer's frontier has not been
					// advanced for this event because the dedup happens
					// in drainLocked() before applier; we need a small
					// re-buffering policy here in a future iteration.
					r.logger.Debug("not leader, dropping remote event",
						slog.String("key", ev.Key))
					continue
				}
				r.logger.Error("apply failed",
					slog.String("key", ev.Key),
					slog.Any("err", err))
				continue
			}
			// Re-publish to peers IF this node has additional outbound
			// peers AND we are configured as a hub. For Milestone 5 we
			// always re-publish remote events: the cloud node forwards
			// to other edges; an edge node forwarding back to cloud is
			// suppressed by the cloud's dedup buffer.
			r.Producer.PublishRemote(ev)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		case <-r.notifyCh:
		}
	}
}
