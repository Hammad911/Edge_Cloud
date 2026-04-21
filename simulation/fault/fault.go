// Package fault injects scheduled network faults (partitions, heals,
// link-profile changes) into a running simulation. A Schedule is a
// deterministic list of timed events; a Runner executes them against a
// simulation/network.Network while the workload is ticking.
//
// The goal is to produce the classic "three-regime" paper figure:
// throughput and replication lag measured *before* a partition, *during*
// a partition, and *after* a heal. Our scheme's claim is that local ops
// keep succeeding throughout and convergence is bounded by (time since
// heal + WAN RTT), with zero causality violations across every regime.
package fault

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"edge-cloud-replication/simulation/network"
)

// Kind identifies what the scheduled event does to the network.
type Kind int

const (
	// PartitionLink severs directed traffic between two addresses.
	PartitionLink Kind = iota
	// HealLink restores a previously partitioned directed link.
	HealLink
	// PartitionAll isolates one address from every other peer (both
	// directions). Equivalent to a node-down in the failure model.
	PartitionAll
	// HealAll undoes PartitionAll.
	HealAll
)

// Event is a single scheduled change.
type Event struct {
	// At is the wall-clock offset from Runner.Start.
	At time.Duration
	// Kind picks the operation.
	Kind Kind
	// A, B are addresses; for PartitionAll / HealAll only A is used.
	A, B network.Address
	// Label is a human-readable tag recorded in the runner's log.
	Label string
}

// Schedule is a time-sorted list of events.
type Schedule struct {
	events []Event
}

// NewSchedule builds a schedule. Events are automatically sorted by At.
func NewSchedule(events ...Event) *Schedule {
	sc := &Schedule{events: append([]Event(nil), events...)}
	sort.Slice(sc.events, func(i, j int) bool { return sc.events[i].At < sc.events[j].At })
	return sc
}

// Add appends an event (re-sorting).
func (s *Schedule) Add(e Event) {
	s.events = append(s.events, e)
	sort.Slice(s.events, func(i, j int) bool { return s.events[i].At < s.events[j].At })
}

// Events returns a copy of the scheduled events.
func (s *Schedule) Events() []Event {
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

// PartitionWindow is a convenience constructor for the common "cut these
// peers off for this long" scenario. It produces 2N events: a partition
// at startOffset and a heal at startOffset+duration for every (a,b) pair.
func PartitionWindow(peers []network.Address, startOffset, duration time.Duration) *Schedule {
	ev := make([]Event, 0, 2*len(peers))
	for _, p := range peers {
		ev = append(ev,
			Event{At: startOffset, Kind: PartitionAll, A: p,
				Label: fmt.Sprintf("partition %s", p)},
			Event{At: startOffset + duration, Kind: HealAll, A: p,
				Label: fmt.Sprintf("heal %s", p)},
		)
	}
	return NewSchedule(ev...)
}

// Callback observes each applied event, useful for phase-aware metric
// collection.
type Callback func(Event, time.Time)

// Runner applies a Schedule to a Network in the background. It is
// idempotent: a second Start is a no-op, and Stop cancels any pending
// events.
type Runner struct {
	net      *network.Network
	schedule *Schedule
	logger   *slog.Logger
	callback Callback

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewRunner wires a Schedule to a Network.
func NewRunner(net *network.Network, sc *Schedule, logger *slog.Logger, cb Callback) *Runner {
	if logger == nil {
		logger = slog.Default()
	}
	return &Runner{net: net, schedule: sc, logger: logger, callback: cb}
}

// Start launches the background goroutine that fires scheduled events.
// The zero-offset is the moment Start returns.
func (r *Runner) Start(parent context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return
	}
	r.started = true
	ctx, cancel := context.WithCancel(parent)
	r.cancel = cancel

	origin := time.Now()
	events := r.schedule.Events()
	r.wg.Add(1)
	go r.run(ctx, origin, events)
}

// Stop cancels any pending events and waits for the runner goroutine to
// exit. Safe to call multiple times.
func (r *Runner) Stop() {
	r.mu.Lock()
	if !r.started {
		r.mu.Unlock()
		return
	}
	cancel := r.cancel
	r.started = false
	r.mu.Unlock()
	cancel()
	r.wg.Wait()
}

func (r *Runner) run(ctx context.Context, origin time.Time, events []Event) {
	defer r.wg.Done()
	for _, ev := range events {
		deadline := origin.Add(ev.At)
		delay := time.Until(deadline)
		if delay > 0 {
			t := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
		r.apply(ev)
	}
}

func (r *Runner) apply(ev Event) {
	switch ev.Kind {
	case PartitionLink:
		r.net.Partition(ev.A, ev.B)
	case HealLink:
		r.net.Heal(ev.A, ev.B)
	case PartitionAll:
		r.net.PartitionAll(ev.A)
	case HealAll:
		r.net.HealAll(ev.A)
	}
	now := time.Now()
	r.logger.Info("fault event applied",
		slog.String("kind", kindString(ev.Kind)),
		slog.String("a", string(ev.A)),
		slog.String("b", string(ev.B)),
		slog.String("label", ev.Label),
	)
	if r.callback != nil {
		r.callback(ev, now)
	}
}

func kindString(k Kind) string {
	switch k {
	case PartitionLink:
		return "partition-link"
	case HealLink:
		return "heal-link"
	case PartitionAll:
		return "partition-all"
	case HealAll:
		return "heal-all"
	default:
		return "unknown"
	}
}
