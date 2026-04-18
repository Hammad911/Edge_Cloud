package causal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"edge-cloud-replication/pkg/hlc"
	"edge-cloud-replication/pkg/storage"
)

// Applier writes a deliverable Event into the local state. Two
// implementations exist:
//
//   - StoreApplier: writes directly to a storage.Store. Used by the cloud
//     hub, which has no Raft layer.
//
//   - RaftApplier: proposes the event through a raft.Replicator (well, a
//     thin shim of one) so followers in the receiving cluster see the
//     replicated state too. Used by edge cluster leaders. When called on a
//     non-leader it returns ErrNotLeader.
//
// Implementations MUST be idempotent on (origin, commit_ts): the buffer
// dedups, but transient races between buffer and applier can still produce
// retries.
type Applier interface {
	Apply(ctx context.Context, e *Event) error
}

// ErrNotLeader is returned by RaftApplier when invoked on a follower. The
// receiver should drop the event and rely on the leader's receiver to
// pick it up; the sender will retry against the new leader on its own.
var ErrNotLeader = errors.New("causal: not raft leader")

// StoreApplier writes events directly to a storage.Store. It also notifies
// the supplied PartitionedClock so the local frontier advances and any
// further locally-produced writes carry the absorbed remote dependencies.
type StoreApplier struct {
	store  storage.Store
	clock  *hlc.PartitionedClock
	logger *slog.Logger
}

// NewStoreApplier constructs an Applier that writes directly to store and
// merges the event's partitioned-HLC frontier into the local clock.
func NewStoreApplier(store storage.Store, clock *hlc.PartitionedClock, logger *slog.Logger) *StoreApplier {
	if store == nil {
		panic("causal.NewStoreApplier: store must not be nil")
	}
	if clock == nil {
		panic("causal.NewStoreApplier: clock must not be nil")
	}
	return &StoreApplier{
		store:  store,
		clock:  clock,
		logger: logger.With(slog.String("component", "causal_store_applier")),
	}
}

// Apply writes the event to the local store and merges its causal metadata
// into the partitioned clock. Stale writes (a higher version of the same
// key already present) are absorbed silently: the partitioned-HLC merge
// still happens so the frontier advances and dependent events unblock.
func (a *StoreApplier) Apply(ctx context.Context, e *Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Merge into the local clock first so the frontier advances even if
	// the store rejects the write as stale.
	if _, err := a.clock.Merge(e.Deps); err != nil {
		a.logger.Warn("merge deps", slog.Any("err", err), slog.String("origin", string(e.Origin)))
	}
	// Also merge the event's own commit timestamp under its origin, so the
	// frontier covers the event itself.
	merged := hlc.NewPartitionedTimestamp(e.Origin)
	merged.Groups[e.Origin] = e.CommitTS
	if _, err := a.clock.Merge(merged); err != nil {
		a.logger.Warn("merge commit ts", slog.Any("err", err))
	}

	var err error
	if e.Deleted {
		err = a.store.Delete(e.Key, e.CommitTS)
	} else {
		err = a.store.Put(e.Key, e.Value, e.CommitTS)
	}
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrStaleWrite) {
		a.logger.Debug("stale remote write absorbed",
			slog.String("key", e.Key),
			slog.String("origin", string(e.Origin)),
			slog.String("commit_ts", e.CommitTS.String()),
		)
		return nil
	}
	return fmt.Errorf("causal apply: %w", err)
}

// RaftCommitter is the narrow surface RaftApplier needs. It is satisfied
// by an adapter over pkg/raft.Node (defined in internal/consensus); we
// keep the interface here so this package does not import the raft library.
type RaftCommitter interface {
	IsLeader() bool
	CommitRemote(ctx context.Context, e *Event) error
}

// RaftApplier funnels remote events through the cluster's Raft log so all
// followers see them. It rejects on followers; the receiver layer will
// not start until the local node is leader and will stop when leadership
// is lost.
type RaftApplier struct {
	committer RaftCommitter
	clock     *hlc.PartitionedClock
	logger    *slog.Logger
}

// NewRaftApplier constructs a RaftApplier.
func NewRaftApplier(c RaftCommitter, clock *hlc.PartitionedClock, logger *slog.Logger) *RaftApplier {
	if c == nil {
		panic("causal.NewRaftApplier: committer must not be nil")
	}
	if clock == nil {
		panic("causal.NewRaftApplier: clock must not be nil")
	}
	return &RaftApplier{
		committer: c,
		clock:     clock,
		logger:    logger.With(slog.String("component", "causal_raft_applier")),
	}
}

// Apply proposes the event through Raft. On success the cluster's Raft
// FSM has applied the write to the local store of every replica.
func (a *RaftApplier) Apply(ctx context.Context, e *Event) error {
	if !a.committer.IsLeader() {
		return ErrNotLeader
	}
	if _, err := a.clock.Merge(e.Deps); err != nil {
		a.logger.Warn("merge deps", slog.Any("err", err), slog.String("origin", string(e.Origin)))
	}
	merged := hlc.NewPartitionedTimestamp(e.Origin)
	merged.Groups[e.Origin] = e.CommitTS
	if _, err := a.clock.Merge(merged); err != nil {
		a.logger.Warn("merge commit ts", slog.Any("err", err))
	}
	if err := a.committer.CommitRemote(ctx, e); err != nil {
		return fmt.Errorf("causal raft apply: %w", err)
	}
	return nil
}
