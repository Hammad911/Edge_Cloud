package raft

import (
	"fmt"
	"io"
	"log/slog"

	hraft "github.com/hashicorp/raft"

	"edge-cloud-replication/pkg/hlc"
	"edge-cloud-replication/pkg/storage"
)

// FSM is the hashicorp/raft FSM that applies replicated Commands to a local
// storage.Store. One FSM instance exists per node; hashicorp/raft invokes
// Apply on every committed log entry, Snapshot when compacting, and Restore
// when a follower catches up from a peer's snapshot.
type FSM struct {
	store  storage.Store
	clock  *hlc.Clock
	logger *slog.Logger
}

// ApplyResult is the type returned by Apply. It is shared between the leader
// (which reads it via raft.ApplyFuture.Response) and the callers of the
// Replicator.
type ApplyResult struct {
	Err error
}

// NewFSM constructs an FSM. The clock is advanced on every Apply so that
// followers absorb the leader's logical time and future leader transitions
// produce strictly-monotonic timestamps.
func NewFSM(store storage.Store, clock *hlc.Clock, logger *slog.Logger) *FSM {
	if store == nil {
		panic("raft.NewFSM: store must not be nil")
	}
	if clock == nil {
		panic("raft.NewFSM: clock must not be nil")
	}
	return &FSM{
		store:  store,
		clock:  clock,
		logger: logger.With(slog.String("component", "raft_fsm")),
	}
}

// Apply implements hraft.FSM.
func (f *FSM) Apply(log *hraft.Log) interface{} {
	var cmd Command
	if err := cmd.UnmarshalBinary(log.Data); err != nil {
		f.logger.Error("decode command", slog.Any("err", err), slog.Uint64("index", log.Index))
		return ApplyResult{Err: fmt.Errorf("raft apply: decode: %w", err)}
	}

	if _, err := f.clock.Update(cmd.Timestamp); err != nil {
		f.logger.Error("absorb timestamp", slog.Any("err", err))
		return ApplyResult{Err: fmt.Errorf("raft apply: clock update: %w", err)}
	}

	switch cmd.Op {
	case OpPut:
		if err := f.store.Put(cmd.Key, cmd.Value, cmd.Timestamp); err != nil {
			return ApplyResult{Err: fmt.Errorf("raft apply: put: %w", err)}
		}
	case OpDelete:
		if err := f.store.Delete(cmd.Key, cmd.Timestamp); err != nil {
			return ApplyResult{Err: fmt.Errorf("raft apply: delete: %w", err)}
		}
	default:
		return ApplyResult{Err: fmt.Errorf("raft apply: unknown op %d", cmd.Op)}
	}
	return ApplyResult{}
}

// Snapshot implements hraft.FSM.
func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	snap, ok := f.store.(storage.Snapshotter)
	if !ok {
		return nil, fmt.Errorf("raft snapshot: store does not implement Snapshotter")
	}
	return &fsmSnapshot{store: snap}, nil
}

// Restore implements hraft.FSM.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	snap, ok := f.store.(storage.Snapshotter)
	if !ok {
		return fmt.Errorf("raft restore: store does not implement Snapshotter")
	}
	if err := snap.Restore(rc); err != nil {
		return fmt.Errorf("raft restore: %w", err)
	}
	f.logger.Info("fsm restored from snapshot")
	return nil
}

// fsmSnapshot is the hraft.FSMSnapshot that streams a storage.Snapshotter's
// state into the sink. It holds no references after Release.
type fsmSnapshot struct {
	store storage.Snapshotter
}

// Persist implements hraft.FSMSnapshot.
func (s *fsmSnapshot) Persist(sink hraft.SnapshotSink) error {
	if err := s.store.Snapshot(sink); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("raft persist: %w", err)
	}
	return sink.Close()
}

// Release implements hraft.FSMSnapshot.
func (s *fsmSnapshot) Release() {}
