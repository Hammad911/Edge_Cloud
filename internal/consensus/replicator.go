// Package consensus adapts a pkg/raft.Node into the kv.Replicator interface,
// keeping pkg/kv free of any knowledge of the raft library.
package consensus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"edge-cloud-replication/pkg/causal"
	"edge-cloud-replication/pkg/hlc"
	"edge-cloud-replication/pkg/kv"
	raftpkg "edge-cloud-replication/pkg/raft"
)

// RaftReplicator adapts a *raftpkg.Node to kv.Replicator. Keeping this
// adapter in internal/ preserves a strict dependency direction: pkg/raft
// knows nothing about the kv package, and pkg/kv knows nothing about the
// hashicorp/raft runtime.
type RaftReplicator struct {
	node         *raftpkg.Node
	applyTimeout time.Duration
}

// NewRaftReplicator wires a raft Node as a kv.Replicator. applyTimeout
// bounds how long a single Apply can wait for commit acknowledgement from
// the cluster; zero selects a 5s default.
func NewRaftReplicator(node *raftpkg.Node, applyTimeout time.Duration) *RaftReplicator {
	if applyTimeout <= 0 {
		applyTimeout = 5 * time.Second
	}
	return &RaftReplicator{node: node, applyTimeout: applyTimeout}
}

// Apply implements kv.Replicator.
func (r *RaftReplicator) Apply(ctx context.Context, op kv.Op, key string, value []byte, ts hlc.Timestamp) error {
	var rop raftpkg.OpType
	switch op {
	case kv.OpPut:
		rop = raftpkg.OpPut
	case kv.OpDelete:
		rop = raftpkg.OpDelete
	default:
		return fmt.Errorf("consensus: unknown kv op %d", op)
	}
	cmd := raftpkg.Command{Op: rop, Timestamp: ts, Key: key, Value: value}
	err := r.node.Apply(ctx, cmd, r.applyTimeout)
	if errors.Is(err, raftpkg.ErrNotLeader) {
		return kv.ErrNotLeader
	}
	return err
}

// IsLeader implements kv.Replicator.
func (r *RaftReplicator) IsLeader() bool { return r.node.IsLeader() }

// Leader implements kv.Replicator.
func (r *RaftReplicator) Leader() string { return r.node.Leader() }

// CommitRemote implements causal.RaftCommitter. It takes a remote causal
// event and proposes it through Raft so every replica in the local cluster
// applies the same write at the originator's timestamp. When the local
// store already has a newer version of the key, the FSM's stale-write
// rejection silently absorbs the no-op (we map ErrStaleWrite to nil at the
// application layer to keep this idempotent).
func (r *RaftReplicator) CommitRemote(ctx context.Context, e *causal.Event) error {
	op := raftpkg.OpPut
	if e.Deleted {
		op = raftpkg.OpDelete
	}
	cmd := raftpkg.Command{
		Op:        op,
		Timestamp: e.CommitTS,
		Key:       e.Key,
		Value:     e.Value,
	}
	err := r.node.Apply(ctx, cmd, r.applyTimeout)
	if errors.Is(err, raftpkg.ErrNotLeader) {
		return causal.ErrNotLeader
	}
	return err
}
