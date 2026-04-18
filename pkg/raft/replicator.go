package raft

import (
	"context"
	"errors"
	"time"
)

// Replicator is the consensus abstraction the KV service depends on. It is
// deliberately narrow: the service only needs to propose mutations and ask
// who the leader is. Behind this interface is either a real Raft cluster or
// a no-op single-node shim (useful for local development and for the
// pre-raft code path).
type Replicator interface {
	// Apply synchronously proposes a command through the consensus layer.
	// Returns once the command has been committed and applied to the local
	// FSM, or an error. The caller is expected to be the leader; if it is
	// not, ErrNotLeader is returned.
	Apply(ctx context.Context, cmd Command, timeout time.Duration) error

	// IsLeader reports whether this node is currently the cluster leader.
	IsLeader() bool

	// Leader returns the current leader's raft address, or "" if unknown.
	Leader() string

	// AddVoter adds a new voting member to the cluster. Only valid on the
	// leader.
	AddVoter(id, addr string) error

	// RemoveServer removes a member by id. Only valid on the leader.
	RemoveServer(id string) error

	// Shutdown halts the replicator. Idempotent.
	Shutdown() error
}

// ErrNotLeader is returned when a write is proposed on a non-leader node.
// The KV service should translate this into a transport-level redirect or
// a FailedPrecondition response.
var ErrNotLeader = errors.New("raft: not leader")

// ErrReplicatorClosed is returned after Shutdown has been called.
var ErrReplicatorClosed = errors.New("raft: replicator closed")
