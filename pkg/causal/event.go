// Package causal implements the cross-cluster causal replication layer.
//
// The unit of replication is an Event: a key/value mutation produced by a
// single originator (an edge cluster's leader, or the cloud hub itself),
// carrying the originator's HLC at commit time and the partitioned-HLC
// frontier the originator had observed at that moment.
//
// Receivers buffer events whose causal dependencies are not yet satisfied
// and apply them in causal order via an Applier. The package is independent
// of any specific transport; pkg/causal/sender.go and the gRPC server in
// internal/server/replication.go provide the network glue.
package causal

import (
	"fmt"

	"edge-cloud-replication/pkg/hlc"
)

// Event is a single replicated mutation. Events are immutable once produced;
// senders enqueue them, receivers buffer them, and the Applier consumes them.
type Event struct {
	// EventID is a sender-scoped monotonic identifier. Used by Push for
	// acknowledgement; it is NOT part of the causal ordering.
	EventID uint64

	// SenderID is the immediate sender on the wire (e.g. "cloud-hub" when
	// the cloud is forwarding an edge-A event to edge-B). Origin is the
	// real producer.
	SenderID string

	// Origin is the GroupID that produced this mutation.
	Origin hlc.GroupID

	// Key, Value, Deleted describe the mutation. A tombstone has
	// Deleted=true and Value=nil.
	Key     string
	Value   []byte
	Deleted bool

	// CommitTS is the HLC the originator stamped on this mutation. The
	// store applies it as the version timestamp.
	CommitTS hlc.Timestamp

	// Deps is the partitioned-HLC frontier the originator had observed at
	// the time of commit. A receiver must have applied every causally <=
	// event before this one becomes deliverable.
	Deps hlc.PartitionedTimestamp
}

// Validate sanity-checks an Event before it is admitted into the buffer.
// We deliberately do NOT enforce that Origin appears in Deps: a brand-new
// originator with no peers has an empty Deps map.
func (e Event) Validate() error {
	if e.Key == "" {
		return fmt.Errorf("causal: event with empty key")
	}
	if e.Origin == "" {
		return fmt.Errorf("causal: event with empty origin")
	}
	if e.CommitTS.Zero() {
		return fmt.Errorf("causal: event with zero commit_ts")
	}
	if e.Deleted && len(e.Value) > 0 {
		return fmt.Errorf("causal: deleted event with non-empty value")
	}
	return nil
}

// String produces a compact, deterministic representation for logs.
func (e Event) String() string {
	tag := "PUT"
	if e.Deleted {
		tag = "DEL"
	}
	return fmt.Sprintf("Event{id=%d origin=%s sender=%s %s key=%q ts=%s deps=%s}",
		e.EventID, e.Origin, e.SenderID, tag, e.Key, e.CommitTS, e.Deps)
}
