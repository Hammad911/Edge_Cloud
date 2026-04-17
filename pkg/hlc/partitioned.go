package hlc

import (
	"fmt"
	"sort"
	"sync"
)

// GroupID identifies a datacenter group in the partitioned scheme. Each edge
// site belongs to exactly one group; multiple sites can share a group.
type GroupID string

// PartitionedTimestamp is the cross-cluster causal metadata attached to every
// replicated write. It contains:
//
//   - Origin: the group that originated the write (useful for provenance and
//     for avoiding self-loops during replication).
//   - Groups: a map from GroupID to the most recent HLC seen for that group
//     as of this event. Unknown groups are implicitly the zero Timestamp.
//
// Wire size is O(k) where k is the number of datacenter GROUPS, not the number
// of sites. This is the core scalability benefit: the proposal targets
// O(log n) in the number of sites, and arranging sites into log-sized groups
// achieves that directly.
//
// A PartitionedTimestamp a "happens before" b (a ⇒ b) iff for every group g,
// a.Groups[g].Compare(b.Groups[g]) <= 0, and strictly less for at least one g.
type PartitionedTimestamp struct {
	Origin GroupID
	Groups map[GroupID]Timestamp
}

// NewPartitionedTimestamp constructs an empty timestamp for the given origin.
func NewPartitionedTimestamp(origin GroupID) PartitionedTimestamp {
	return PartitionedTimestamp{
		Origin: origin,
		Groups: make(map[GroupID]Timestamp),
	}
}

// Clone returns a deep copy safe for independent mutation.
func (p PartitionedTimestamp) Clone() PartitionedTimestamp {
	out := PartitionedTimestamp{Origin: p.Origin, Groups: make(map[GroupID]Timestamp, len(p.Groups))}
	for k, v := range p.Groups {
		out.Groups[k] = v
	}
	return out
}

// HappensBefore reports whether p ⇒ q (p causally precedes q).
// Returns false if they are equal or concurrent.
func (p PartitionedTimestamp) HappensBefore(q PartitionedTimestamp) bool {
	if len(p.Groups) == 0 && len(q.Groups) == 0 {
		return false
	}

	strictlyLessSomewhere := false

	for g, pv := range p.Groups {
		qv := q.Groups[g]
		c := pv.Compare(qv)
		if c > 0 {
			return false
		}
		if c < 0 {
			strictlyLessSomewhere = true
		}
	}

	for g, qv := range q.Groups {
		if _, ok := p.Groups[g]; ok {
			continue
		}
		if !qv.Zero() {
			strictlyLessSomewhere = true
		}
	}

	return strictlyLessSomewhere
}

// Concurrent reports whether p and q are concurrent (neither precedes the
// other, and they are not equal).
func (p PartitionedTimestamp) Concurrent(q PartitionedTimestamp) bool {
	if p.Equal(q) {
		return false
	}
	return !p.HappensBefore(q) && !q.HappensBefore(p)
}

// Equal reports whether p and q carry identical per-group state (Origin is not
// compared, since two replicas may independently observe the same causal cut
// from different origins).
func (p PartitionedTimestamp) Equal(q PartitionedTimestamp) bool {
	if len(p.Groups) != len(q.Groups) {
		// Allow explicit zero entries to match missing entries.
		if !equalIgnoreZero(p.Groups, q.Groups) {
			return false
		}
		return true
	}
	for g, pv := range p.Groups {
		if q.Groups[g] != pv {
			return false
		}
	}
	return true
}

func equalIgnoreZero(a, b map[GroupID]Timestamp) bool {
	for g, av := range a {
		bv, ok := b[g]
		if !ok {
			if !av.Zero() {
				return false
			}
			continue
		}
		if av != bv {
			return false
		}
	}
	for g, bv := range b {
		if _, ok := a[g]; ok {
			continue
		}
		if !bv.Zero() {
			return false
		}
	}
	return true
}

// String produces a deterministic, sorted representation useful for logs and
// tests. The Origin is prefixed and entries are sorted by GroupID.
func (p PartitionedTimestamp) String() string {
	keys := make([]string, 0, len(p.Groups))
	for k := range p.Groups {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)

	out := fmt.Sprintf("[origin=%s", p.Origin)
	for _, k := range keys {
		out += fmt.Sprintf(" %s=%s", k, p.Groups[GroupID(k)])
	}
	out += "]"
	return out
}

// PartitionedClock maintains a per-group HLC view for a single node. It is
// designed to be owned by one site and advanced on local writes (Stamp) and
// receive events (Merge).
//
// Concurrency: safe for concurrent use.
type PartitionedClock struct {
	local    *Clock
	ownGroup GroupID

	mu     sync.Mutex
	groups map[GroupID]Timestamp
}

// NewPartitionedClock creates a clock owned by ownGroup. The local HLC is used
// to stamp the ownGroup's entry on every Stamp call.
func NewPartitionedClock(ownGroup GroupID, local *Clock) *PartitionedClock {
	if local == nil {
		local = New()
	}
	return &PartitionedClock{
		local:    local,
		ownGroup: ownGroup,
		groups:   make(map[GroupID]Timestamp),
	}
}

// OwnGroup returns the group this clock stamps its writes as.
func (p *PartitionedClock) OwnGroup() GroupID { return p.ownGroup }

// Stamp produces a PartitionedTimestamp for a fresh local event. The local
// HLC is advanced, and the ownGroup entry is updated to the resulting time.
// All other groups retain their most-recently-seen values.
func (p *PartitionedClock) Stamp() PartitionedTimestamp {
	ts := p.local.Now()

	p.mu.Lock()
	defer p.mu.Unlock()

	p.groups[p.ownGroup] = ts
	return p.snapshotLocked()
}

// Merge incorporates a received PartitionedTimestamp, advancing each known
// group's entry to the componentwise max, and returns the resulting
// timestamp for the receive event (i.e. the timestamp to stamp on any
// subsequent local event caused by this receive).
//
// Returns ErrClockDrift if the remote ownGroup entry exceeds the local HLC's
// drift budget - this is the one entry we can sanity-check against physical
// time, since it was stamped by a well-defined source.
func (p *PartitionedClock) Merge(remote PartitionedTimestamp) (PartitionedTimestamp, error) {
	if remote.Origin != "" {
		if rt, ok := remote.Groups[remote.Origin]; ok {
			if _, err := p.local.Update(rt); err != nil {
				return PartitionedTimestamp{}, err
			}
		}
	}

	newLocal := p.local.Now()

	p.mu.Lock()
	defer p.mu.Unlock()

	for g, rt := range remote.Groups {
		cur := p.groups[g]
		if rt.After(cur) {
			p.groups[g] = rt
		}
	}
	p.groups[p.ownGroup] = newLocal

	return p.snapshotLocked(), nil
}

// Peek returns a snapshot of the current per-group state without advancing.
func (p *PartitionedClock) Peek() PartitionedTimestamp {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshotLocked()
}

// Size reports the number of groups currently tracked. This is the per-write
// metadata cost and the quantity the proposal's O(log n) target bounds.
func (p *PartitionedClock) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.groups)
}

func (p *PartitionedClock) snapshotLocked() PartitionedTimestamp {
	out := PartitionedTimestamp{
		Origin: p.ownGroup,
		Groups: make(map[GroupID]Timestamp, len(p.groups)),
	}
	for g, t := range p.groups {
		out.Groups[g] = t
	}
	return out
}
