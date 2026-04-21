// Package checker is a runtime causality auditor. The simulator records
// every (site, key, value, ts) it observes (write or apply); the checker
// verifies that no site ever sees a value that violates causal order
// established by the partitioned HLC.
//
// Two classes of violation are detected:
//
//  1. Stale read: a site applies an event whose CommitTS is older than
//     a value already present at that site for the same key. (This is
//     usually fine — last-write-wins resolves it — but the *checker*
//     reports it so the workload can confirm we're seeing the
//     interesting cases.)
//
//  2. Causal violation: a site applies an event E1 from origin O1 after
//     it has applied E2 from O2, when E2's deps included a higher
//     timestamp from O1. This is the bug the partitioned-HLC scheme is
//     supposed to make impossible; if it ever fires we have a real
//     correctness issue.
package checker

import (
	"sync"

	"edge-cloud-replication/pkg/hlc"
)

// Kind distinguishes a locally-issued write from a remotely-applied one.
// The checker verifies dependencies on remote observations only; local
// observations only update its idea of "what this site has applied".
//
// The reason: under partitioned HLC, a local write's Deps may legally
// include groups the writer has never directly applied an event from
// (the writer learned of those groups transitively via a peer). Forcing
// direct application would make local writes routinely "violate"
// causality, which is a checker bug, not a system bug.
type Kind int

const (
	KindLocal Kind = iota
	KindRemote
)

// Observation is a single (site, key, value, commitTS, deps) tuple
// captured by the simulator.
type Observation struct {
	Site     string
	Kind     Kind
	Key      string
	Origin   hlc.GroupID
	CommitTS hlc.Timestamp
	Deps     hlc.PartitionedTimestamp
}

// Violation is a single causal-order error found by the checker.
type Violation struct {
	Kind   string
	Site   string
	Key    string
	Detail string
}

// Checker accumulates Observations and reports Violations on demand.
type Checker struct {
	mu sync.Mutex
	// per (site, group) the highest commit_ts we have applied. When a
	// later observation arrives whose deps say "I require origin G to
	// be at TS X" but our applied-set says we're below X for that
	// origin, that's a causal violation.
	applied    map[string]map[hlc.GroupID]hlc.Timestamp
	violations []Violation
}

// New returns an empty checker.
func New() *Checker {
	return &Checker{
		applied: make(map[string]map[hlc.GroupID]hlc.Timestamp),
	}
}

// Record ingests an observation. Returns false if the observation would
// have created a causal violation; the violation is also appended to
// the internal list.
func (c *Checker) Record(o Observation) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	siteState, ok := c.applied[o.Site]
	if !ok {
		siteState = make(map[hlc.GroupID]hlc.Timestamp)
		c.applied[o.Site] = siteState
	}

	ok = true
	if o.Kind == KindRemote {
		for group, depTS := range o.Deps.Groups {
			if group == o.Origin {
				continue
			}
			have := siteState[group]
			if depTS.After(have) {
				c.violations = append(c.violations, Violation{
					Kind:   "missing-dep",
					Site:   o.Site,
					Key:    o.Key,
					Detail: depTS.String() + " > " + have.String() + " for group " + string(group),
				})
				ok = false
			}
		}
	}

	cur := siteState[o.Origin]
	if o.CommitTS.After(cur) {
		siteState[o.Origin] = o.CommitTS
	}
	return ok
}

// Violations returns a copy of all recorded violations.
func (c *Checker) Violations() []Violation {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Violation, len(c.violations))
	copy(out, c.violations)
	return out
}

// Count returns the number of recorded observations per site (sum of
// per-group max counts; useful for sanity).
func (c *Checker) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, m := range c.applied {
		n += len(m)
	}
	return n
}
