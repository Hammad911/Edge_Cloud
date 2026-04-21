package checker

import (
	"fmt"
	"sort"

	"edge-cloud-replication/pkg/hlc"
)

// Violation is a single property breach discovered by Check.
type Violation struct {
	Property string `json:"property"`
	Session  string `json:"session,omitempty"`
	Site     string `json:"site,omitempty"`
	Key      string `json:"key,omitempty"`
	Message  string `json:"message"`
	Seq      int64  `json:"seq"`
}

// Report summarises the checker's verdict on a full history.
type Report struct {
	Events               int         `json:"events"`
	Sessions             int         `json:"sessions"`
	Sites                int         `json:"sites"`
	Keys                 int         `json:"keys"`
	MonotonicReads       int         `json:"violations_monotonic_reads"`
	ReadYourWrites       int         `json:"violations_read_your_writes"`
	NoStaleReadsAtOrigin int         `json:"violations_no_stale_reads_at_origin"`
	Convergence          int         `json:"violations_convergence"`
	Violations           []Violation `json:"violations,omitempty"`
}

// OK reports whether every property held.
func (r Report) OK() bool {
	return r.MonotonicReads == 0 &&
		r.ReadYourWrites == 0 &&
		r.NoStaleReadsAtOrigin == 0 &&
		r.Convergence == 0
}

// Check evaluates all four properties against a history. Properties are
// independent; a single op may contribute to multiple violation buckets
// (e.g. a stale read fails both monotonic-reads and read-your-writes).
//
// The checker assumes Events are supplied in issue order (which the
// JSONL recorder enforces via Seq). If they are not, pass them through
// SortByIssue first.
func Check(events []Event) Report {
	var rep Report
	rep.Events = len(events)

	sessions := make(map[string]struct{})
	sites := make(map[string]struct{})
	keys := make(map[string]struct{})
	for _, ev := range events {
		sessions[ev.SessionID] = struct{}{}
		sites[ev.Site] = struct{}{}
		keys[ev.Key] = struct{}{}
	}
	rep.Sessions = len(sessions)
	rep.Sites = len(sites)
	rep.Keys = len(keys)

	// A tombstone observation is consistent whenever any Delete for
	// the same key has already been recorded in the history, because
	// causal replication lets any site eventually observe it. We
	// precompute per-key delete state once and share it with the RYW
	// and origin-freshness checks; that way a session that wrote a
	// Put and then sees a tombstone caused by someone else's Delete
	// is not flagged.
	globalDeletes := latestDeletesPerKey(events)

	rep.Violations = append(rep.Violations, checkMonotonicReads(events)...)
	rep.Violations = append(rep.Violations, checkReadYourWrites(events, globalDeletes)...)
	rep.Violations = append(rep.Violations, checkNoStaleReadsAtOrigin(events, globalDeletes)...)
	rep.Violations = append(rep.Violations, checkConvergence(events)...)

	for _, v := range rep.Violations {
		switch v.Property {
		case "monotonic_reads":
			rep.MonotonicReads++
		case "read_your_writes":
			rep.ReadYourWrites++
		case "no_stale_reads_at_origin":
			rep.NoStaleReadsAtOrigin++
		case "convergence":
			rep.Convergence++
		}
	}
	sort.Slice(rep.Violations, func(i, j int) bool {
		return rep.Violations[i].Seq < rep.Violations[j].Seq
	})
	return rep
}

// --- property 1: monotonic reads ---------------------------------------

// Monotonic reads: for a fixed (session, key), successive Gets must
// observe WriteTS values that are non-decreasing. A strictly older read
// after a newer one is a violation.
//
// Tombstones get special treatment. When a Get returns Deleted=true the
// underlying storage reports zero as the "latest version" (our Store
// masks tombstone timestamps). Rather than invent a ts or relax the
// check to the point of uselessness, we skip the regression test when
// the current read is a tombstone but we also refuse to let the
// tombstone's apparent ts of zero reset the monotonicity watermark.
func checkMonotonicReads(events []Event) []Violation {
	var out []Violation
	type key struct{ session, k string }
	lastTS := make(map[key]hlc.Timestamp)
	for _, ev := range events {
		if ev.Kind != OpGet || ev.Err != "" {
			continue
		}
		// The FinalSessionID sweep reads the same key across every
		// site; that isn't a real session and cross-site variance
		// is the convergence check's job, not monotonic-reads.
		if ev.SessionID == FinalSessionID {
			continue
		}
		k := key{ev.SessionID, ev.Key}
		if ev.Deleted {
			// A tombstone read conveys no usable ts; accept it and
			// don't touch the watermark.
			continue
		}
		prev, seen := lastTS[k]
		if seen && ev.WriteTS.Before(prev) {
			out = append(out, Violation{
				Property: "monotonic_reads",
				Session:  ev.SessionID,
				Site:     ev.Site,
				Key:      ev.Key,
				Seq:      ev.Seq,
				Message: fmt.Sprintf(
					"session %s observed ts=%v after already observing ts=%v on key %q",
					ev.SessionID, ev.WriteTS, prev, ev.Key,
				),
			})
		}
		if !seen || prev.Before(ev.WriteTS) {
			lastTS[k] = ev.WriteTS
		}
	}
	return out
}

// --- property 2: read-your-writes --------------------------------------

// Read-your-writes: once a session has written (key, ts), any later Get
// from that session must observe a version with ts >= the written ts.
// A tombstone counts as a write; the subsequent Get must see either
// the tombstone or a newer version.
//
// Because our Store reports tombstones as ErrNotFound (ts=0), we track
// the last write's *kind* alongside its ts. A Deleted=true Get is
// acceptable iff:
//  1. the session's own last write on the key was a Delete (our
//     tombstone propagated back), or
//  2. some other session issued a Delete on the same key with a ts
//     strictly greater than our last Put - i.e. our write was
//     legitimately superseded by a later Delete we haven't yet
//     observed the commit ts for.
func checkReadYourWrites(events []Event, globalDeletes map[string]hlc.Timestamp) []Violation {
	var out []Violation
	type key struct{ session, k string }
	type writeInfo struct {
		ts   hlc.Timestamp
		kind OpKind
	}
	lastWrite := make(map[key]writeInfo)
	for _, ev := range events {
		k := key{ev.SessionID, ev.Key}
		switch ev.Kind {
		case OpPut, OpDelete:
			if ev.Err == "" {
				if cur, ok := lastWrite[k]; !ok || cur.ts.Before(ev.WriteTS) {
					lastWrite[k] = writeInfo{ts: ev.WriteTS, kind: ev.Kind}
				}
			}
		case OpGet:
			if ev.Err != "" {
				continue
			}
			wrote, ok := lastWrite[k]
			if !ok {
				continue
			}
			if ev.Deleted {
				if wrote.kind == OpDelete {
					continue
				}
				if delTS, ok := globalDeletes[ev.Key]; ok && wrote.ts.Before(delTS) {
					continue
				}
				out = append(out, Violation{
					Property: "read_your_writes",
					Session:  ev.SessionID,
					Site:     ev.Site,
					Key:      ev.Key,
					Seq:      ev.Seq,
					Message: fmt.Sprintf(
						"session %s wrote ts=%v then observed tombstone on key %q",
						ev.SessionID, wrote.ts, ev.Key,
					),
				})
				continue
			}
			if ev.WriteTS.Before(wrote.ts) {
				out = append(out, Violation{
					Property: "read_your_writes",
					Session:  ev.SessionID,
					Site:     ev.Site,
					Key:      ev.Key,
					Seq:      ev.Seq,
					Message: fmt.Sprintf(
						"session %s wrote ts=%v then observed ts=%v on key %q",
						ev.SessionID, wrote.ts, ev.WriteTS, ev.Key,
					),
				})
			}
		}
	}
	return out
}

// --- property 3: no stale reads at origin ------------------------------

// A stronger local guarantee: when a session issues a Get on the same
// site that produced the most recent write for the key (regardless of
// which session issued the write), that Get must observe the write -
// replication delay shouldn't cause the origin to go stale against
// itself. In the simulator this translates to: on site S, if S has
// ever issued Put(k, ts_w), any Get on S for k must see ts >= ts_w.
//
// Tombstones follow the same kind-aware rule as read-your-writes:
// reading "not found" on a site whose latest local mutation was a
// Delete is consistent; reading "not found" on a site whose latest
// local mutation was a Put means the store lost the write. If a
// later Delete for the same key exists in the history (from any
// site/session), we accept the tombstone because replication may
// have carried the newer Delete back to the origin and superseded
// the local Put.
// checkNoStaleReadsAtOrigin verifies that any read issued at site S for
// key K must observe every write at (S, K) whose response arrived
// strictly before the read was issued. Writes that are concurrent with
// the read are excluded, since the server is free to serialise them in
// either order under linearisability.
//
// Concurrency window: a write W is considered "happens-before" a read R
// iff W.IssuedAt + W.Latency <= R.IssuedAt (i.e. the client saw W's
// response before sending R). Writes that overlap R's request are
// ignored to avoid spurious failures under real gRPC concurrency.
func checkNoStaleReadsAtOrigin(events []Event, globalDeletes map[string]hlc.Timestamp) []Violation {
	var out []Violation
	type key struct{ site, k string }
	type writeEntry struct {
		ts       hlc.Timestamp
		kind     OpKind
		finished int64 // IssuedAt.UnixNano() + Latency.Nanoseconds()
	}
	// Completed writes per (site, key), sorted by finished time.
	writes := make(map[key][]writeEntry)

	for _, ev := range events {
		k := key{ev.Site, ev.Key}
		switch ev.Kind {
		case OpPut, OpDelete:
			if ev.Err != "" {
				continue
			}
			writes[k] = append(writes[k], writeEntry{
				ts:       ev.WriteTS,
				kind:     ev.Kind,
				finished: ev.IssuedAt.UnixNano() + ev.Latency.Nanoseconds(),
			})
		case OpGet:
			if ev.Err != "" {
				continue
			}
			history := writes[k]
			if len(history) == 0 {
				continue
			}
			readStart := ev.IssuedAt.UnixNano()

			// Pick the latest write that fully completed before this
			// read started. Everything after that is concurrent and may
			// or may not be visible.
			var latest writeEntry
			var found bool
			for _, w := range history {
				if w.finished > readStart {
					continue
				}
				if !found || latest.ts.Before(w.ts) {
					latest = w
					found = true
				}
			}
			if !found {
				continue
			}

			if ev.Deleted {
				if latest.kind == OpDelete {
					continue
				}
				if delTS, ok := globalDeletes[ev.Key]; ok && latest.ts.Before(delTS) {
					continue
				}
				out = append(out, Violation{
					Property: "no_stale_reads_at_origin",
					Session:  ev.SessionID,
					Site:     ev.Site,
					Key:      ev.Key,
					Seq:      ev.Seq,
					Message: fmt.Sprintf(
						"site %s held ts=%v locally but read returned tombstone on key %q",
						ev.Site, latest.ts, ev.Key,
					),
				})
				continue
			}
			if ev.WriteTS.Before(latest.ts) {
				out = append(out, Violation{
					Property: "no_stale_reads_at_origin",
					Session:  ev.SessionID,
					Site:     ev.Site,
					Key:      ev.Key,
					Seq:      ev.Seq,
					Message: fmt.Sprintf(
						"site %s held ts=%v locally but read returned ts=%v on key %q",
						ev.Site, latest.ts, ev.WriteTS, ev.Key,
					),
				})
			}
		}
	}
	return out
}

// --- property 4: eventual convergence ----------------------------------

// Convergence: consider the final state of every (site, key) pair, i.e.
// the latest write each site acknowledged for the key. If replication
// eventually converges, every site that has any value for the key must
// agree on its current version timestamp. Sites that never saw the key
// are permitted (e.g. a site that wrote it and then crashed out of the
// trace would still count - but in a well-formed simulator history
// every origin write is eventually observed by every site).
//
// The check is conservative: it flags (key, site_a, site_b) tuples
// where both sites have a stored state for the key but their latest
// WriteTS differ. For that we need a separate "final snapshot" record
// type; the simulator emits one FinalRead event per (site, key) during
// the quiescence phase. See RecordFinalRead below.
func checkConvergence(events []Event) []Violation {
	var out []Violation
	type finalObs struct {
		ts      hlc.Timestamp
		deleted bool
	}
	finals := make(map[string]map[string]finalObs)
	for _, ev := range events {
		if ev.Kind != OpGet || ev.SessionID != FinalSessionID {
			continue
		}
		bySite, ok := finals[ev.Key]
		if !ok {
			bySite = make(map[string]finalObs)
			finals[ev.Key] = bySite
		}
		bySite[ev.Site] = finalObs{ts: ev.WriteTS, deleted: ev.Deleted}
	}
	for key, bySite := range finals {
		if len(bySite) < 2 {
			continue
		}
		var (
			ref      finalObs
			refSite  string
			first    = true
		)
		for site, obs := range bySite {
			if first {
				ref, refSite = obs, site
				first = false
				continue
			}
			// Two tombstones agree even though ts=0 at both.
			if ref.deleted && obs.deleted {
				continue
			}
			if ref.deleted != obs.deleted || ref.ts != obs.ts {
				rendered := func(o finalObs) string {
					if o.deleted {
						return "<tombstone>"
					}
					return fmt.Sprintf("%v", o.ts)
				}
				out = append(out, Violation{
					Property: "convergence",
					Key:      key,
					Site:     site,
					Message: fmt.Sprintf(
						"divergent final state for key %q: site %s = %s, site %s = %s",
						key, refSite, rendered(ref), site, rendered(obs),
					),
				})
			}
		}
	}
	return out
}

// latestDeletesPerKey returns, for every key that saw at least one
// Delete, the maximum WriteTS across all Delete events. Used by the
// tombstone-aware RYW and origin-freshness checks: a Deleted=true
// read is consistent with an earlier Put iff some Delete with a
// strictly greater ts exists somewhere in the history.
func latestDeletesPerKey(events []Event) map[string]hlc.Timestamp {
	out := make(map[string]hlc.Timestamp)
	for _, ev := range events {
		if ev.Kind != OpDelete || ev.Err != "" {
			continue
		}
		if cur, ok := out[ev.Key]; !ok || cur.Before(ev.WriteTS) {
			out[ev.Key] = ev.WriteTS
		}
	}
	return out
}

// SortByIssue sorts events by Seq in place. Callers with out-of-order
// inputs should invoke this before Check.
func SortByIssue(events []Event) {
	sort.Slice(events, func(i, j int) bool { return events[i].Seq < events[j].Seq })
}

// FinalSessionID is the synthetic session tag for convergence-phase
// reads. Workload harnesses should emit one FinalRead per (site, key)
// after quiescence.
const FinalSessionID = "__final__"
