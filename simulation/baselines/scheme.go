// Package baselines models the metadata overhead of the consistency
// schemes we compare against in the paper:
//
//   - Eventual         : no ordering metadata, pure LWW broadcast.
//   - Lamport          : one 8-byte scalar per event.
//   - Partitioned HLC  : one (group-id + hlc) pair per causal dependency.
//   - Vector Clock     : one 8-byte logical counter per site in the system.
//
// Behaviourally, every async scheme in this list commits the local write
// at microsecond latency and fans the update out to peers; the *only*
// material difference is how many bytes of causal metadata ride along
// with each replicated event. The paper's central scaling claim is that
// our Partitioned HLC stays sub-linear while Full Vector Clocks grow
// linearly with the site count.
//
// Rather than spin up three parallel simulators (each with its own
// storage, clock, and buffer) and hope the timing-sensitive numbers are
// comparable, we take a cleaner approach: capture the *actual* event
// stream our partitioned-HLC simulator produces and ask each scheme how
// many metadata bytes it would attach. Same workload, same deliveries,
// same causal graph - only the metadata accounting differs.
//
// This is exactly the quantity plotted in the "metadata-size vs sites"
// figure and is trivial to compute deterministically from a
// causal.Event.
package baselines

import (
	"encoding/json"

	"edge-cloud-replication/pkg/causal"
	"edge-cloud-replication/pkg/hlc"
)

// Scheme enumerates the consistency baselines we track metadata for.
type Scheme int

const (
	// Eventual is the zero-metadata baseline: a broadcast LWW store.
	// Every replicated write carries only (key, value, deleted-flag)
	// with no causal ordering information. This produces the lowest
	// wire cost and the weakest guarantees (no causal consistency,
	// reorderings visible to clients).
	Eventual Scheme = iota
	// Lamport attaches one scalar Lamport clock per event. It captures
	// "happens-before" between events that actually observe each
	// other, but not concurrency. Metadata is O(1).
	Lamport
	// PartitionedHLC is *our* scheme: each event carries an HLC
	// dependency vector keyed by group, with one entry per dependent
	// group. For single-group-per-site deployments this degenerates to
	// a per-site vector, but in multi-site clusters (K sites sharing
	// one Raft group) metadata stays at O(G) not O(K*G).
	PartitionedHLC
	// VectorClock is the classic full vector clock: one logical
	// counter per *site* in the system. Metadata is O(N).
	VectorClock
)

// String returns a stable identifier for the scheme (used in JSON keys).
func (s Scheme) String() string {
	switch s {
	case Eventual:
		return "eventual"
	case Lamport:
		return "lamport"
	case PartitionedHLC:
		return "partitioned_hlc"
	case VectorClock:
		return "vector_clock"
	default:
		return "unknown"
	}
}

// AllSchemes returns the schemes reported by the simulator.
func AllSchemes() []Scheme { return []Scheme{Eventual, Lamport, PartitionedHLC, VectorClock} }

// Sizes captures the byte budget of a single event under every scheme.
// Payload is the raw (key + value) size the underlying store needs;
// every scheme pays this cost. Metadata is the *additional* bytes of
// ordering information that ride alongside the payload on the wire.
type Sizes struct {
	Payload  int `json:"payload"`
	Metadata int `json:"metadata"`
}

// MeasureEvent returns the (payload, metadata) sizes of ev under every
// scheme. NumSites is the total number of sites in the system and is
// only used by VectorClock; the other schemes are site-count invariant.
//
// Costs are computed against a deliberately simple wire format:
//
//   - Key / value bytes are counted verbatim.
//   - Logical counters are 8 bytes (uint64).
//   - HLC timestamps are 12 bytes (int64 physical + uint32 logical).
//   - GroupID strings are counted by their UTF-8 byte length + a 1-byte
//     length prefix. This matches what a protobuf or msgpack encoder
//     would actually emit and is the right unit to plot.
func MeasureEvent(ev *causal.Event, numSites int) map[Scheme]Sizes {
	if ev == nil {
		return map[Scheme]Sizes{}
	}
	payload := len(ev.Key) + len(ev.Value)

	return map[Scheme]Sizes{
		Eventual:       {Payload: payload, Metadata: 0},
		Lamport:        {Payload: payload, Metadata: 8},
		PartitionedHLC: {Payload: payload, Metadata: partitionedHLCMetadata(ev.Deps)},
		VectorClock:    {Payload: payload, Metadata: numSites * 8},
	}
}

func partitionedHLCMetadata(deps hlc.PartitionedTimestamp) int {
	// For each (group, timestamp) pair the wire format is:
	//   length-prefix(1B) + groupID bytes + HLC(12B)
	// which is what protobuf's varint length-delimited embedding costs
	// for short group ids.
	total := 0
	for group := range deps.Groups {
		total += 1 + len(group) + 12
	}
	return total
}

// RollingStats tracks running totals of payload/metadata across many
// events. It is goroutine-safe only to the extent that callers
// serialise Record()/Snapshot(); the simulator does so by feeding
// samples from a single observer goroutine.
type RollingStats struct {
	events   int64
	sites    int
	payloadB int64
	metaB    map[Scheme]int64
}

// NewRollingStats returns a fresh accumulator keyed to the given site
// count (used for the VectorClock metadata model).
func NewRollingStats(numSites int) *RollingStats {
	m := make(map[Scheme]int64, 4)
	for _, s := range AllSchemes() {
		m[s] = 0
	}
	return &RollingStats{sites: numSites, metaB: m}
}

// Record a single event. Safe to call many thousands of times per
// second; the per-call cost is a constant-size map lookup.
func (r *RollingStats) Record(ev *causal.Event) {
	if r == nil || ev == nil {
		return
	}
	sizes := MeasureEvent(ev, r.sites)
	r.events++
	r.payloadB += int64(sizes[Eventual].Payload)
	for s, sz := range sizes {
		r.metaB[s] += int64(sz.Metadata)
	}
}

// Events reports the number of recorded events.
func (r *RollingStats) Events() int64 { return r.events }

// Snapshot returns a JSON-friendly view with per-scheme totals and
// per-event averages. Averages are floats in bytes.
func (r *RollingStats) Snapshot() Report {
	out := Report{
		NumSites:        r.sites,
		Events:          r.events,
		AvgPayloadBytes: ratioFloat(r.payloadB, r.events),
		Schemes:         make(map[string]SchemeReport, 4),
	}
	for _, s := range AllSchemes() {
		total := r.metaB[s]
		out.Schemes[s.String()] = SchemeReport{
			MetadataBytesTotal: total,
			MetadataBytesMean:  ratioFloat(total, r.events),
		}
	}
	return out
}

func ratioFloat(num, den int64) float64 {
	if den == 0 {
		return 0
	}
	return float64(num) / float64(den)
}

// Report is the JSON-ready snapshot of a run's metadata accounting.
type Report struct {
	NumSites        int                     `json:"num_sites"`
	Events          int64                   `json:"events"`
	AvgPayloadBytes float64                 `json:"avg_payload_bytes"`
	Schemes         map[string]SchemeReport `json:"schemes"`
}

// SchemeReport aggregates a single scheme's metadata footprint.
type SchemeReport struct {
	MetadataBytesTotal int64   `json:"metadata_bytes_total"`
	MetadataBytesMean  float64 `json:"metadata_bytes_mean"`
}

// MarshalJSON stabilises field order for snapshot comparison in tests
// and shell scripts that diff JSON output.
func (r Report) MarshalJSON() ([]byte, error) {
	type alias Report
	return json.Marshal(alias(r))
}

// Projection holds per-event metadata byte counts for every scheme at a
// specific (N_sites, G_groups) configuration. It is computed
// analytically from the model: we assume each event depends on G
// groups' frontiers (worst case for partitioned HLC) and use a
// constant-size group identifier of groupIDBytesProjected bytes.
type Projection struct {
	NumSites       int     `json:"num_sites"`
	NumGroups      int     `json:"num_groups"`
	AvgDeps        float64 `json:"avg_deps_per_event"`
	Eventual       float64 `json:"eventual"`
	Lamport        float64 `json:"lamport"`
	PartitionedHLC float64 `json:"partitioned_hlc"`
	VectorClock    float64 `json:"vector_clock"`
}

// Number of bytes a group identifier occupies on the wire in a
// projected deployment. 4 bytes is the natural choice: a uint32 index
// into a site-registry table covers 4 billion groups and is what any
// production partitioned-HLC implementation would use rather than a
// human-readable string.
const groupIDBytesProjected = 4

// hlcBytes is the wire size of a single HLC timestamp (physical int64
// + logical uint32).
const hlcBytes = 12

// Project returns the metadata footprint under a configuration that
// clusters numSites into numGroups groups. avgDeps is the mean number
// of distinct-group dependencies observed per event in the measured
// run; in the worst case avgDeps == numGroups, which is the value the
// simulator reports directly. Use this to extrapolate a measured run
// to real deployments where K sites share a Raft group.
func Project(avgDeps float64, numSites, numGroups int) Projection {
	if numGroups > numSites {
		numGroups = numSites
	}
	if avgDeps > float64(numGroups) {
		avgDeps = float64(numGroups)
	}
	return Projection{
		NumSites:       numSites,
		NumGroups:      numGroups,
		AvgDeps:        avgDeps,
		Eventual:       0,
		Lamport:        8,
		PartitionedHLC: avgDeps * float64(groupIDBytesProjected+hlcBytes),
		VectorClock:    float64(numSites * 8),
	}
}

// EventsAvgDeps recovers the mean partitioned-HLC dep fan-out per event
// from a Report, inverting the per-entry encoding used in MeasureEvent.
// Used by the CLI to feed Project() with the actual observed dep density.
func EventsAvgDeps(r *Report, numSites int) float64 {
	if r == nil || r.Events == 0 {
		return 0
	}
	_ = numSites
	mean := r.Schemes[PartitionedHLC.String()].MetadataBytesMean
	// Each dep entry costs (1 + len(groupID) + 12) in the wire format.
	// The simulator uses "edge-XXXX" / "cloud" group ids; len averages
	// 8 bytes. 1 + 8 + 12 = 21 bytes per entry.
	const bytesPerEntry = 21.0
	return mean / bytesPerEntry
}
