package baselines_test

import (
	"testing"

	"edge-cloud-replication/pkg/causal"
	"edge-cloud-replication/pkg/hlc"
	"edge-cloud-replication/simulation/baselines"
)

// mkEvent builds a causal.Event with the requested dep fan-out and
// payload size. Group ids are short fixed-width strings so callers can
// predict metadata sizes.
func mkEvent(key, value string, depGroups ...hlc.GroupID) *causal.Event {
	deps := hlc.NewPartitionedTimestamp("edge-0000")
	for _, g := range depGroups {
		deps.Groups[g] = hlc.Timestamp{Physical: 1, Logical: 2}
	}
	return &causal.Event{
		Origin:  "edge-0000",
		Key:     key,
		Value:   []byte(value),
		Deps:    deps,
		CommitTS: hlc.Timestamp{Physical: 1, Logical: 2},
	}
}

// TestMeasureEvent_Constants verifies per-event metadata for each scheme
// against the arithmetic specified in scheme.go.
func TestMeasureEvent_Constants(t *testing.T) {
	ev := mkEvent("k0", "val-16-bytes-xx", "edge-0001", "edge-0002")
	sizes := baselines.MeasureEvent(ev, /*numSites=*/ 50)

	if got := sizes[baselines.Eventual].Metadata; got != 0 {
		t.Errorf("eventual metadata = %d, want 0", got)
	}
	if got := sizes[baselines.Lamport].Metadata; got != 8 {
		t.Errorf("lamport metadata = %d, want 8", got)
	}
	// Two 9-byte group ids ("edge-XXXX"): 2 * (1 + 9 + 12) = 44.
	if got := sizes[baselines.PartitionedHLC].Metadata; got != 44 {
		t.Errorf("partitioned_hlc metadata = %d, want 44", got)
	}
	if got := sizes[baselines.VectorClock].Metadata; got != 50*8 {
		t.Errorf("vector_clock metadata = %d, want %d", got, 50*8)
	}
	// Payload is just key+value bytes.
	if got := sizes[baselines.Eventual].Payload; got != len("k0")+len("val-16-bytes-xx") {
		t.Errorf("payload = %d, want %d", got, len("k0")+len("val-16-bytes-xx"))
	}
}

// TestRollingStats_Accumulates confirms the accumulator's snapshot
// matches the per-scheme sum of per-event sizes.
func TestRollingStats_Accumulates(t *testing.T) {
	rs := baselines.NewRollingStats(100)
	for i := 0; i < 10; i++ {
		rs.Record(mkEvent("k", "v", "edge-0001"))
	}
	rep := rs.Snapshot()

	if rep.Events != 10 {
		t.Fatalf("events = %d, want 10", rep.Events)
	}
	// Vector clock: 100 * 8 = 800 bytes per event * 10 events = 8000.
	if got := rep.Schemes["vector_clock"].MetadataBytesTotal; got != 8000 {
		t.Errorf("vector_clock total = %d, want 8000", got)
	}
	if got := rep.Schemes["eventual"].MetadataBytesTotal; got != 0 {
		t.Errorf("eventual total = %d, want 0", got)
	}
	// Mean for lamport = 8 bytes.
	if got := rep.Schemes["lamport"].MetadataBytesMean; got != 8 {
		t.Errorf("lamport mean = %v, want 8", got)
	}
}

// TestScaling_VectorClockDominates is the paper's central claim: at
// large site counts the full vector clock's metadata per event blows
// past the partitioned-HLC's even when every dep slot is populated.
func TestScaling_VectorClockDominates(t *testing.T) {
	// 5 dep groups, i.e. the causal fan-out for a typical write.
	ev := mkEvent("k", "value", "g1", "g2", "g3", "g4", "g5")

	for _, n := range []int{50, 100, 500, 1000} {
		sizes := baselines.MeasureEvent(ev, n)
		pHLC := sizes[baselines.PartitionedHLC].Metadata
		vc := sizes[baselines.VectorClock].Metadata
		if vc <= pHLC {
			t.Errorf("at %d sites expected VC (%d) > pHLC (%d)", n, vc, pHLC)
		}
	}
}

// TestMeasureEvent_NilEvent is defensive: a nil event must return no
// metadata rather than panicking.
func TestMeasureEvent_NilEvent(t *testing.T) {
	if got := baselines.MeasureEvent(nil, 100); len(got) != 0 {
		t.Fatalf("expected empty map, got %v", got)
	}
	rs := baselines.NewRollingStats(100)
	rs.Record(nil)
	if rs.Events() != 0 {
		t.Fatalf("expected 0 events after nil Record")
	}
}

// TestProject_RealisticClustering demonstrates that under realistic
// clustering (K sites sharing a Raft group), partitioned-HLC metadata
// grows with G not N. At N=100, G=4 the ratio must be strictly in
// pHLC's favour.
func TestProject_RealisticClustering(t *testing.T) {
	// Worst case for pHLC: every one of the 4 groups has an entry in
	// every event's dep vector.
	proj := baselines.Project(4, /*numSites=*/ 100, /*numGroups=*/ 4)
	if proj.PartitionedHLC >= proj.VectorClock {
		t.Fatalf("partitioned-HLC (%.0f) should be smaller than VC (%.0f) at N=100, G=4",
			proj.PartitionedHLC, proj.VectorClock)
	}
	if proj.Lamport != 8 {
		t.Errorf("lamport projection = %.0f, want 8", proj.Lamport)
	}
	if proj.Eventual != 0 {
		t.Errorf("eventual projection = %.0f, want 0", proj.Eventual)
	}
}

// TestProject_Saturation: at G == N, partitioned-HLC degenerates to
// per-site metadata and should be roughly comparable to (though still
// larger than) a full vector clock due to the group-id overhead.
func TestProject_Saturation(t *testing.T) {
	proj := baselines.Project(64, /*numSites=*/ 64, /*numGroups=*/ 64)
	if proj.PartitionedHLC <= proj.VectorClock {
		t.Errorf("expected pHLC > VC when G=N=64, got pHLC=%.0f VC=%.0f",
			proj.PartitionedHLC, proj.VectorClock)
	}
}
