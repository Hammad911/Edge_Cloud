package hlc

import (
	"testing"
)

func TestPartitionedClock_StampAndPeek(t *testing.T) {
	f := &fakeClock{}
	f.Set(1000)
	local := New(WithPhysicalClock(f.Now))
	p := NewPartitionedClock("dc-a", local)

	ts := p.Stamp()
	if ts.Origin != "dc-a" {
		t.Fatalf("origin: got %q want dc-a", ts.Origin)
	}
	if len(ts.Groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(ts.Groups))
	}
	if ts.Groups["dc-a"].Physical != 1000 {
		t.Fatalf("own group entry wrong: %v", ts.Groups["dc-a"])
	}

	peek := p.Peek()
	if !peek.Equal(ts) {
		t.Fatalf("peek != stamp: %v vs %v", peek, ts)
	}
}

func TestPartitionedClock_Merge_TracksRemoteGroups(t *testing.T) {
	fa := &fakeClock{}
	fa.Set(500)
	pa := NewPartitionedClock("dc-a", New(WithPhysicalClock(fa.Now)))

	fb := &fakeClock{}
	fb.Set(700)
	pb := NewPartitionedClock("dc-b", New(WithPhysicalClock(fb.Now)))

	// pa stamps a local write so its own entry exists.
	_ = pa.Stamp()

	remote := pb.Stamp()
	_, err := pa.Merge(remote)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	snap := pa.Peek()
	if len(snap.Groups) != 2 {
		t.Fatalf("expected 2 groups after merge, got %d: %v", len(snap.Groups), snap)
	}
	if snap.Groups["dc-b"].Physical < 700 {
		t.Fatalf("expected dc-b entry >= 700, got %v", snap.Groups["dc-b"])
	}
}

// TestPartitionedClock_Merge_DoesNotInflateOwn checks that Merge does not
// advance the local ownGroup entry. Without this guarantee, locally
// stamped events would advertise causal dependencies on a value no peer
// has ever observed, making the events un-deliverable across the
// replication boundary (this caused a real bug in causal replication).
func TestPartitionedClock_Merge_DoesNotInflateOwn(t *testing.T) {
	fa := &fakeClock{}
	fa.Set(500)
	pa := NewPartitionedClock("dc-a", New(WithPhysicalClock(fa.Now)))

	// First stamp records own=500.
	first := pa.Stamp()
	if first.Groups["dc-a"].Physical != 500 {
		t.Fatalf("first stamp: %v", first)
	}

	// A remote merge from dc-b advances the local HLC under the hood,
	// but must NOT touch dc-a's own-frontier entry.
	fb := &fakeClock{}
	fb.Set(700)
	pb := NewPartitionedClock("dc-b", New(WithPhysicalClock(fb.Now)))
	remote := pb.Stamp()
	if _, err := pa.Merge(remote); err != nil {
		t.Fatalf("merge: %v", err)
	}

	snap := pa.Peek()
	if got := snap.Groups["dc-a"].Physical; got != 500 {
		t.Fatalf("merge inflated dc-a entry to %d (expected 500)", got)
	}
}

func TestPartitionedTimestamp_HappensBefore(t *testing.T) {
	a := PartitionedTimestamp{
		Origin: "dc-a",
		Groups: map[GroupID]Timestamp{
			"dc-a": {Physical: 100, Logical: 0},
			"dc-b": {Physical: 50, Logical: 0},
		},
	}
	b := PartitionedTimestamp{
		Origin: "dc-b",
		Groups: map[GroupID]Timestamp{
			"dc-a": {Physical: 100, Logical: 0},
			"dc-b": {Physical: 80, Logical: 0},
		},
	}
	if !a.HappensBefore(b) {
		t.Fatalf("expected a ⇒ b")
	}
	if b.HappensBefore(a) {
		t.Fatalf("expected not b ⇒ a")
	}
	if a.Concurrent(b) {
		t.Fatalf("expected not concurrent")
	}
}

func TestPartitionedTimestamp_Concurrent(t *testing.T) {
	a := PartitionedTimestamp{
		Origin: "dc-a",
		Groups: map[GroupID]Timestamp{
			"dc-a": {Physical: 100},
			"dc-b": {Physical: 50},
		},
	}
	b := PartitionedTimestamp{
		Origin: "dc-b",
		Groups: map[GroupID]Timestamp{
			"dc-a": {Physical: 80},
			"dc-b": {Physical: 90},
		},
	}
	if a.HappensBefore(b) || b.HappensBefore(a) {
		t.Fatalf("expected concurrent, got one ⇒ other")
	}
	if !a.Concurrent(b) {
		t.Fatalf("Concurrent returned false for concurrent pair")
	}
}

func TestPartitionedTimestamp_HappensBefore_MissingGroupTreatedAsZero(t *testing.T) {
	a := PartitionedTimestamp{
		Origin: "dc-a",
		Groups: map[GroupID]Timestamp{"dc-a": {Physical: 100}},
	}
	b := PartitionedTimestamp{
		Origin: "dc-a",
		Groups: map[GroupID]Timestamp{
			"dc-a": {Physical: 100},
			"dc-b": {Physical: 50},
		},
	}
	if !a.HappensBefore(b) {
		t.Fatalf("expected a ⇒ b when b adds a new group")
	}
	if b.HappensBefore(a) {
		t.Fatalf("expected b ⇏ a")
	}
}

func TestPartitionedClock_StampMonotonic(t *testing.T) {
	f := &fakeClock{}
	f.Set(1000)
	p := NewPartitionedClock("dc-a", New(WithPhysicalClock(f.Now)))

	var prev PartitionedTimestamp
	for i := 0; i < 200; i++ {
		cur := p.Stamp()
		if i > 0 {
			pa := prev.Groups["dc-a"]
			ca := cur.Groups["dc-a"]
			if !pa.Before(ca) {
				t.Fatalf("stamp %d regressed own group: %v -> %v", i, pa, ca)
			}
		}
		prev = cur
	}
}

func TestPartitionedTimestamp_Clone_Independent(t *testing.T) {
	a := PartitionedTimestamp{
		Origin: "dc-a",
		Groups: map[GroupID]Timestamp{"dc-a": {Physical: 1}},
	}
	b := a.Clone()
	b.Groups["dc-a"] = Timestamp{Physical: 999}
	if a.Groups["dc-a"].Physical == 999 {
		t.Fatalf("clone shared underlying map with original")
	}
}

// BenchmarkPartitionedClock_Stamp measures per-write cost to validate the
// scalability narrative. Metadata size = Size(), which must equal the number
// of groups observed, NOT the number of sites.
func BenchmarkPartitionedClock_Stamp(b *testing.B) {
	p := NewPartitionedClock("dc-a", New())
	for i := 0; i < b.N; i++ {
		_ = p.Stamp()
	}
}
