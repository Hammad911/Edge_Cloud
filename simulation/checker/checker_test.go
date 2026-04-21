package checker

import (
	"testing"

	"edge-cloud-replication/pkg/hlc"
)

func ts(wall int64, log uint32) hlc.Timestamp {
	return hlc.Timestamp{Physical: wall, Logical: log}
}

func TestChecker_RemoteWithSatisfiedDepsPasses(t *testing.T) {
	c := New()
	if !c.Record(Observation{
		Site: "s1", Kind: KindRemote, Origin: "g1", CommitTS: ts(10, 0),
		Deps: hlc.PartitionedTimestamp{Groups: map[hlc.GroupID]hlc.Timestamp{}},
	}) {
		t.Fatal("expected pass for empty deps")
	}
	if !c.Record(Observation{
		Site: "s1", Kind: KindRemote, Origin: "g2", CommitTS: ts(15, 0),
		Deps: hlc.PartitionedTimestamp{Groups: map[hlc.GroupID]hlc.Timestamp{
			"g1": ts(10, 0),
		}},
	}) {
		t.Fatal("expected pass: g1@10 already applied")
	}
	if got := c.Violations(); len(got) != 0 {
		t.Fatalf("violations: %v", got)
	}
}

func TestChecker_RemoteMissingDepIsReported(t *testing.T) {
	c := New()
	pass := c.Record(Observation{
		Site: "s1", Kind: KindRemote, Origin: "g2", CommitTS: ts(15, 0),
		Deps: hlc.PartitionedTimestamp{Groups: map[hlc.GroupID]hlc.Timestamp{
			"g1": ts(10, 0),
		}},
	})
	if pass {
		t.Fatal("expected fail: g1 was never applied at s1")
	}
	v := c.Violations()
	if len(v) != 1 {
		t.Fatalf("violations: %d", len(v))
	}
	if v[0].Site != "s1" || v[0].Kind != "missing-dep" {
		t.Fatalf("violation: %+v", v[0])
	}
}

func TestChecker_LocalIsNotChecked(t *testing.T) {
	c := New()
	if !c.Record(Observation{
		Site: "s1", Kind: KindLocal, Origin: "s1g", CommitTS: ts(5, 0),
		Deps: hlc.PartitionedTimestamp{Groups: map[hlc.GroupID]hlc.Timestamp{
			"never-seen": ts(100, 0), // would fail if checked
		}},
	}) {
		t.Fatal("local should never be flagged")
	}
	if v := c.Violations(); len(v) != 0 {
		t.Fatalf("local triggered violations: %v", v)
	}
}
