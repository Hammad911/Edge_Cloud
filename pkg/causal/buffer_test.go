package causal

import (
	"testing"

	"edge-cloud-replication/pkg/hlc"
)

func evt(id uint64, origin hlc.GroupID, key string, ts int64, deps map[hlc.GroupID]int64) *Event {
	depsMap := make(map[hlc.GroupID]hlc.Timestamp, len(deps))
	for g, t := range deps {
		depsMap[g] = hlc.Timestamp{Physical: t}
	}
	return &Event{
		EventID:  id,
		Origin:   origin,
		Key:      key,
		Value:    []byte(key + "/v"),
		CommitTS: hlc.Timestamp{Physical: ts},
		Deps:     hlc.PartitionedTimestamp{Origin: origin, Groups: depsMap},
	}
}

func TestBuffer_DeliversWhenDepsSatisfied(t *testing.T) {
	b := NewBuffer()

	// e1 depends on group "B" being at >= 50.
	e1 := evt(1, "A", "k1", 100, map[hlc.GroupID]int64{"B": 50})
	res, err := b.Admit(e1)
	if err != nil {
		t.Fatalf("admit e1: %v", err)
	}
	if res != AdmitBuffered {
		t.Fatalf("expected AdmitBuffered, got %v", res)
	}

	if got := b.Deliver(); len(got) != 0 {
		t.Fatalf("expected nothing deliverable, got %d", len(got))
	}

	// Advance B to 60 — e1 should now drain on the next Deliver call.
	b.AdvanceFrontier("B", hlc.Timestamp{Physical: 60})
	got := b.Deliver()
	if len(got) != 1 || got[0].EventID != 1 {
		t.Fatalf("expected [e1], got %v", got)
	}

	// Frontier now reflects A=100, B=60.
	f := b.Frontier()
	if f.Groups["A"].Physical != 100 || f.Groups["B"].Physical != 60 {
		t.Fatalf("unexpected frontier: %v", f)
	}
}

func TestBuffer_ChainsCausalUnblock(t *testing.T) {
	b := NewBuffer()

	// e2 depends on e1 (origin A, ts 100). e1 depends on B>=50.
	e1 := evt(1, "A", "k1", 100, map[hlc.GroupID]int64{"B": 50})
	e2 := evt(2, "A", "k2", 200, map[hlc.GroupID]int64{"A": 100, "B": 50})

	for _, e := range []*Event{e2, e1} {
		if _, err := b.Admit(e); err != nil {
			t.Fatalf("admit %d: %v", e.EventID, err)
		}
	}
	if b.PendingCount() != 2 {
		t.Fatalf("expected 2 pending, got %d", b.PendingCount())
	}

	b.AdvanceFrontier("B", hlc.Timestamp{Physical: 50})
	got := b.Deliver()
	if len(got) != 2 {
		t.Fatalf("expected 2 deliverables, got %d", len(got))
	}
	if got[0].EventID != 1 || got[1].EventID != 2 {
		t.Fatalf("expected ordered [1,2], got [%d,%d]", got[0].EventID, got[1].EventID)
	}
}

func TestBuffer_DedupsByOriginAndCommitTS(t *testing.T) {
	b := NewBuffer()
	e := evt(1, "A", "k", 10, nil)
	if r, _ := b.Admit(e); r != AdmitDelivered {
		t.Fatalf("first admit should be delivered, got %v", r)
	}
	// drain to advance frontier and dedup
	if got := b.Deliver(); len(got) != 1 {
		t.Fatalf("expected 1 delivered, got %d", len(got))
	}
	if r, _ := b.Admit(e); r != AdmitDuplicate {
		t.Fatalf("second admit should be duplicate, got %v", r)
	}
}

func TestBuffer_EnforcesCapacity(t *testing.T) {
	b := NewBuffer(WithMaxBuffered(1))
	e1 := evt(1, "A", "k1", 100, map[hlc.GroupID]int64{"B": 999})
	if _, err := b.Admit(e1); err != nil {
		t.Fatalf("admit e1: %v", err)
	}
	e2 := evt(2, "A", "k2", 200, map[hlc.GroupID]int64{"B": 999})
	if _, err := b.Admit(e2); err != ErrBufferFull {
		t.Fatalf("expected ErrBufferFull, got %v", err)
	}
}
