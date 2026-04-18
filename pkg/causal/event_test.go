package causal

import (
	"testing"

	"edge-cloud-replication/pkg/hlc"
)

func TestEvent_Validate(t *testing.T) {
	good := Event{
		Origin:   "A",
		Key:      "k",
		CommitTS: hlc.Timestamp{Physical: 1},
	}
	if err := good.Validate(); err != nil {
		t.Fatalf("good event rejected: %v", err)
	}

	cases := map[string]Event{
		"empty key":     {Origin: "A", CommitTS: hlc.Timestamp{Physical: 1}},
		"empty origin":  {Key: "k", CommitTS: hlc.Timestamp{Physical: 1}},
		"zero ts":       {Origin: "A", Key: "k"},
		"deleted+value": {Origin: "A", Key: "k", CommitTS: hlc.Timestamp{Physical: 1}, Deleted: true, Value: []byte("x")},
	}
	for name, e := range cases {
		if err := e.Validate(); err == nil {
			t.Errorf("%s: expected error, got nil", name)
		}
	}
}

func TestEventProto_RoundTrip(t *testing.T) {
	in := &Event{
		EventID:  42,
		SenderID: "node-1",
		Origin:   "edge-A",
		Key:      "alpha",
		Value:    []byte("hello world"),
		Deleted:  false,
		CommitTS: hlc.Timestamp{Physical: 1234, Logical: 5},
		Deps: hlc.PartitionedTimestamp{
			Origin: "edge-A",
			Groups: map[hlc.GroupID]hlc.Timestamp{
				"edge-A": {Physical: 1230, Logical: 1},
				"edge-B": {Physical: 900, Logical: 0},
			},
		},
	}
	out := EventFromProto(EventToProto(in))

	if out.EventID != in.EventID || out.SenderID != in.SenderID ||
		out.Origin != in.Origin || out.Key != in.Key ||
		string(out.Value) != string(in.Value) || out.Deleted != in.Deleted ||
		out.CommitTS != in.CommitTS {
		t.Fatalf("round-trip mismatch:\nin=%+v\nout=%+v", in, out)
	}
	if !out.Deps.Equal(in.Deps) {
		t.Fatalf("deps mismatch:\nin=%v\nout=%v", in.Deps, out.Deps)
	}
}
