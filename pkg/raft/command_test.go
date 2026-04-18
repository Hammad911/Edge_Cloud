package raft

import (
	"bytes"
	"testing"

	"edge-cloud-replication/pkg/hlc"
)

func TestCommand_RoundTrip(t *testing.T) {
	cases := []Command{
		{Op: OpPut, Timestamp: hlc.Timestamp{Physical: 1, Logical: 0}, Key: "a", Value: []byte("v1")},
		{Op: OpPut, Timestamp: hlc.Timestamp{Physical: 2, Logical: 5}, Key: "", Value: nil},
		{Op: OpDelete, Timestamp: hlc.Timestamp{Physical: 1 << 40, Logical: 1 << 20}, Key: "k/with/slashes", Value: nil},
		{Op: OpPut, Timestamp: hlc.Timestamp{Physical: 7}, Key: "bin", Value: []byte{0, 1, 2, 3, 0xff}},
	}

	for i, want := range cases {
		data, err := want.MarshalBinary()
		if err != nil {
			t.Fatalf("case %d: marshal: %v", i, err)
		}
		var got Command
		if err := got.UnmarshalBinary(data); err != nil {
			t.Fatalf("case %d: unmarshal: %v", i, err)
		}
		if got.Op != want.Op {
			t.Errorf("case %d: op mismatch: %d vs %d", i, got.Op, want.Op)
		}
		if got.Timestamp != want.Timestamp {
			t.Errorf("case %d: ts mismatch: %v vs %v", i, got.Timestamp, want.Timestamp)
		}
		if got.Key != want.Key {
			t.Errorf("case %d: key mismatch: %q vs %q", i, got.Key, want.Key)
		}
		if !bytes.Equal(got.Value, want.Value) {
			t.Errorf("case %d: value mismatch: %v vs %v", i, got.Value, want.Value)
		}
	}
}

func TestCommand_Unmarshal_RejectsTooShort(t *testing.T) {
	var c Command
	if err := c.UnmarshalBinary([]byte{1, 2, 3}); err == nil {
		t.Fatalf("expected error for short buffer")
	}
}

func TestCommand_Unmarshal_RejectsBadVersion(t *testing.T) {
	buf := make([]byte, 22)
	buf[0] = 0xff
	var c Command
	if err := c.UnmarshalBinary(buf); err == nil {
		t.Fatalf("expected error for bad version")
	}
}
