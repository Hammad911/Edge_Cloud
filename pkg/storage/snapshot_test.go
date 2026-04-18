package storage

import (
	"bytes"
	"testing"

	"edge-cloud-replication/pkg/hlc"
)

func TestMemStore_SnapshotRoundTrip(t *testing.T) {
	src := NewMemStore()
	_ = src.Put("user:1", []byte("alice"), hlc.Timestamp{Physical: 100})
	_ = src.Put("user:1", []byte("alice2"), hlc.Timestamp{Physical: 200})
	_ = src.Delete("user:1", hlc.Timestamp{Physical: 300})
	_ = src.Put("session:x", []byte("token"), hlc.Timestamp{Physical: 150})
	_ = src.Put("session:x", nil, hlc.Timestamp{Physical: 250})

	var buf bytes.Buffer
	if err := src.Snapshot(&buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	dst := NewMemStore()
	if err := dst.Restore(&buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if got, want := dst.Size(), src.Size(); got != want {
		t.Fatalf("size mismatch: got %d want %d", got, want)
	}

	for _, key := range src.Keys() {
		sh, err := src.History(key)
		if err != nil {
			t.Fatalf("src history %q: %v", key, err)
		}
		dh, err := dst.History(key)
		if err != nil {
			t.Fatalf("dst history %q: %v", key, err)
		}
		if len(sh) != len(dh) {
			t.Fatalf("history length mismatch for %q: %d vs %d", key, len(sh), len(dh))
		}
		for i := range sh {
			if sh[i].Timestamp != dh[i].Timestamp {
				t.Errorf("ts mismatch key=%q idx=%d: %v vs %v", key, i, sh[i].Timestamp, dh[i].Timestamp)
			}
			if sh[i].Deleted != dh[i].Deleted {
				t.Errorf("deleted mismatch key=%q idx=%d: %v vs %v", key, i, sh[i].Deleted, dh[i].Deleted)
			}
			if !bytes.Equal(sh[i].Value, dh[i].Value) {
				t.Errorf("value mismatch key=%q idx=%d: %q vs %q", key, i, sh[i].Value, dh[i].Value)
			}
		}
	}
}

func TestMemStore_Snapshot_EmptyStore(t *testing.T) {
	src := NewMemStore()
	var buf bytes.Buffer
	if err := src.Snapshot(&buf); err != nil {
		t.Fatalf("snapshot empty: %v", err)
	}
	dst := NewMemStore()
	if err := dst.Restore(&buf); err != nil {
		t.Fatalf("restore empty: %v", err)
	}
	if dst.Size() != 0 {
		t.Fatalf("expected empty restored store, got size=%d", dst.Size())
	}
}

func TestMemStore_Restore_RejectsBadMagic(t *testing.T) {
	buf := bytes.NewReader([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0})
	if err := NewMemStore().Restore(buf); err == nil {
		t.Fatalf("expected error for bad magic")
	}
}
