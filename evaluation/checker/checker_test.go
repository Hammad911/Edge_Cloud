package checker_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"edge-cloud-replication/evaluation/checker"
	"edge-cloud-replication/pkg/hlc"
)

func ts(phys int64, log uint32) hlc.Timestamp {
	return hlc.Timestamp{Physical: phys, Logical: log}
}

func put(seq int64, session, site, key string, val []byte, wts hlc.Timestamp) checker.Event {
	return checker.Event{Seq: seq, SessionID: session, Site: site, Kind: checker.OpPut,
		Key: key, Value: val, WriteTS: wts, IssuedAt: time.Now()}
}

func get(seq int64, session, site, key string, val []byte, wts hlc.Timestamp) checker.Event {
	return checker.Event{Seq: seq, SessionID: session, Site: site, Kind: checker.OpGet,
		Key: key, Value: val, WriteTS: wts, IssuedAt: time.Now()}
}

// TestCheck_HappyPath: a fully-ordered history must have zero
// violations on every property.
func TestCheck_HappyPath(t *testing.T) {
	events := []checker.Event{
		put(1, "s1", "edge-A", "k", []byte("v1"), ts(100, 0)),
		get(2, "s1", "edge-A", "k", []byte("v1"), ts(100, 0)),
		put(3, "s1", "edge-A", "k", []byte("v2"), ts(200, 0)),
		get(4, "s1", "edge-A", "k", []byte("v2"), ts(200, 0)),
		get(5, "s2", "edge-A", "k", []byte("v2"), ts(200, 0)),
	}
	rep := checker.Check(events)
	if !rep.OK() {
		t.Fatalf("expected zero violations, got %+v", rep)
	}
	if rep.Events != 5 || rep.Sessions != 2 {
		t.Errorf("unexpected counts: %+v", rep)
	}
}

// TestCheck_MonotonicReads_Violated detects a read going backwards in
// time for the same session+key.
func TestCheck_MonotonicReads_Violated(t *testing.T) {
	events := []checker.Event{
		get(1, "s1", "edge-A", "k", []byte("v2"), ts(200, 0)),
		get(2, "s1", "edge-A", "k", []byte("v1"), ts(100, 0)),
	}
	rep := checker.Check(events)
	if rep.MonotonicReads != 1 {
		t.Fatalf("expected 1 monotonic-reads violation, got %+v", rep)
	}
	if rep.Violations[0].Seq != 2 {
		t.Errorf("violation should be flagged at seq 2, got %d", rep.Violations[0].Seq)
	}
}

// TestCheck_ReadYourWrites_Violated detects a session that sees an
// older version than one it wrote itself.
func TestCheck_ReadYourWrites_Violated(t *testing.T) {
	events := []checker.Event{
		put(1, "s1", "edge-A", "k", []byte("v2"), ts(200, 0)),
		get(2, "s1", "edge-B", "k", []byte("v1"), ts(100, 0)),
	}
	rep := checker.Check(events)
	if rep.ReadYourWrites != 1 {
		t.Fatalf("expected 1 RYW violation, got %+v", rep)
	}
}

// TestCheck_OriginFreshness detects a site failing to read back a
// write it just produced locally.
func TestCheck_OriginFreshness(t *testing.T) {
	events := []checker.Event{
		put(1, "s1", "edge-A", "k", []byte("v2"), ts(200, 0)),
		get(2, "s2", "edge-A", "k", []byte("v1"), ts(100, 0)), // different session, same site
	}
	rep := checker.Check(events)
	if rep.NoStaleReadsAtOrigin != 1 {
		t.Fatalf("expected 1 origin-freshness violation, got %+v", rep)
	}
}

// TestCheck_Convergence detects divergent final state across sites.
func TestCheck_Convergence(t *testing.T) {
	events := []checker.Event{
		put(1, "s1", "edge-A", "k", []byte("v1"), ts(100, 0)),
		get(10, checker.FinalSessionID, "edge-A", "k", []byte("v1"), ts(100, 0)),
		get(11, checker.FinalSessionID, "edge-B", "k", []byte("v2"), ts(200, 0)),
	}
	rep := checker.Check(events)
	if rep.Convergence == 0 {
		t.Fatalf("expected convergence violation, got %+v", rep)
	}
}

func del(seq int64, session, site, key string, wts hlc.Timestamp) checker.Event {
	return checker.Event{Seq: seq, SessionID: session, Site: site, Kind: checker.OpDelete,
		Key: key, Deleted: true, WriteTS: wts, IssuedAt: time.Now()}
}

func getTombstone(seq int64, session, site, key string) checker.Event {
	return checker.Event{Seq: seq, SessionID: session, Site: site, Kind: checker.OpGet,
		Key: key, Deleted: true, IssuedAt: time.Now()}
}

// TestCheck_Tombstone_RYW_Satisfied: deleting then reading a tombstone
// from the same session is consistent.
func TestCheck_Tombstone_RYW_Satisfied(t *testing.T) {
	events := []checker.Event{
		put(1, "s1", "edge-A", "k", []byte("v"), ts(100, 0)),
		del(2, "s1", "edge-A", "k", ts(200, 0)),
		getTombstone(3, "s1", "edge-A", "k"),
	}
	rep := checker.Check(events)
	if !rep.OK() {
		t.Fatalf("expected zero violations, got %+v", rep)
	}
}

// TestCheck_Tombstone_LostWrite_Detected: writing then reading a
// tombstone you never issued is flagged.
func TestCheck_Tombstone_LostWrite_Detected(t *testing.T) {
	events := []checker.Event{
		put(1, "s1", "edge-A", "k", []byte("v"), ts(100, 0)),
		getTombstone(2, "s1", "edge-A", "k"),
	}
	rep := checker.Check(events)
	if rep.ReadYourWrites != 1 {
		t.Fatalf("expected 1 RYW violation, got %+v", rep)
	}
}

// TestCheck_Convergence_Tombstones: two sites both reporting tombstone
// converge even with ts=0.
func TestCheck_Convergence_Tombstones(t *testing.T) {
	events := []checker.Event{
		put(1, "s1", "edge-A", "k", []byte("v"), ts(100, 0)),
		del(2, "s1", "edge-A", "k", ts(200, 0)),
		getTombstone(10, checker.FinalSessionID, "edge-A", "k"),
		getTombstone(11, checker.FinalSessionID, "edge-B", "k"),
	}
	rep := checker.Check(events)
	if rep.Convergence != 0 {
		t.Fatalf("unexpected convergence violation for matching tombstones: %+v", rep)
	}
}

// TestCheck_Convergence_Agreement: all sites agreeing is fine.
func TestCheck_Convergence_Agreement(t *testing.T) {
	events := []checker.Event{
		put(1, "s1", "edge-A", "k", []byte("v1"), ts(100, 0)),
		get(10, checker.FinalSessionID, "edge-A", "k", []byte("v1"), ts(100, 0)),
		get(11, checker.FinalSessionID, "edge-B", "k", []byte("v1"), ts(100, 0)),
		get(12, checker.FinalSessionID, "cloud", "k", []byte("v1"), ts(100, 0)),
	}
	rep := checker.Check(events)
	if rep.Convergence != 0 {
		t.Fatalf("unexpected convergence violation: %+v", rep)
	}
}

// TestJSONLRecorder_RoundTrip writes some events, reads them back,
// confirms field-level equality and sequence numbering.
func TestJSONLRecorder_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "history.jsonl")

	rec, err := checker.NewJSONLRecorder(path)
	if err != nil {
		t.Fatalf("NewJSONLRecorder: %v", err)
	}
	rec.Record(put(0, "s1", "edge-A", "k", []byte("v"), ts(1, 0)))
	rec.Record(get(0, "s1", "edge-A", "k", []byte("v"), ts(1, 0)))
	if err := rec.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	loaded, err := checker.LoadJSONL(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("expected 2 events, got %d", len(loaded))
	}
	// Seq should be filled in by the recorder even though we passed 0.
	if loaded[0].Seq != 1 || loaded[1].Seq != 2 {
		t.Errorf("seq = %d,%d; want 1,2", loaded[0].Seq, loaded[1].Seq)
	}
	if !bytes.Equal(loaded[0].Value, []byte("v")) {
		t.Errorf("value mismatch: %q", loaded[0].Value)
	}
	if loaded[0].Kind != checker.OpPut || loaded[1].Kind != checker.OpGet {
		t.Errorf("kinds: %v %v", loaded[0].Kind, loaded[1].Kind)
	}
}

// TestStream_Forwards is a quick smoke test for the streaming reader
// (useful when histories exceed memory).
func TestStream_Forwards(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stream.jsonl")
	rec, err := checker.NewJSONLRecorder(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 5; i++ {
		rec.Record(put(0, "s", "edge", "k", []byte{byte(i)}, ts(int64(i+1), 0)))
	}
	if err := rec.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	count := 0
	if err := checker.Stream(f, func(ev checker.Event) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("stream: %v", err)
	}
	if count != 5 {
		t.Fatalf("streamed %d events, want 5", count)
	}
}

// TestReport_JSONEncoding confirms the Report's JSON shape (used by
// cmd/checker).
func TestReport_JSONEncoding(t *testing.T) {
	rep := checker.Check([]checker.Event{
		put(1, "s1", "edge-A", "k", []byte("v"), ts(1, 0)),
	})
	b, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !bytes.Contains(b, []byte(`"violations_monotonic_reads":0`)) {
		t.Errorf("encoded report missing expected field: %s", b)
	}
}
