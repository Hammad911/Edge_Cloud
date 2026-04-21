// Package checker is a Jepsen-style offline history auditor. It ingests
// a sequence of operations that were issued against the simulator (or
// any real edge-cloud deployment) and checks that the resulting history
// satisfies the consistency guarantees the paper claims:
//
//   - Monotonic reads: a given session, reading the same key over time,
//     never sees a version whose commit timestamp goes backwards.
//   - Read-your-writes: after a session writes (key, value), any later
//     read from that session returns either the value it wrote or a
//     strictly newer version.
//   - No stale reads at origin: the site that produced a write must
//     always be able to read it back immediately, independent of
//     cross-cluster replication delay.
//   - Eventual convergence: once the workload has quiesced, every site
//     agrees on the latest (key, value) pair for every key ever
//     written.
//
// History records are produced by the simulator's workload runner and
// optionally persisted as JSON Lines. Each record captures the minimal
// information a property checker needs: session id, site, op kind,
// key, value, and - for writes - the HLC commit timestamp the origin
// stamped; for reads, the commit timestamp of the observed version.
//
// The file format is one JSON object per line. This is cheap to stream
// (no need to parse a giant array), trivially greppable, and plays
// nicely with jq for ad-hoc inspection.
package checker

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"edge-cloud-replication/pkg/hlc"
)

// OpKind enumerates the operations the checker understands.
type OpKind string

const (
	OpPut    OpKind = "put"
	OpGet    OpKind = "get"
	OpDelete OpKind = "delete"
)

// Event is a single history record. JSON-serialisable by design.
type Event struct {
	// Seq is a monotonic, history-global sequence number the writer
	// assigns at record time. Lets the checker reconstruct issue
	// order deterministically even when events arrive out of order.
	Seq int64 `json:"seq"`

	// SessionID identifies the logical client. The simulator's
	// workload.Run uses worker-goroutine index; real clients would
	// use a durable client id. Monotonic reads + read-your-writes
	// are checked per-session.
	SessionID string `json:"session"`

	// Site is the replica the session was talking to when it
	// issued the op. For monotonic-reads we require per-(session,
	// key) ordering regardless of site; for convergence we
	// compare final state across sites.
	Site string `json:"site"`

	// Kind is put / get / delete.
	Kind OpKind `json:"kind"`

	// Key is the object key.
	Key string `json:"key"`

	// Value is the value written (on Put) or the value read (on Get).
	// Tombstones (Delete and missing-key Get) carry an empty slice
	// and Deleted=true.
	Value   []byte `json:"value,omitempty"`
	Deleted bool   `json:"deleted,omitempty"`

	// WriteTS is populated on Put/Delete with the HLC commit
	// timestamp the origin site stamped. On Get it carries the
	// commit timestamp of the version the read observed (or zero
	// if the key was absent).
	WriteTS hlc.Timestamp `json:"write_ts"`

	// IssuedAt is the wallclock at issue time; useful for sanity
	// checks and figuring out what was in flight during a partition.
	IssuedAt time.Time `json:"issued_at"`

	// Latency is the op's local wall-time.
	Latency time.Duration `json:"latency"`

	// Err is a stringified error if the op failed; empty on success.
	Err string `json:"err,omitempty"`
}

// Recorder persists events to a write-once sink. It is thread-safe: the
// workload may call Record from many goroutines concurrently.
type Recorder interface {
	Record(ev Event)
	Close() error
}

// NopRecorder discards every event. Useful as a default so callers can
// avoid nil checks.
type NopRecorder struct{}

func (NopRecorder) Record(Event) {}
func (NopRecorder) Close() error { return nil }

// jsonlRecorder writes one JSON object per line with an internal buffer
// and a mutex so concurrent Record calls don't interleave within a line.
type jsonlRecorder struct {
	w   *bufio.Writer
	enc *json.Encoder
	f   *os.File

	mu  chan struct{} // 1-slot semaphore avoids allocating a *sync.Mutex on hot path
	seq int64
}

// NewJSONLRecorder opens path for writing and returns a streaming
// recorder. The caller must Close() to flush and fsync.
func NewJSONLRecorder(path string) (Recorder, error) {
	if path == "" {
		return nil, errors.New("checker: empty history path")
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("checker: open history file: %w", err)
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	return &jsonlRecorder{
		w:   bw,
		enc: json.NewEncoder(bw),
		f:   f,
		mu:  make(chan struct{}, 1),
	}, nil
}

func (r *jsonlRecorder) Record(ev Event) {
	r.mu <- struct{}{}
	defer func() { <-r.mu }()
	r.seq++
	ev.Seq = r.seq
	_ = r.enc.Encode(ev)
}

func (r *jsonlRecorder) Close() error {
	r.mu <- struct{}{}
	defer func() { <-r.mu }()
	if err := r.w.Flush(); err != nil {
		_ = r.f.Close()
		return err
	}
	return r.f.Close()
}

// LoadJSONL reads a history file produced by jsonlRecorder and returns
// the events in write order. For very large histories a streaming API
// is cheaper - see Stream.
func LoadJSONL(path string) ([]Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("checker: open history: %w", err)
	}
	defer f.Close()
	var events []Event
	dec := json.NewDecoder(f)
	for dec.More() {
		var ev Event
		if err := dec.Decode(&ev); err != nil {
			return nil, fmt.Errorf("checker: decode history: %w", err)
		}
		events = append(events, ev)
	}
	return events, nil
}

// Stream decodes events from r one at a time, calling fn for each. It
// stops at EOF or the first decode error.
func Stream(r io.Reader, fn func(Event) error) error {
	dec := json.NewDecoder(r)
	for dec.More() {
		var ev Event
		if err := dec.Decode(&ev); err != nil {
			return fmt.Errorf("checker: decode history: %w", err)
		}
		if err := fn(ev); err != nil {
			return err
		}
	}
	return nil
}
