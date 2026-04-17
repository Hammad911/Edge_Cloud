// Package storage provides a multi-version key/value store. Every write is
// stamped with an HLC timestamp and preserved as a new version of the key.
// Reads can ask for the latest version OR the latest version whose timestamp
// is <= a supplied "read-after" bound - the latter is what the causal
// consistency layer uses to enforce "reads see their own writes" and
// "monotonic reads" across replicas.
//
// The current backend is in-memory. Disk persistence is a future concern and
// will slot in as another Store implementation.
package storage

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"edge-cloud-replication/pkg/hlc"
)

// ErrNotFound is returned by Get when a key has no versions visible to the
// caller (either no writes at all, or no writes at or before the requested
// read bound).
var ErrNotFound = errors.New("storage: key not found")

// ErrStaleWrite is returned by Put when the provided timestamp is not
// strictly greater than the latest existing version for the key. The store
// refuses to regress; the caller should retry with a freshly advanced HLC.
var ErrStaleWrite = errors.New("storage: write timestamp not newer than latest version")

// Version is a single historical value of a key.
type Version struct {
	Timestamp hlc.Timestamp
	Value     []byte
	// Deleted is true when this version is a tombstone. Tombstones are kept
	// so that causal reads can correctly observe a delete that happened
	// before a concurrent surviving write.
	Deleted bool
}

// Store is a multi-version KV store. Implementations must be safe for
// concurrent use by multiple goroutines.
type Store interface {
	// Put stores a new version of key. The timestamp must be strictly after
	// every existing version of the key.
	Put(key string, value []byte, ts hlc.Timestamp) error

	// Delete writes a tombstone for key.
	Delete(key string, ts hlc.Timestamp) error

	// Get returns the latest Version for key. Returns ErrNotFound if the key
	// has no non-deleted version (a tombstone counts as not-found).
	Get(key string) (Version, error)

	// GetAt returns the most recent Version whose timestamp is <= bound.
	// This is the primitive reads go through when the client has a causal
	// dependency to satisfy (read-your-writes, monotonic reads).
	GetAt(key string, bound hlc.Timestamp) (Version, error)

	// History returns all versions of key in timestamp order (oldest first).
	// Used by replication, debugging, and the Jepsen-style causality checker.
	History(key string) ([]Version, error)

	// Keys returns a snapshot of all keys in the store (including those with
	// only tombstone versions).
	Keys() []string

	// Size reports the number of unique keys stored.
	Size() int
}

// MemStore is an in-memory implementation of Store. Versions per key are
// stored as an append-only slice kept in strictly ascending timestamp order.
type MemStore struct {
	mu       sync.RWMutex
	data     map[string][]Version
	maxVersions int
}

// MemStoreOption configures a MemStore.
type MemStoreOption func(*MemStore)

// WithMaxVersionsPerKey bounds the number of versions retained per key.
// Older versions are evicted from the front when the bound is exceeded.
// A value of 0 (the default) disables eviction. Only use this when the
// causal replication layer has acknowledged receipt; dropping versions that
// have not yet been replicated can violate causal reads on peers.
func WithMaxVersionsPerKey(n int) MemStoreOption {
	return func(m *MemStore) { m.maxVersions = n }
}

// NewMemStore constructs an empty in-memory store.
func NewMemStore(opts ...MemStoreOption) *MemStore {
	m := &MemStore{data: make(map[string][]Version)}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Put implements Store.
func (m *MemStore) Put(key string, value []byte, ts hlc.Timestamp) error {
	return m.appendVersion(key, Version{Timestamp: ts, Value: cloneBytes(value)})
}

// Delete implements Store.
func (m *MemStore) Delete(key string, ts hlc.Timestamp) error {
	return m.appendVersion(key, Version{Timestamp: ts, Deleted: true})
}

func (m *MemStore) appendVersion(key string, v Version) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	versions := m.data[key]
	if n := len(versions); n > 0 && !versions[n-1].Timestamp.Before(v.Timestamp) {
		return fmt.Errorf("%w: key=%q latest=%s incoming=%s",
			ErrStaleWrite, key, versions[n-1].Timestamp, v.Timestamp)
	}
	versions = append(versions, v)

	if m.maxVersions > 0 && len(versions) > m.maxVersions {
		drop := len(versions) - m.maxVersions
		versions = append([]Version(nil), versions[drop:]...)
	}
	m.data[key] = versions
	return nil
}

// Get implements Store.
func (m *MemStore) Get(key string) (Version, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	versions, ok := m.data[key]
	if !ok || len(versions) == 0 {
		return Version{}, ErrNotFound
	}
	latest := versions[len(versions)-1]
	if latest.Deleted {
		return Version{}, ErrNotFound
	}
	return cloneVersion(latest), nil
}

// GetAt implements Store.
func (m *MemStore) GetAt(key string, bound hlc.Timestamp) (Version, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	versions, ok := m.data[key]
	if !ok || len(versions) == 0 {
		return Version{}, ErrNotFound
	}

	idx := sort.Search(len(versions), func(i int) bool {
		return versions[i].Timestamp.After(bound)
	})
	if idx == 0 {
		return Version{}, ErrNotFound
	}
	v := versions[idx-1]
	if v.Deleted {
		return Version{}, ErrNotFound
	}
	return cloneVersion(v), nil
}

// History implements Store.
func (m *MemStore) History(key string) ([]Version, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	versions, ok := m.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	out := make([]Version, len(versions))
	for i, v := range versions {
		out[i] = cloneVersion(v)
	}
	return out, nil
}

// Keys implements Store.
func (m *MemStore) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]string, 0, len(m.data))
	for k := range m.data {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Size implements Store.
func (m *MemStore) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func cloneVersion(v Version) Version {
	return Version{
		Timestamp: v.Timestamp,
		Value:     cloneBytes(v.Value),
		Deleted:   v.Deleted,
	}
}
