// Package kv is the edge-node's domain layer: a thin facade that combines an
// HLC for timestamping with a versioned Store for persistence, and exposes
// the operations the gRPC service needs (Get, Put, Delete) with causal-token
// semantics.
//
// The token returned by every mutation is the HLC timestamp assigned to that
// mutation. Clients are expected to pass the latest token they received back
// on subsequent requests; the service uses it as a lower bound for reads to
// implement read-your-writes and monotonic reads. When the edge node is
// later federated across a cluster, the same token mechanism will be the
// integration point for partitioned HLC metadata.
package kv

import (
	"context"
	"errors"
	"fmt"

	"edge-cloud-replication/pkg/hlc"
	"edge-cloud-replication/pkg/storage"
)

// Service is the domain-level KV API. The gRPC server in internal/server/kv
// is a thin adapter over this interface.
type Service interface {
	Get(ctx context.Context, key string, after hlc.Timestamp) (value []byte, found bool, token hlc.Timestamp, err error)
	Put(ctx context.Context, key string, value []byte, after hlc.Timestamp) (token hlc.Timestamp, err error)
	Delete(ctx context.Context, key string, after hlc.Timestamp) (token hlc.Timestamp, err error)
}

// Errors surfaced by the Service. Callers (including the gRPC layer) translate
// these to the appropriate transport-level status codes.
var (
	ErrEmptyKey   = errors.New("kv: key must not be empty")
	ErrValueLimit = errors.New("kv: value exceeds configured size limit")
)

// Config controls Service behavior.
type Config struct {
	// MaxValueBytes caps individual value size. 0 disables the check.
	MaxValueBytes int
}

// DefaultConfig returns a Config with the library defaults.
func DefaultConfig() Config {
	return Config{MaxValueBytes: 1 << 20} // 1 MiB
}

// service is the default Service implementation, backed by an HLC + Store.
type service struct {
	cfg   Config
	clock *hlc.Clock
	store storage.Store
}

// New constructs a Service. The clock and store must be non-nil.
func New(cfg Config, clock *hlc.Clock, store storage.Store) Service {
	if clock == nil {
		panic("kv.New: clock must not be nil")
	}
	if store == nil {
		panic("kv.New: store must not be nil")
	}
	return &service{cfg: cfg, clock: clock, store: store}
}

func (s *service) Get(ctx context.Context, key string, after hlc.Timestamp) ([]byte, bool, hlc.Timestamp, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, hlc.Timestamp{}, err
	}
	if key == "" {
		return nil, false, hlc.Timestamp{}, ErrEmptyKey
	}

	// Advance local HLC so the returned token never regresses w.r.t. anything
	// the client has already observed through a prior response.
	if !after.Zero() {
		if _, err := s.clock.Update(after); err != nil {
			return nil, false, hlc.Timestamp{}, fmt.Errorf("kv.Get: absorb client token: %w", err)
		}
	}

	var v storage.Version
	var err error
	if after.Zero() {
		v, err = s.store.Get(key)
	} else {
		// Read the most recent version at or before the furthest point the
		// client has observed. Anything newer is allowed to exist on the
		// replica, but the client's causal view cannot have seen it yet.
		v, err = s.store.GetAt(key, maxTS(s.clock.Peek(), after))
	}

	token := s.clock.Now()

	if errors.Is(err, storage.ErrNotFound) {
		return nil, false, token, nil
	}
	if err != nil {
		return nil, false, token, err
	}
	return v.Value, true, token, nil
}

func (s *service) Put(ctx context.Context, key string, value []byte, after hlc.Timestamp) (hlc.Timestamp, error) {
	if err := ctx.Err(); err != nil {
		return hlc.Timestamp{}, err
	}
	if key == "" {
		return hlc.Timestamp{}, ErrEmptyKey
	}
	if s.cfg.MaxValueBytes > 0 && len(value) > s.cfg.MaxValueBytes {
		return hlc.Timestamp{}, fmt.Errorf("%w: got=%d limit=%d", ErrValueLimit, len(value), s.cfg.MaxValueBytes)
	}

	if !after.Zero() {
		if _, err := s.clock.Update(after); err != nil {
			return hlc.Timestamp{}, fmt.Errorf("kv.Put: absorb client token: %w", err)
		}
	}

	ts := s.clock.Now()
	if err := s.store.Put(key, value, ts); err != nil {
		return hlc.Timestamp{}, fmt.Errorf("kv.Put: %w", err)
	}
	return ts, nil
}

func (s *service) Delete(ctx context.Context, key string, after hlc.Timestamp) (hlc.Timestamp, error) {
	if err := ctx.Err(); err != nil {
		return hlc.Timestamp{}, err
	}
	if key == "" {
		return hlc.Timestamp{}, ErrEmptyKey
	}

	if !after.Zero() {
		if _, err := s.clock.Update(after); err != nil {
			return hlc.Timestamp{}, fmt.Errorf("kv.Delete: absorb client token: %w", err)
		}
	}

	ts := s.clock.Now()
	if err := s.store.Delete(key, ts); err != nil {
		return hlc.Timestamp{}, fmt.Errorf("kv.Delete: %w", err)
	}
	return ts, nil
}

func maxTS(a, b hlc.Timestamp) hlc.Timestamp {
	if a.Compare(b) >= 0 {
		return a
	}
	return b
}
