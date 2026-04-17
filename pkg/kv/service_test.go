package kv

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"edge-cloud-replication/pkg/hlc"
	"edge-cloud-replication/pkg/storage"
)

func newTestService(t *testing.T) (Service, *hlc.Clock, storage.Store) {
	t.Helper()
	c := hlc.New()
	s := storage.NewMemStore()
	return New(DefaultConfig(), c, s), c, s
}

func TestService_PutThenGet_RoundTrip(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx := context.Background()

	put, err := svc.Put(ctx, "user:1", []byte("alice"), hlc.Timestamp{})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if put.Zero() {
		t.Fatalf("put returned zero token")
	}

	val, found, tok, err := svc.Get(ctx, "user:1", put)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !found {
		t.Fatalf("expected found=true")
	}
	if !bytes.Equal(val, []byte("alice")) {
		t.Fatalf("value mismatch: got %q", val)
	}
	if !tok.After(put) {
		t.Fatalf("expected response token %v to strictly follow put %v", tok, put)
	}
}

func TestService_Get_MissingKey_ReturnsNotFound(t *testing.T) {
	svc, _, _ := newTestService(t)
	val, found, _, err := svc.Get(context.Background(), "nope", hlc.Timestamp{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatalf("expected found=false")
	}
	if val != nil {
		t.Fatalf("expected nil value, got %q", val)
	}
}

func TestService_EmptyKey_Rejected(t *testing.T) {
	svc, _, _ := newTestService(t)
	if _, err := svc.Put(context.Background(), "", []byte("x"), hlc.Timestamp{}); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey on Put, got %v", err)
	}
	if _, _, _, err := svc.Get(context.Background(), "", hlc.Timestamp{}); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey on Get, got %v", err)
	}
	if _, err := svc.Delete(context.Background(), "", hlc.Timestamp{}); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey on Delete, got %v", err)
	}
}

func TestService_Put_ValueSizeLimit(t *testing.T) {
	cfg := Config{MaxValueBytes: 4}
	svc := New(cfg, hlc.New(), storage.NewMemStore())
	if _, err := svc.Put(context.Background(), "k", []byte("abcd"), hlc.Timestamp{}); err != nil {
		t.Fatalf("unexpected error on boundary: %v", err)
	}
	if _, err := svc.Put(context.Background(), "k2", []byte("abcde"), hlc.Timestamp{}); !errors.Is(err, ErrValueLimit) {
		t.Fatalf("expected ErrValueLimit, got %v", err)
	}
}

func TestService_ReadYourWrites(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx := context.Background()

	tok1, err := svc.Put(ctx, "k", []byte("v1"), hlc.Timestamp{})
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	val, found, _, err := svc.Get(ctx, "k", tok1)
	if err != nil || !found {
		t.Fatalf("read-your-writes failed: err=%v found=%v", err, found)
	}
	if !bytes.Equal(val, []byte("v1")) {
		t.Fatalf("expected v1, got %q", val)
	}
}

func TestService_MonotonicReads_SessionTokenAdvances(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx := context.Background()

	_, _ = svc.Put(ctx, "k", []byte("v1"), hlc.Timestamp{})
	_, found1, t1, err := svc.Get(ctx, "k", hlc.Timestamp{})
	if err != nil || !found1 {
		t.Fatalf("first get: %v found=%v", err, found1)
	}

	_, found2, t2, err := svc.Get(ctx, "k", t1)
	if err != nil || !found2 {
		t.Fatalf("second get: %v found=%v", err, found2)
	}
	if !t2.After(t1) {
		t.Fatalf("expected monotonically advancing session token: %v -> %v", t1, t2)
	}
}

func TestService_Delete_HidesSubsequentGet(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx := context.Background()

	_, _ = svc.Put(ctx, "k", []byte("v"), hlc.Timestamp{})
	dtok, err := svc.Delete(ctx, "k", hlc.Timestamp{})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, found, _, err := svc.Get(ctx, "k", dtok)
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if found {
		t.Fatalf("expected deleted key to read as not-found")
	}
}

func TestService_ContextCancellationHonored(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := svc.Put(ctx, "k", []byte("v"), hlc.Timestamp{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if _, _, _, err := svc.Get(ctx, "k", hlc.Timestamp{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
