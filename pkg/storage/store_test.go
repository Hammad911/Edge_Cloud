package storage

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"edge-cloud-replication/pkg/hlc"
)

func ts(phys int64, logical uint32) hlc.Timestamp {
	return hlc.Timestamp{Physical: phys, Logical: logical}
}

func TestMemStore_PutGet(t *testing.T) {
	m := NewMemStore()

	if err := m.Put("k", []byte("v1"), ts(100, 0)); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := m.Get("k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got.Value, []byte("v1")) {
		t.Fatalf("value mismatch: got %q", got.Value)
	}
	if got.Timestamp != ts(100, 0) {
		t.Fatalf("timestamp mismatch: %v", got.Timestamp)
	}
}

func TestMemStore_Put_RejectsStaleWrite(t *testing.T) {
	m := NewMemStore()
	if err := m.Put("k", []byte("v1"), ts(100, 0)); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := m.Put("k", []byte("v2"), ts(100, 0)); !errors.Is(err, ErrStaleWrite) {
		t.Fatalf("expected ErrStaleWrite for duplicate ts, got %v", err)
	}
	if err := m.Put("k", []byte("v3"), ts(50, 0)); !errors.Is(err, ErrStaleWrite) {
		t.Fatalf("expected ErrStaleWrite for older ts, got %v", err)
	}
}

func TestMemStore_Get_NotFound(t *testing.T) {
	m := NewMemStore()
	if _, err := m.Get("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemStore_Delete_TombstonesHideValue(t *testing.T) {
	m := NewMemStore()
	_ = m.Put("k", []byte("v1"), ts(100, 0))
	_ = m.Delete("k", ts(200, 0))

	if _, err := m.Get("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected tombstone to hide value, got %v", err)
	}
}

func TestMemStore_Delete_ReadAtHistoricBoundSeesValue(t *testing.T) {
	m := NewMemStore()
	_ = m.Put("k", []byte("v1"), ts(100, 0))
	_ = m.Delete("k", ts(200, 0))

	got, err := m.GetAt("k", ts(150, 0))
	if err != nil {
		t.Fatalf("historic get: %v", err)
	}
	if !bytes.Equal(got.Value, []byte("v1")) {
		t.Fatalf("expected historic read to see v1, got %q", got.Value)
	}
}

func TestMemStore_GetAt_SelectsLatestAtOrBeforeBound(t *testing.T) {
	m := NewMemStore()
	_ = m.Put("k", []byte("v1"), ts(100, 0))
	_ = m.Put("k", []byte("v2"), ts(200, 0))
	_ = m.Put("k", []byte("v3"), ts(300, 0))

	cases := []struct {
		bound hlc.Timestamp
		want  string
		err   error
	}{
		{ts(50, 0), "", ErrNotFound},
		{ts(100, 0), "v1", nil},
		{ts(150, 0), "v1", nil},
		{ts(200, 0), "v2", nil},
		{ts(299, 0), "v2", nil},
		{ts(300, 0), "v3", nil},
		{ts(1000, 0), "v3", nil},
	}
	for _, tc := range cases {
		got, err := m.GetAt("k", tc.bound)
		if tc.err != nil {
			if !errors.Is(err, tc.err) {
				t.Errorf("bound=%v: err=%v want %v", tc.bound, err, tc.err)
			}
			continue
		}
		if err != nil {
			t.Errorf("bound=%v: unexpected err %v", tc.bound, err)
			continue
		}
		if string(got.Value) != tc.want {
			t.Errorf("bound=%v: got %q want %q", tc.bound, got.Value, tc.want)
		}
	}
}

func TestMemStore_History_ReturnsAllVersionsInOrder(t *testing.T) {
	m := NewMemStore()
	stamps := []hlc.Timestamp{ts(10, 0), ts(20, 0), ts(30, 0)}
	for i, s := range stamps {
		if err := m.Put("k", []byte{byte('a' + i)}, s); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	hist, err := m.History("k")
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(hist) != 3 {
		t.Fatalf("expected 3 versions, got %d", len(hist))
	}
	for i := 1; i < len(hist); i++ {
		if !hist[i-1].Timestamp.Before(hist[i].Timestamp) {
			t.Fatalf("history not strictly increasing at %d: %v", i, hist)
		}
	}
}

func TestMemStore_MaxVersionsEvictsOldest(t *testing.T) {
	m := NewMemStore(WithMaxVersionsPerKey(2))
	_ = m.Put("k", []byte("v1"), ts(10, 0))
	_ = m.Put("k", []byte("v2"), ts(20, 0))
	_ = m.Put("k", []byte("v3"), ts(30, 0))

	hist, err := m.History("k")
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(hist) != 2 {
		t.Fatalf("expected 2 retained versions, got %d", len(hist))
	}
	if !bytes.Equal(hist[0].Value, []byte("v2")) || !bytes.Equal(hist[1].Value, []byte("v3")) {
		t.Fatalf("unexpected retained values: %v, %v", string(hist[0].Value), string(hist[1].Value))
	}
}

func TestMemStore_ConcurrentReadsAndWrites(t *testing.T) {
	m := NewMemStore()
	c := hlc.New()

	var wg sync.WaitGroup
	writers := 8
	readers := 8
	perWriter := 200

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				_ = m.Put("k", []byte{byte(i)}, c.Now())
			}
		}()
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				_, _ = m.Get("k")
			}
		}()
	}
	wg.Wait()

	hist, err := m.History("k")
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	for i := 1; i < len(hist); i++ {
		if !hist[i-1].Timestamp.Before(hist[i].Timestamp) {
			t.Fatalf("history not ordered under concurrency at %d", i)
		}
	}
}

func TestMemStore_ValueMutationByCallerDoesNotAffectStore(t *testing.T) {
	m := NewMemStore()
	val := []byte("original")
	if err := m.Put("k", val, ts(100, 0)); err != nil {
		t.Fatalf("put: %v", err)
	}
	val[0] = 'X'

	got, err := m.Get("k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got.Value, []byte("original")) {
		t.Fatalf("store held caller reference; got %q", got.Value)
	}
}
