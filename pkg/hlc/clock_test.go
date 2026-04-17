package hlc

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a test PhysicalClock backed by an atomic counter.
type fakeClock struct{ now atomic.Int64 }

func (f *fakeClock) Set(v int64) { f.now.Store(v) }
func (f *fakeClock) Now() int64  { return f.now.Load() }

func TestClock_Now_MonotonicUnderStaticPhysical(t *testing.T) {
	f := &fakeClock{}
	f.Set(100)
	c := New(WithPhysicalClock(f.Now))

	first := c.Now()
	second := c.Now()
	third := c.Now()

	if !first.Before(second) || !second.Before(third) {
		t.Fatalf("expected strictly increasing timestamps, got %v %v %v", first, second, third)
	}
	if first.Physical != 100 || second.Physical != 100 || third.Physical != 100 {
		t.Fatalf("expected physical=100 for all, got %v %v %v", first, second, third)
	}
	if first.Logical != 0 || second.Logical != 1 || third.Logical != 2 {
		t.Fatalf("expected logical 0,1,2, got %d %d %d", first.Logical, second.Logical, third.Logical)
	}
}

func TestClock_Now_AdvancesWithPhysical(t *testing.T) {
	f := &fakeClock{}
	f.Set(100)
	c := New(WithPhysicalClock(f.Now))

	_ = c.Now()
	_ = c.Now()

	f.Set(500)
	ts := c.Now()
	if ts.Physical != 500 {
		t.Fatalf("expected physical=500 after physical advance, got %d", ts.Physical)
	}
	if ts.Logical != 0 {
		t.Fatalf("expected logical reset to 0 after physical jump, got %d", ts.Logical)
	}
}

func TestClock_Update_AbsorbsFutureRemote(t *testing.T) {
	f := &fakeClock{}
	f.Set(100)
	c := New(WithPhysicalClock(f.Now))

	remote := Timestamp{Physical: 200, Logical: 5}
	got, err := c.Update(remote)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Physical != 200 {
		t.Fatalf("expected local physical to jump to 200, got %d", got.Physical)
	}
	if got.Logical != 6 {
		t.Fatalf("expected logical=remote.logical+1=6, got %d", got.Logical)
	}

	next := c.Now()
	if !got.Before(next) {
		t.Fatalf("expected next local event to strictly follow update, got %v then %v", got, next)
	}
}

func TestClock_Update_PastRemoteDoesNotRegress(t *testing.T) {
	f := &fakeClock{}
	f.Set(1000)
	c := New(WithPhysicalClock(f.Now))

	local := c.Now()
	remote := Timestamp{Physical: 500, Logical: 99}
	got, err := c.Update(remote)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Compare(local) <= 0 {
		t.Fatalf("expected result %v to be strictly after local %v", got, local)
	}
	if got.Physical < local.Physical {
		t.Fatalf("physical regressed: %d -> %d", local.Physical, got.Physical)
	}
}

func TestClock_Update_RejectsExcessiveDrift(t *testing.T) {
	f := &fakeClock{}
	f.Set(int64(time.Hour))
	c := New(WithPhysicalClock(f.Now), WithMaxDrift(time.Minute))

	remote := Timestamp{Physical: int64(2 * time.Hour), Logical: 0}
	_, err := c.Update(remote)
	if !errors.Is(err, ErrClockDrift) {
		t.Fatalf("expected ErrClockDrift, got %v", err)
	}
}

func TestClock_Peek_DoesNotAdvance(t *testing.T) {
	f := &fakeClock{}
	f.Set(100)
	c := New(WithPhysicalClock(f.Now))

	_ = c.Now()
	p1 := c.Peek()
	p2 := c.Peek()
	if p1 != p2 {
		t.Fatalf("Peek mutated state: %v vs %v", p1, p2)
	}
}

func TestClock_Now_ThreadSafety(t *testing.T) {
	c := New()
	const goroutines = 32
	const perG = 500
	var wg sync.WaitGroup
	seen := make([][]Timestamp, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			out := make([]Timestamp, perG)
			for j := 0; j < perG; j++ {
				out[j] = c.Now()
			}
			seen[i] = out
		}()
	}
	wg.Wait()

	all := make([]Timestamp, 0, goroutines*perG)
	for _, s := range seen {
		all = append(all, s...)
	}
	set := make(map[Timestamp]struct{}, len(all))
	for _, ts := range all {
		if _, dup := set[ts]; dup {
			t.Fatalf("duplicate timestamp emitted under concurrency: %v", ts)
		}
		set[ts] = struct{}{}
	}
}

func TestTimestamp_CompareOrdering(t *testing.T) {
	cases := []struct {
		a, b Timestamp
		want int
	}{
		{Timestamp{1, 0}, Timestamp{2, 0}, -1},
		{Timestamp{2, 0}, Timestamp{1, 0}, 1},
		{Timestamp{2, 0}, Timestamp{2, 0}, 0},
		{Timestamp{2, 1}, Timestamp{2, 2}, -1},
		{Timestamp{2, 3}, Timestamp{2, 2}, 1},
	}
	for _, tc := range cases {
		if got := tc.a.Compare(tc.b); got != tc.want {
			t.Errorf("Compare(%v,%v)=%d want %d", tc.a, tc.b, got, tc.want)
		}
	}
}
