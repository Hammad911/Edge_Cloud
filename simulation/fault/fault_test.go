package fault_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"edge-cloud-replication/simulation/fault"
	"edge-cloud-replication/simulation/network"
)

func newSilentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestSchedule_Sort verifies that NewSchedule time-orders its events.
func TestSchedule_Sort(t *testing.T) {
	sc := fault.NewSchedule(
		fault.Event{At: 200 * time.Millisecond, Kind: fault.HealAll, A: "a"},
		fault.Event{At: 50 * time.Millisecond, Kind: fault.PartitionAll, A: "a"},
	)
	events := sc.Events()
	if len(events) != 2 || events[0].At > events[1].At {
		t.Fatalf("events not sorted: %+v", events)
	}
}

// TestRunner_PartitionHeal exercises the partition -> heal path end to
// end against a real Network: a message sent during the partition window
// is dropped, a message sent after heal is delivered.
func TestRunner_PartitionHeal(t *testing.T) {
	t.Parallel()

	net := network.New(
		network.WithSeed(1),
		network.WithDefaultLink(network.LinkProfile{MeanLatency: 5 * time.Millisecond}),
	)
	t.Cleanup(func() { net.Close() })

	const src, dst network.Address = "src", "dst"
	_ = net.Register(src)
	inbox := net.Register(dst)

	sc := fault.PartitionWindow([]network.Address{src}, 30*time.Millisecond, 50*time.Millisecond)

	var (
		mu      sync.Mutex
		history []fault.Kind
	)
	cb := func(ev fault.Event, _ time.Time) {
		mu.Lock()
		history = append(history, ev.Kind)
		mu.Unlock()
	}

	runner := fault.NewRunner(net, sc, newSilentLogger(), cb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner.Start(ctx)
	t.Cleanup(runner.Stop)

	// Wait for the partition to take effect.
	time.Sleep(40 * time.Millisecond)

	// Message sent during partition must surface ErrPartitioned to
	// the caller (so shippers can retry) and must not arrive within
	// the partition window.
	if err := net.Send(src, dst, "during"); !errors.Is(err, network.ErrPartitioned) {
		t.Fatalf("send during: expected ErrPartitioned, got %v", err)
	}
	select {
	case msg := <-inbox:
		t.Fatalf("message leaked through partition: %+v", msg)
	case <-time.After(30 * time.Millisecond):
	}

	// Wait for the scheduled heal.
	time.Sleep(40 * time.Millisecond)

	if err := net.Send(src, dst, "after"); err != nil {
		t.Fatalf("send after: %v", err)
	}
	select {
	case msg := <-inbox:
		if msg.Payload != "after" {
			t.Fatalf("unexpected payload: %+v", msg)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("heal did not restore delivery")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(history) < 2 || history[0] != fault.PartitionAll || history[1] != fault.HealAll {
		t.Fatalf("unexpected fault history: %+v", history)
	}
}

// TestRunner_Stop ensures Stop cancels pending events gracefully.
func TestRunner_Stop(t *testing.T) {
	t.Parallel()

	net := network.New(network.WithSeed(2))
	t.Cleanup(func() { net.Close() })

	sc := fault.NewSchedule(
		fault.Event{At: time.Hour, Kind: fault.PartitionAll, A: "nope"},
	)
	runner := fault.NewRunner(net, sc, newSilentLogger(), nil)

	runner.Start(context.Background())
	done := make(chan struct{})
	go func() {
		runner.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runner.Stop did not return promptly")
	}
}
