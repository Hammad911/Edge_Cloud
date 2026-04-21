package network

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestNetwork_DeliversAfterLatency(t *testing.T) {
	n := New(WithDefaultLink(LinkProfile{MeanLatency: 20 * time.Millisecond}))
	defer n.Close()

	a := n.Register("a")
	_ = n.Register("b")

	start := time.Now()
	if err := n.Send("b", "a", "hi"); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case msg := <-a:
		elapsed := time.Since(start)
		if elapsed < 18*time.Millisecond {
			t.Fatalf("delivered too quickly: %v", elapsed)
		}
		if msg.Payload != "hi" {
			t.Fatalf("payload: got %v", msg.Payload)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timeout waiting for delivery")
	}
}

func TestNetwork_PartitionDropsBoth(t *testing.T) {
	n := New(WithDefaultLink(LinkProfile{}))
	defer n.Close()

	a := n.Register("a")
	b := n.Register("b")
	n.Partition("a", "b")
	defer n.Heal("a", "b")

	if err := n.Send("a", "b", "x"); !errors.Is(err, ErrPartitioned) {
		t.Fatalf("partitioned send should return ErrPartitioned, got %v", err)
	}
	select {
	case <-b:
		t.Fatal("partitioned message was delivered")
	case <-time.After(50 * time.Millisecond):
	}

	// reverse direction is unaffected
	if err := n.Send("b", "a", "y"); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case msg := <-a:
		if msg.Payload != "y" {
			t.Fatalf("unexpected payload: %v", msg.Payload)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("reverse direction was unexpectedly blocked")
	}
}

func TestNetwork_LossRate(t *testing.T) {
	n := New(
		WithDefaultLink(LinkProfile{MeanLatency: 0, LossRate: 0.5}),
		WithSeed(42),
	)
	defer n.Close()

	a := n.Register("a")
	_ = n.Register("b")

	const total = 200
	for i := 0; i < total; i++ {
		_ = n.Send("b", "a", i)
	}

	deadline := time.After(500 * time.Millisecond)
	var received int32
loop:
	for {
		select {
		case <-a:
			atomic.AddInt32(&received, 1)
		case <-deadline:
			break loop
		}
	}
	delivered := atomic.LoadInt32(&received)
	if delivered <= 0 || delivered >= total {
		t.Fatalf("delivered %d/%d (expected substantially in between)", delivered, total)
	}
}

func TestNetwork_UnknownDestination(t *testing.T) {
	n := New()
	defer n.Close()
	if err := n.Send("a", "ghost", "x"); err != ErrUnknownAddress {
		t.Fatalf("expected ErrUnknownAddress, got %v", err)
	}
}
