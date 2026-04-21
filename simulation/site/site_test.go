package site_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"edge-cloud-replication/simulation/network"
	"edge-cloud-replication/simulation/site"
	"edge-cloud-replication/simulation/topology"
)

func TestSite_LocalReadAfterWrite(t *testing.T) {
	topo, err := topology.BuildHubAndSpoke(topology.HubAndSpokeConfig{
		NumEdges:           1,
		EdgeToCloudLatency: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("topology: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	topo.Start(ctx)
	defer topo.Stop()

	edge := topo.Edges[0]
	if _, err := edge.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	val, _, err := edge.Get(ctx, "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(val) != "v" {
		t.Fatalf("get: got %q want v", val)
	}
}

func TestSite_PropagatesEdgeToEdgeViaCloud(t *testing.T) {
	topo, err := topology.BuildHubAndSpoke(topology.HubAndSpokeConfig{
		NumEdges:           2,
		EdgeToCloudLatency: 5 * time.Millisecond,
		JitterFraction:     0,
	})
	if err != nil {
		t.Fatalf("topology: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	topo.Start(ctx)
	defer topo.Stop()

	a, b := topo.Edges[0], topo.Edges[1]
	if _, err := a.Put(ctx, "shared", []byte("from-a")); err != nil {
		t.Fatalf("put: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		v, _, err := b.Get(ctx, "shared")
		if err == nil && string(v) == "from-a" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("edge-B never observed edge-A's write")
}

func TestSite_ObserverFiresLocalAndRemote(t *testing.T) {
	var local, remote atomic.Int64
	obs := func(ev site.ApplyEvent) {
		switch ev.Kind {
		case site.ApplyLocal:
			local.Add(1)
		case site.ApplyRemote:
			remote.Add(1)
		}
	}
	topo, err := topology.BuildHubAndSpoke(topology.HubAndSpokeConfig{
		NumEdges:           2,
		EdgeToCloudLatency: 2 * time.Millisecond,
		Observer:           obs,
	}, network.WithSeed(7))
	if err != nil {
		t.Fatalf("topology: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	topo.Start(ctx)
	defer topo.Stop()

	for i := 0; i < 5; i++ {
		if _, err := topo.Edges[0].Put(ctx, "k", []byte{byte(i)}); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if local.Load() == 5 && remote.Load() >= 5 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("observer fired %d local / %d remote (want 5 / >=5)", local.Load(), remote.Load())
}
