// Package topology builds canonical simulator topologies. The dominant
// shape for this project is hub-and-spoke: one cloud site at the center,
// N edge sites that talk only to the cloud. The cloud re-publishes
// remote events so they fan out across the edges (just like the real
// cloud-node binary).
package topology

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"edge-cloud-replication/pkg/hlc"
	"edge-cloud-replication/simulation/network"
	"edge-cloud-replication/simulation/site"
)

// HubAndSpokeConfig parameterises BuildHubAndSpoke.
type HubAndSpokeConfig struct {
	NumEdges int

	// EdgeToCloudLatency profiles the WAN hop in either direction. If
	// zero MeanLatency is passed the network's default is used.
	EdgeToCloudLatency time.Duration
	// JitterFraction is multiplied by EdgeToCloudLatency to derive
	// per-link jitter; 0.2 means up to ±20%.
	JitterFraction float64
	// EdgeToCloudLoss is the probability a message on any edge<->cloud
	// hop is silently dropped. 0 disables. When non-zero the per-link
	// profile is written explicitly so the default profile doesn't
	// mask it.
	EdgeToCloudLoss float64

	// Logger is propagated to every site.
	Logger *slog.Logger

	// Observer, if non-nil, is wired into every site so the simulator
	// driver can collect metrics and run a causality checker.
	Observer site.ApplyObserver
}

// Topology is the build product: a network plus the constructed sites.
type Topology struct {
	Network *network.Network
	Cloud   *site.Site
	Edges   []*site.Site
}

// BuildHubAndSpoke creates one cloud + N edges on a freshly constructed
// network. The caller is responsible for Start()ing the topology and
// Stop()ping it during shutdown.
func BuildHubAndSpoke(cfg HubAndSpokeConfig, opts ...network.Option) (*Topology, error) {
	if cfg.NumEdges <= 0 {
		return nil, fmt.Errorf("topology: NumEdges must be positive, got %d", cfg.NumEdges)
	}
	net := network.New(opts...)

	const cloudAddr network.Address = "cloud"
	cloudPeers := make([]network.Address, 0, cfg.NumEdges)
	edgeAddrs := make([]network.Address, 0, cfg.NumEdges)
	for i := 0; i < cfg.NumEdges; i++ {
		addr := network.Address(fmt.Sprintf("edge-%04d", i))
		edgeAddrs = append(edgeAddrs, addr)
		cloudPeers = append(cloudPeers, addr)
	}

	if cfg.EdgeToCloudLatency > 0 || cfg.EdgeToCloudLoss > 0 {
		jitter := time.Duration(float64(cfg.EdgeToCloudLatency) * cfg.JitterFraction)
		lp := network.LinkProfile{
			MeanLatency: cfg.EdgeToCloudLatency,
			Jitter:      jitter,
			LossRate:    cfg.EdgeToCloudLoss,
		}
		for _, e := range edgeAddrs {
			net.Link(cloudAddr, e, lp)
		}
	}

	cloud := site.New(site.Config{
		Address:  cloudAddr,
		GroupID:  hlc.GroupID("cloud"),
		Role:     site.RoleCloud,
		Peers:    cloudPeers,
		Logger:   cfg.Logger,
		Observer: cfg.Observer,
	}, net)

	edges := make([]*site.Site, 0, cfg.NumEdges)
	for i, addr := range edgeAddrs {
		group := hlc.GroupID(fmt.Sprintf("edge-%04d", i))
		s := site.New(site.Config{
			Address:  addr,
			GroupID:  group,
			Role:     site.RoleEdge,
			Peers:    []network.Address{cloudAddr},
			Logger:   cfg.Logger,
			Observer: cfg.Observer,
		}, net)
		edges = append(edges, s)
	}

	return &Topology{Network: net, Cloud: cloud, Edges: edges}, nil
}

// Start launches every site. Idempotent.
func (t *Topology) Start(ctx context.Context) {
	t.Cloud.Start(ctx)
	for _, e := range t.Edges {
		e.Start(ctx)
	}
}

// Stop tears down every site and the network. Safe to call once.
func (t *Topology) Stop() {
	for _, e := range t.Edges {
		e.Stop()
	}
	t.Cloud.Stop()
	t.Network.Close()
}

// Sites returns cloud + every edge in a single slice (cloud first).
// Convenient for workload generators that want to iterate.
func (t *Topology) Sites() []*site.Site {
	out := make([]*site.Site, 0, len(t.Edges)+1)
	out = append(out, t.Cloud)
	out = append(out, t.Edges...)
	return out
}
