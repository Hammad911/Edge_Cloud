// Command cloud-node runs a cloud-tier process: serves a versioned KV
// API, accepts causal replication streams from edge cluster leaders, and
// fans replicated events out to peer edges. Same lifecycle infrastructure
// as the edge node but without intra-cluster Raft.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"google.golang.org/grpc"

	"edge-cloud-replication/internal/app"
	"edge-cloud-replication/internal/config"
	"edge-cloud-replication/internal/server"
	"edge-cloud-replication/pkg/causal"
	"edge-cloud-replication/pkg/hlc"
	"edge-cloud-replication/pkg/kv"
	"edge-cloud-replication/pkg/storage"
)

func main() {
	cfgPath := flag.String("config", "", "path to config file (yaml). If empty, defaults + env vars are used.")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(2)
	}
	cfg.Node.Role = config.RoleCloud
	if cfg.Admin.ListenAddr == "127.0.0.1:8081" {
		cfg.Admin.ListenAddr = "127.0.0.1:9081"
	}
	if cfg.GRPC.ListenAddr == "127.0.0.1:7001" {
		cfg.GRPC.ListenAddr = "127.0.0.1:9001"
	}

	a, err := app.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build app: %v\n", err)
		os.Exit(2)
	}

	clock := hlc.New()
	store := storage.NewMemStore()

	var kvOpts []kv.Option
	if cfg.Replication.Enabled {
		repl, err := startCausal(cfg, store, clock, a.Logger)
		if err != nil {
			fmt.Fprintf(os.Stderr, "causal bring-up: %v\n", err)
			os.Exit(2)
		}
		a.OnShutdown(repl.Stop)
		kvOpts = append(kvOpts, kv.WithCausalPublisher(producerAdapter{p: repl.Producer}))

		grpcRepl := server.NewReplicationGRPCServer(cfg.Node.ID, repl, server.AlwaysLeader(), a.Logger)
		a.GRPC.Register(func(s *grpc.Server) { grpcRepl.Register(s) })
		a.Logger.Info("causal replication enabled (cloud hub)",
			slog.String("group", cfg.Replication.GroupID),
			slog.Int("peers", len(cfg.Replication.Peers)),
		)
	}

	kvSvc := kv.New(kv.DefaultConfig(), clock, store, kvOpts...)
	kvGRPC := server.NewKVGRPCServer(kvSvc, a.Logger)
	a.GRPC.Register(func(s *grpc.Server) { kvGRPC.Register(s) })

	if err := a.Run(context.Background()); err != nil {
		os.Exit(1)
	}
}

// startCausal builds a cloud-side causal replicator. The cloud has no
// Raft, so a StoreApplier writes events directly to the local store.
func startCausal(cfg *config.Config, store storage.Store, localClock *hlc.Clock, logger *slog.Logger) (*causal.Replicator, error) {
	pclock := hlc.NewPartitionedClock(hlc.GroupID(cfg.Replication.GroupID), localClock)
	applier := causal.NewStoreApplier(store, pclock, logger)

	peers := make([]causal.PeerSpec, 0, len(cfg.Replication.Peers))
	for _, p := range cfg.Replication.Peers {
		peers = append(peers, causal.PeerSpec{Name: p.Name, Addr: p.Addr})
	}

	repl := causal.New(causal.Config{
		NodeID:         cfg.Node.ID,
		GroupID:        hlc.GroupID(cfg.Replication.GroupID),
		Peers:          peers,
		OutboxCapacity: cfg.Replication.OutboxCapacity,
	}, pclock, applier, logger)

	if err := repl.Start(context.Background()); err != nil {
		return nil, err
	}
	return repl, nil
}

type producerAdapter struct{ p *causal.Producer }

func (a producerAdapter) Publish(key string, value []byte, deleted bool, ts hlc.Timestamp) {
	a.p.Publish(key, value, deleted, ts)
}
