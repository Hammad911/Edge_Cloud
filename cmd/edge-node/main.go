// Command edge-node runs an edge-tier process: intra-cluster Raft replica
// plus (eventually) causal replication client to the cloud tier. Today it
// serves the KV API against a versioned store stamped with an HLC, with
// optional Raft consensus for strong consistency within the cluster.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"google.golang.org/grpc"

	"edge-cloud-replication/internal/app"
	"edge-cloud-replication/internal/config"
	"edge-cloud-replication/internal/consensus"
	"edge-cloud-replication/internal/server"
	"edge-cloud-replication/pkg/hlc"
	"edge-cloud-replication/pkg/kv"
	raftpkg "edge-cloud-replication/pkg/raft"
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
	cfg.Node.Role = config.RoleEdge

	a, err := app.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build app: %v\n", err)
		os.Exit(2)
	}

	clock := hlc.New()
	store := storage.NewMemStore()

	var kvOpts []kv.Option
	if cfg.Raft.Enabled {
		node, err := startRaft(cfg, store, clock, a.Logger)
		if err != nil {
			fmt.Fprintf(os.Stderr, "raft bring-up: %v\n", err)
			os.Exit(2)
		}
		a.OnShutdown(node.Shutdown)
		replicator := consensus.NewRaftReplicator(node, cfg.Raft.ApplyTimeout)
		kvOpts = append(kvOpts, kv.WithReplicator(replicator))

		cluster := server.NewClusterAdmin(node, a.Logger)
		cluster.Register(a.Admin)
		a.Logger.Info("raft enabled",
			slog.String("cluster_id", cfg.Raft.ClusterID),
			slog.Bool("bootstrap", cfg.Raft.Bootstrap),
			slog.Int("peers", len(cfg.Raft.Peers)),
		)
	}

	kvSvc := kv.New(kv.DefaultConfig(), clock, store, kvOpts...)
	kvGRPC := server.NewKVGRPCServer(kvSvc, a.Logger)
	a.GRPC.Register(func(s *grpc.Server) { kvGRPC.Register(s) })

	if err := a.Run(context.Background()); err != nil {
		os.Exit(1)
	}
}

func startRaft(cfg *config.Config, store storage.Store, clock *hlc.Clock, logger *slog.Logger) (*raftpkg.Node, error) {
	rc := raftpkg.Config{
		NodeID:            cfg.Node.ID,
		BindAddr:          cfg.Raft.Bind,
		AdvertiseAddr:     cfg.Raft.AdvertiseAddr,
		DataDir:           cfg.Raft.DataDir,
		Bootstrap:         cfg.Raft.Bootstrap,
		SnapshotInterval:  cfg.Raft.SnapshotInterval,
		SnapshotThreshold: cfg.Raft.SnapshotThreshold,
	}
	node, err := raftpkg.New(rc, store, clock, logger)
	if err != nil {
		return nil, err
	}

	// Best-effort peer registration from config. Only the bootstrap node
	// runs AddVoter; the others will be added by it (or discovered out of
	// band). If we are not the leader, AddVoter will return an error and
	// we ignore it — the intent here is to let a single YAML describe the
	// whole cluster for the dev experience.
	if cfg.Raft.Bootstrap && len(cfg.Raft.Peers) > 0 {
		go registerPeers(node, cfg.Raft.Peers, logger)
	}
	return node, nil
}

func registerPeers(node *raftpkg.Node, peers []config.RaftPeer, logger *slog.Logger) {
	deadline := time.Now().Add(30 * time.Second)
	for _, p := range peers {
		for time.Now().Before(deadline) {
			if !node.IsLeader() {
				time.Sleep(250 * time.Millisecond)
				continue
			}
			if err := node.AddVoter(p.ID, p.Addr); err != nil {
				logger.Warn("add voter failed, retrying",
					slog.String("id", p.ID),
					slog.String("addr", p.Addr),
					slog.Any("err", err),
				)
				time.Sleep(500 * time.Millisecond)
				continue
			}
			logger.Info("added voter",
				slog.String("id", p.ID),
				slog.String("addr", p.Addr),
			)
			break
		}
	}
}
