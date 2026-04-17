// Command edge-node runs an edge-tier process: intra-cluster Raft replica
// plus causal replication client to the cloud tier. Currently it serves the
// KV API against an in-memory versioned store stamped with a local HLC.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"google.golang.org/grpc"

	"edge-cloud-replication/internal/app"
	"edge-cloud-replication/internal/config"
	"edge-cloud-replication/internal/server"
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
	cfg.Node.Role = config.RoleEdge

	a, err := app.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build app: %v\n", err)
		os.Exit(2)
	}

	clock := hlc.New()
	store := storage.NewMemStore()
	kvSvc := kv.New(kv.DefaultConfig(), clock, store)

	kvGRPC := server.NewKVGRPCServer(kvSvc, a.Logger)
	a.GRPC.Register(func(s *grpc.Server) { kvGRPC.Register(s) })

	if err := a.Run(context.Background()); err != nil {
		os.Exit(1)
	}
}
