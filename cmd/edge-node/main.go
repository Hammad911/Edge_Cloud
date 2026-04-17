// Command edge-node runs an edge-tier process: intra-cluster Raft replica
// plus causal replication client to the cloud tier. In the current scaffold
// the node provides lifecycle, logging, metrics, and gRPC surface only -
// domain services are attached as they are implemented.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"edge-cloud-replication/internal/app"
	"edge-cloud-replication/internal/config"
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

	if err := a.Run(context.Background()); err != nil {
		os.Exit(1)
	}
}
