// Command cloud-node runs a cloud-tier process: receives causal replication
// updates from edge sites, applies cross-region ordering, and (optionally)
// drives the adaptive escalation path for hot keys. Same lifecycle
// infrastructure as the edge node.
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

	if err := a.Run(context.Background()); err != nil {
		os.Exit(1)
	}
}
