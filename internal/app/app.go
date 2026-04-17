// Package app wires configuration, logging, metrics, and servers into a single
// lifecycle unit. Both the edge-node and cloud-node binaries build on this
// package so that startup, shutdown, and observability behave identically
// regardless of role.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"edge-cloud-replication/internal/config"
	"edge-cloud-replication/internal/logging"
	"edge-cloud-replication/internal/observability"
	"edge-cloud-replication/internal/server"
)

// Version is overridden at build time via -ldflags.
var Version = "dev"

// App is the top-level runtime object for a node. It owns the logger,
// metrics registry, and the admin + gRPC servers. It does NOT own business
// logic: each role (edge/cloud) attaches its own services via the Register
// hook before calling Run.
type App struct {
	Cfg     *config.Config
	Logger  *slog.Logger
	Metrics *observability.Registry
	Admin   *server.AdminServer
	GRPC    *server.GRPCServer
}

// New constructs an App from the given config. It builds the logger, metric
// registry, and both servers but does NOT start them. Callers can inspect /
// mutate the App (e.g. register gRPC services) and then call Run.
func New(cfg *config.Config) (*App, error) {
	logger := logging.New(logging.Options{
		Level:  cfg.Logging.Level,
		Format: cfg.Logging.Format,
	}).With(
		slog.String("node_id", cfg.Node.ID),
		slog.String("role", string(cfg.Node.Role)),
		slog.String("dc", cfg.Node.Datacenter),
	)

	metrics := observability.NewRegistry(cfg.Node.ID, string(cfg.Node.Role), Version)

	admin, err := server.NewAdminServer(cfg.Admin, cfg.Node, logger, metrics.Prom)
	if err != nil {
		return nil, fmt.Errorf("admin server: %w", err)
	}

	grpcSrv, err := server.NewGRPCServer(cfg.GRPC, logger)
	if err != nil {
		return nil, fmt.Errorf("grpc server: %w", err)
	}

	return &App{
		Cfg:     cfg,
		Logger:  logger,
		Metrics: metrics,
		Admin:   admin,
		GRPC:    grpcSrv,
	}, nil
}

// Run starts all servers and blocks until a signal is received, ctx is
// cancelled, or any server fails. All servers are stopped gracefully.
//
// It installs handlers for SIGINT and SIGTERM. The caller is expected to be
// main() of the binary.
func (a *App) Run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a.Logger.Info("node starting",
		slog.String("version", Version),
		slog.String("admin_addr", a.Admin.Addr()),
		slog.String("grpc_addr", a.GRPC.Addr()),
	)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return a.Admin.Serve(gctx) })
	g.Go(func() error { return a.GRPC.Serve(gctx) })

	a.Admin.MarkReady()

	if err := g.Wait(); err != nil && !isShutdown(ctx, err) {
		a.Logger.Error("node exited with error", slog.Any("err", err))
		return err
	}
	a.Logger.Info("node stopped")
	return nil
}

func isShutdown(ctx context.Context, err error) bool {
	if err == nil {
		return true
	}
	return ctx.Err() != nil
}
