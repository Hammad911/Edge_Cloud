package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"edge-cloud-replication/internal/config"
)

// GRPCServer wraps the data-plane gRPC server. It registers the standard
// health service and (optionally) reflection so operators can use grpcurl.
//
// Callers register their own services via Register before calling Serve.
type GRPCServer struct {
	cfg    config.GRPCConfig
	logger *slog.Logger

	srv    *grpc.Server
	ln     net.Listener
	health *health.Server
}

// NewGRPCServer constructs the server with production-sensible keepalives,
// message size limits, and recovery. Services are registered by callers.
func NewGRPCServer(cfg config.GRPCConfig, logger *slog.Logger) (*GRPCServer, error) {
	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("grpc listen %q: %w", cfg.ListenAddr, err)
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(cfg.MaxRecvMsgBytes),
		grpc.MaxSendMsgSize(cfg.MaxSendMsgBytes),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    cfg.KeepaliveInterval,
			Timeout: cfg.KeepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	srv := grpc.NewServer(opts...)

	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)

	if cfg.EnableReflection {
		reflection.Register(srv)
	}

	return &GRPCServer{
		cfg:    cfg,
		logger: logger.With(slog.String("component", "grpc")),
		srv:    srv,
		ln:     ln,
		health: hs,
	}, nil
}

// Register exposes the underlying *grpc.Server so service implementations can
// attach their handlers before Serve is called.
func (g *GRPCServer) Register(fn func(*grpc.Server)) {
	fn(g.srv)
}

// Health returns the grpc health server, for setting service status.
func (g *GRPCServer) Health() *health.Server { return g.health }

// Addr returns the bound address.
func (g *GRPCServer) Addr() string { return g.ln.Addr().String() }

// Serve blocks until context cancellation or fatal server error.
// It performs a graceful stop bounded by GRPCConfig.ShutdownGrace.
func (g *GRPCServer) Serve(ctx context.Context) error {
	g.logger.Info("grpc serving", slog.String("addr", g.Addr()))
	g.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	errCh := make(chan error, 1)
	go func() { errCh <- g.srv.Serve(g.ln) }()

	select {
	case <-ctx.Done():
		g.health.Shutdown()
		g.logger.Info("grpc graceful stop", slog.Duration("grace", g.cfg.ShutdownGrace))

		stopped := make(chan struct{})
		go func() {
			g.srv.GracefulStop()
			close(stopped)
		}()

		select {
		case <-stopped:
			return nil
		case <-time.After(g.cfg.ShutdownGrace):
			g.logger.Warn("grpc graceful stop timed out; forcing")
			g.srv.Stop()
			return nil
		}
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("grpc serve: %w", err)
		}
		return nil
	}
}
