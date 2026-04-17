// Package server provides the network-facing lifecycle primitives for a node:
// an HTTP admin server (for health, metrics, pprof) and a gRPC data-plane
// server. Both are designed to be composed under a single errgroup so that
// failure in either tears down the whole process cleanly.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"edge-cloud-replication/internal/config"
)

// AdminServer hosts operational HTTP endpoints:
//
//	GET /healthz   liveness probe (always 200 once serving)
//	GET /readyz    readiness probe (200 only after MarkReady)
//	GET /metrics   Prometheus metrics (if enabled)
//	GET /debug/... pprof (if enabled)
//	GET /info      build/node identity
type AdminServer struct {
	cfg    config.AdminConfig
	node   config.NodeConfig
	logger *slog.Logger
	prom   *prometheus.Registry

	ready atomic.Bool
	srv   *http.Server
	ln    net.Listener
}

// NewAdminServer wires the admin mux but does not start listening yet.
// Call Serve to begin accepting connections.
func NewAdminServer(
	cfg config.AdminConfig,
	node config.NodeConfig,
	logger *slog.Logger,
	prom *prometheus.Registry,
) (*AdminServer, error) {
	a := &AdminServer{
		cfg:    cfg,
		node:   node,
		logger: logger.With(slog.String("component", "admin_http")),
		prom:   prom,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.handleHealth)
	mux.HandleFunc("GET /readyz", a.handleReady)
	mux.HandleFunc("GET /info", a.handleInfo)

	if cfg.EnableMetrics && prom != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(prom, promhttp.HandlerOpts{
			Registry: prom,
		}))
	}
	if cfg.EnablePprof {
		mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	}

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("admin listen %q: %w", cfg.ListenAddr, err)
	}

	a.ln = ln
	a.srv = &http.Server{
		Handler:           mux,
		ReadTimeout:       cfg.ReadTimeout,
		ReadHeaderTimeout: cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	return a, nil
}

// Addr returns the actual bound address (useful when cfg used :0).
func (a *AdminServer) Addr() string {
	if a.ln == nil {
		return a.cfg.ListenAddr
	}
	return a.ln.Addr().String()
}

// MarkReady flips the /readyz endpoint to 200. Call this once all subsystems
// (Raft joined cluster, replication connected upstream, etc.) are operational.
func (a *AdminServer) MarkReady()    { a.ready.Store(true) }
func (a *AdminServer) MarkNotReady() { a.ready.Store(false) }

// Serve blocks until the server stops or ctx is cancelled. On ctx cancel it
// triggers a graceful shutdown bounded by AdminConfig.ShutdownGrace.
func (a *AdminServer) Serve(ctx context.Context) error {
	a.logger.Info("admin http serving", slog.String("addr", a.Addr()))

	errCh := make(chan error, 1)
	go func() { errCh <- a.srv.Serve(a.ln) }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownGrace)
		defer cancel()
		a.logger.Info("admin http shutting down", slog.Duration("grace", a.cfg.ShutdownGrace))
		if err := a.srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("admin shutdown: %w", err)
		}
		return nil
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("admin serve: %w", err)
		}
		return nil
	}
}

func (a *AdminServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (a *AdminServer) handleReady(w http.ResponseWriter, _ *http.Request) {
	if !a.ready.Load() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

func (a *AdminServer) handleInfo(w http.ResponseWriter, _ *http.Request) {
	body := map[string]any{
		"node_id":    a.node.ID,
		"role":       string(a.node.Role),
		"datacenter": a.node.Datacenter,
		"time":       time.Now().UTC().Format(time.RFC3339Nano),
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(body)
}
