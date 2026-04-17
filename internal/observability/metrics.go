// Package observability wires Prometheus metrics for the whole process.
// All application metrics MUST be registered on Registry.Prom so that the
// /metrics endpoint served by the admin HTTP server exposes them.
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Registry is the per-process metric registry. It is intentionally a custom
// registry (not the default one) so tests and multiple processes in a single
// binary don't collide.
type Registry struct {
	Prom *prometheus.Registry

	// Process-level metrics that every node exports.
	BuildInfo      *prometheus.GaugeVec
	RPCRequests    *prometheus.CounterVec
	RPCLatencySecs *prometheus.HistogramVec
}

// NewRegistry constructs a Registry with standard Go + process collectors
// pre-registered, plus the application-level metrics used across the codebase.
func NewRegistry(nodeID, role, version string) *Registry {
	r := prometheus.NewRegistry()
	r.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ecr",
		Name:      "build_info",
		Help:      "Static build and role information for this process.",
	}, []string{"node_id", "role", "version"})
	buildInfo.WithLabelValues(nodeID, role, version).Set(1)

	rpcRequests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ecr",
		Subsystem: "rpc",
		Name:      "requests_total",
		Help:      "Total RPCs handled, keyed by service, method, and status code.",
	}, []string{"service", "method", "code"})

	rpcLatency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ecr",
		Subsystem: "rpc",
		Name:      "latency_seconds",
		Help:      "RPC latency distribution.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"service", "method"})

	r.MustRegister(buildInfo, rpcRequests, rpcLatency)

	return &Registry{
		Prom:           r,
		BuildInfo:      buildInfo,
		RPCRequests:    rpcRequests,
		RPCLatencySecs: rpcLatency,
	}
}
