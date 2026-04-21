// Command simulator drives the in-process edge-cloud simulator. It builds
// a hub-and-spoke topology of N edges + 1 cloud, runs a workload for a
// fixed duration, and emits throughput / latency / replication-lag
// statistics suitable for plotting scaling curves.
//
// The simulator reuses the production primitives in pkg/causal,
// pkg/hlc, and pkg/storage; only the network transport is mocked. This
// means a passing simulator run is meaningful evidence about the real
// system's behaviour.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"edge-cloud-replication/simulation/checker"
	"edge-cloud-replication/simulation/metrics"
	"edge-cloud-replication/simulation/network"
	"edge-cloud-replication/simulation/site"
	"edge-cloud-replication/simulation/topology"
	"edge-cloud-replication/simulation/workload"
)

type flags struct {
	sites        int
	duration     time.Duration
	concurrency  int
	qpsPerWorker float64
	keys         int
	valueSize    int
	dist         string
	writeRatio   float64
	deleteRatio  float64
	wanLatency   time.Duration
	jitterFrac   float64
	lossRate     float64
	seed         int64
	out          string
	scenario     string
	logLevel     string
	progress     time.Duration
}

func parseFlags() flags {
	var f flags
	flag.IntVar(&f.sites, "sites", 10, "number of edge sites")
	flag.DurationVar(&f.duration, "duration", 10*time.Second, "wall-clock duration to run the workload")
	flag.IntVar(&f.concurrency, "concurrency", 0, "workload worker goroutines (0 = 4*sites)")
	flag.Float64Var(&f.qpsPerWorker, "qps", 0, "per-worker max QPS (0 = uncapped)")
	flag.IntVar(&f.keys, "keys", 1000, "key-space size")
	flag.IntVar(&f.valueSize, "value-size", 64, "value size in bytes")
	flag.StringVar(&f.dist, "dist", "uniform", "key distribution: uniform|zipf")
	flag.Float64Var(&f.writeRatio, "write-ratio", 0.3, "fraction of ops that are Put")
	flag.Float64Var(&f.deleteRatio, "delete-ratio", 0.05, "fraction of ops that are Delete")
	flag.DurationVar(&f.wanLatency, "wan-latency", 25*time.Millisecond, "edge<->cloud mean latency")
	flag.Float64Var(&f.jitterFrac, "jitter-frac", 0.2, "jitter as a fraction of mean WAN latency")
	flag.Float64Var(&f.lossRate, "loss-rate", 0, "probability of dropping a message")
	flag.Int64Var(&f.seed, "seed", 1, "RNG seed")
	flag.StringVar(&f.out, "out", "", "write JSON results to this path (default: stdout only)")
	flag.StringVar(&f.scenario, "scenario", "", "named scenario: small|medium|large|xlarge (overrides -sites)")
	flag.StringVar(&f.logLevel, "log-level", "silent", "site logger level: debug|info|warn|error|silent")
	flag.DurationVar(&f.progress, "progress", 2*time.Second, "interval to print progress")
	flag.Parse()
	return f
}

func applyScenario(f *flags) {
	switch f.scenario {
	case "":
		return
	case "small":
		f.sites = 10
	case "medium":
		f.sites = 50
	case "large":
		f.sites = 100
	case "xlarge":
		f.sites = 500
	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q (small|medium|large|xlarge)\n", f.scenario)
		os.Exit(2)
	}
}

func makeLogger(level string) *slog.Logger {
	var lv slog.Level
	switch level {
	case "debug":
		lv = slog.LevelDebug
	case "info":
		lv = slog.LevelInfo
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	case "silent", "":
		return slog.New(slog.NewTextHandler(io.Discard, nil))
	default:
		lv = slog.LevelWarn
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lv}))
}

type sceneResult struct {
	Scenario string `json:"scenario,omitempty"`

	NumSites     int           `json:"num_sites"`
	Duration     time.Duration `json:"duration"`
	Concurrency  int           `json:"concurrency"`
	WANLatency   time.Duration `json:"wan_latency"`
	Distribution string        `json:"distribution"`
	WriteRatio   float64       `json:"write_ratio"`
	DeleteRatio  float64       `json:"delete_ratio"`

	Workload  workload.Result `json:"workload"`
	OpsPerSec float64         `json:"ops_per_sec"`

	LocalLatency       metrics.LatencyStats `json:"local_latency"`
	ReplicationLag     metrics.LatencyStats `json:"replication_lag"`
	ReplicationPending int                  `json:"replication_pending"`

	Network struct {
		Sent      int64 `json:"sent"`
		Delivered int64 `json:"delivered"`
		Dropped   int64 `json:"dropped"`
	} `json:"network"`

	Apply struct {
		LocalCount  int64 `json:"local_count"`
		RemoteCount int64 `json:"remote_count"`
	} `json:"apply"`

	CausalViolations int `json:"causal_violations"`
	NumGoroutines    int `json:"num_goroutines"`
}

func main() {
	f := parseFlags()
	applyScenario(&f)

	if f.concurrency <= 0 {
		f.concurrency = 4 * f.sites
		if f.concurrency < 8 {
			f.concurrency = 8
		}
	}

	logger := makeLogger(f.logLevel)
	fmt.Printf("simulator: sites=%d duration=%s concurrency=%d wan=%s dist=%s\n",
		f.sites, f.duration, f.concurrency, f.wanLatency, f.dist)

	dist := workload.Uniform
	if f.dist == "zipf" || f.dist == "zipfian" {
		dist = workload.Zipfian
	}

	gen := workload.NewGenerator(workload.Spec{
		NumKeys:      f.keys,
		WriteRatio:   f.writeRatio,
		DeleteRatio:  f.deleteRatio,
		ValueSize:    f.valueSize,
		Distribution: dist,
		Seed:         f.seed,
	})

	chk := checker.New()
	lag := metrics.NewReplicationLagTracker(1) // record everything; switch to >1 for huge runs
	localLat := metrics.NewLatencyHistogram(1024)

	var localApplies, remoteApplies atomic.Int64

	observer := func(ev site.ApplyEvent) {
		switch ev.Kind {
		case site.ApplyLocal:
			localApplies.Add(1)
			if !ev.Deleted {
				lag.RecordWrite(ev.Key, ev.Value, ev.At)
			}
		case site.ApplyRemote:
			remoteApplies.Add(1)
			if !ev.Deleted {
				lag.RecordObservation(ev.Key, ev.Value, ev.At)
			}
		}
		k := checker.KindLocal
		if ev.Kind == site.ApplyRemote {
			k = checker.KindRemote
		}
		chk.Record(checker.Observation{
			Site:     string(ev.Site),
			Kind:     k,
			Key:      ev.Key,
			Origin:   ev.Origin,
			CommitTS: ev.CommitTS,
			Deps:     ev.Deps,
		})
	}

	topo, err := topology.BuildHubAndSpoke(topology.HubAndSpokeConfig{
		NumEdges:           f.sites,
		EdgeToCloudLatency: f.wanLatency,
		JitterFraction:     f.jitterFrac,
		EdgeToCloudLoss:    f.lossRate,
		Logger:             logger,
		Observer:           observer,
	},
		network.WithSeed(f.seed),
		network.WithDefaultLink(network.LinkProfile{
			MeanLatency: f.wanLatency,
			Jitter:      time.Duration(float64(f.wanLatency) * f.jitterFrac),
			LossRate:    f.lossRate,
		}),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "topology: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	topo.Start(ctx)
	defer topo.Stop()

	progressCtx, progressCancel := context.WithCancel(ctx)
	defer progressCancel()
	go progressLoop(progressCtx, f.progress, &localApplies, &remoteApplies, topo, lag)

	runStart := time.Now()
	runRes := workload.Run(ctx, workload.RunSpec{
		Sites:        topo.Sites(),
		Generator:    gen,
		Concurrency:  f.concurrency,
		Duration:     f.duration,
		QPSPerWorker: f.qpsPerWorker,
		Hook: func(_ int, _ workload.Op, lat time.Duration, _ error) {
			localLat.Record(lat)
		},
	})
	progressCancel()

	// Allow propagation to settle so replication-lag samples are
	// representative. Wait at most ~5x the WAN latency.
	settleCtx, settleCancel := context.WithTimeout(context.Background(), 5*f.wanLatency+200*time.Millisecond)
	waitForDrain(settleCtx, topo)
	settleCancel()

	res := sceneResult{
		Scenario:     f.scenario,
		NumSites:     f.sites,
		Duration:     runRes.WallTime,
		Concurrency:  f.concurrency,
		WANLatency:   f.wanLatency,
		Distribution: f.dist,
		WriteRatio:   f.writeRatio,
		DeleteRatio:  f.deleteRatio,
		Workload:     runRes,
		OpsPerSec:    float64(runRes.TotalOps) / runRes.WallTime.Seconds(),

		LocalLatency:       localLat.Snapshot(),
		ReplicationLag:     lag.Stats(),
		ReplicationPending: lag.PendingCount(),

		Apply: struct {
			LocalCount  int64 `json:"local_count"`
			RemoteCount int64 `json:"remote_count"`
		}{
			LocalCount:  localApplies.Load(),
			RemoteCount: remoteApplies.Load(),
		},

		CausalViolations: len(chk.Violations()),
		NumGoroutines:    runtime.NumGoroutine(),
	}

	netStats := topo.Network.Stats()
	res.Network.Sent = netStats.Sent
	res.Network.Delivered = netStats.Delivered
	res.Network.Dropped = netStats.Dropped

	printHumanResults(res, time.Since(runStart))

	if f.out != "" {
		if err := writeJSON(f.out, res); err != nil {
			fmt.Fprintf(os.Stderr, "write results: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("\nresults written to %s\n", f.out)
	}

	if res.CausalViolations > 0 {
		fmt.Fprintf(os.Stderr, "\nFAIL: %d causal violations detected\n", res.CausalViolations)
		os.Exit(3)
	}
}

func progressLoop(ctx context.Context, period time.Duration, local, remote *atomic.Int64, topo *topology.Topology, lag *metrics.ReplicationLagTracker) {
	if period <= 0 {
		return
	}
	tick := time.NewTicker(period)
	defer tick.Stop()
	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			elapsed := time.Since(start)
			localN := local.Load()
			remoteN := remote.Load()
			ns := topo.Network.Stats()
			fmt.Printf("[%6s] local=%d remote=%d net.sent=%d delivered=%d dropped=%d pending-lag=%d\n",
				elapsed.Truncate(100*time.Millisecond),
				localN, remoteN,
				ns.Sent, ns.Delivered, ns.Dropped,
				lag.PendingCount(),
			)
		}
	}
}

// waitForDrain polls the cloud + edges until either ctx expires or the
// network has been quiet (no new sent messages for one tick).
func waitForDrain(ctx context.Context, topo *topology.Topology) {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	prev := topo.Network.Stats().Sent
	stable := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			cur := topo.Network.Stats().Sent
			if cur == prev {
				stable++
				if stable >= 3 {
					return
				}
			} else {
				stable = 0
				prev = cur
			}
		}
	}
}

func writeJSON(path string, v any) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func printHumanResults(r sceneResult, total time.Duration) {
	fmt.Println()
	fmt.Println("─── results ──────────────────────────────────────────────────")
	fmt.Printf("  sites             : %d edges + 1 cloud\n", r.NumSites)
	fmt.Printf("  workload          : %s, write=%.2f delete=%.2f\n", r.Distribution, r.WriteRatio, r.DeleteRatio)
	fmt.Printf("  total ops         : %d  (%.0f ops/sec)\n", r.Workload.TotalOps, r.OpsPerSec)
	fmt.Printf("    puts=%d gets=%d dels=%d errors=%d\n",
		r.Workload.PutOps, r.Workload.GetOps, r.Workload.DeleteOps, r.Workload.Errors)
	fmt.Printf("  local op latency  : p50=%s p95=%s p99=%s max=%s\n",
		r.LocalLatency.P50, r.LocalLatency.P95, r.LocalLatency.P99, r.LocalLatency.Max)
	fmt.Printf("  replication lag   : n=%d p50=%s p95=%s p99=%s max=%s pending=%d\n",
		r.ReplicationLag.Count,
		r.ReplicationLag.P50, r.ReplicationLag.P95, r.ReplicationLag.P99, r.ReplicationLag.Max,
		r.ReplicationPending,
	)
	fmt.Printf("  apply (local/rem) : %d / %d\n", r.Apply.LocalCount, r.Apply.RemoteCount)
	fmt.Printf("  network           : sent=%d delivered=%d dropped=%d (%.2f%% loss)\n",
		r.Network.Sent, r.Network.Delivered, r.Network.Dropped,
		percent(r.Network.Dropped, r.Network.Sent),
	)
	fmt.Printf("  causal violations : %d\n", r.CausalViolations)
	fmt.Printf("  goroutines        : %d\n", r.NumGoroutines)
	fmt.Printf("  wallclock         : %s\n", total.Truncate(time.Millisecond))
	fmt.Println("──────────────────────────────────────────────────────────────")
}

func percent(num, denom int64) float64 {
	if denom == 0 {
		return 0
	}
	return 100.0 * float64(num) / float64(denom)
}
