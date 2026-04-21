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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"edge-cloud-replication/pkg/causal"
	"edge-cloud-replication/simulation/baselines"
	"edge-cloud-replication/simulation/checker"
	"edge-cloud-replication/simulation/fault"
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

	faultPartitionAt       time.Duration
	faultPartitionDuration time.Duration
	faultPartitionFraction float64
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
	flag.DurationVar(&f.faultPartitionAt, "partition-at", 0, "offset from run start to isolate a subset of edges (0 disables)")
	flag.DurationVar(&f.faultPartitionDuration, "partition-duration", 0, "how long the partition lasts; heals automatically afterwards")
	flag.Float64Var(&f.faultPartitionFraction, "partition-fraction", 0.3, "fraction of edges to isolate during the partition window (0,1]")
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

	// Fault is populated only when a partition scenario was scheduled.
	Fault *faultResult `json:"fault,omitempty"`

	// Metadata reports the metadata-per-event cost this workload would
	// impose under each replication scheme we compare against. It is
	// computed from the exact event stream our partitioned-HLC runs
	// produced, so the schemes are directly comparable.
	Metadata baselines.Report `json:"metadata"`
}

type faultResult struct {
	PartitionAt       time.Duration         `json:"partition_at"`
	PartitionDuration time.Duration         `json:"partition_duration"`
	PartitionedEdges  int                   `json:"partitioned_edges"`
	Phases            metrics.PhaseSnapshot `json:"phases"`
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

	faultEnabled := f.faultPartitionAt > 0 && f.faultPartitionDuration > 0
	var phases *metrics.PhaseTracker
	if faultEnabled {
		phases = metrics.NewPhaseTracker()
	}

	// Metadata accounting spans the total system size (edges + cloud)
	// so the VectorClock model sees the real N.
	meta := baselines.NewRollingStats(f.sites + 1)
	var metaMu sync.Mutex

	var localApplies, remoteApplies atomic.Int64

	observer := func(ev site.ApplyEvent) {
		switch ev.Kind {
		case site.ApplyLocal:
			localApplies.Add(1)
			if !ev.Deleted {
				lag.RecordWrite(ev.Key, ev.Value, ev.At)
			}
			metaMu.Lock()
			meta.Record(&causal.Event{
				Origin:   ev.Origin,
				Key:      ev.Key,
				Value:    ev.Value,
				Deleted:  ev.Deleted,
				CommitTS: ev.CommitTS,
				Deps:     ev.Deps,
			})
			metaMu.Unlock()
		case site.ApplyRemote:
			remoteApplies.Add(1)
			if !ev.Deleted {
				lag.RecordObservation(ev.Key, ev.Value, ev.At)
				if phases != nil && ev.CommitTS.Physical > 0 {
					originWall := time.Unix(0, ev.CommitTS.Physical)
					phases.RecordLag(ev.At.Sub(originWall))
				}
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

	var (
		faultRunner      *fault.Runner
		partitionedEdges []network.Address
		partitionResult  *faultResult
	)
	if faultEnabled {
		partitionedEdges = selectPartitionEdges(topo.Edges, f.faultPartitionFraction)
		schedule := fault.PartitionWindow(partitionedEdges, f.faultPartitionAt, f.faultPartitionDuration)
		faultCallback := func(ev fault.Event, _ time.Time) {
			switch ev.Kind {
			case fault.PartitionAll, fault.PartitionLink:
				phases.Set(metrics.PhaseDuring)
			case fault.HealAll, fault.HealLink:
				phases.Set(metrics.PhaseAfter)
			}
		}
		faultRunner = fault.NewRunner(topo.Network, schedule, logger, faultCallback)
		faultRunner.Start(ctx)
		partitionResult = &faultResult{
			PartitionAt:       f.faultPartitionAt,
			PartitionDuration: f.faultPartitionDuration,
			PartitionedEdges:  len(partitionedEdges),
		}
		fmt.Printf("fault: partitioning %d/%d edges at +%s for %s\n",
			len(partitionedEdges), len(topo.Edges), f.faultPartitionAt, f.faultPartitionDuration)
	}

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
		Hook: func(_ int, _ workload.Op, lat time.Duration, err error) {
			localLat.Record(lat)
			if phases != nil {
				phases.RecordOp(lat, err == nil)
			}
		},
	})
	progressCancel()

	// For fault-injection runs the settle window has to span the post-heal
	// convergence phase. Give the system at least the partition duration
	// again to drain the backlog before we snapshot metrics.
	settleBudget := 5*f.wanLatency + 200*time.Millisecond
	if faultEnabled {
		settleBudget = f.faultPartitionDuration + 10*f.wanLatency + 500*time.Millisecond
	}
	settleCtx, settleCancel := context.WithTimeout(context.Background(), settleBudget)
	waitForDrainAndConverge(settleCtx, topo, lag, phases)
	settleCancel()

	if faultRunner != nil {
		faultRunner.Stop()
	}

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

	if partitionResult != nil && phases != nil {
		partitionResult.Phases = phases.Snapshot()
		res.Fault = partitionResult
	}

	metaMu.Lock()
	res.Metadata = meta.Snapshot()
	metaMu.Unlock()

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

// waitForDrainAndConverge polls the network until it has been quiet for
// three consecutive ticks (no new Sent traffic). When a phase tracker is
// present and we are in PhaseAfter, the first moment network activity
// stabilises is our post-heal convergence time: the backlog that built
// up during the partition has been shipped, the cloud has fanned it out,
// and no more edge<->cloud messages are in flight.
func waitForDrainAndConverge(
	ctx context.Context,
	topo *topology.Topology,
	_ *metrics.ReplicationLagTracker,
	phases *metrics.PhaseTracker,
) {
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
					if phases != nil && phases.Current() == metrics.PhaseAfter {
						phases.NoteConverged()
					}
					return
				}
			} else {
				stable = 0
				prev = cur
			}
		}
	}
}

// selectPartitionEdges returns the first N edges according to the
// requested fraction (clamped to [1, len(edges)]). Deterministic: we
// take a prefix so runs are reproducible under a given seed.
func selectPartitionEdges(edges []*site.Site, fraction float64) []network.Address {
	if fraction <= 0 {
		return nil
	}
	n := int(float64(len(edges)) * fraction)
	if n < 1 {
		n = 1
	}
	if n > len(edges) {
		n = len(edges)
	}
	out := make([]network.Address, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, edges[i].Address())
	}
	return out
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
	if r.Metadata.Events > 0 {
		fmt.Println("─── metadata-per-event vs. baseline schemes ──────────────────")
		for _, s := range []string{"eventual", "lamport", "partitioned_hlc", "vector_clock"} {
			sr := r.Metadata.Schemes[s]
			fmt.Printf("  %-16s : mean=%.2f B/event  total=%d B\n",
				s, sr.MetadataBytesMean, sr.MetadataBytesTotal)
		}
		pHLC := r.Metadata.Schemes["partitioned_hlc"].MetadataBytesMean
		vc := r.Metadata.Schemes["vector_clock"].MetadataBytesMean
		if pHLC > 0 && vc > 0 {
			switch {
			case pHLC < vc:
				fmt.Printf("  partitioned-HLC is %.1fx smaller than vector clock\n", vc/pHLC)
			case pHLC > vc:
				fmt.Printf("  partitioned-HLC is %.1fx larger than vector clock "+
					"(simulator runs one group per site; the win materialises when "+
					"G << N — see metadata projection below)\n", pHLC/vc)
			}
		}
		// Projection: what each scheme would cost in a deployment with
		// N sites clustered into G groups (K sites per group), holding
		// the measured average dep fan-out fixed.
		if r.NumSites > 0 && r.Metadata.Events > 0 {
			avgDeps := baselines.EventsAvgDeps(&r.Metadata, r.NumSites+1)
			fmt.Printf("  projected metadata at N=%d, avg deps/event=%.1f:\n",
				r.NumSites+1, avgDeps)
			for _, g := range []int{4, 8, 16, 32} {
				if g > r.NumSites+1 {
					break
				}
				proj := baselines.Project(avgDeps, r.NumSites+1, g)
				fmt.Printf("    G=%2d groups :  pHLC=%6.1f B   VC=%6.1f B   ratio=%.2fx\n",
					g, proj.PartitionedHLC, proj.VectorClock,
					proj.VectorClock/proj.PartitionedHLC)
			}
		}
	}
	if r.Fault != nil {
		fmt.Println("─── fault-injection phases ───────────────────────────────────")
		fmt.Printf("  partition         : %d edges, +%s for %s\n",
			r.Fault.PartitionedEdges, r.Fault.PartitionAt, r.Fault.PartitionDuration)
		for _, p := range r.Fault.Phases.Phases {
			fmt.Printf("  %-7s ops=%d err=%d local p50=%s p99=%s lag p50=%s p99=%s\n",
				p.Phase, p.OpsSucceeded, p.OpsFailed,
				p.LocalLatency.P50, p.LocalLatency.P99,
				p.ReplicationLag.P50, p.ReplicationLag.P99,
			)
		}
		if r.Fault.Phases.ConvergenceTime > 0 {
			fmt.Printf("  post-heal converged in %s\n",
				r.Fault.Phases.ConvergenceTime.Truncate(time.Millisecond))
		} else {
			fmt.Println("  post-heal convergence: not yet reached within settle window")
		}
	}
	fmt.Println("──────────────────────────────────────────────────────────────")
}

func percent(num, denom int64) float64 {
	if denom == 0 {
		return 0
	}
	return 100.0 * float64(num) / float64(denom)
}
