// Command ycsb is a closed-loop YCSB-style workload driver for the
// edge-node gRPC API. It speaks the real client protocol (causal tokens
// and all) so measurements reflect production code paths, and can
// optionally emit a history compatible with evaluation/checker for
// post-hoc consistency auditing.
//
// Examples:
//
//	# Load 10k keys then run YCSB-A for 30s against one edge:
//	ycsb -targets 127.0.0.1:7001 -workload A -records 10000 \
//	     -duration 30s -concurrency 64
//
//	# Load-only phase (skip run phase):
//	ycsb -targets 127.0.0.1:7001 -records 10000 -operations 0 -duration 0 \
//	     -load-only
//
//	# Run against two edges with session stickiness + history recording:
//	ycsb -targets 127.0.0.1:7001,127.0.0.1:7002 -workload B \
//	     -duration 20s -sticky -history-out out.jsonl
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"edge-cloud-replication/evaluation/ycsb"
)

func main() {
	targetsCSV := flag.String("targets", "127.0.0.1:7001", "comma-separated gRPC target addresses (edge-node KV endpoints)")
	workloadName := flag.String("workload", "A", "YCSB preset (A/B/C/D/F)")
	records := flag.Int("records", 10000, "number of records to load in the load phase")
	valueSize := flag.Int("value-size", 128, "payload size in bytes for Put/Insert ops")
	concurrency := flag.Int("concurrency", 16, "number of closed-loop workers")
	duration := flag.Duration("duration", 10*time.Second, "wall-clock budget for the run phase (0 = operations bound)")
	operations := flag.Int("operations", 0, "total operation budget for the run phase (0 = duration bound)")
	opTimeout := flag.Duration("op-timeout", 2*time.Second, "per-operation RPC timeout")
	sticky := flag.Bool("sticky", true, "pin each worker to one target (required for RYW/MR over multi-site)")
	seed := flag.Int64("seed", 0, "RNG seed (0 = time-based)")
	loadOnly := flag.Bool("load-only", false, "populate the keyspace and exit without running the mix")
	runOnly := flag.Bool("run-only", false, "skip the load phase (assume the keyspace is already populated)")
	distOverride := flag.String("dist", "", "override the workload's default key distribution (uniform|zipfian|latest)")
	out := flag.String("out", "", "write JSON report to this path (defaults to stdout)")
	historyOut := flag.String("history-out", "", "optional JSONL history for evaluation/checker")
	mixReadP := flag.Float64("mix-read", -1, "override read ratio (-1 = use preset)")
	mixUpdateP := flag.Float64("mix-update", -1, "override update ratio")
	mixInsertP := flag.Float64("mix-insert", -1, "override insert ratio")
	mixRmwP := flag.Float64("mix-rmw", -1, "override read-modify-write ratio")
	mixDeleteP := flag.Float64("mix-delete", -1, "override delete ratio")
	keyPrefix := flag.String("key-prefix", "user", "key-space prefix (YCSB default: user)")
	logLevel := flag.String("log-level", "info", "slog level: debug|info|warn|error")
	quiet := flag.Bool("quiet", false, "suppress progress logs (report still printed)")
	flag.Parse()

	if *loadOnly && *runOnly {
		die("-load-only and -run-only are mutually exclusive")
	}

	logger := newLogger(*logLevel, *quiet)

	targets := splitAndTrim(*targetsCSV)
	if len(targets) == 0 {
		die("no -targets provided")
	}

	spec, err := ycsb.Preset(ycsb.Workload(strings.ToUpper(*workloadName)))
	if err != nil {
		die("workload: %v", err)
	}
	spec.RecordCount = *records
	spec.ValueSize = *valueSize
	if *distOverride != "" {
		spec.Distribution = ycsb.KeyDistribution(strings.ToLower(*distOverride))
	}
	if *mixReadP >= 0 {
		spec.Mix.Read = *mixReadP
	}
	if *mixUpdateP >= 0 {
		spec.Mix.Update = *mixUpdateP
	}
	if *mixInsertP >= 0 {
		spec.Mix.Insert = *mixInsertP
	}
	if *mixRmwP >= 0 {
		spec.Mix.ReadModifyWrite = *mixRmwP
	}
	if *mixDeleteP >= 0 {
		spec.Mix.Delete = *mixDeleteP
	}

	cfg := ycsb.Config{
		Targets:     targets,
		Spec:        spec,
		Concurrency: *concurrency,
		Operations:  *operations,
		Duration:    *duration,
		OpTimeout:   *opTimeout,
		Stickiness:  *sticky,
		Seed:        *seed,
		KeyPrefix:   *keyPrefix,
		ValueSize:   *valueSize,
		HistoryOut:  *historyOut,
		Logger:      logger,
	}

	runner, err := ycsb.NewRunner(cfg)
	if err != nil {
		die("new runner: %v", err)
	}
	defer runner.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	installSignalHandler(ctx, cancel, logger)

	if !*runOnly {
		logger.Info("ycsb load phase starting", "records", spec.RecordCount, "targets", targets)
		if err := runner.Load(ctx); err != nil {
			die("load: %v", err)
		}
		logger.Info("ycsb load phase complete")
	}

	if !*loadOnly {
		logger.Info("ycsb run phase starting", "workload", string(spec.Workload),
			"concurrency", cfg.Concurrency, "duration", cfg.Duration,
			"operations", cfg.Operations, "sticky", cfg.Stickiness)
		if err := runner.Run(ctx); err != nil {
			die("run: %v", err)
		}
		logger.Info("ycsb run phase complete")
	}

	if err := emitReport(runner.Report(), *out); err != nil {
		die("emit report: %v", err)
	}
}

func emitReport(rep ycsb.Report, path string) error {
	payload, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	if path == "" {
		_, err := os.Stdout.Write(append(payload, '\n'))
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, payload, 0o644)
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func newLogger(level string, quiet bool) *slog.Logger {
	lvl := new(slog.LevelVar)
	switch strings.ToLower(level) {
	case "debug":
		lvl.Set(slog.LevelDebug)
	case "warn":
		lvl.Set(slog.LevelWarn)
	case "error":
		lvl.Set(slog.LevelError)
	default:
		lvl.Set(slog.LevelInfo)
	}
	var w io.Writer = os.Stderr
	if quiet {
		w = io.Discard
	}
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: lvl}))
}

func installSignalHandler(ctx context.Context, cancel context.CancelFunc, logger *slog.Logger) {
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		select {
		case <-ctx.Done():
		case sig := <-ch:
			logger.Info("ycsb signal received, cancelling", "sig", sig.String())
			cancel()
		}
	}()
}

func die(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if errors.Is(context.Cause(context.Background()), context.Canceled) {
		msg += " (context cancelled)"
	}
	fmt.Fprintln(os.Stderr, "ycsb: "+msg)
	os.Exit(1)
}
