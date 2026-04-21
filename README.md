# Edge–Cloud Replication

[![CI](https://github.com/Hammad911/Edge_Cloud/actions/workflows/ci.yml/badge.svg)](https://github.com/Hammad911/Edge_Cloud/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/go-1.25%2B-00ADD8)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Prototype of a two-tier distributed store with **Raft inside each edge cluster** and **scalable causal replication between clusters**, targeting industrial-grade engineering practices from day one.

This is the implementation companion to the project proposal *Scalable Consistency in Edge–Cloud Replication* (Distributed Systems, Spring 2026).

---

## Status

Milestone 6 complete — an in-process discrete-event simulator now drives the same `pkg/causal`/`pkg/hlc`/`pkg/storage` stack across hub-and-spoke topologies of 10, 50, 100, and 500 edge sites. A scheduler-backed mock network injects per-link latency, jitter, loss, and partitions; workloads (uniform / Zipfian, configurable read-write mix) emit reproducible op streams; a runtime causality checker verifies that no remote apply ever violates partitioned-HLC dependencies. The latest scaling sweep (8s, 8 QPS/site) completes 0 violations across all four scales, with replication lag growing as the cloud hub becomes the bottleneck — exactly the behaviour the proposal aims to characterise.

**Milestone 7 complete** — fault-injection harness (`simulation/fault`), metadata-size baselines (`simulation/baselines`), a Jepsen-style **offline history checker** (`evaluation/checker`), a **YCSB-style closed-loop driver for the real gRPC binaries** (`evaluation/ycsb`, `cmd/ycsb`), and a **reproducible figure generator** (`scripts/gen_figures.py` → `docs/figures/`) are all live. A scheduled partition/heal runner drives the WAN bus while the simulator records per-phase (before / during / after) throughput, local-op latency, replication lag, and post-heal convergence time; sweeps across (10→50 sites) × (10%→50% partitioned edges) report **zero causality violations** and convergence bounded to ~6.2 s regardless of scale. The baselines module measures the metadata footprint of eventual / Lamport / partitioned-HLC / full vector clock against the same event stream: at 200 sites clustered into 8 groups, partitioned HLC uses **~12.6× less metadata per event** than a full vector clock. The offline checker replays every op the workload issued — both in-sim and over real gRPC via the YCSB driver — then verifies **monotonic reads, read-your-writes, no stale reads at origin, and eventual convergence** end to end. A local single-edge YCSB-A smoke run drives the full `pkg/kv` → gRPC → HLC → store stack at **~71 k ops/s** with p99 read/update latency under 550 µs, producing a 285 k-event history that the offline checker audits cleanly.

What already works:
- Structured logging (`log/slog`), JSON or text
- Typed config via Viper with env-var overrides (`ECR_*`)
- Prometheus metrics endpoint (`/metrics`)
- Liveness + readiness endpoints (`/healthz`, `/readyz`)
- Optional pprof (`/debug/pprof/*`)
- gRPC KV service (`Get`/`Put`/`Delete`) with HLC-based causal tokens
- Versioned in-memory KV store with snapshot/restore
- Hybrid Logical Clock (`pkg/hlc`) + partitioned HLC for scalable causal metadata
- Raft consensus (`pkg/raft`) wrapping `hashicorp/raft` + BoltDB log/stable stores
- Cluster admin HTTP API (`/cluster/status`, `/cluster/join`, `/cluster/leave`)
- **Causal replication (`pkg/causal`)** with bidi gRPC streaming, out-of-order buffer, dedup, and pluggable Applier (direct-to-store on cloud, Raft-funneled on edges)
- Graceful shutdown via `errgroup` + signal handling
- Multi-stage Dockerfiles (distroless runtime)
- docker-compose for 1 cloud + 2 edge nodes
- Local 3-node Raft cluster scripts (`make cluster-up`)
- **Local 1-cloud + 2-edge causal topology scripts (`make causal-up`)**
- **In-process simulator** (`cmd/simulator`, `simulation/{network,site,topology,workload,metrics,checker}`) scaling 10 → 500 sites, with a runtime causality checker that asserts zero partitioned-HLC violations under uniform/Zipfian workloads and lossy WAN links
- **In-process simulator** (`cmd/simulator`, `simulation/{network,site,topology,workload,metrics,checker}`) that scales 10 → 500 sites, with a runtime causality checker

---

## Getting started

Clone and build:

```bash
git clone https://github.com/Hammad911/Edge_Cloud.git
cd Edge_Cloud
make build
```

Run locally:

```bash
make run-edge
```

In another terminal:

```bash
make run-cloud
```

Verify the node is healthy:

```bash
curl -s http://127.0.0.1:8081/healthz
curl -s http://127.0.0.1:8081/info
curl -s http://127.0.0.1:8081/metrics | head
```

Inspect gRPC services:

```bash
grpcurl -plaintext 127.0.0.1:7001 list
```

### Running a 3-node Raft edge cluster

```bash
make cluster-up          # boots edge-1 (leader) + edge-2 + edge-3
make cluster-status      # shows leader across all three nodes

# Drive traffic through the leader
./bin/kvsmoke -addr 127.0.0.1:7001 put hello world
./bin/kvsmoke -addr 127.0.0.1:7002 get hello   # reads replicated on a follower
./bin/kvsmoke -addr 127.0.0.1:7001 bench 500   # 500 serial Puts through Raft

make cluster-down        # clean shutdown
make clean-data          # wipe raft data dirs
```

Writes to a follower are refused with `FailedPrecondition: kv: not leader`; the
follower exposes the current leader via `GET /cluster/status` so a smart client
can redirect.

### Running cross-cluster causal replication (1 cloud + 2 edges)

```bash
make causal-up           # boots cloud-0 + edge-a-0 + edge-b-0 (raft disabled, just causal)

# Write on edge-A
./bin/kvsmoke -addr 127.0.0.1:7011 put greeting hello

# Read on cloud and edge-B (causal propagation visible within milliseconds)
./bin/kvsmoke -addr 127.0.0.1:9001 get greeting
./bin/kvsmoke -addr 127.0.0.1:7021 get greeting

# Reverse direction works too
./bin/kvsmoke -addr 127.0.0.1:7021 put from-b reply
./bin/kvsmoke -addr 127.0.0.1:7011 get from-b

make causal-down
```

How it works:
- Each edge's leader stamps every write with its HLC plus a partitioned-HLC
  *frontier* (vector of last-seen ts per remote group).
- The cloud is the hub: it receives streams from every edge and fans them out.
- Receivers admit incoming events into a per-node `Buffer`; events whose
  causal deps aren't yet satisfied are held until the local frontier catches
  up. Duplicates (echoes through the hub) are absorbed by `(origin, commit_ts)`
  dedup.
- Applier strategy is pluggable: cloud writes directly to its store; edges
  with raft enabled funnel remote events through the raft log so every
  follower sees them.

### Running the in-process simulator (10 → 500 sites)

```bash
make sim-small     # 10 edges, 5s
make sim-medium    # 50 edges
make sim-large     # 100 edges
make sim-xlarge    # 500 edges

# scaling sweep — JSON per scale + summary table in simulation/results/
make sim-scaling
```

Or drive it directly:

```bash
./bin/simulator -sites 100 -duration 10s -qps 10 \
                -wan-latency 25ms -dist zipf -loss-rate 0.02 \
                -out simulation/results/run.json
```

The simulator reuses the production `pkg/causal`, `pkg/hlc`, and `pkg/storage`
code paths; only the transport is mocked (a heap-scheduled delay/jitter/loss/
partition bus). A runtime causality checker verifies that no remote apply
violates partitioned-HLC dependencies. Representative scaling numbers
(8s run, 8 QPS/site, Uniform, 25ms WAN):

| sites | ops/sec | lag p50 | lag p95 | lag p99 | violations |
|------:|--------:|--------:|--------:|--------:|-----------:|
|    10 |   ~160  |  51 ms  |  59 ms  |  60 ms  |      0     |
|    50 |   ~790  |  54 ms  |  68 ms  |  77 ms  |      0     |
|   100 | ~1 580  |  72 ms  | 138 ms  | 188 ms  |      0     |
|   500 | ~7 570  | 1.68 s  | 5.63 s  | 5.95 s  |      0     |

### Fault-injection scenarios (Milestone 7, part 1)

`simulation/fault` ships a scheduler that applies `Partition` / `Heal`
events to the in-process WAN at configurable offsets. The simulator then
tags every operation with its *phase* (before / during / after the
partition window) and reports per-phase throughput, local-op latency,
and replication lag, plus the post-heal convergence time.

```bash
make sim-fault-demo        # 20 edges, 30% partitioned for 4s, 15ms WAN
make sim-fault             # full (sites × fraction) sweep via sim_fault.sh
```

Representative sweep (partition at +4s, heal at +9s, 15ms WAN, 50 QPS):

| sites | fraction | lag p99 before | during | after | convergence | violations |
|------:|:--------:|---------------:|-------:|------:|------------:|-----------:|
|    10 |   0.1    |       35.7 ms  | 40.7 ms | 38.8 ms | 6.15 s |   **0** |
|    10 |   0.5    |       36.6 ms  | 35.7 ms | 42.7 ms | 6.20 s |   **0** |
|    20 |   0.3    |       35.9 ms  | 87.6 ms | 66.0 ms | 6.20 s |   **0** |
|    50 |   0.1    |       50.2 ms  | 88.1 ms | 213.4 ms | 6.28 s | **0** |
|    50 |   0.5    |       69.0 ms  | 56.6 ms | 106.8 ms | 6.22 s | **0** |

Local-op latency remains sub-millisecond across every phase (writes
never block on the WAN) and **zero causality violations are observed
throughout** — the partitioned HLC keeps the buffer honest even while a
third of the edges are isolated.

### Metadata-size baselines (Milestone 7, part 2)

`simulation/baselines` models the per-event metadata cost of four
consistency schemes and measures all of them against the *exact* event
stream the partitioned-HLC simulator produces, so the numbers are
directly comparable:

- **Eventual** (LWW broadcast, 0 bytes metadata)
- **Lamport** (single 8-byte scalar)
- **Partitioned HLC** (our scheme — one `(group_id, hlc)` pair per dep)
- **Vector Clock** (one 8-byte counter per site, O(N))

```bash
make sim-baselines          # sweep sites=10..200, tabulate bytes/event
```

Representative run (5s, 20 QPS/site, uniform workload, 25 ms WAN):

| sites | VC metadata | pHLC metadata @ G=8 clusters |
|------:|------------:|-----------------------------:|
|    10 |     88 B    |    128 B                     |
|    25 |    208 B    |    128 B                     |
|    50 |    408 B    |    128 B                     |
|   100 |    808 B    |    128 B  (**6.3× smaller**) |
|   200 |  1 608 B    |    128 B  (**12.6× smaller**) |

Every simulator run prints a *projection* block showing what each scheme
would cost under clustering configurations of G = 4, 8, 16, 32 groups.
At N = 100 sites, clustered into 8 groups, partitioned HLC is 6.3×
smaller than a vector clock; at 200 sites it is 12.6× smaller. This is
the paper's central scaling claim, grounded in the system's actual
event stream.

### Offline history checker (Milestone 7, part 3)

`evaluation/checker` is a Jepsen-style post-hoc auditor. The simulator
optionally streams a JSONL history of every op — with its session id,
site, HLC commit timestamp, and (for reads) the version's timestamp —
plus a per-site *final-state* sweep after the pipeline has fully
drained. The checker replays that history and verifies four
user-visible properties the paper promises:

- **Monotonic reads** — for any (session, key), successive Gets see
  non-decreasing commit timestamps.
- **Read-your-writes** — after a session writes (k, v), any later Get
  by that session sees that write or a strictly newer version (with
  explicit tombstone-aware handling for cross-session deletes).
- **No stale reads at origin** — a site that produced write (k, v)
  must read back at least that write regardless of cross-cluster lag.
- **Eventual convergence** — after quiescence, every site agrees on
  the latest value for every key touched during the run.

```bash
make sim-check           # healthy 8-edge scene
make sim-check-fault     # 30% of edges partitioned for 4 s mid-run
./bin/checker -history simulation/results/history.jsonl -json
```

Representative verdicts (default config, `-pin-sessions` on for
standard session-stickiness semantics):

| scenario                    | events | MR  | RYW | origin | converge |
|-----------------------------|-------:|:---:|:---:|:------:|:--------:|
| healthy, 8 edges, 6 s       |  4 680 |  ✓  |  ✓  |   ✓    |    ✓     |
| partition 30% × 4 s, 8 edges |  7 544 |  ✓  |  ✓  |   ✓    |    ✓     |

Writing the checker surfaced **three real replication bugs in the
simulator that the runtime HLC-dependency check could not see**:
(1) the shipper advanced its cursor even when the network returned a
send error, so all traffic emitted during a partition was silently
discarded; (2) the outbox pruned events based on `Next()` return,
racing with shipper retries and deleting events that hadn't actually
been delivered yet; (3) the network scheduler re-checked partition
state at delivery time, losing messages that were already in flight
when the partition started. Each fix was validated by the checker
re-running on the same history and flipping the relevant property
from FAIL → PASS.

### YCSB-style closed-loop driver (Milestone 7, part 4)

`evaluation/ycsb` + `cmd/ycsb` is a production-path workload generator
that talks **real gRPC** to one or more `edge-node` binaries. It carries
the server's `CausalToken` between ops so session stickiness /
read-your-writes flows end-to-end, supports the canonical YCSB
presets (A/B/C/D/F), uniform + Zipfian + latest key distributions,
and emits a latency-percentile + throughput report plus an optional
JSONL history that feeds straight into the offline checker described
above.

```bash
# One-shot smoke test: spins up a throwaway edge-node, runs YCSB-A and
# YCSB-B for a few seconds, audits the recorded history:
make ycsb-smoke

# Custom run against an already-running cluster (any number of
# endpoints, comma-separated):
TARGETS=127.0.0.1:7001,127.0.0.1:7002 make ycsb-a
TARGETS=127.0.0.1:7001               make ycsb-b

# Full flag surface:
./bin/ycsb -h
```

Representative numbers from a local single-edge smoke run on an
Apple-silicon laptop (concurrency = 16, value-size = 64 B, duration
4 s, sticky sessions, full history capture):

| workload | ops/sec | read p50 / p99 | update p50 / p99 | checker verdict |
|:--------:|--------:|----------------|------------------|:---------------:|
| A (50 / 50)  | ~71 k  | 172 µs / 512 µs | 168 µs / 512 µs | all 4 PASS |
| B (95 / 5)   | ~69 k  | 168 µs / 544 µs | 172 µs / 528 µs | all 4 PASS |

The driver also surfaced a **real server-side contention bug** under
hot-key Zipfian writes: when two near-simultaneous `Put`s land on the
same key at very high concurrency, the storage layer rejects the
second one because its HLC timestamp is not strictly newer (error
rate ~0.03% in the smoke run, exposed as `kv.Put: storage: write
timestamp not newer than latest version`). Left as-is for the paper
so the behaviour is visible; a follow-up will add driver-side retry
with clock tick.

### Paper figures (Milestone 7, part 5)

`scripts/gen_figures.py` walks the JSON sweeps under
`simulation/results/` and emits publication-ready PNGs into
`docs/figures/`. The script is self-contained (matplotlib + numpy),
skips any figure whose inputs are missing, and caches nothing — re-
running after a fresh sweep produces a deterministic new set. The five
figures below are what the project write-up ships with and are all
regenerated by `make figures`.

```bash
make figures-deps   # one-shot: pip install matplotlib + numpy (--user)
make figures        # regenerate docs/figures/*.png from latest JSON
```

| Figure | File | Source data | What it shows |
|---|---|---|---|
| Scaling | `docs/figures/scaling.png` | `sim-*sites-*.json` | Throughput, local p99 latency, and replication lag (mean/p50/p99) as the cluster grows from 10 → 500 sites. Throughput scales linearly up to 100 sites, then replication lag takes off as the cloud hub saturates — matching the proposal's predicted regime. |
| Metadata (measured) | `docs/figures/metadata_measured.png` | `baselines-*.json` | Per-event metadata bytes across Eventual / Lamport / Partitioned HLC / full Vector Clock, measured on the **same event stream** at N = 10 … 200. The simulator runs one HLC group per site, so Partitioned HLC here is an upper bound (G = N). |
| Metadata (projected) | `docs/figures/metadata_projected.png` | closed-form model | Per-event metadata as N grows to 1024 sites for the real deployment case where K sites share a Raft group (G = 4 / 8 / 16 / 32). Vector-clock metadata grows O(N); Partitioned HLC stays flat at O(G), crossing the VC line around N = 25–50 for G = 4. **This is the central scaling claim of the paper.** |
| Partition tolerance | `docs/figures/fault_phases.png` | `fault-*.json` | Ops completed, local p99 latency (log scale), and mean replication lag, bucketed into before / during / after a mid-run WAN partition. The during-partition regime shows the edge still serving local ops while the WAN backs up; the after-regime catches up without data loss. |
| Convergence after heal | `docs/figures/fault_convergence.png` | `fault-*.json` | Wall-clock time from partition heal to full causal convergence, grouped by cluster size and fraction of edges partitioned. Convergence is **essentially flat at ~6.2 s** across every (N, fraction) combination tested, i.e. bounded by the partition's own duration rather than by cluster scale. |

If a YCSB run was captured with `-out`, the script additionally emits
`docs/figures/ycsb_latency.png` (throughput by workload + p50/p99
latency per op kind). The figure is skipped when no `ycsb-*.json` is
present.

---

## Configuration

Precedence: **flags → env vars → config file → defaults**.

Env vars use the `ECR_` prefix and double underscores for nested keys. Examples:

| Key | Env var |
|---|---|
| `node.id` | `ECR_NODE_ID` |
| `admin.listen_addr` | `ECR_ADMIN_LISTEN_ADDR` |
| `grpc.listen_addr` | `ECR_GRPC_LISTEN_ADDR` |
| `replication.cloud_addr` | `ECR_REPLICATION_CLOUD_ADDR` |
| `logging.format` | `ECR_LOGGING_FORMAT` |

Defaults and shape live in `configs/default.yaml`.

---

## Repo layout

```
cmd/                 # Binaries (edge-node, cloud-node, simulator, checker, ycsb, kvsmoke)
internal/
  app/               # Lifecycle orchestration
  config/            # Viper-backed typed config
  logging/           # slog setup
  observability/     # Prometheus registry + common metrics
  server/            # Admin HTTP + gRPC server scaffolding
  testutil/          # Test helpers
pkg/                 # Reusable distributed systems primitives
  raft/              # Intra-cluster Raft consensus
  hlc/               # Hybrid Logical Clock + partitioned variant
  causal/            # Inter-cluster causal replication
  escalation/        # Adaptive cross-region Paxos for hot keys
  storage/           # Versioned KV store
  cluster/           # Topology, membership, reintegration
  network/           # Transport helpers / WAN modeling
  metrics/           # Application metric definitions
proto/               # gRPC API definitions (placeholders)
simulation/          # Phase 1: discrete-event simulator + baselines
evaluation/          # YCSB runner, fault injection, causality checker
deploy/              # Dockerfiles, testbed setup
configs/             # YAML configs for local + testbed runs
docs/                # Architecture notes and final report material
```

---

## Make targets

| Target | Purpose |
|---|---|
| `make build` | Build all binaries into `bin/` |
| `make run-edge` / `make run-cloud` | Run a single node with local config |
| `make cluster-up` / `make cluster-down` | Launch/stop a local 3-node Raft edge cluster |
| `make cluster-status` | Print leader/follower status for all local nodes |
| `make causal-up` / `make causal-down` | Launch/stop a 1-cloud + 2-edge causal-replication topology |
| `make sim-small|medium|large|xlarge` | Run the in-process simulator at 10/50/100/500 sites |
| `make sim-scaling` | Run the scaling sweep and emit per-scale JSON + summary table |
| `make sim-fault-demo` | Single-partition demo with phase-aware metrics |
| `make sim-fault` | Sweep partition scenarios across site counts and fractions |
| `make sim-baselines` | Metadata-per-event comparison vs. eventual / Lamport / vector clock |
| `make sim-check` | Record a history from a healthy run and audit MR / RYW / origin / convergence |
| `make sim-check-fault` | Same, with a mid-run network partition of 30% of the edges |
| `make ycsb-smoke` | Launch a throwaway edge-node, run YCSB-A + YCSB-B against real gRPC, audit history |
| `TARGETS=host:port make ycsb-a` / `ycsb-b` | Run YCSB-A (50/50) / YCSB-B (95/5) against an already-running cluster |
| `make figures-deps` | Install matplotlib + numpy into `--user` site-packages |
| `make figures` | Regenerate all paper figures under `docs/figures/` from JSON in `simulation/results/` |
| `make test` | Race-enabled test suite |
| `make cover` | Test with coverage + HTML report |
| `make lint` | Run `golangci-lint` |
| `make proto` | Regenerate gRPC stubs (needs `protoc`) |
| `make docker-edge` / `make docker-cloud` | Build container images |

---

## Authors

- Hammad Ahmed (464773)
- Soaem Luhana (458608)
- M. Faseeh (456267)

## License

MIT — see [LICENSE](LICENSE).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

---

## Implementation roadmap

1. ~~**HLC** in `pkg/hlc` (with partitioned variant, full tests)~~ ✓ *Milestone 1*
2. ~~**Versioned KV** in `pkg/storage` (multi-version store for causal reads)~~ ✓ *Milestone 1*
3. ~~**Client KV service** on gRPC, wired into `edge-node`~~ ✓ *Milestone 2*
4. ~~**Raft** in `pkg/raft` (in-cluster consensus)~~ ✓ *Milestone 3*
5. ~~**Causal replication** in `pkg/causal` (cross-cluster)~~ ✓ *Milestone 5*
6. ~~**Simulator** in `simulation/` (10 → 500 edge sites)~~ ✓ *Milestone 6*
7. ~~**Evaluation** — Milestone 7 complete:~~
   - ~~Fault-injection harness (`simulation/fault`) + phase-aware metrics~~ ✓
   - ~~Metadata-size baselines (eventual / Lamport / vector clock) with clustering projection~~ ✓
   - ~~Offline history checker (`evaluation/checker`) — MR, RYW, origin freshness, convergence~~ ✓
   - ~~YCSB-style closed-loop driver for the real gRPC binaries (`evaluation/ycsb`, `cmd/ycsb`)~~ ✓
   - ~~Paper figures generated from JSON results (`scripts/gen_figures.py` + `make figures`)~~ ✓
