# Edge–Cloud Replication

[![CI](https://github.com/Hammad911/Edge_Cloud/actions/workflows/ci.yml/badge.svg)](https://github.com/Hammad911/Edge_Cloud/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/go-1.25%2B-00ADD8)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Prototype of a two-tier distributed store with **Raft inside each edge cluster** and **scalable causal replication between clusters**, targeting industrial-grade engineering practices from day one.

This is the implementation companion to the project proposal *Scalable Consistency in Edge–Cloud Replication* (Distributed Systems, Spring 2026).

---

## Status

Milestone 6 complete — an in-process discrete-event simulator now drives the same `pkg/causal`/`pkg/hlc`/`pkg/storage` stack across hub-and-spoke topologies of 10, 50, 100, and 500 edge sites. A scheduler-backed mock network injects per-link latency, jitter, loss, and partitions; workloads (uniform / Zipfian, configurable read-write mix) emit reproducible op streams; a runtime causality checker verifies that no remote apply ever violates partitioned-HLC dependencies. The latest scaling sweep (8s, 8 QPS/site) completes 0 violations across all four scales, with replication lag growing as the cloud hub becomes the bottleneck — exactly the behaviour the proposal aims to characterise.

**Milestone 7 in progress** — fault-injection harness (`simulation/fault`) and metadata-size baselines (`simulation/baselines`) are live. A scheduled partition/heal runner drives the WAN bus while the simulator records per-phase (before / during / after) throughput, local-op latency, replication lag, and post-heal convergence time; sweeps across (10→50 sites) × (10%→50% partitioned edges) report **zero causality violations** and convergence bounded to ~6.2 s regardless of scale. The baselines module measures the metadata footprint of eventual / Lamport / partitioned-HLC / full vector clock against the same event stream: at 200 sites clustered into 8 groups, partitioned HLC uses **~12.6× less metadata per event** than a full vector clock.

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
cmd/                 # Binaries (edge-node, cloud-node, simulator, checker, benchmark)
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
7. **Evaluation** — Milestone 7 in progress:
   - ~~Fault-injection harness (`simulation/fault`) + phase-aware metrics~~ ✓
   - ~~Metadata-size baselines (eventual / Lamport / vector clock) with clustering projection~~ ✓
   - Offline history checker (monotonic reads, read-your-writes, convergence) — *next*
   - YCSB-style closed-loop driver for the real gRPC binaries
   - Paper figures generated from JSON results
