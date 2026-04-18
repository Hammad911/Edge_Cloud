# Edge–Cloud Replication

[![CI](https://github.com/Hammad911/Edge_Cloud/actions/workflows/ci.yml/badge.svg)](https://github.com/Hammad911/Edge_Cloud/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/go-1.25%2B-00ADD8)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Prototype of a two-tier distributed store with **Raft inside each edge cluster** and **scalable causal replication between clusters**, targeting industrial-grade engineering practices from day one.

This is the implementation companion to the project proposal *Scalable Consistency in Edge–Cloud Replication* (Distributed Systems, Spring 2026).

---

## Status

Milestone 5 complete — cross-cluster causal replication is live. Edge clusters and the cloud hub stream causally-tagged writes over a long-lived gRPC bidi channel; receivers buffer events whose dependencies aren't yet satisfied and apply them in causal order via partitioned HLCs. A 1-cloud + 2-edge topology demonstrates write on edge-A reading on edge-B (and vice versa), including deletes, with self-loop suppression and dedup. Up next: scaling the simulator from 10 → 500 sites and a Jepsen-style causality checker.

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
6. **Simulator** in `simulation/` (10 → 500 edge sites) — *next*
7. **Evaluation** — YCSB, fault injection, Jepsen-style checker
