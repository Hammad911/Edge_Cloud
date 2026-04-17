# Edge–Cloud Replication

[![CI](https://github.com/OWNER/REPO/actions/workflows/ci.yml/badge.svg)](https://github.com/OWNER/REPO/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/go-1.25%2B-00ADD8)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Prototype of a two-tier distributed store with **Raft inside each edge cluster** and **scalable causal replication between clusters**, targeting industrial-grade engineering practices from day one.

This is the implementation companion to the project proposal *Scalable Consistency in Edge–Cloud Replication* (Distributed Systems, Spring 2026).

---

## Status

Phase 0 — infrastructure is in place. Services (Raft, causal replication, storage) plug into the lifecycle via `internal/app` as they are implemented.

What already works:
- Structured logging (`log/slog`), JSON or text
- Typed config via Viper with env-var overrides (`ECR_*`)
- Prometheus metrics endpoint (`/metrics`)
- Liveness + readiness endpoints (`/healthz`, `/readyz`)
- Optional pprof (`/debug/pprof/*`)
- gRPC server with health service + reflection
- Graceful shutdown via `errgroup` + signal handling
- Multi-stage Dockerfiles (distroless runtime)
- docker-compose for 1 cloud + 2 edge nodes

---

## Getting started

Clone and build:

```bash
git clone https://github.com/OWNER/REPO.git
cd REPO
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

## Next implementation milestones

1. **HLC** in `pkg/hlc` (with partitioned variant, full tests)
2. **Versioned KV** in `pkg/storage` (multi-version store for causal reads)
3. **Client KV service** on gRPC, wired into `edge-node`
4. **Raft** in `pkg/raft` (in-cluster consensus)
5. **Causal replication** in `pkg/causal` (cross-cluster)
6. **Simulator** in `simulation/` (10 → 500 edge sites)
7. **Evaluation** — YCSB, fault injection, Jepsen-style checker
