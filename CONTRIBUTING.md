# Contributing

Thanks for taking an interest in this project. This document covers the mechanics of working on the repo: development setup, expected workflow, and the bar for a change to be accepted.

## Development setup

Requirements:
- Go 1.25+
- `make`
- (optional) `golangci-lint`, `protoc`, Docker, `grpcurl`

Clone and build:

```bash
git clone <repo-url>
cd edge-cloud-replication
make build
```

Run a node locally:

```bash
make run-edge    # terminal 1
make run-cloud   # terminal 2
```

Verify health endpoints:

```bash
curl http://127.0.0.1:8081/healthz
curl http://127.0.0.1:8081/info
curl http://127.0.0.1:8081/metrics | head
```

## Workflow

1. Create a branch: `git checkout -b <short-topic>`.
2. Make the change. Keep PRs focused — one logical change per PR.
3. Run:

```bash
make fmt
make vet
make test
make lint   # if installed
```

4. Commit with a clear message explaining **why**, not just what.
5. Open a PR against `main`.

## Code expectations

- **No `fmt.Println` / `log.Printf` in non-test code.** Use the injected `*slog.Logger`.
- **No default Prometheus registry.** Register new metrics on `observability.Registry.Prom`.
- **Context propagation.** Every blocking call in server paths takes a `context.Context`.
- **Error wrapping.** Wrap with `fmt.Errorf("scope: %w", err)` — preserve the cause.
- **Tests.** New packages need tests. Use `-race` locally before pushing.
- **Config.** Any new tunable goes in `internal/config` with a default and validation.

## Project structure

Read `README.md` for the top-level tour. Key rule of thumb:
- `pkg/` holds reusable distributed systems primitives (raft, hlc, causal, storage, …).
- `internal/` holds process-only glue (config, logging, server lifecycle).
- `cmd/` holds binaries; main should be thin and delegate to `internal/app`.
