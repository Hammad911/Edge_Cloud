SHELL := /bin/bash

# ---- build metadata ----
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X edge-cloud-replication/internal/app.Version=$(VERSION) -s -w

BIN_DIR := bin

BINARIES := edge-node cloud-node kvsmoke simulator checker

.PHONY: all
all: build

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: build
build: $(addprefix $(BIN_DIR)/,$(BINARIES))

$(BIN_DIR)/%: FORCE
	@mkdir -p $(BIN_DIR)
	go build -trimpath -ldflags "$(LDFLAGS)" -o $@ ./cmd/$*

.PHONY: FORCE
FORCE:

.PHONY: test
test:
	go test ./... -race -count=1

.PHONY: cover
cover:
	go test ./... -race -coverprofile=coverage.out -covermode=atomic
	go tool cover -html=coverage.out -o coverage.html

.PHONY: vet
vet:
	go vet ./...

.PHONY: fmt
fmt:
	gofmt -s -w .

.PHONY: lint
lint:
	@which golangci-lint >/dev/null || (echo "install golangci-lint: https://golangci-lint.run/usage/install/"; exit 1)
	golangci-lint run

# ---- proto generation (requires protoc + protoc-gen-go + protoc-gen-go-grpc) ----
.PHONY: proto-tools
proto-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

.PHONY: proto
proto:
	PATH="$$(go env GOPATH)/bin:$$PATH" ./scripts/proto_gen.sh

# ---- run helpers ----
.PHONY: run-edge
run-edge:
	go run ./cmd/edge-node -config configs/default.yaml

.PHONY: run-cloud
run-cloud:
	go run ./cmd/cloud-node -config configs/cloud.yaml

# Launch a local 3-node Raft edge cluster. Each target runs in the
# foreground; open three terminals (or use `make cluster-up`).
.PHONY: run-edge-1 run-edge-2 run-edge-3
run-edge-1:
	go run ./cmd/edge-node -config configs/edge-1.yaml
run-edge-2:
	go run ./cmd/edge-node -config configs/edge-2.yaml
run-edge-3:
	go run ./cmd/edge-node -config configs/edge-3.yaml

# ---- local cluster scripts ----
.PHONY: cluster-up cluster-down cluster-status
cluster-up:
	./scripts/cluster_up.sh
cluster-down:
	./scripts/cluster_down.sh
cluster-status:
	@for p in 8081 8082 8083; do \
	  echo "=== 127.0.0.1:$$p ==="; \
	  curl -s http://127.0.0.1:$$p/cluster/status | python3 -m json.tool || true; \
	done

# ---- causal-replication topology (1 cloud + 2 edges) ----
.PHONY: causal-up causal-down causal-status
causal-up: build
	./scripts/causal_up.sh
causal-down:
	./scripts/causal_down.sh
causal-status:
	@for p in 9081 8011 8021; do \
	  echo "=== 127.0.0.1:$$p ==="; \
	  curl -s http://127.0.0.1:$$p/healthz || true; echo; \
	done

.PHONY: clean-data
clean-data:
	rm -rf data/

# ---- simulator (in-process N-edge + 1-cloud) ----
.PHONY: sim-small sim-medium sim-large sim-xlarge sim-scaling
sim-small: build
	./bin/simulator -scenario small -duration 5s -qps 10 -wan-latency 25ms
sim-medium: build
	./bin/simulator -scenario medium -duration 8s -qps 10 -wan-latency 25ms
sim-large: build
	./bin/simulator -scenario large -duration 10s -qps 8 -wan-latency 25ms
sim-xlarge: build
	./bin/simulator -scenario xlarge -duration 12s -qps 4 -wan-latency 30ms
sim-scaling: build
	./scripts/sim_scaling.sh

# Fault-injection: sweep partition scenarios and emit per-phase metrics.
.PHONY: sim-fault sim-fault-demo
sim-fault: build
	./scripts/sim_fault.sh
sim-fault-demo: build
	./bin/simulator -sites 20 -duration 12s -partition-at 3s -partition-duration 4s \
	    -partition-fraction 0.3 -wan-latency 15ms -concurrency 20 -qps 50 \
	    -keys 500 -write-ratio 0.4 -progress 0 \
	    -out simulation/results/fault_demo.json

# Baselines: emit metadata-per-event comparison across consistency
# schemes (eventual, lamport, partitioned-HLC, vector clock) at
# multiple scales.
.PHONY: sim-baselines
sim-baselines: build
	./scripts/sim_baselines.sh

# Offline consistency checker: run the simulator with -history-out,
# then audit the recorded history for monotonic reads, read-your-
# writes, origin freshness, and convergence.
.PHONY: sim-check sim-check-fault
sim-check: build
	@mkdir -p simulation/results
	./bin/simulator -sites 8 -duration 6s -qps 30 -concurrency 16 \
	    -keys 200 -write-ratio 0.4 -wan-latency 20ms -progress 0 \
	    -history-out simulation/results/history.jsonl \
	    -out simulation/results/history_run.json
	./bin/checker -history simulation/results/history.jsonl
sim-check-fault: build
	@mkdir -p simulation/results
	./bin/simulator -sites 8 -duration 12s -qps 30 -concurrency 16 \
	    -keys 200 -write-ratio 0.4 -wan-latency 20ms -progress 0 \
	    -partition-at 3s -partition-duration 4s -partition-fraction 0.3 \
	    -history-out simulation/results/history_fault.jsonl \
	    -out simulation/results/history_fault_run.json
	./bin/checker -history simulation/results/history_fault.jsonl

# ---- docker ----
.PHONY: docker-edge docker-cloud
docker-edge:
	docker build -f deploy/docker/Dockerfile.edge -t ecr-edge:$(VERSION) .

docker-cloud:
	docker build -f deploy/docker/Dockerfile.cloud -t ecr-cloud:$(VERSION) .

.PHONY: clean
clean:
	rm -rf $(BIN_DIR) coverage.out coverage.html
