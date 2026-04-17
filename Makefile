SHELL := /bin/bash

# ---- build metadata ----
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X edge-cloud-replication/internal/app.Version=$(VERSION) -s -w

BIN_DIR := bin

BINARIES := edge-node cloud-node simulator checker benchmark

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

# ---- docker ----
.PHONY: docker-edge docker-cloud
docker-edge:
	docker build -f deploy/docker/Dockerfile.edge -t ecr-edge:$(VERSION) .

docker-cloud:
	docker build -f deploy/docker/Dockerfile.cloud -t ecr-cloud:$(VERSION) .

.PHONY: clean
clean:
	rm -rf $(BIN_DIR) coverage.out coverage.html
