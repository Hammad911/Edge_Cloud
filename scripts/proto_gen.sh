#!/usr/bin/env bash
# Regenerate gRPC stubs from every *.proto under proto/.
# Requires: protoc, protoc-gen-go, protoc-gen-go-grpc on PATH.
#
# Install plugins:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

command -v protoc >/dev/null || { echo "protoc not on PATH"; exit 1; }
command -v protoc-gen-go >/dev/null || { echo "protoc-gen-go not on PATH (run: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest)"; exit 1; }
command -v protoc-gen-go-grpc >/dev/null || { echo "protoc-gen-go-grpc not on PATH (run: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest)"; exit 1; }

rm -rf gen/proto
mkdir -p gen/proto

protoc \
  -I proto \
  --go_out=gen/proto --go_opt=paths=source_relative \
  --go-grpc_out=gen/proto --go-grpc_opt=paths=source_relative \
  $(find proto -name '*.proto')

echo "generated into gen/proto/"
