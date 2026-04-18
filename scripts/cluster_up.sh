#!/usr/bin/env bash
# Launch a local 3-node Raft edge cluster for smoke testing.
# Logs land in ./data/edge-N/node.log; pids in ./data/edge-N/node.pid.
# Use scripts/cluster_down.sh to stop everything.
set -euo pipefail

cd "$(dirname "$0")/.."

mkdir -p data/edge-1 data/edge-2 data/edge-3

start_node() {
  local id="$1"
  local cfg="$2"
  local dir="data/${id}"

  if [[ -f "${dir}/node.pid" ]] && kill -0 "$(cat "${dir}/node.pid")" 2>/dev/null; then
    echo "[cluster] ${id} already running (pid $(cat "${dir}/node.pid"))"
    return
  fi

  echo "[cluster] starting ${id} with ${cfg}"
  ( go run ./cmd/edge-node -config "${cfg}" >"${dir}/node.log" 2>&1 ) &
  echo $! >"${dir}/node.pid"
}

start_node edge-1 configs/edge-1.yaml
sleep 2 # let the leader bootstrap before followers come up
start_node edge-2 configs/edge-2.yaml
start_node edge-3 configs/edge-3.yaml

echo
echo "[cluster] 3 nodes launching. Probe:"
echo "  curl -s http://127.0.0.1:8081/cluster/status"
echo "  grpcurl -plaintext 127.0.0.1:7001 list"
echo
echo "[cluster] logs in data/edge-*/node.log"
