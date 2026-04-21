#!/usr/bin/env bash
# Closed-loop YCSB smoke test against a fresh edge-node.
#
# Spins up one edge-node on a throwaway port, loads a small keyspace,
# runs workloads A and B for a few seconds each, records a history,
# replays it through the offline checker, and tears everything down.
#
# Outputs land in simulation/results/ycsb-<workload>.json + .jsonl.
set -euo pipefail

cd "$(dirname "$0")/.."

BIN="${BIN:-./bin}"
RESULTS="${RESULTS:-simulation/results}"
TMPROOT="$(mktemp -d -t ycsb-edge.XXXXXX)"
CONFIG="${CONFIG:-$TMPROOT/edge.yaml}"
DATA_DIR="${DATA_DIR:-$TMPROOT/data}"
mkdir -p "$DATA_DIR"

GRPC_PORT="${GRPC_PORT:-17101}"
ADMIN_PORT="${ADMIN_PORT:-17102}"
RAFT_PORT="${RAFT_PORT:-17103}"

DURATION="${DURATION:-4s}"
RECORDS="${RECORDS:-2000}"
CONCURRENCY="${CONCURRENCY:-16}"
VALUE_SIZE="${VALUE_SIZE:-64}"

mkdir -p "$RESULTS"

cleanup() {
  if [[ -n "${EDGE_PID:-}" ]]; then
    kill "$EDGE_PID" >/dev/null 2>&1 || true
    wait "$EDGE_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMPROOT"
}
trap cleanup EXIT

cat > "$CONFIG" <<EOF
node: { id: "ycsb-edge", role: "edge", datacenter: "dc-ycsb" }
admin: { listen_addr: "127.0.0.1:${ADMIN_PORT}", read_timeout: "5s", write_timeout: "30s", shutdown_grace: "5s", enable_metrics: true, enable_pprof: false }
grpc:  { listen_addr: "127.0.0.1:${GRPC_PORT}", max_recv_msg_bytes: 16777216, max_send_msg_bytes: 16777216, keepalive_interval: "30s", keepalive_timeout: "10s", enable_reflection: true, shutdown_grace: "5s" }
raft:  { enabled: false, cluster_id: "ycsb", bind: "127.0.0.1:${RAFT_PORT}", peers: [], data_dir: "${DATA_DIR}/raft" }
replication: { enabled: false, cloud_addr: "127.0.0.1:19001", peers: [], buffer_dir: "${DATA_DIR}/buf" }
logging: { level: "warn", format: "text" }
EOF

echo "[ycsb] starting edge-node on 127.0.0.1:${GRPC_PORT}"
"$BIN/edge-node" -config "$CONFIG" >"$DATA_DIR/node.log" 2>&1 &
EDGE_PID=$!

# Wait for health
for _ in $(seq 1 20); do
  if curl -sf "http://127.0.0.1:${ADMIN_PORT}/healthz" >/dev/null; then break; fi
  sleep 0.25
done
curl -sf "http://127.0.0.1:${ADMIN_PORT}/healthz" >/dev/null || {
  echo "[ycsb] edge-node failed to come up"; tail "$DATA_DIR/node.log"; exit 1;
}

run_workload() {
  local w="$1"
  local out="$RESULTS/ycsb-${w}.json"
  local hist="$RESULTS/ycsb-${w}.jsonl"
  echo "[ycsb] running workload $w"
  "$BIN/ycsb" -targets "127.0.0.1:${GRPC_PORT}" -workload "$w" \
    -records "$RECORDS" -concurrency "$CONCURRENCY" -duration "$DURATION" \
    -value-size "$VALUE_SIZE" -sticky -quiet \
    -out "$out" -history-out "$hist"
  echo "[ycsb] auditing workload $w"
  "$BIN/checker" -history "$hist" || true
}

run_workload A
run_workload B

echo "[ycsb] results in $RESULTS/"
ls -1 "$RESULTS" | grep '^ycsb-' || true
