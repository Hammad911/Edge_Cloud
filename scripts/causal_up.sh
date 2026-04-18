#!/usr/bin/env bash
# Bring up a 1-cloud + 2-edge causal-replication topology in the
# background. PIDs are written to /tmp so causal_down.sh can find them.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN_DIR="${ROOT}/bin"
LOG_DIR="${ROOT}/logs"
mkdir -p "${LOG_DIR}"

run() {
  local name=$1 bin=$2 cfg=$3
  rm -f "/tmp/${name}.pid"
  : > "${LOG_DIR}/${name}.log"
  ( cd "${ROOT}" && "${BIN_DIR}/${bin}" -config "configs/${cfg}.yaml" \
      >>"${LOG_DIR}/${name}.log" 2>&1 ) &
  echo $! > "/tmp/${name}.pid"
  echo "started ${name} pid=$(cat /tmp/${name}.pid) log=${LOG_DIR}/${name}.log"
}

run cloud-0 cloud-node cloud
sleep 0.4
run edge-a-0 edge-node edge-a
run edge-b-0 edge-node edge-b
sleep 0.6

echo
echo "topology up. try:"
echo "  ${BIN_DIR}/kvsmoke -addr 127.0.0.1:7011 put hello world      # write on edge-A"
echo "  ${BIN_DIR}/kvsmoke -addr 127.0.0.1:9001 get hello            # read on cloud"
echo "  ${BIN_DIR}/kvsmoke -addr 127.0.0.1:7021 get hello            # read on edge-B (causal)"
echo
echo "tail -f ${LOG_DIR}/{cloud-0,edge-a-0,edge-b-0}.log"
