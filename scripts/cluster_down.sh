#!/usr/bin/env bash
# Stop the local 3-node Raft edge cluster launched via cluster_up.sh.
set -euo pipefail

cd "$(dirname "$0")/.."

for id in edge-1 edge-2 edge-3; do
  pidfile="data/${id}/node.pid"
  if [[ -f "${pidfile}" ]]; then
    pid="$(cat "${pidfile}")"
    if kill -0 "${pid}" 2>/dev/null; then
      echo "[cluster] stopping ${id} (pid ${pid})"
      kill "${pid}" || true
      # wait up to 5s for graceful exit
      for _ in $(seq 1 50); do
        if ! kill -0 "${pid}" 2>/dev/null; then break; fi
        sleep 0.1
      done
      if kill -0 "${pid}" 2>/dev/null; then
        echo "[cluster] force-killing ${id}"
        kill -9 "${pid}" || true
      fi
    fi
    rm -f "${pidfile}"
  fi
done
echo "[cluster] stopped."
