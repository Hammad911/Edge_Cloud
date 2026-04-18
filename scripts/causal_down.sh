#!/usr/bin/env bash
# Tear down the topology started by causal_up.sh.
set -euo pipefail

stop() {
  local name=$1
  local pidfile="/tmp/${name}.pid"
  if [[ ! -f "${pidfile}" ]]; then
    echo "${name}: no pidfile"
    return
  fi
  local pid
  pid=$(cat "${pidfile}")
  if kill -0 "${pid}" 2>/dev/null; then
    kill "${pid}" 2>/dev/null || true
    for _ in {1..20}; do
      kill -0 "${pid}" 2>/dev/null || break
      sleep 0.1
    done
    if kill -0 "${pid}" 2>/dev/null; then
      echo "${name}: forcing"
      kill -9 "${pid}" 2>/dev/null || true
    fi
    echo "${name}: stopped (pid ${pid})"
  else
    echo "${name}: not running"
  fi
  rm -f "${pidfile}"
}

stop edge-a-0
stop edge-b-0
stop cloud-0
