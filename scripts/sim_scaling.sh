#!/usr/bin/env bash
#
# Run the in-process simulator across a sweep of site counts and write
# JSON results to simulation/results/. Useful for generating the scaling
# curves promised in the project proposal (10 -> 500 sites).
#
# Usage:
#   ./scripts/sim_scaling.sh                 # default sweep
#   SITES="10 100" DUR=5s ./scripts/sim_scaling.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${ROOT}/bin/simulator"
OUT_DIR="${ROOT}/simulation/results"
mkdir -p "${OUT_DIR}"

if [[ ! -x "${BIN}" ]]; then
  echo "building simulator..."
  (cd "${ROOT}" && go build -o "${BIN}" ./cmd/simulator)
fi

SITES="${SITES:-10 50 100 500}"
DUR="${DUR:-8s}"
QPS="${QPS:-10}"
WAN="${WAN:-25ms}"
KEYS="${KEYS:-2000}"
WRITE="${WRITE:-0.3}"
DELETE="${DELETE:-0.05}"

stamp="$(date +%Y%m%d-%H%M%S)"
summary="${OUT_DIR}/scaling-${stamp}.txt"
echo "writing summary to ${summary}"

{
  printf "%-6s  %-10s  %-12s  %-12s  %-12s  %s\n" \
    "sites" "ops/sec" "lag-p50" "lag-p95" "lag-p99" "violations"
  printf "%s\n" "------------------------------------------------------------------------"
} | tee "${summary}"

for n in ${SITES}; do
  out_file="${OUT_DIR}/sim-${n}sites-${stamp}.json"
  conc=$(( n * 2 ))
  (( conc < 16 )) && conc=16
  echo ">> sites=${n} duration=${DUR} qps=${QPS} concurrency=${conc}"
  "${BIN}" \
    -sites "${n}" \
    -duration "${DUR}" \
    -concurrency "${conc}" \
    -qps "${QPS}" \
    -keys "${KEYS}" \
    -write-ratio "${WRITE}" \
    -delete-ratio "${DELETE}" \
    -wan-latency "${WAN}" \
    -progress 0 \
    -out "${out_file}" \
    >/dev/null

  read -r sites_n ops_ps lag_p50 lag_p95 lag_p99 viols < <(
    jq -r '"\(.num_sites) \(.ops_per_sec) \(.replication_lag.p50) \(.replication_lag.p95) \(.replication_lag.p99) \(.causal_violations)"' \
      "${out_file}"
  )
  ms() { awk -v n="$1" 'BEGIN{printf "%.1fms", n/1e6}'; }
  printf "%-6s  %-10.0f  %-12s  %-12s  %-12s  %s\n" \
    "${sites_n}" "${ops_ps}" "$(ms ${lag_p50})" "$(ms ${lag_p95})" "$(ms ${lag_p99})" "${viols}" \
    | tee -a "${summary}"
done

echo
echo "done. JSON files in ${OUT_DIR}"
