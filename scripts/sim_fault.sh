#!/usr/bin/env bash
# sim_fault.sh — sweep partition scenarios across site counts so the
# paper can plot (a) replication lag per phase and (b) post-heal
# convergence time vs. partitioned-edge fraction.
#
# Results are written as simulation/results/fault-<sites>-<fraction>.json
# and summarised on stdout via jq.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN_DIR="${ROOT}/bin"
OUT_DIR="${ROOT}/simulation/results"
mkdir -p "${OUT_DIR}"

if [ ! -x "${BIN_DIR}/simulator" ]; then
    (cd "${ROOT}" && go build -o "${BIN_DIR}/simulator" ./cmd/simulator)
fi

sites_list=(${SITES:-20 50 100})
fraction_list=(${FRACTIONS:-0.1 0.3 0.5})
duration="${DURATION:-15s}"
partition_at="${PARTITION_AT:-4s}"
partition_dur="${PARTITION_DURATION:-5s}"
wan="${WAN_LATENCY:-15ms}"
qps="${QPS:-50}"

printf "\n%-6s %-10s %-14s %-14s %-14s %-14s %-10s\n" \
    "sites" "fraction" "before-p99" "during-p99" "after-p99" "convergence" "violations"
echo "---------------------------------------------------------------------------------"

for sites in "${sites_list[@]}"; do
    for frac in "${fraction_list[@]}"; do
        out="${OUT_DIR}/fault-${sites}-${frac}.json"
        "${BIN_DIR}/simulator" \
            -sites "${sites}" \
            -duration "${duration}" \
            -partition-at "${partition_at}" \
            -partition-duration "${partition_dur}" \
            -partition-fraction "${frac}" \
            -wan-latency "${wan}" \
            -concurrency "${sites}" \
            -qps "${qps}" \
            -keys 500 \
            -write-ratio 0.4 \
            -progress 0 \
            -out "${out}" > /dev/null

        jq -r \
          '[
             (.fault.phases.phases[] | select(.phase=="before") | .replication_lag.p99),
             (.fault.phases.phases[] | select(.phase=="during") | .replication_lag.p99),
             (.fault.phases.phases[] | select(.phase=="after")  | .replication_lag.p99),
             .fault.phases.convergence_time_after_heal,
             .causal_violations
           ] | @tsv' "${out}" | \
        awk -v sites="${sites}" -v frac="${frac}" '
            {
              b = $1 / 1e6; d = $2 / 1e6; a = $3 / 1e6; c = $4 / 1e6
              printf "%-6s %-10s %-14s %-14s %-14s %-14s %-10s\n",
                sites, frac, sprintf("%.2fms", b), sprintf("%.2fms", d),
                sprintf("%.2fms", a), sprintf("%.2fms", c), $5
            }'
    done
done
