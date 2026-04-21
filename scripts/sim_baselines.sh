#!/usr/bin/env bash
# sim_baselines.sh — emit the metadata-vs-sites curve that the paper's
# scaling figure uses. Runs the simulator at several site counts, reads
# the metadata section of each JSON result, and tabulates per-scheme
# bytes/event plus the projected values at canonical group counts.
#
# Output: a table on stdout plus JSON per scale in simulation/results/.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN_DIR="${ROOT}/bin"
OUT_DIR="${ROOT}/simulation/results"
mkdir -p "${OUT_DIR}"

if [ ! -x "${BIN_DIR}/simulator" ]; then
    (cd "${ROOT}" && go build -o "${BIN_DIR}/simulator" ./cmd/simulator)
fi

sites_list=(${SITES:-10 25 50 100 200})
duration="${DURATION:-5s}"
qps="${QPS:-20}"
wan="${WAN_LATENCY:-25ms}"

printf "\n%-6s %-10s %-12s %-12s %-12s %-12s %-12s\n" \
    "sites" "events" "eventual" "lamport" "pHLC(meas)" "VC(meas)" "pHLC@G=8"
echo "------------------------------------------------------------------------------------"

for sites in "${sites_list[@]}"; do
    out="${OUT_DIR}/baselines-${sites}.json"
    "${BIN_DIR}/simulator" \
        -sites "${sites}" \
        -duration "${duration}" \
        -concurrency "${sites}" \
        -qps "${qps}" \
        -keys 500 \
        -write-ratio 0.4 \
        -wan-latency "${wan}" \
        -progress 0 \
        -out "${out}" > /dev/null

    # Extract measured metadata means + compute the "projected G=8" cell.
    # The projection is pHLC = min(8, G) * (4 + 12) = 128 B because
    # G=8 is the realistic clustered case. We compute that here for
    # display convenience.
    jq -r --argjson target_g 8 \
       '. as $r | [
          $r.num_sites,
          $r.metadata.events,
          $r.metadata.schemes.eventual.metadata_bytes_mean,
          $r.metadata.schemes.lamport.metadata_bytes_mean,
          $r.metadata.schemes.partitioned_hlc.metadata_bytes_mean,
          $r.metadata.schemes.vector_clock.metadata_bytes_mean
        ] | @tsv' "${out}" | \
    awk -v g=8 '
        {
          n = $1; events = $2
          e = $3; l = $4; ph = $5; vc = $6
          phproj = g * 16
          printf "%-6d %-10d %-12.1f %-12.1f %-12.1f %-12.1f %-12.1f\n",
                 n, events, e, l, ph, vc, phproj
        }'
done

echo
echo "Legend:"
echo "  pHLC(meas) — measured partitioned-HLC, 1 group/site (worst case)"
echo "  VC(meas)   — measured vector clock, one slot per site"
echo "  pHLC@G=8   — projected pHLC if the N sites were clustered into 8 groups"
