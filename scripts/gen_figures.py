#!/usr/bin/env python3
"""
gen_figures.py -- produce the report figures from simulator/YCSB JSON.

This script walks ``simulation/results/`` and renders the figures used in the
project write-up under ``docs/figures/``. Each figure's input sources are
self-describing JSON files produced by the simulator binary (``cmd/simulator``)
and the YCSB driver (``cmd/ycsb``). Missing inputs are skipped gracefully so
the script can run in any state (fresh checkout, partial results, or full
sweep).

Outputs:
    docs/figures/scaling.png          -- throughput + p99 local latency + lag
    docs/figures/metadata_measured.png-- bytes/event across schemes (measured)
    docs/figures/metadata_projected.png -- projected bytes/event vs N, per G
    docs/figures/fault_phases.png     -- latency/lag per phase for a partition
    docs/figures/fault_convergence.png-- convergence time vs partition size
    docs/figures/ycsb_latency.png     -- YCSB A/B percentile bars (if present)

The script is intentionally self-contained: just matplotlib + numpy from the
standard scientific stack.
"""

from __future__ import annotations

import argparse
import glob
import json
import math
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable

import matplotlib
matplotlib.use("Agg")  # headless; must come before pyplot import.
import matplotlib.pyplot as plt
import numpy as np

# ---------- styling ---------------------------------------------------------

plt.rcParams.update({
    "figure.dpi": 140,
    "savefig.dpi": 200,
    "savefig.bbox": "tight",
    "font.size": 10,
    "axes.titlesize": 11,
    "axes.labelsize": 10,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "legend.frameon": False,
    "legend.fontsize": 9,
    "grid.alpha": 0.25,
    "grid.linestyle": "--",
})

SCHEME_COLOUR = {
    "eventual":        "#6c757d",
    "lamport":         "#1b9e77",
    "partitioned_hlc": "#d95f02",
    "vector_clock":    "#7570b3",
}
SCHEME_LABEL = {
    "eventual":        "Eventual (no metadata)",
    "lamport":         "Lamport (8 B)",
    "partitioned_hlc": "Partitioned HLC",
    "vector_clock":    "Vector clock (N × 8 B)",
}

PHASE_ORDER = ["before", "during", "after"]
PHASE_COLOUR = {"before": "#4c78a8", "during": "#f58518", "after": "#54a24b"}


# ---------- helpers ---------------------------------------------------------

def ns_to_ms(x: float) -> float:
    return x / 1e6


def ns_to_us(x: float) -> float:
    return x / 1e3


def safe_load(path: str) -> dict | None:
    try:
        with open(path) as fh:
            return json.load(fh)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as exc:
        print(f"[warn] {path}: {exc}", file=sys.stderr)
        return None


def pick_latest_by(sites: Iterable[str], rx: re.Pattern[str]) -> dict[int, str]:
    """Given a list of paths, return the newest file per captured site count."""
    buckets: dict[int, list[tuple[str, str]]] = defaultdict(list)
    for p in sites:
        m = rx.match(os.path.basename(p))
        if not m:
            continue
        n = int(m.group(1))
        buckets[n].append((os.path.basename(p), p))
    out: dict[int, str] = {}
    for n, entries in buckets.items():
        entries.sort()  # lexicographic on filename sorts by timestamp suffix.
        out[n] = entries[-1][1]
    return out


# ---------- figures ---------------------------------------------------------

def figure_scaling(results_dir: str, out_dir: str) -> str | None:
    rx = re.compile(r"^sim-(\d+)sites-\d+-\d+\.json$")
    files = pick_latest_by(glob.glob(os.path.join(results_dir, "sim-*sites-*.json")), rx)
    if not files:
        print("[skip] scaling: no sim-*sites-*.json inputs")
        return None

    sites_sorted = sorted(files)
    ns = np.array(sites_sorted)
    ops = []
    lat_p99 = []
    lag_p50 = []
    lag_p99 = []
    lag_mean = []
    for n in sites_sorted:
        d = safe_load(files[n])
        if not d:
            continue
        ops.append(d["ops_per_sec"])
        lat_p99.append(ns_to_us(d["local_latency"]["p99"]))
        lag_p50.append(ns_to_ms(d["replication_lag"]["p50"]))
        lag_p99.append(ns_to_ms(d["replication_lag"]["p99"]))
        lag_mean.append(ns_to_ms(d["replication_lag"]["mean"]))

    fig, axes = plt.subplots(1, 3, figsize=(13, 3.8))

    ax = axes[0]
    ax.plot(ns, ops, marker="o", color="#1f77b4", linewidth=2)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("edge sites (N)")
    ax.set_ylabel("throughput (ops/sec)")
    ax.set_title("Aggregate throughput")
    ax.grid(True, which="both")

    ax = axes[1]
    ax.plot(ns, lat_p99, marker="s", color="#d62728", linewidth=2)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("edge sites (N)")
    ax.set_ylabel(r"local op p99 latency ($\mu$s)")
    ax.set_title("Local operation tail latency")
    ax.grid(True, which="both")

    ax = axes[2]
    ax.plot(ns, lag_mean, marker="o", color="#2ca02c", linewidth=2, label="mean")
    ax.plot(ns, lag_p50,  marker="s", color="#ff7f0e", linewidth=2, label="p50")
    ax.plot(ns, lag_p99,  marker="^", color="#9467bd", linewidth=2, label="p99")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("edge sites (N)")
    ax.set_ylabel("cross-site replication lag (ms)")
    ax.set_title("Replication lag")
    ax.legend(loc="upper left")
    ax.grid(True, which="both")

    fig.suptitle("Scalability of the partitioned-HLC causal pipeline", y=1.02)
    path = os.path.join(out_dir, "scaling.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"[ok ] {path}  (N = {sites_sorted})")
    return path


def figure_metadata_measured(results_dir: str, out_dir: str) -> str | None:
    rx = re.compile(r"^baselines-(\d+)\.json$")
    files = pick_latest_by(glob.glob(os.path.join(results_dir, "baselines-*.json")), rx)
    if not files:
        print("[skip] metadata_measured: no baselines-*.json inputs")
        return None

    ns = sorted(files)
    # Bar chart per scheme, grouped by N. We put schemes on the x-axis
    # grouped by N because the dynamic range across schemes dominates.
    schemes = ["eventual", "lamport", "partitioned_hlc", "vector_clock"]
    per_scheme: dict[str, list[float]] = {s: [] for s in schemes}
    events_per_n: list[int] = []
    for n in ns:
        d = safe_load(files[n])
        if not d or "metadata" not in d:
            continue
        meta = d["metadata"]
        events_per_n.append(meta.get("events", 0))
        for s in schemes:
            v = meta["schemes"].get(s, {}).get("metadata_bytes_mean", 0.0) or 0.0
            # Matplotlib can't plot 0 on a log y-axis. Substitute a floor of
            # 0.5 B so Eventual stays visible but flagged as <1 B.
            per_scheme[s].append(max(v, 0.5))

    x = np.arange(len(ns))
    width = 0.2

    fig, ax = plt.subplots(figsize=(9, 4.2))
    for i, s in enumerate(schemes):
        offset = (i - (len(schemes) - 1) / 2) * width
        ax.bar(x + offset, per_scheme[s], width=width,
               color=SCHEME_COLOUR[s], label=SCHEME_LABEL[s])

    ax.set_xticks(x, [f"N={n}" for n in ns])
    ax.set_yscale("log")
    ax.set_ylabel("metadata per event (bytes, log scale)")
    ax.set_title("Per-event metadata cost — measured on simulator runs")
    ax.grid(True, which="both", axis="y")
    ax.legend(loc="upper left", ncol=2)

    # Annotate bars with their values.
    for i, s in enumerate(schemes):
        offset = (i - (len(schemes) - 1) / 2) * width
        for xi, yi in zip(x, per_scheme[s]):
            if yi < 1.0:
                txt = "<1"
            elif yi >= 1000:
                txt = f"{yi/1000:.1f}k"
            else:
                txt = f"{yi:.0f}"
            ax.text(xi + offset, yi * 1.08, txt, ha="center",
                    va="bottom", fontsize=7.5)

    fig.text(0.01, -0.08,
             "Eventual carries no per-event metadata; its bar is pinned to "
             "0.5 B so it stays visible on a log axis.\nThe simulator runs one "
             "HLC group per site, so Partitioned HLC's measured cost here "
             "is an upper bound (G = N). See metadata_projected.png for the\n"
             "deployment case where K sites share a Raft group (G << N).",
             fontsize=8, color="#555")
    path = os.path.join(out_dir, "metadata_measured.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"[ok ] {path}  (N = {ns})")
    return path


def figure_metadata_projected(out_dir: str) -> str:
    """Analytical projection of metadata cost as the cluster grows.

    Even without a fresh simulator sweep we can *project* the per-event
    metadata of each scheme as a function of the site count N, using the
    same closed-form formulae the baselines package uses internally.
    Partitioned HLC is shown for several group counts G to illustrate
    the O(log N) trade-off knob.
    """
    Ns = np.logspace(np.log10(8), np.log10(1024), 60).astype(int)
    Ns = np.unique(Ns)

    # Headline sizes (see simulation/baselines/baselines.go for rationale):
    #   Eventual        :  0 B
    #   Lamport         :  8 B  (single uint64)
    #   Vector clock    :  N * 8 B (+ framing negligible)
    #   Partitioned HLC :  G * (4 id + 8 ts) B + 8 B header  -> 12G + 8
    eventual = np.zeros_like(Ns, dtype=float) + 0.5  # log floor
    lamport = np.full_like(Ns, 8, dtype=float)
    vc = Ns.astype(float) * 8.0

    Gs = [4, 8, 16, 32]
    phlc = {g: np.full_like(Ns, 12.0 * g + 8.0, dtype=float) for g in Gs}

    fig, ax = plt.subplots(figsize=(8.5, 4.2))
    ax.plot(Ns, eventual, color=SCHEME_COLOUR["eventual"],
            linestyle=":", linewidth=2, label=SCHEME_LABEL["eventual"])
    ax.plot(Ns, lamport, color=SCHEME_COLOUR["lamport"],
            linewidth=2, label=SCHEME_LABEL["lamport"])
    ax.plot(Ns, vc, color=SCHEME_COLOUR["vector_clock"],
            linewidth=2.2, label=SCHEME_LABEL["vector_clock"])

    cmap = plt.get_cmap("Oranges")
    for i, g in enumerate(Gs):
        shade = cmap(0.35 + 0.18 * i)
        ax.plot(Ns, phlc[g], color=shade, linewidth=2,
                label=f"Partitioned HLC (G={g})")

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("edge sites (N)")
    ax.set_ylabel("metadata per event (bytes, log scale)")
    ax.set_title("Projected per-event metadata cost vs cluster size")
    ax.grid(True, which="both")
    ax.legend(loc="center left", bbox_to_anchor=(1.02, 0.5))
    fig.subplots_adjust(right=0.70)

    path = os.path.join(out_dir, "metadata_projected.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"[ok ] {path}")
    return path


@dataclass
class FaultPoint:
    sites: int
    fraction: float
    convergence_s: float
    phases: dict[str, dict[str, Any]]
    partition_duration_s: float


def _load_faults(results_dir: str) -> list[FaultPoint]:
    rx = re.compile(r"^fault-(\d+)-([\d.]+)\.json$")
    out: list[FaultPoint] = []
    for path in sorted(glob.glob(os.path.join(results_dir, "fault-*.json"))):
        m = rx.match(os.path.basename(path))
        if not m:
            continue
        d = safe_load(path)
        if not d or "fault" not in d:
            continue
        ph_list = d["fault"].get("phases", {}).get("phases", [])
        phases = {p["phase"]: p for p in ph_list}
        if not phases:
            continue
        conv_ns = d["fault"].get("phases", {}).get("convergence_time_after_heal", 0)
        out.append(FaultPoint(
            sites=int(m.group(1)),
            fraction=float(m.group(2)),
            convergence_s=conv_ns / 1e9,
            phases=phases,
            partition_duration_s=d["fault"].get("partition_duration", 0) / 1e9,
        ))
    return out


def figure_fault_phases(results_dir: str, out_dir: str) -> str | None:
    faults = _load_faults(results_dir)
    if not faults:
        print("[skip] fault_phases: no fault-*.json inputs")
        return None

    # Pick the largest site count we have at fraction 0.3 for the phase
    # panel; if none, fall back to any available fault point.
    target = None
    for f in faults:
        if f.fraction == 0.3 and (target is None or f.sites > target.sites):
            target = f
    if target is None:
        target = max(faults, key=lambda f: (f.sites, f.fraction))

    phases = [p for p in PHASE_ORDER if p in target.phases]
    lat_p99 = [ns_to_us(target.phases[p]["local_latency"]["p99"]) for p in phases]
    lag_mean = [ns_to_ms(target.phases[p]["replication_lag"]["mean"]) for p in phases]
    ops = [target.phases[p]["ops_succeeded"] for p in phases]

    fig, axes = plt.subplots(1, 3, figsize=(12, 3.8))
    colours = [PHASE_COLOUR[p] for p in phases]

    ax = axes[0]
    ax.bar(phases, ops, color=colours)
    ax.set_ylabel("operations completed")
    ax.set_title(f"Throughput by phase (N={target.sites}, {int(target.fraction*100)}% edges partitioned)")
    for i, v in enumerate(ops):
        ax.text(i, v, f"{v:,}", ha="center", va="bottom", fontsize=8)
    ax.grid(True, axis="y")

    ax = axes[1]
    ax.bar(phases, lat_p99, color=colours)
    ax.set_ylabel(r"local op p99 ($\mu$s, log)")
    ax.set_yscale("log")
    ax.set_title("Local tail latency")
    for i, v in enumerate(lat_p99):
        ax.text(i, v * 1.05, f"{v:.0f}", ha="center", va="bottom", fontsize=8)
    ax.grid(True, which="both", axis="y")

    ax = axes[2]
    ax.bar(phases, lag_mean, color=colours)
    ax.set_ylabel("mean replication lag (ms)")
    ax.set_title("Cross-site replication lag")
    for i, v in enumerate(lag_mean):
        ax.text(i, v, f"{v:.1f}", ha="center", va="bottom", fontsize=8)
    ax.grid(True, axis="y")

    fig.suptitle(
        "Partition tolerance: metrics across before / during / after a WAN partition",
        y=1.02,
    )
    path = os.path.join(out_dir, "fault_phases.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"[ok ] {path}  (N={target.sites}, f={target.fraction})")
    return path


def figure_fault_convergence(results_dir: str, out_dir: str) -> str | None:
    faults = _load_faults(results_dir)
    if not faults:
        print("[skip] fault_convergence: no fault-*.json inputs")
        return None

    # Grouped bars: sites on x, fraction as hue.
    sites = sorted({f.sites for f in faults})
    fractions = sorted({f.fraction for f in faults})
    lookup = {(f.sites, f.fraction): f for f in faults}

    x = np.arange(len(sites))
    width = 0.8 / max(len(fractions), 1)

    fig, ax = plt.subplots(figsize=(9, 4.0))
    cmap = plt.get_cmap("viridis")
    for i, frac in enumerate(fractions):
        offset = (i - (len(fractions) - 1) / 2) * width
        ys = []
        for s in sites:
            fp = lookup.get((s, frac))
            ys.append(fp.convergence_s if fp else 0.0)
        bars = ax.bar(x + offset, ys, width=width,
                      color=cmap(0.15 + 0.65 * i / max(len(fractions) - 1, 1)),
                      label=f"{int(frac*100)}% edges partitioned")
        for xi, yi in zip(x + offset, ys):
            if yi <= 0:
                continue
            ax.text(xi, yi, f"{yi:.1f}s", ha="center", va="bottom", fontsize=7.5)

    ax.set_xticks(x, [f"N={s}" for s in sites])
    ax.set_ylabel("convergence time after heal (s)")
    ax.set_title("Time to rejoin: partition heal to full causal convergence")
    ax.legend(loc="center left", bbox_to_anchor=(1.01, 0.5))
    ax.grid(True, axis="y")
    # leave room for the legend to sit outside the axes
    fig.subplots_adjust(right=0.78)

    path = os.path.join(out_dir, "fault_convergence.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"[ok ] {path}  (sites={sites}, fractions={fractions})")
    return path


def figure_ycsb(results_dir: str, out_dir: str) -> str | None:
    files: list[tuple[str, str]] = []
    for wl in ("A", "B", "C", "D", "F"):
        for candidate in (
            f"ycsb-{wl.lower()}.json",
            f"ycsb_{wl.lower()}.json",
            f"ycsb-workload-{wl}.json",
        ):
            p = os.path.join(results_dir, candidate)
            if os.path.exists(p):
                files.append((wl, p))
                break
    if not files:
        print("[skip] ycsb: no ycsb-*.json inputs")
        return None

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.0))
    width = 0.3
    op_colours = {"read": "#1f77b4", "update": "#ff7f0e",
                  "insert": "#2ca02c", "rmw": "#d62728"}

    op_latencies: dict[str, list[tuple[str, float, float]]] = defaultdict(list)
    throughput: list[tuple[str, float]] = []

    for wl, path in files:
        d = safe_load(path)
        if not d:
            continue
        run = d.get("run") or d
        ops = run.get("ops") or run.get("operations") or {}
        throughput.append((wl, run.get("throughput_ops_sec")
                           or run.get("ops_per_sec") or 0.0))
        for op_name, st in ops.items():
            lat = st.get("latency") or {}
            p50 = lat.get("p50") or 0
            p99 = lat.get("p99") or 0
            op_latencies[op_name].append((wl, p50, p99))

    ax = axes[0]
    wls = [w for w, _ in throughput]
    ys = [v for _, v in throughput]
    ax.bar(wls, ys, color="#4c78a8")
    ax.set_ylabel("throughput (ops/sec)")
    ax.set_title("YCSB throughput")
    for xi, yi in zip(wls, ys):
        ax.text(xi, yi, f"{yi:,.0f}", ha="center", va="bottom", fontsize=8)
    ax.grid(True, axis="y")

    ax = axes[1]
    op_names = sorted(op_latencies)
    wls_union = sorted({wl for vals in op_latencies.values() for wl, _, _ in vals})
    x = np.arange(len(wls_union))
    for i, op in enumerate(op_names):
        offset = (i - (len(op_names) - 1) / 2) * width
        ys_p50 = []
        ys_p99 = []
        for wl in wls_union:
            entry = next(((p50, p99) for (w, p50, p99) in op_latencies[op]
                          if w == wl), (0, 0))
            ys_p50.append(ns_to_us(entry[0]))
            ys_p99.append(ns_to_us(entry[1]))
        ax.bar(x + offset, ys_p99, width=width,
               color=op_colours.get(op, "#888"),
               label=f"{op} p99")
        ax.bar(x + offset, ys_p50, width=width,
               color=op_colours.get(op, "#888"),
               alpha=0.45, label=f"{op} p50")
    ax.set_xticks(x, [f"Workload {w}" for w in wls_union])
    ax.set_ylabel(r"latency ($\mu$s)")
    ax.set_title("YCSB latency (p50 shaded, p99 solid)")
    ax.set_yscale("log")
    ax.grid(True, which="both", axis="y")
    ax.legend(loc="upper left", ncol=2)

    path = os.path.join(out_dir, "ycsb_latency.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"[ok ] {path}  (workloads = {[w for w,_ in files]})")
    return path


# ---------- entry point -----------------------------------------------------

def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results", default="simulation/results",
                    help="directory with simulator/ycsb JSON (default: simulation/results)")
    ap.add_argument("--out", default="docs/figures",
                    help="directory to write PNG figures (default: docs/figures)")
    args = ap.parse_args(argv)

    if not os.path.isdir(args.results):
        print(f"error: results dir {args.results!r} does not exist",
              file=sys.stderr)
        return 2
    os.makedirs(args.out, exist_ok=True)

    produced = []
    for fn in (
        figure_scaling,
        figure_metadata_measured,
        figure_fault_phases,
        figure_fault_convergence,
        figure_ycsb,
    ):
        p = fn(args.results, args.out)
        if p:
            produced.append(p)
    p = figure_metadata_projected(args.out)
    produced.append(p)

    print(f"\nwrote {len(produced)} figure(s) to {args.out}/")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
