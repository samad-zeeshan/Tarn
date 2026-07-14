"""
Measure the daily activity curve and derive the off-hours band from it.

LANL has no wall clock, so "off-hours" cannot be assumed. It has to be measured or dropped.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import functions as F

from pipeline.common import spark_session

# Below this ratio the curve is too flat to call any hour quiet, and an off-hours claim built
# on it would be fiction.
MIN_PEAK_TROUGH_RATIO = 1.5


def derive_off_hours(counts: dict[int, int], near_min_pct: float) -> dict:
    """Off-hours is the longest run of hours sitting within near_min_pct of the quietest hour."""
    total = sum(counts.values())
    if not total:
        return {"band": [], "verdict": "no events", "cutoff": 0}

    volumes = [counts.get(h, 0) for h in range(24)]
    lo, hi = min(volumes), max(volumes)
    mean = total / 24
    ratio = hi / lo if lo else float("inf")

    if ratio < MIN_PEAK_TROUGH_RATIO:
        return {
            "band": [],
            "peak_to_trough_ratio": round(ratio, 2),
            "verdict": (
                f"curve too flat (peak:trough {ratio:.2f}x < {MIN_PEAK_TROUGH_RATIO}x). "
                "Hour of day carries no usable signal here and off-hours MUST NOT be claimed"
            ),
            "cutoff": 0,
        }

    # Anchored to the measured minimum rather than a fraction of the mean. The minimum is an
    # observation. "0.6 x mean" is a knob that can be turned until the answer looks tidy.
    cutoff = lo * (1 + near_min_pct)
    quiet_set = {h for h in range(24) if counts.get(h, 0) <= cutoff}

    best_start, best_len = None, 0
    for start in range(24):
        if start not in quiet_set:
            continue
        # Only start a run where the previous hour is noisy, so we measure whole runs and not
        # the tail of one. Hours wrap, so 23 can be the start.
        if (start - 1) % 24 in quiet_set and len(quiet_set) < 24:
            continue
        length = 0
        while (start + length) % 24 in quiet_set and length < 24:
            length += 1
        if length > best_len:
            best_start, best_len = start, length

    if best_start is None:
        return {"band": [], "verdict": "no contiguous quiet run", "cutoff": round(cutoff, 1)}

    band = sorted((best_start + i) % 24 for i in range(best_len))
    off_volume = sum(counts.get(h, 0) for h in band)
    return {
        "band": band,
        "band_hours": best_len,
        "band_start_hour": best_start,
        "band_end_hour": (best_start + best_len - 1) % 24,
        "rule": (
            f"hour is off-hours when its human-account volume is within "
            f"{near_min_pct:.0%} of the quietest hour ({lo:,} events)"
        ),
        "cutoff": round(cutoff, 1),
        "quietest_hour_volume": lo,
        "busiest_hour_volume": hi,
        "peak_to_trough_ratio": round(ratio, 2),
        "mean_hourly_volume": round(mean, 1),
        "share_of_human_events_in_band": round(off_volume / total, 5),
        "verdict": "diurnal cycle present; off-hours band derived from the measured trough",
    }


def _ratio(counts: dict[int, int]) -> float | None:
    vals = [counts.get(h, 0) for h in range(24)]
    lo, hi = min(vals), max(vals)
    return round(hi / lo, 2) if lo else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--lake", required=True)
    ap.add_argument("--out", default="bench/diurnal.json")
    ap.add_argument("--near-min-pct", type=float, default=0.15)
    args = ap.parse_args()

    spark = spark_session("diurnal")
    lake = spark.read.parquet(args.lake)

    rows = (
        lake.groupBy("hour_of_day")
        .agg(
            F.count("*").alias("events"),
            F.sum(F.when(~F.col("src_is_machine"), 1).otherwise(0)).alias("human_events"),
            F.sum(F.when(F.col("src_is_machine"), 1).otherwise(0)).alias("machine_events"),
            F.sum(F.col("is_failure").cast("int")).alias("failures"),
            F.countDistinct("src_user").alias("distinct_identities"),
        )
        .orderBy("hour_of_day")
        .collect()
    )

    histogram = [
        {
            "hour": int(r["hour_of_day"]),
            "events": int(r["events"]),
            "human_events": int(r["human_events"]),
            "machine_events": int(r["machine_events"]),
            "failures": int(r["failures"]),
            "distinct_identities": int(r["distinct_identities"]),
        }
        for r in rows
    ]

    all_counts = {r["hour"]: r["events"] for r in histogram}
    human_counts = {r["hour"]: r["human_events"] for r in histogram}
    machine_counts = {r["hour"]: r["machine_events"] for r in histogram}

    # The first version of this job ran on all accounts and refused to emit a band, because
    # machine accounts authenticate around the clock and are most of the traffic, which
    # flattens the curve into uselessness. Splitting them out is what makes a band defensible.
    off_hours = derive_off_hours(human_counts, args.near_min_pct)

    human_peak = max(histogram, key=lambda r: r["human_events"])
    human_trough = min(histogram, key=lambda r: r["human_events"])

    payload = {
        "what": (
            "Hour of day activity curve. The off-hours band that fact_auth_event.is_off_hours "
            "and warehouse Q2 use is derived from this measurement, never assumed."
        ),
        "caveat": (
            "hour = (t mod 86400) // 3600, so the labels are offsets from the start of "
            "collection, not certified clock hours. The shape of the curve is a real "
            "measurement. That the human trough lands on 00-05 is evidence t=0 sits near local "
            "midnight, but that is an inference and nothing here depends on it."
        ),
        "key_finding": (
            "Machine accounts run around the clock and are the bulk of the traffic, which "
            f"flattens the aggregate curve to {_ratio(all_counts)}x and hides the cycle. Human "
            f"accounts alone show {_ratio(human_counts)}x with a clean overnight trough."
        ),
        "peak_to_trough_ratio": {
            "all_accounts": _ratio(all_counts),
            "human_accounts": _ratio(human_counts),
            "machine_accounts": _ratio(machine_counts),
        },
        "human_peak_hour": human_peak,
        "human_trough_hour": human_trough,
        "total_events": sum(all_counts.values()),
        "total_human_events": sum(human_counts.values()),
        "total_machine_events": sum(machine_counts.values()),
        "off_hours": off_hours,
        "histogram": histogram,
        "lake": args.lake,
        "measured_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(payload, indent=2) + "\n")

    r = payload["peak_to_trough_ratio"]
    print(f"[diurnal] peak:trough - all {r['all_accounts']}x | "
          f"human {r['human_accounts']}x | machine {r['machine_accounts']}x")
    print(f"[diurnal] human peak hour {human_peak['hour']} ({human_peak['human_events']:,}), "
          f"trough hour {human_trough['hour']} ({human_trough['human_events']:,})")
    print(f"[diurnal] off-hours band = {off_hours.get('band')}")
    print(f"[diurnal] {off_hours['verdict']}")
    print(f"[diurnal] wrote {args.out}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
