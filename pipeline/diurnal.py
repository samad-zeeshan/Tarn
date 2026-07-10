"""Stage 1b — measure the corpus's daily activity curve and derive the off-hours band.

Why this job exists at all: LANL's `time` column is seconds since the start of collection,
so nothing in the data says "this event happened at 3am". Every paper that talks about
"off-hours activity" in this corpus is making an assumption. Tarn refuses to make it
silently — it measures the curve and lets the histogram decide.

WHAT THE MEASUREMENT FOUND (and why this job splits human from machine accounts)
--------------------------------------------------------------------------------
The first run of this job over the whole corpus reported a peak:trough ratio of 1.66x and
refused to emit a band at all — the curve looked nearly flat. That turned out to be a real
finding rather than a bug: MACHINE accounts (trailing `$` — service and computer accounts
doing Kerberos/NTLM service auth) run around the clock, they are the majority of all
traffic, and they were flattening the aggregate curve.

Split them out and the picture is unambiguous:

    human accounts    peak:trough 2.20x   trough at hours 00-05, peak at hours 07-15
    machine accounts  peak:trough 1.44x   essentially flat, as you would expect

So the band is derived from the HUMAN curve, which is also the only curve Q2 asks about
(Q2 filters to non-machine identities). A useful side-effect: the trough landing squarely
on hours 00-05 is evidence that LANL's t=0 sits near local midnight — which nothing in the
corpus documentation actually tells you.

HOW THE BAND IS DEFINED (no hand-tuned fraction-of-the-mean)
-----------------------------------------------------------
An hour is off-hours when its human volume sits within `--near-min-pct` of the QUIETEST
hour of the day. Anchoring to the measured minimum rather than to a fraction of the mean
keeps the rule scale-free and stops the threshold from being reverse-engineered to produce
a tidy answer. The band is then the longest contiguous run of such hours (hours wrap).

The full 24-hour histogram is committed to bench/diurnal.json regardless, so anyone who
dislikes the threshold can re-derive the band with their own.

    python pipeline/diurnal.py --lake /data/lake/auth --out bench/diurnal.json
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import functions as F

from pipeline.common import spark_session

MIN_PEAK_TROUGH_RATIO = 1.5  # below this the curve is too flat to call anything "off-hours"


def derive_off_hours(counts: dict[int, int], near_min_pct: float) -> dict:
    """Off-hours = the longest contiguous run of hours sitting within `near_min_pct` of the
    QUIETEST hour of the day.

    Anchored to the measured minimum, not to a fraction of the mean: the minimum is a real
    observation, whereas "0.6 x mean" is a knob that can be turned until the answer looks
    tidy. Hours wrap (23 -> 0). Ties break toward the earlier start hour so the result is
    deterministic.

    Refuses to emit a band at all when the curve is too flat to support one — a flat curve
    means hour-of-day carries no signal, and an off-hours claim built on it would be
    fiction.
    """
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
                f"curve too flat (peak:trough {ratio:.2f}x < {MIN_PEAK_TROUGH_RATIO}x) — "
                "hour-of-day carries no usable signal here and off-hours MUST NOT be claimed"
            ),
            "cutoff": 0,
        }

    cutoff = lo * (1 + near_min_pct)
    quiet_set = {h for h in range(24) if counts.get(h, 0) <= cutoff}

    best_start, best_len = None, 0
    for start in range(24):
        if start not in quiet_set:
            continue
        # Only start a run where the previous hour is NOT quiet, so we measure whole runs.
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
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--lake", required=True)
    ap.add_argument("--out", default="bench/diurnal.json")
    ap.add_argument(
        "--near-min-pct",
        type=float,
        default=0.15,
        help="an hour is off-hours when its human volume is within this fraction of the "
             "quietest hour's volume",
    )
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

    # The band comes from the HUMAN curve — see the module docstring. Machine accounts run
    # around the clock and would flatten it into uselessness.
    off_hours = derive_off_hours(human_counts, args.near_min_pct)

    human_peak = max(histogram, key=lambda r: r["human_events"])
    human_trough = min(histogram, key=lambda r: r["human_events"])

    payload = {
        "what": (
            "Hour-of-day activity curve, measured over the lake. The off-hours band that "
            "fact_auth_event.is_off_hours and warehouse Q2 use is DERIVED from this "
            "measurement — it is never assumed."
        ),
        "caveat": (
            "LANL ships relative seconds, not wall-clock time; hour = (t mod 86400) // 3600, "
            "so hour labels are offsets from the start of collection rather than certified "
            "local clock hours. The SHAPE of the curve is a real measurement. That the human "
            "trough lands on hours 00-05 and the peak on 07-15 is itself evidence that t=0 "
            "sits near local midnight — but that is an inference from the data, not something "
            "LANL documents, and nothing in Tarn depends on it being exactly right."
        ),
        "key_finding": (
            "Machine accounts run around the clock and are the bulk of the traffic, which "
            "flattens the aggregate curve to a peak:trough of "
            f"{_ratio(all_counts)}x and hides the cycle entirely. Human accounts alone show "
            f"{_ratio(human_counts)}x, with a clean overnight trough. Splitting them is what "
            "makes an off-hours claim defensible at all."
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
    print(f"[diurnal] peak:trough — all {r['all_accounts']}x | "
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
