"""Stage 1c — per-identity daily rollups (the feature table every later stage reads).

One row per (src_user, event_date) with the behavioural features that identity-security
analytics actually key on:

  auth_count, success_count, failure_count, failure_ratio
  distinct_dst_computers      fan-out — the lateral-movement precursor
  distinct_src_computers      how many hosts this identity authenticated *from*
  new_dst_computers           destinations this identity had never touched on any prior
                              day — the "new access path" signal. This is the expensive
                              one: it needs the identity's entire history, not just today.
  off_hours_events / share    scored against the band bench/diurnal.py MEASURED
  is_redteam_day              did a labelled compromise event land on this identity today

`new_dst_computers` is computed with a first-seen join rather than a per-day expanding
window over a set: for each (user, dst_computer) we take the MIN event_date the pair ever
appears, then a destination is "new" on day D exactly when its first-seen date == D. One
shuffle instead of an O(days^2) self-join.

    python pipeline/rollup.py --lake /data/lake/auth --output /data/lake/rollup
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.common import read_redteam_any, spark_session

DEFAULT_OFF_HOURS_BAND = list(range(0, 6))  # only a fallback; real band comes from bench/


def load_off_hours_band(path: str | None) -> list[int]:
    """Read the measured off-hours band. Never guess it — see pipeline/diurnal.py."""
    if not path or not Path(path).exists():
        print(f"  [warn] {path} missing — falling back to hours {DEFAULT_OFF_HOURS_BAND}; "
              "run pipeline/diurnal.py to derive the real band")
        return DEFAULT_OFF_HOURS_BAND
    band = json.loads(Path(path).read_text())["off_hours"]["band"]
    if not band:
        print("  [warn] diurnal.json reports no trough — off-hours share will be 0 and "
              "must not be claimed")
    return band


def first_seen_destinations(events: DataFrame) -> DataFrame:
    """MIN(event_date) per (identity, destination) — the first time that edge ever existed."""
    return (
        events.filter(F.col("dst_computer").isNotNull())
        .groupBy("src_user", "dst_computer")
        .agg(F.min("event_date").alias("first_seen_date"))
    )


def build_rollup(
    spark: SparkSession,
    lake_path: str,
    redteam_path: str,
    off_hours_band: list[int],
    broadcast_redteam: bool = True,
    dedup_distincts: bool = True,
    max_day: int | None = None,
) -> DataFrame:
    """Per-identity daily rollup.

    `broadcast_redteam` and `dedup_distincts` are the two knobs pipeline/optimize_bench.py
    flips. They change execution strategy ONLY — the output is asserted identical across
    all four combinations (tests/test_pipeline.py::test_optimization_variants_produce_identical_results).

    `max_day` bounds the benchmark to an identical slice across every variant. The bound is
    applied here, inside the function all four variants call, rather than by pre-filtering the
    lake — so there is no way for one variant to accidentally see different bytes than another.
    """
    events = spark.read.parquet(lake_path).filter(F.col("src_user").isNotNull())
    if max_day is not None:
        events = events.filter(F.col("day_index") < max_day)

    is_off_hours = (
        F.col("hour_of_day").isin(off_hours_band) if off_hours_band else F.lit(False)
    )

    base = events.select(
        "src_user",
        "event_date",
        "dst_computer",
        "src_computer",
        "hour_of_day",
        F.col("is_success").cast("int").alias("succ"),
        F.col("is_failure").cast("int").alias("fail"),
        is_off_hours.cast("int").alias("off_hours"),
    )

    if dedup_distincts:
        # OPTIMIZED — ONE SHARED GRAIN, TWO CONSUMERS.
        #
        # The naive rollup touches the 113M-row event set TWICE: once for the daily
        # aggregation, and once more inside first_seen_destinations(). That is two full
        # scans of the lake and two wide shuffles of the same rows.
        #
        # But both consumers only ever need the DISTINCT (identity, date, dst, src) tuples:
        #   - daily      groups that grain by (identity, date)
        #   - first_seen groups the SAME grain by (identity, dst) taking min(date)
        #
        # So materialize the grain once, persist it, and let both read it. That replaces
        # {scan + Expand-shuffle} + {scan + shuffle} with {scan + one shuffle} + two cheap
        # aggregations over a set an order of magnitude smaller than the input.
        #
        # The Expand elimination comes along for free: with the grain already deduplicated,
        # COUNT(DISTINCT) over it is a plain count, so Spark no longer replays every input
        # row once per distinct expression.
        #
        # MEMORY_AND_DISK, not MEMORY_ONLY: the grain is large and a silent re-computation
        # on eviction would put the second scan straight back in.
        grain = (
            base.groupBy("src_user", "event_date", "dst_computer", "src_computer")
            .agg(
                F.count("*").alias("n"),
                F.sum("succ").alias("succ"),
                F.sum("fail").alias("fail"),
                F.sum("off_hours").alias("off_hours"),
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        daily = grain.groupBy("src_user", "event_date").agg(
            F.sum("n").alias("auth_count"),
            F.sum("succ").alias("success_count"),
            F.sum("fail").alias("failure_count"),
            F.sum("off_hours").alias("off_hours_events"),
            F.countDistinct("dst_computer").alias("distinct_dst_computers"),
            F.countDistinct("src_computer").alias("distinct_src_computers"),
        )

        # first_seen, derived from the grain rather than from a second scan of the lake.
        first_seen = (
            grain.filter(F.col("dst_computer").isNotNull())
            .groupBy("src_user", "dst_computer")
            .agg(F.min("event_date").alias("first_seen_date"))
        )
    else:
        # BASELINE: the obvious version. Two COUNT(DISTINCT) in a single groupBy (which
        # Spark plans with an Expand), and first_seen computed from an independent second
        # pass over the events.
        daily = base.groupBy("src_user", "event_date").agg(
            F.count("*").alias("auth_count"),
            F.sum("succ").alias("success_count"),
            F.sum("fail").alias("failure_count"),
            F.sum("off_hours").alias("off_hours_events"),
            F.countDistinct("dst_computer").alias("distinct_dst_computers"),
            F.countDistinct("src_computer").alias("distinct_src_computers"),
        )
        first_seen = first_seen_destinations(events)

    # New-access-path rate: destinations whose first-ever appearance for this identity is
    # this very day.
    new_dst = (
        first_seen.groupBy("src_user", F.col("first_seen_date").alias("event_date"))
        .agg(F.count("*").alias("new_dst_computers"))
    )

    daily = daily.join(new_dst, ["src_user", "event_date"], "left").fillna(
        {"new_dst_computers": 0}
    )

    # Red-team enrichment. The label table is 749 rows; broadcasting it turns a sort-merge
    # join (both sides shuffled) into a map-side hash lookup.
    redteam = read_redteam_any(spark, redteam_path)
    rt_days = (
        redteam.select(
            F.col("user").alias("src_user"),
            (F.col("time") / F.lit(86400)).cast("int").alias("day_index"),
        )
        .withColumn(
            "event_date", F.date_add(F.lit("2015-01-01").cast("date"), F.col("day_index"))
        )
        .select("src_user", "event_date")
        .distinct()
        .withColumn("is_redteam_day", F.lit(True))
    )
    if broadcast_redteam:
        rt_days = F.broadcast(rt_days)

    daily = daily.join(rt_days, ["src_user", "event_date"], "left").fillna(
        {"is_redteam_day": False}
    )

    return daily.withColumn(
        "failure_ratio",
        F.when(F.col("auth_count") > 0, F.col("failure_count") / F.col("auth_count")).otherwise(
            0.0
        ),
    ).withColumn(
        "off_hours_share",
        F.when(F.col("auth_count") > 0, F.col("off_hours_events") / F.col("auth_count")).otherwise(
            0.0
        ),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--lake", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--redteam", default="/data/raw/redteam.txt.gz")
    ap.add_argument("--diurnal", default="bench/diurnal.json")
    ap.add_argument("--stats-out", default=None)
    args = ap.parse_args()

    spark = spark_session("rollup")
    band = load_off_hours_band(args.diurnal)

    t0 = time.perf_counter()
    rollup = build_rollup(spark, args.lake, args.redteam, band)
    rollup.write.mode("overwrite").partitionBy("event_date").parquet(args.output)
    elapsed = round(time.perf_counter() - t0, 2)

    written = spark.read.parquet(args.output)
    stats = written.agg(
        F.count("*").alias("rows"),
        F.countDistinct("src_user").alias("identities"),
        F.sum(F.col("is_redteam_day").cast("int")).alias("redteam_identity_days"),
        F.max("distinct_dst_computers").alias("max_fanout"),
    ).collect()[0]

    report = {
        "rows": int(stats["rows"]),
        "identities": int(stats["identities"]),
        "redteam_identity_days": int(stats["redteam_identity_days"]),
        "max_fanout": int(stats["max_fanout"]),
        "off_hours_band": band,
        "seconds": elapsed,
        "output": args.output,
    }
    print(f"[rollup] {report['rows']:,} identity-days over {report['identities']:,} identities "
          f"({report['redteam_identity_days']} red-team identity-days) in {elapsed}s")

    if args.stats_out:
        Path(args.stats_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.stats_out).write_text(json.dumps(report, indent=2) + "\n")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
