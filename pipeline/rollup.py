"""
Per-identity daily rollups. One row per identity per day, joined to the red-team labels.

This is the feature table every later stage reads.
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

DEFAULT_OFF_HOURS_BAND = list(range(0, 6))


def load_off_hours_band(path: str | None) -> list[int]:
    """Read the measured band. Never guess it, see pipeline/diurnal.py."""
    if not path or not Path(path).exists():
        print(f"  [warn] {path} missing, falling back to hours {DEFAULT_OFF_HOURS_BAND}. "
              "Run pipeline/diurnal.py to derive the real band")
        return DEFAULT_OFF_HOURS_BAND
    band = json.loads(Path(path).read_text())["off_hours"]["band"]
    if not band:
        print("  [warn] diurnal.json reports no trough, off-hours share will be 0 and "
              "must not be claimed")
    return band


def first_seen_destinations(events: DataFrame) -> DataFrame:
    """The first date each (identity, destination) edge ever appeared."""
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
    """Build the identity-day rollup.

    broadcast_redteam and dedup_distincts are the knobs optimize_bench.py flips. They change
    execution strategy only, and a test asserts all four combinations produce identical output.
    """
    events = spark.read.parquet(lake_path).filter(F.col("src_user").isNotNull())
    if max_day is not None:
        # The bound is applied inside the function all four variants call, so there is no way
        # for one variant to accidentally see different bytes than another.
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
        # The naive version below touches the event set twice, once for the daily aggregation
        # and once inside first_seen_destinations. Both only ever need the distinct
        # (identity, date, dst, src) tuples, so build that grain once and let both read it.
        # Removing the Expand behind COUNT(DISTINCT) comes free, since counting an already
        # deduplicated grain is a plain count. Spark cannot do this rewrite itself: it has no
        # way to know two separately written aggregations share a grain.
        grain = (
            base.groupBy("src_user", "event_date", "dst_computer", "src_computer")
            .agg(
                F.count("*").alias("n"),
                F.sum("succ").alias("succ"),
                F.sum("fail").alias("fail"),
                F.sum("off_hours").alias("off_hours"),
            )
            # MEMORY_AND_DISK, not MEMORY_ONLY. The grain is large, and a silent recompute on
            # eviction would put the second scan straight back in.
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

        first_seen = (
            grain.filter(F.col("dst_computer").isNotNull())
            .groupBy("src_user", "dst_computer")
            .agg(F.min("event_date").alias("first_seen_date"))
        )
    else:
        daily = base.groupBy("src_user", "event_date").agg(
            F.count("*").alias("auth_count"),
            F.sum("succ").alias("success_count"),
            F.sum("fail").alias("failure_count"),
            F.sum("off_hours").alias("off_hours_events"),
            F.countDistinct("dst_computer").alias("distinct_dst_computers"),
            F.countDistinct("src_computer").alias("distinct_src_computers"),
        )
        first_seen = first_seen_destinations(events)

    new_dst = (
        first_seen.groupBy("src_user", F.col("first_seen_date").alias("event_date"))
        .agg(F.count("*").alias("new_dst_computers"))
    )

    daily = daily.join(new_dst, ["src_user", "event_date"], "left").fillna(
        {"new_dst_computers": 0}
    )

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
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--lake", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--redteam", default="/data/lake/redteam")
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
