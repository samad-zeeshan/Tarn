"""Stage 1a — raw LANL auth events -> date-partitioned Parquet lake + logon sessions.

Two outputs:

  <output>/            the lake: every parsed auth event, partitioned by event_date.
                       Written once, read by every stage after it.
  <output>-sessions/   logon sessions: consecutive auth events from the same identity on
                       the same source computer, collapsed into a session whenever the
                       idle gap exceeds --idle-gap (default 30 min).

Sessionization is the window-function workload: LAG over (src_user, src_computer) ordered
by time, a boolean "this row starts a new session", then a running sum of that boolean to
number the sessions. It is a genuine wide shuffle over the whole corpus, which is the
point — this is the distributed-processing stage.

    python pipeline/sessionize.py --input /data/raw/auth.txt.gz --output /data/lake/auth
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

from pipeline.common import (
    ANCHOR_EPOCH,
    derive_columns,
    read_auth_raw,
    read_redteam,
    spark_session,
)

IDLE_GAP_DEFAULT = 1800  # 30 minutes


def build_redteam(spark, redteam_path: str, output_path: str) -> dict:
    """Land the labelled compromise events in the lake as Parquet.

    Raw redteam.txt.gz is headerless; the committed CI slice has a header. Normalising
    both into one Parquet table here means the warehouse and graph stages never have to
    know which one they are looking at.
    """
    redteam = read_redteam(spark, redteam_path)
    labelled = (
        redteam.withColumn("day_index", (F.col("time") / F.lit(86_400)).cast("int"))
        .withColumn(
            "event_date", F.date_add(F.lit(ANCHOR_EPOCH).cast("date"), F.col("day_index"))
        )
        .withColumn("hour_of_day", ((F.col("time") % F.lit(86_400)) / 3600).cast("int"))
    )
    labelled.write.mode("overwrite").parquet(output_path)

    written = spark.read.parquet(output_path)
    stats = written.agg(
        F.count("*").alias("rows"),
        F.countDistinct("user").alias("users"),
        F.countDistinct("src_computer").alias("src_computers"),
        F.countDistinct("dst_computer").alias("dst_computers"),
        F.min("time").alias("t_min"),
        F.max("time").alias("t_max"),
    ).collect()[0]
    return {
        "rows": int(stats["rows"]),
        "compromised_identities": int(stats["users"]),
        "pivot_source_computers": int(stats["src_computers"]),
        "target_computers": int(stats["dst_computers"]),
        "time_min_seconds": int(stats["t_min"]),
        "time_max_seconds": int(stats["t_max"]),
        "path": output_path,
    }


def build_lake(spark, input_path: str, output_path: str, coalesce: int) -> dict:
    """Parse raw events and write the date-partitioned Parquet lake."""
    raw = read_auth_raw(spark, input_path)
    events = derive_columns(raw)

    writer = events.repartition(coalesce, "event_date") if coalesce else events
    (
        writer.write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(output_path)
    )

    # Count from the written lake, not the in-memory DataFrame: this is the number that
    # ends up in bench/ and on the site, so it must describe bytes that exist on disk.
    written = spark.read.parquet(output_path)
    return {
        "rows": written.count(),
        "partitions": written.select("event_date").distinct().count(),
    }


def build_sessions(spark, lake_path: str, output_path: str, idle_gap: int) -> dict:
    """Collapse consecutive auth events into logon sessions per (identity, source host)."""
    events = spark.read.parquet(lake_path).filter(F.col("src_user").isNotNull())

    by_identity_host = Window.partitionBy("src_user", "src_computer").orderBy("time")

    with_gap = (
        events.select(
            "time", "src_user", "src_computer", "dst_computer", "event_date", "is_success"
        )
        .withColumn("prev_time", F.lag("time").over(by_identity_host))
        .withColumn(
            "is_session_start",
            F.when(
                F.col("prev_time").isNull()
                | ((F.col("time") - F.col("prev_time")) > F.lit(idle_gap)),
                1,
            ).otherwise(0),
        )
        # Running sum of the session-start flag numbers each identity's sessions 1..N.
        .withColumn(
            "session_seq",
            F.sum("is_session_start").over(
                by_identity_host.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
    )

    sessions = (
        with_gap.groupBy("src_user", "src_computer", "session_seq")
        .agg(
            F.min("time").alias("session_start"),
            F.max("time").alias("session_end"),
            F.count("*").alias("event_count"),
            F.countDistinct("dst_computer").alias("distinct_destinations"),
            F.sum(F.col("is_success").cast("int")).alias("success_count"),
            F.min("event_date").alias("event_date"),
        )
        .withColumn("duration_seconds", F.col("session_end") - F.col("session_start"))
        .withColumn(
            "session_id",
            F.sha2(
                F.concat_ws(
                    "|", F.col("src_user"), F.col("src_computer"), F.col("session_seq")
                ),
                256,
            ).substr(1, 16),
        )
        .drop("session_seq")
    )

    sessions.write.mode("overwrite").partitionBy("event_date").parquet(output_path)

    written = spark.read.parquet(output_path)
    stats = written.agg(
        F.count("*").alias("sessions"),
        F.avg("duration_seconds").alias("mean_duration_s"),
        F.expr("percentile_approx(duration_seconds, 0.5)").alias("median_duration_s"),
        F.avg("event_count").alias("mean_events_per_session"),
    ).collect()[0]

    return {
        "sessions": int(stats["sessions"]),
        "mean_duration_seconds": round(float(stats["mean_duration_s"] or 0), 1),
        "median_duration_seconds": float(stats["median_duration_s"] or 0),
        "mean_events_per_session": round(float(stats["mean_events_per_session"] or 0), 2),
        "idle_gap_seconds": idle_gap,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--input", required=True, help="auth.txt.gz or the CI sample csv.gz")
    ap.add_argument("--output", required=True, help="lake path (Parquet, partitioned)")
    ap.add_argument("--idle-gap", type=int, default=IDLE_GAP_DEFAULT)
    ap.add_argument(
        "--coalesce",
        type=int,
        default=0,
        help="repartition by event_date before write; 0 = let Spark decide",
    )
    ap.add_argument("--skip-sessions", action="store_true")
    ap.add_argument(
        "--redteam",
        default=None,
        help="redteam.txt.gz (or the CI slice); landed in the lake as Parquet alongside auth",
    )
    ap.add_argument("--stats-out", default=None, help="write timing/shape JSON here")
    args = ap.parse_args()

    spark = spark_session("sessionize")
    report: dict = {"input": args.input, "output": args.output}

    t0 = time.perf_counter()
    report["lake"] = build_lake(spark, args.input, args.output, args.coalesce)
    report["lake"]["seconds"] = round(time.perf_counter() - t0, 2)
    print(f"[lake] {report['lake']['rows']:,} rows -> {args.output} "
          f"({report['lake']['partitions']} date partitions, {report['lake']['seconds']}s)")

    if not args.skip_sessions:
        sessions_path = args.output.rstrip("/") + "-sessions"
        t1 = time.perf_counter()
        report["sessions"] = build_sessions(spark, args.output, sessions_path, args.idle_gap)
        report["sessions"]["seconds"] = round(time.perf_counter() - t1, 2)
        report["sessions"]["path"] = sessions_path
        print(f"[sessions] {report['sessions']['sessions']:,} sessions -> {sessions_path} "
              f"({report['sessions']['seconds']}s)")

    if args.redteam:
        # The lake root is the parent of the auth path: /data/lake/auth -> /data/lake/redteam
        redteam_out = str(Path(args.output).parent / "redteam")
        report["redteam"] = build_redteam(spark, args.redteam, redteam_out)
        print(f"[redteam] {report['redteam']['rows']} labelled events "
              f"({report['redteam']['compromised_identities']} identities) -> {redteam_out}")

    if args.stats_out:
        Path(args.stats_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.stats_out).write_text(json.dumps(report, indent=2) + "\n")
        print(f"[stats] {args.stats_out}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
