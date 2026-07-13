"""Stage 3b — Spark Structured Streaming: 1-minute tumbling windows, watermarked.

Consumes the replayed auth events from Redpanda and maintains, per identity, per 1-minute
tumbling window of EVENT time:

    auth_count, failure_count, distinct destinations, distinct sources

with a watermark on event_ts so late records are still folded into their rightful window
(up to the watermark delay) and state for closed windows is dropped rather than grown
forever. Output is appended to Parquet, which dbt then picks up as a streaming mart —
same warehouse, same star, no second system.

WHY `append` AND NOT `update`: append mode emits a window exactly once, when the watermark
passes its end. That gives the sink immutable, non-duplicated rows — which is what makes
the checkpoint-recovery test meaningful (kill the job mid-run, restart, and assert no
window is written twice). Update mode would re-emit windows on every trigger and the test
would be vacuous.

Lag instrumentation: each output row carries `max_produce_ts_ms` (the newest wall-clock
produce timestamp of any record in that window). foreachBatch stamps the wall-clock commit
time and writes one JSON line per batch to the lag log, which streaming/lag_probe.py turns
into bench/streaming_lag.json.

    python streaming/stream_job.py --duration 300
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipeline.common import spark_session

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

MESSAGE_SCHEMA = StructType(
    [
        StructField("time", IntegerType()),
        StructField("event_ts", TimestampType()),
        StructField("src_user", StringType()),
        StructField("dst_user", StringType()),
        StructField("src_computer", StringType()),
        StructField("dst_computer", StringType()),
        StructField("auth_type", StringType()),
        StructField("logon_type", StringType()),
        StructField("auth_orientation", StringType()),
        StructField("outcome", StringType()),
        StructField("produce_ts_ms", LongType()),
    ]
)


def windowed_aggregate(events, window: str, watermark: str):
    """The actual streaming transformation, factored out so the tests exercise THIS code
    and not a re-implementation of it.

    Works on a streaming OR a batch DataFrame — Structured Streaming's whole premise — which
    is what lets tests/test_streaming.py drive it with a file source and assert on watermark
    and late-data behaviour without standing up Kafka.
    """
    return (
        events
        # Watermark on EVENT time (the replayed LANL clock). Records arriving more than
        # `watermark` behind the max event-time seen are dropped; everything else lands in
        # its true window even if it arrives out of order.
        .withWatermark("event_ts", watermark)
        .groupBy(F.window(F.col("event_ts"), window), F.col("src_user"))
        .agg(
            F.count("*").alias("auth_count"),
            F.sum(F.when(F.col("outcome") == "Fail", 1).otherwise(0)).alias("failure_count"),
            F.sum(F.when(F.col("outcome") == "Success", 1).otherwise(0)).alias("success_count"),
            F.approx_count_distinct("dst_computer").alias("distinct_dst_computers"),
            F.approx_count_distinct("src_computer").alias("distinct_src_computers"),
            # Newest produce timestamp in this window — the lag anchor.
            F.max("produce_ts_ms").alias("max_produce_ts_ms"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("src_user"),
            "auth_count",
            "failure_count",
            "success_count",
            "distinct_dst_computers",
            "distinct_src_computers",
            "max_produce_ts_ms",
        )
        .withColumn("event_date", F.to_date("window_start"))
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--topic", default="tarn.auth")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092"))
    ap.add_argument("--output", default="/data/lake/streaming_windows")
    ap.add_argument("--checkpoint", default="/data/work/checkpoints/tarn-auth")
    ap.add_argument("--lag-log", default="/data/work/lag_log.jsonl")
    ap.add_argument("--window", default="1 minute")
    ap.add_argument("--watermark", default="2 minutes")
    ap.add_argument("--trigger", default="5 seconds")
    ap.add_argument("--duration", type=int, default=300, help="seconds to run before stopping")
    ap.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    args = ap.parse_args()

    spark = spark_session(
        "stream-job",
        shuffle_partitions=8,
        spark__jars__packages=KAFKA_PACKAGE,
        spark__jars__ivy="/opt/ivy",
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", 200_000)
        .load()
    )

    events = (
        raw.select(F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("m"))
        .select("m.*")
        .filter(F.col("src_user").isNotNull() & F.col("event_ts").isNotNull())
    )

    windowed = windowed_aggregate(events, args.window, args.watermark)

    lag_log = Path(args.lag_log)
    lag_log.parent.mkdir(parents=True, exist_ok=True)

    def commit_batch(batch_df, batch_id: int) -> None:
        """Write the batch, then record how stale its freshest record was at commit time."""
        batch_df.persist()
        try:
            rows = batch_df.count()
            if rows == 0:
                return

            (
                batch_df.write.mode("append")
                .partitionBy("event_date")
                .parquet(args.output)
            )

            # Commit-time lag. Stamped AFTER the write returns, so it includes the sink.
            commit_ms = int(time.time() * 1000)
            stats = batch_df.agg(
                F.min("max_produce_ts_ms").alias("oldest"),
                F.max("max_produce_ts_ms").alias("newest"),
                F.sum("auth_count").alias("events"),
                F.count("*").alias("windows"),
            ).collect()[0]

            record = {
                "batch_id": batch_id,
                "commit_ts_ms": commit_ms,
                "windows_committed": int(stats["windows"]),
                "events_in_windows": int(stats["events"]),
                # Lag per window = commit time - the newest produce ts inside that window.
                "lag_ms_min": commit_ms - int(stats["newest"]),
                "lag_ms_max": commit_ms - int(stats["oldest"]),
                "lag_samples": [
                    commit_ms - int(r["max_produce_ts_ms"])
                    for r in batch_df.select("max_produce_ts_ms").collect()
                ],
            }
            with lag_log.open("a") as fh:
                fh.write(json.dumps(record) + "\n")

            print(f"[batch {batch_id}] {stats['windows']} windows, {stats['events']} events, "
                  f"lag {record['lag_ms_min']}-{record['lag_ms_max']} ms")
        finally:
            batch_df.unpersist()

    query = (
        windowed.writeStream.outputMode("append")
        .foreachBatch(commit_batch)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
        .start()
    )

    print(f"[stream] running for {args.duration}s "
          f"(window={args.window}, watermark={args.watermark}, trigger={args.trigger})")

    query.awaitTermination(timeout=args.duration)

    # Capture Spark's own view of throughput before tearing the query down.
    progress = [
        {
            "batch_id": p["batchId"],
            "input_rows_per_second": p.get("inputRowsPerSecond"),
            "processed_rows_per_second": p.get("processedRowsPerSecond"),
            "num_input_rows": p.get("numInputRows"),
            "batch_duration_ms": p.get("batchDuration"),
        }
        for p in query.recentProgress
    ]
    Path("/data/work/stream_progress.json").write_text(json.dumps(progress, indent=2) + "\n")

    query.stop()
    spark.stop()
    print(f"[stream] stopped; {len(progress)} progress records captured")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
