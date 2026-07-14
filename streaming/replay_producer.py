"""
Replay auth events from the lake into Redpanda at a controlled rate.

Event time is preserved in the payload, so the streaming job's windows and watermark operate
on the original LANL clock rather than on arrival time.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import timedelta

from kafka import KafkaProducer

from pipeline.common import ANCHOR_EPOCH, spark_session


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--lake", default="/data/lake/auth")
    ap.add_argument("--topic", default="tarn.auth")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092"))
    ap.add_argument("--rate", type=int, default=5000, help="target events per second")
    ap.add_argument("--duration", type=int, default=300)
    ap.add_argument("--limit", type=int, default=0, help="stop after N events, 0 = rate x duration")
    ap.add_argument("--start-day", type=int, default=0)
    ap.add_argument("--max-day", type=int, default=3)
    ap.add_argument("--progress-every", type=int, default=50_000)
    args = ap.parse_args()

    target = args.limit or args.rate * args.duration

    spark = spark_session("replay-producer")

    # Prune on event_date, not day_index. event_date is the lake's partition key so Spark can
    # skip whole directories. day_index is an ordinary column, so filtering on it still opens
    # all 58 partitions to look inside. Same rows either way, one reads 70 GB to find them.
    date_lo = (ANCHOR_EPOCH + timedelta(days=args.start_day)).isoformat()
    date_hi = (ANCHOR_EPOCH + timedelta(days=args.max_day)).isoformat()

    rows = (
        spark.read.parquet(args.lake)
        .filter(f"event_date >= date '{date_lo}' and event_date < date '{date_hi}'")
        .select(
            "time", "event_ts", "src_user", "dst_user",
            "src_computer", "dst_computer", "auth_type", "logon_type",
            "auth_orientation", "outcome",
        )
        .orderBy("time")
        .limit(target)
    )

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode() if k else None,
        linger_ms=20,
        batch_size=64 * 1024,
        compression_type="lz4",
        acks=1,
    )

    print(f"[producer] replaying {target:,} events -> {args.topic} at ~{args.rate:,}/s")

    sent = 0
    t_start = time.perf_counter()
    interval = 1.0 / args.rate if args.rate else 0

    # toLocalIterator streams partition by partition. Collecting the whole slice to the driver
    # would need gigabytes.
    for row in rows.toLocalIterator():
        payload = {
            "time": int(row["time"]),
            "event_ts": row["event_ts"].isoformat() if row["event_ts"] else None,
            "src_user": row["src_user"],
            "dst_user": row["dst_user"],
            "src_computer": row["src_computer"],
            "dst_computer": row["dst_computer"],
            "auth_type": row["auth_type"],
            "logon_type": row["logon_type"],
            "auth_orientation": row["auth_orientation"],
            "outcome": row["outcome"],
            # Wall clock at hand-off. This is the only basis for the lag numbers.
            "produce_ts_ms": int(time.time() * 1000),
        }
        # Keyed by identity so one user's events land on one partition and keep their relative
        # order. The windowed aggregation is per identity.
        producer.send(args.topic, key=row["src_user"], value=payload)
        sent += 1

        if sent % args.progress_every == 0:
            elapsed = time.perf_counter() - t_start
            print(f"  {sent:,} sent  ({sent / elapsed:,.0f}/s actual)")

        # Pace against the global start rather than sleeping a fixed interval per message, so
        # drift does not accumulate.
        if interval:
            expected = t_start + sent * interval
            drift = expected - time.perf_counter()
            if drift > 0:
                time.sleep(drift)

    producer.flush()
    elapsed = time.perf_counter() - t_start
    print(f"[producer] {sent:,} events in {elapsed:.1f}s ({sent / elapsed:,.0f}/s actual)")

    producer.close()
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
