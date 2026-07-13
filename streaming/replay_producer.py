"""Stage 3a — replay auth events into Redpanda (Kafka API) at a controlled rate.

Reads events in event-time order from the Parquet lake and publishes them to a topic,
accelerated: the corpus's 58 days are replayed at --rate events/second. Event-time is
PRESERVED in the payload (event_ts), so the streaming job's windows and watermark operate
on the original LANL clock, not on wall-clock arrival.

Each message also carries `produce_ts_ms` — the wall-clock instant this process handed the
record to Kafka. That field is the ONLY basis for the lag numbers in bench/streaming_lag.json,
and the reason the lag figure means something concrete:

    lag = (wall-clock when the window containing this record was committed to the sink)
        - (wall-clock when the record was produced to Kafka)

Note what is NOT being measured: "event-time to now". Under an accelerated replay that
number is an artefact of the acceleration factor and would be meaningless — a 58-day corpus
squeezed into ten minutes has an event-time lag of weeks by construction. The artifact says
so explicitly rather than quietly reporting a flattering number.

    python streaming/replay_producer.py --rate 5000 --duration 300
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
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--lake", default="/data/lake/auth")
    ap.add_argument("--topic", default="tarn.auth")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092"))
    ap.add_argument("--rate", type=int, default=5000, help="target events/second")
    ap.add_argument("--duration", type=int, default=300, help="seconds to replay for")
    ap.add_argument("--limit", type=int, default=0, help="stop after N events (0 = rate x duration)")
    ap.add_argument("--start-day", type=int, default=0, help="replay from this day_index")
    ap.add_argument(
        "--max-day",
        type=int,
        default=3,
        help="bound the scan to day_index < N. This matters: `orderBy(time).limit(N)` over the "
             "unbounded lake makes Spark scan and top-N a billion rows just to publish a few "
             "hundred thousand. Bounding to the first few days keeps the producer a producer.",
    )
    ap.add_argument("--progress-every", type=int, default=50_000)
    args = ap.parse_args()

    target = args.limit or args.rate * args.duration

    # Pull the slice out of the lake in event-time order. Collecting to the driver in one
    # go would need gigabytes; toLocalIterator streams partition by partition.
    spark = spark_session("replay-producer")

    # Prune on event_date, NOT day_index. event_date is the lake's PARTITION key, so filtering
    # on it lets Spark skip whole directories; day_index is an ordinary column inside the files,
    # so filtering on that alone still opens all 58 partitions to look. Same rows either way —
    # one of them just reads 70 GB to find them.
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
            # Wall-clock at hand-off. This is what lag is measured from.
            "produce_ts_ms": int(time.time() * 1000),
        }
        # Key by identity so a given user's events land on one partition and keep their
        # relative order — the windowed aggregation is per-identity.
        producer.send(args.topic, key=row["src_user"], value=payload)
        sent += 1

        if sent % args.progress_every == 0:
            elapsed = time.perf_counter() - t_start
            print(f"  {sent:,} sent  ({sent / elapsed:,.0f}/s actual)")

        # Pace to the target rate. Recomputing against the global start (rather than
        # sleeping a fixed interval per message) stops drift from accumulating.
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
