"""
Run the producer and the streaming job together, then turn the lag log into an artifact.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def ensure_topic(bootstrap: str, topic: str, partitions: int = 4) -> None:
    """Create the topic before either side starts."""
    # Without this the stream job attaches to a topic that does not exist yet, because the
    # producer is still booting its Spark session, and dies with UnknownTopicOrPartition.
    # Relying on broker auto-creation is a race, and it lost.
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=1)])
        print(f"[stage3] created topic {topic} ({partitions} partitions)")
    except TopicAlreadyExistsError:
        print(f"[stage3] topic {topic} already exists")
    finally:
        admin.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--rate", type=int, default=5000)
    ap.add_argument("--duration", type=int, default=300)
    ap.add_argument("--lake", default="/data/lake/auth")
    ap.add_argument("--topic", default="tarn.auth")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092"))
    ap.add_argument("--output", default="/data/lake/streaming_windows")
    ap.add_argument("--checkpoint", default="/data/work/checkpoints/tarn-auth")
    ap.add_argument("--lag-log", default="/data/work/lag_log.jsonl")
    ap.add_argument("--window", default="1 minute")
    ap.add_argument("--watermark", default="2 minutes")
    ap.add_argument("--max-day", type=int, default=3)
    ap.add_argument("--fresh", action="store_true", help="wipe checkpoint, sink and lag log first")
    args = ap.parse_args()

    if args.fresh:
        for p in (args.checkpoint, args.output):
            shutil.rmtree(p, ignore_errors=True)
        Path(args.lag_log).unlink(missing_ok=True)
        print("[stage3] wiped checkpoint, sink and lag log")

    ensure_topic(args.bootstrap, args.topic)

    print(f"[stage3] producer: {args.rate:,} eps for {args.duration}s")
    producer_log = Path("/data/work/producer.log")
    producer_log.parent.mkdir(parents=True, exist_ok=True)

    # Redirect to a file, never to a PIPE nobody drains. The producer is a Spark job and is
    # chatty, and an undrained 64 KB pipe buffer made it block forever on its own log output.
    with producer_log.open("w") as log:
        producer = subprocess.Popen(
            [
                sys.executable, "streaming/replay_producer.py",
                "--lake", args.lake,
                "--topic", args.topic,
                "--rate", str(args.rate),
                "--duration", str(args.duration),
                "--max-day", str(args.max_day),
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )

    # Give the producer a head start, or the first few batches measure an empty topic and skew
    # the lag downward.
    time.sleep(25)

    # The producer once died on startup, its error went into a log nobody was reading, and the
    # stream then ran happily for five minutes against an empty topic and reported zero windows.
    # A pipeline that cannot tell "nothing happened" from "the producer crashed" will eventually
    # publish a benchmark of nothing.
    if producer.poll() is not None:
        print(
            f"[stage3] PRODUCER DIED before the stream started (exit {producer.returncode}).\n"
            f"[stage3] Its output is in {producer_log}. Last lines:\n",
            file=sys.stderr,
        )
        for line in producer_log.read_text(errors="replace").splitlines()[-12:]:
            print(f"    {line}", file=sys.stderr)
        return 1

    print("[stage3] stream job starting")
    stream = subprocess.run(
        [
            sys.executable, "streaming/stream_job.py",
            "--topic", args.topic,
            "--output", args.output,
            "--checkpoint", args.checkpoint,
            "--lag-log", args.lag_log,
            "--window", args.window,
            "--watermark", args.watermark,
            "--duration", str(args.duration),
        ],
    )

    producer.terminate()
    try:
        producer.wait(timeout=30)
    except subprocess.TimeoutExpired:
        producer.kill()

    if stream.returncode != 0:
        print(f"[stage3] stream job failed ({stream.returncode})")
        return stream.returncode

    if not Path(args.lag_log).exists() or Path(args.lag_log).stat().st_size == 0:
        print(
            "[stage3] the stream emitted ZERO windows. Not writing a lag artifact.\n"
            f"[stage3] check {producer_log}, the producer most likely never published.",
            file=sys.stderr,
        )
        return 1

    print("[stage3] probing lag")
    probe = subprocess.run(
        [
            sys.executable, "streaming/lag_probe.py",
            "--lag-log", args.lag_log,
            "--sink", args.output,
            "--window", args.window,
            "--watermark", args.watermark,
            "--target-rate", str(args.rate),
        ],
    )
    return probe.returncode


if __name__ == "__main__":
    raise SystemExit(main())
