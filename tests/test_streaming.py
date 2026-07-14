"""
Windowing, watermark, and checkpoint recovery.

These drive the real transformation through a real streaming query, using a file source so
they run in CI with no broker. Watermarks and checkpoint state are properties of the query,
not of the source.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
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

from streaming.stream_job import windowed_aggregate

BASE = datetime(2015, 1, 1, 0, 0, 0, tzinfo=UTC)

# The file source needs an explicit schema (it cannot infer from a stream).
JSON_SCHEMA = StructType(
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


def _event(offset_seconds: int, user: str = "U1@DOM1", dst: str = "C9",
           outcome: str = "Success") -> dict:
    ts = BASE + timedelta(seconds=offset_seconds)
    return {
        "time": offset_seconds,
        "event_ts": ts.isoformat(),
        "src_user": user,
        "dst_user": user,
        "src_computer": "C1",
        "dst_computer": dst,
        "auth_type": "Kerberos",
        "logon_type": "Network",
        "auth_orientation": "LogOn",
        "outcome": outcome,
        "produce_ts_ms": 1_700_000_000_000 + offset_seconds * 1000,
    }


def _write_batch(directory: Path, name: str, events: list[dict]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{name}.json").write_text(
        "\n".join(json.dumps(e) for e in events) + "\n"
    )


def _run_once(spark, src: Path, sink: Path, checkpoint: Path,
              window: str = "1 minute", watermark: str = "2 minutes") -> None:
    """Run the streaming query until it has drained the currently-available files, then stop.

    availableNow drains what exists and terminates, which is what makes these tests
    deterministic, a processing-time trigger would race the assertions.
    """
    stream = (
        spark.readStream.schema(JSON_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .json(str(src))
    )
    query = (
        windowed_aggregate(stream, window, watermark)
        .writeStream.outputMode("append")
        .format("parquet")
        .option("path", str(sink))
        .option("checkpointLocation", str(checkpoint))
        .partitionBy("event_date")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination(timeout=120)
    query.stop()


def _windows(spark, sink: Path) -> dict[tuple[str, str], int]:
    """Emitted windows as {(user, window_start): auth_count}."""
    if not any(sink.rglob("*.parquet")):
        return {}
    rows = spark.read.parquet(str(sink)).collect()
    return {(r["src_user"], str(r["window_start"])): r["auth_count"] for r in rows}


def test_events_land_in_their_event_time_window(spark, tmp_path):
    """Three events in minute 0, two in minute 1 -> counts of 3 and 2, keyed by event time
    (NOT by when they arrived)."""
    src, sink, ckpt = tmp_path / "src", tmp_path / "sink", tmp_path / "ckpt"
    _write_batch(src, "b1", [
        _event(0), _event(10), _event(20),      # window [00:00, 01:00)
        _event(70), _event(80),                 # window [01:00, 02:00)
        # Push the watermark far enough forward that both windows close and emit.
        _event(600, user="U2@DOM1"),
    ])
    _run_once(spark, src, sink, ckpt)

    got = _windows(spark, sink)
    assert got[("U1@DOM1", "2015-01-01 00:00:00")] == 3
    assert got[("U1@DOM1", "2015-01-01 00:01:00")] == 2


def test_late_event_within_watermark_lands_in_its_own_window(spark, tmp_path):
    """An out-of-order event arriving in a later batch, but still inside the watermark, must
    be folded into ITS window, not the current one."""
    src, sink, ckpt = tmp_path / "src", tmp_path / "sink", tmp_path / "ckpt"

    # Batch 1: two events in minute 3. Max event-time = 03:10, so watermark = 01:10.
    _write_batch(src, "b1", [_event(180), _event(190)])
    _run_once(spark, src, sink, ckpt)

    # Batch 2: an event at 02:30, EARLIER than what we already saw, but still ahead of the
    # 01:10 watermark, so it is legally late and must count. Plus a far-future event to push
    # the watermark past minute 2 and force that window to emit.
    _write_batch(src, "b2", [_event(150), _event(900, user="U2@DOM1")])
    _run_once(spark, src, sink, ckpt)

    got = _windows(spark, sink)
    # The late 02:30 event belongs to window [02:00, 03:00), on its own.
    assert got.get(("U1@DOM1", "2015-01-01 00:02:00")) == 1, (
        f"late-but-within-watermark event was lost or misfiled; got {got}"
    )


def test_event_beyond_the_watermark_is_dropped(spark, tmp_path):
    """The watermark's actual job. An event so late that its window is already closed must be
    DISCARDED, silently folding it into a new window would emit a duplicate window for a
    period the sink already reported, and every downstream count would drift."""
    src, sink, ckpt = tmp_path / "src", tmp_path / "sink", tmp_path / "ckpt"

    # Batch 1: events at minute 10 -> watermark advances to 08:00.
    _write_batch(src, "b1", [_event(600), _event(610)])
    _run_once(spark, src, sink, ckpt)

    # Batch 2: an event at 00:30, which is ~7.5 minutes behind the watermark. Way too late.
    _write_batch(src, "b2", [_event(30), _event(1500, user="U2@DOM1")])
    _run_once(spark, src, sink, ckpt)

    got = _windows(spark, sink)
    assert ("U1@DOM1", "2015-01-01 00:00:00") not in got, (
        "an event beyond the watermark created a window it should never have created"
    )


def test_checkpoint_recovery_does_not_duplicate_windows(spark, tmp_path):
    """Kill and resume. Windows already committed must not be emitted a second time.

    This is the test that earns the phrase 'checkpoint recovery' in the resume claim. If
    append-mode + checkpointing did not hold, the streaming mart would double-count on every
    restart and nobody would notice until the totals disagreed with the batch layer.
    """
    src, sink, ckpt = tmp_path / "src", tmp_path / "sink", tmp_path / "ckpt"

    _write_batch(src, "b1", [_event(0), _event(10), _event(600, user="U2@DOM1")])
    _run_once(spark, src, sink, ckpt)
    first = _windows(spark, sink)
    assert first, "first run emitted nothing, the test would be vacuous"

    # Restart against the SAME checkpoint with no new input. A fresh query would replay the
    # files and re-emit; a recovered one must know it already did.
    _run_once(spark, src, sink, ckpt)
    second = _windows(spark, sink)

    assert second == first, f"restart re-emitted windows: {first} -> {second}"

    rows = spark.read.parquet(str(sink))
    dupes = (
        rows.groupBy("src_user", "window_start")
        .count()
        .filter(F.col("count") > 1)
        .collect()
    )
    assert not dupes, f"duplicate windows in the sink after recovery: {dupes}"


def test_failure_counts_are_correct_within_a_window(spark, tmp_path):
    src, sink, ckpt = tmp_path / "src", tmp_path / "sink", tmp_path / "ckpt"
    _write_batch(src, "b1", [
        _event(0, outcome="Success"),
        _event(5, outcome="Fail"),
        _event(10, outcome="Fail"),
        _event(600, user="U2@DOM1"),  # pushes the watermark so window 0 emits
    ])
    _run_once(spark, src, sink, ckpt)

    rows = {
        (r["src_user"], str(r["window_start"])): r
        for r in spark.read.parquet(str(sink)).collect()
    }
    w = rows[("U1@DOM1", "2015-01-01 00:00:00")]
    assert w["auth_count"] == 3
    assert w["failure_count"] == 2
    assert w["success_count"] == 1
