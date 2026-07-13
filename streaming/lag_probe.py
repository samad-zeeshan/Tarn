"""Stage 3c — turn the streaming run's raw lag log into bench/streaming_lag.json.

WHAT THE LAG NUMBER MEANS (and what it does not)
------------------------------------------------
Every message carries `produce_ts_ms`: the wall-clock instant replay_producer.py handed it
to Kafka. Every emitted window carries the newest such timestamp among its records. When
foreachBatch finishes writing a batch to Parquet it stamps the wall clock again. So:

    lag = commit_wallclock - newest_produce_wallclock_in_that_window

That interval genuinely contains everything the pipeline does: Kafka publish -> broker ->
Spark fetch -> parse -> shuffle -> windowed aggregation -> WATERMARK HOLD -> Parquet write
-> batch commit. The watermark hold is the dominant term by design (a 1-minute window with
a 2-minute watermark cannot legally emit before its window has closed and the watermark has
advanced past it), and the artifact says so — a sub-second "lag" here would mean the
watermark wasn't doing its job.

WHAT IS NOT REPORTED: "event-time to now". The replay is accelerated, so a 58-day corpus is
pushed through in minutes and event-time lag would be weeks by construction. Reporting it
would be a meaningless (and flattering-sounding) number.

    python streaming/lag_probe.py --lag-log /data/work/lag_log.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
from datetime import UTC, datetime
from pathlib import Path


def percentile(values: list[float], p: float) -> float:
    """Nearest-rank percentile. Explicit so the p95 in the artifact has one definition."""
    if not values:
        return 0.0
    ordered = sorted(values)
    k = max(0, min(len(ordered) - 1, int(round(p / 100 * len(ordered) + 0.5)) - 1))
    return float(ordered[k])


def cpu_model() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def event_time_acceleration(sink: str, run_seconds: float) -> dict:
    """How much faster than real time did EVENT time advance?

    This is the number that makes the lag figure interpretable, and leaving it out would be a
    quiet lie. A 1-minute window under a 2-minute watermark cannot emit until 3 minutes of
    EVENT time have elapsed — but the replay compresses the corpus, so those 3 minutes of event
    time cost only (3 min / acceleration) of wall clock. Report a p50 lag without the
    acceleration factor and a reader will reasonably assume the watermark hold was ~180s of
    real waiting. It was not.
    """
    try:
        import duckdb

        con = duckdb.connect()
        row = con.execute(
            f"select min(window_start), max(window_end) "
            f"from read_parquet('{sink}/**/*.parquet', hive_partitioning=1)"
        ).fetchone()
        con.close()
        if not row or row[0] is None:
            return {}
        span = (row[1] - row[0]).total_seconds()
        factor = round(span / run_seconds, 1) if run_seconds else None
        return {
            "event_time_span_seconds": int(span),
            "wall_clock_seconds": round(run_seconds, 1),
            "acceleration_factor": factor,
            "note": (
                f"Event time advanced ~{factor}x faster than wall clock during the replay. The "
                f"1-minute window + 2-minute watermark means a window cannot emit until 3 minutes "
                f"of EVENT time have passed, which at this acceleration is roughly "
                f"{round(180 / factor, 1) if factor else '?'}s of real waiting — that is the floor "
                "the measured lag sits on. Quoting the lag without this factor would invite the "
                "reader to assume a 180s hold that never happened."
            ),
        }
    except Exception as exc:  # noqa: BLE001 — the artifact is still valid without this
        return {"error": f"could not measure acceleration: {exc}"}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--lag-log", default="/data/work/lag_log.jsonl")
    ap.add_argument("--progress", default="/data/work/stream_progress.json")
    ap.add_argument("--sink", default="/data/lake/streaming_windows")
    ap.add_argument("--out", default="bench/streaming_lag.json")
    ap.add_argument("--window", default="1 minute")
    ap.add_argument("--watermark", default="2 minutes")
    ap.add_argument("--trigger", default="5 seconds")
    ap.add_argument("--target-rate", type=int, default=5000)
    args = ap.parse_args()

    log_path = Path(args.lag_log)
    if not log_path.exists():
        print(f"no lag log at {log_path} — did the stream job run?")
        return 2

    batches = [json.loads(line) for line in log_path.read_text().splitlines() if line.strip()]
    if not batches:
        print("lag log is empty — the stream emitted no windows. Nothing to claim.")
        return 2

    lags: list[int] = []
    for b in batches:
        lags.extend(b.get("lag_samples", []))

    total_windows = sum(b["windows_committed"] for b in batches)
    total_events = sum(b["events_in_windows"] for b in batches)
    first_commit = min(b["commit_ts_ms"] for b in batches)
    last_commit = max(b["commit_ts_ms"] for b in batches)
    wall_seconds = max((last_commit - first_commit) / 1000, 0.001)

    progress: list[dict] = []
    if Path(args.progress).exists():
        progress = json.loads(Path(args.progress).read_text())
    processed_rates = [
        p["processed_rows_per_second"]
        for p in progress
        if p.get("processed_rows_per_second") not in (None, 0)
    ]
    input_rates = [
        p["input_rows_per_second"]
        for p in progress
        if p.get("input_rows_per_second") not in (None, 0)
    ]
    total_input_rows = sum(p.get("num_input_rows") or 0 for p in progress)

    payload = {
        "what": (
            "End-to-end lag and throughput of the Stage-3 Spark Structured Streaming job "
            "consuming replayed LANL auth events from Redpanda (Kafka API)."
        ),
        "lag_definition": (
            "commit_wallclock - newest_produce_wallclock_in_window. Spans Kafka publish, "
            "broker, Spark fetch, parse, shuffle, windowed aggregation, WATERMARK HOLD, "
            "Parquet write, and batch commit. The watermark hold dominates by design: a "
            f"{args.window} window under a {args.watermark} watermark cannot legally emit "
            "before its window closes and the watermark passes it. A sub-second figure here "
            "would mean the watermark was not doing its job."
        ),
        "not_measured": (
            "Event-time-to-now lag. The replay is accelerated, so the 58-day corpus is "
            "pushed through in minutes; event-time lag would be weeks by construction and "
            "reporting it would be meaningless."
        ),
        "honesty": (
            "This is an ACCELERATED LOCAL REPLAY from a file into a single-broker Redpanda "
            "on the same laptop as the Spark driver — not a live production feed. No network "
            "hop, no broker cluster, no competing load. Treat these figures as a measurement "
            "of this pipeline's processing latency, not of a production system's."
        ),
        "config": {
            "window": args.window,
            "watermark": args.watermark,
            "trigger": args.trigger,
            "output_mode": "append",
            "target_replay_rate_eps": args.target_rate,
            "sink": "Parquet, partitioned by event_date",
            "broker": "Redpanda v24.2.7, single node, 1 core / 1 GB",
        },
        "event_time_acceleration": event_time_acceleration(args.sink, wall_seconds),
        "lag_ms": {
            "samples": len(lags),
            "p50": percentile(lags, 50),
            "p90": percentile(lags, 90),
            "p95": percentile(lags, 95),
            "p99": percentile(lags, 99),
            "min": min(lags) if lags else 0,
            "max": max(lags) if lags else 0,
            "mean": round(statistics.mean(lags), 1) if lags else 0,
        },
        "throughput": {
            "windows_committed": total_windows,
            "events_aggregated": total_events,
            "batches": len(batches),
            "run_seconds": round(wall_seconds, 1),
            "events_per_second_sustained": round(total_events / wall_seconds, 1),
            "spark_processed_rows_per_second_median": (
                round(statistics.median(processed_rates), 1) if processed_rates else None
            ),
            "spark_input_rows_per_second_median": (
                round(statistics.median(input_rates), 1) if input_rates else None
            ),
            "spark_total_input_rows": total_input_rows,
        },
        "environment": {
            "cpu": cpu_model(),
            "cpu_count": os.cpu_count(),
            "container_os": platform.platform(),
            "host": "Windows 11 Pro 26200, Docker Desktop (WSL2 backend)",
            "python": platform.python_version(),
            "spark": "3.5.3",
        },
        "measured_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(payload, indent=2) + "\n")

    lm = payload["lag_ms"]
    tp = payload["throughput"]
    print(f"[lag] samples={lm['samples']}  p50={lm['p50']:.0f}ms  p95={lm['p95']:.0f}ms  "
          f"p99={lm['p99']:.0f}ms  max={lm['max']}ms")
    print(f"[throughput] {tp['events_aggregated']:,} events -> {tp['windows_committed']:,} windows "
          f"in {tp['run_seconds']}s ({tp['events_per_second_sustained']:,.0f} eps sustained)")
    print(f"[lag] wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
