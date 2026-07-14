"""
Turn the streaming run's raw lag log into bench/streaming_lag.json.

Lag is measured from when the producer handed a record to Kafka to when the window containing
it was committed to the sink.
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
    """Nearest rank percentile, written out so the p95 in the artifact has one definition."""
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
    """How much faster than real time did event time advance during the replay?"""
    # This is the number that makes the lag figure readable. A 1-minute window under a 2-minute
    # watermark cannot emit until 3 minutes of event time have passed, but the replay compresses
    # the corpus, so those 3 minutes cost only 3/acceleration of real waiting. Report a p50
    # without this and a reader will assume the hold was 180 seconds. It was not.
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
                f"Event time advanced about {factor}x faster than wall clock. The 1-minute window "
                f"plus 2-minute watermark means a window cannot emit until 3 minutes of event time "
                f"have passed, which at this acceleration is roughly "
                f"{round(180 / factor, 1) if factor else '?'}s of real waiting. That is the floor "
                "the measured lag sits on."
            ),
        }
    except Exception as exc:  # noqa: BLE001
        return {"error": f"could not measure acceleration: {exc}"}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
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
        print(f"no lag log at {log_path}, did the stream job run?")
        return 2

    batches = [json.loads(line) for line in log_path.read_text().splitlines() if line.strip()]
    if not batches:
        print("lag log is empty, the stream emitted no windows. Nothing to claim.")
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
            "End to end lag and throughput of the Structured Streaming job consuming replayed "
            "LANL auth events from Redpanda."
        ),
        "lag_definition": (
            "commit wall clock minus the newest produce wall clock in the window. It spans the "
            "Kafka publish, the broker, the Spark fetch, parse, shuffle, windowed aggregation, "
            f"the watermark hold, the Parquet write and the batch commit. A {args.window} window "
            f"under a {args.watermark} watermark cannot emit before its window closes and the "
            "watermark passes it, so a sub-second figure here would mean the watermark was not "
            "doing its job."
        ),
        "not_measured": (
            "Event-time-to-now lag. The replay is accelerated, so the 58-day corpus is pushed "
            "through in minutes and that number would be weeks by construction."
        ),
        "honesty": (
            "This is an accelerated local replay from a file into a single-broker Redpanda on the "
            "same laptop as the Spark driver. No network hop, no broker cluster, no competing "
            "load. It measures this pipeline's processing latency, not a production system's."
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
