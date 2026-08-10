"""Run the graph detector over the extracted candidates and write one score per event.

Fits the frozen context and the histogram models on day 0, then scores every later event from
state that only holds earlier events.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from detect.graph.features import FEATURES, FeatureEngine, FitContext
from detect.graph.model import GROUPS, HEADLINE, HistogramModel
from eval import protocol

BATCH = 1_000_000


def off_hours_band(times: np.ndarray) -> list[int]:
    """The quiet band from the fit window's own hourly curve, with the v1 rule."""
    from pipeline.diurnal import derive_off_hours

    hours = (times % protocol.SECONDS_PER_DAY) // 3600
    counts = {int(h): int(c) for h, c in zip(*np.unique(hours, return_counts=True), strict=True)}
    return derive_off_hours(counts, near_min_pct=0.15).get("band", [])


def run(work: Path, out: Path) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    src = pq.ParquetFile(work / "candidates.parquet")
    n = src.metadata.num_rows

    fit = pq.read_table(work / "candidates.parquet",
                        filters=[("time", "<", protocol.FIT_END)]).to_pydict()
    band = off_hours_band(np.asarray(fit["time"]))
    ctx = FitContext.build(fit["time"], fit["uid"], fit["sid"], fit["did"], off_band=band)
    del fit

    keys = np.empty((n, 4), dtype=np.int32)
    feats = np.empty((n, len(FEATURES)), dtype=np.int8)
    engine = FeatureEngine(ctx)
    filled = 0
    t0 = time.perf_counter()

    def take(rows):
        nonlocal filled
        if not rows:
            return
        arr = np.array(rows, dtype=np.int32)
        keys[filled:filled + len(arr)] = arr[:, :4]
        feats[filled:filled + len(arr)] = arr[:, 4:]
        filled += len(arr)

    for batch in src.iter_batches(batch_size=BATCH, columns=["time", "uid", "sid", "did", "fail"]):
        cols = [batch.column(i).to_numpy(zero_copy_only=False).tolist() for i in range(5)]
        take(engine.push(*cols))
        if filled and filled % (20 * BATCH) < BATCH:
            print(f"  {filled:,} events, {time.perf_counter() - t0:.0f}s", flush=True)
    take(engine.flush())
    feature_seconds = time.perf_counter() - t0
    keys, feats = keys[:filled], feats[:filled]

    fit_rows = (keys[:, 0] >= protocol.WARMUP_END) & (keys[:, 0] < protocol.FIT_END)
    fit_max = int(keys[fit_rows, 0].max()) if fit_rows.any() else -1
    models = {g: HistogramModel.fit(feats[fit_rows], g, fit_max_time=fit_max) for g in GROUPS}

    test = keys[:, 0] >= protocol.TEST_START
    table = {
        "time": keys[test, 0], "uid": keys[test, 1], "sid": keys[test, 2], "did": keys[test, 3],
    }
    for i, name in enumerate(FEATURES):
        table[name] = feats[test, i]
    for g, m in models.items():
        table[f"score_{g}"] = m.score(feats[test]).astype(np.float32)
    pq.write_table(pa.table(table), out / "scores.parquet", compression="zstd")

    for g, m in models.items():
        m.save(out / f"model_{g}.json")
    (out / "context.json").write_text(json.dumps(ctx.to_json()))

    # The streaming job cannot rank a whole day before alerting, so it needs a fixed cut. This
    # one is the score that would have raised the budget's worth of alerts on day 0.
    fit_scores = np.sort(models[HEADLINE].score(feats[fit_rows]))[::-1]
    per_window = max(1, int(protocol.BUDGET_PER_DAY * (protocol.FIT_END - protocol.WARMUP_END)
                            / protocol.SECONDS_PER_DAY))
    threshold = float(fit_scores[min(per_window, len(fit_scores)) - 1]) if len(fit_scores) else 0.0
    manifest = {
        "events_in": n,
        "events_scored": int(test.sum()),
        "late_dropped": engine.late,
        "fit_rows": int(fit_rows.sum()),
        "fit_max_time": fit_max,
        "off_hours_band": band,
        "high_value_hosts": sum(1 for v in ctx.hv_dist.values() if v == 0),
        "alert_threshold": threshold,
        "feature_seconds": round(feature_seconds, 1),
        "events_per_second": round(n / feature_seconds) if feature_seconds else None,
    }
    protocol.assert_fit_window(manifest)
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--work", required=True, help="directory written by detect/graph/extract.py")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    print(json.dumps(run(Path(args.work), Path(args.out)), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
