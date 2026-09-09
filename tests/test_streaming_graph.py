"""The graph detector inside Spark Structured Streaming must agree with the batch run exactly.

A file source stands in for Redpanda so this runs in CI. Micro-batch boundaries are the thing
under test, and a file source produces them the same way.
"""

from __future__ import annotations

import json
import os
import random

import numpy as np
import pandas as pd
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from detect.graph.features import FeatureEngine, FitContext
from detect.graph.model import HEADLINE, HistogramModel
from detect.graph.stream import GraphScorer
from detect.graph.verify import verify_point_in_time
from eval import protocol
from streaming.stream_job import candidate_events, graph_sink

SCHEMA = StructType([
    StructField("time", IntegerType()),
    StructField("src_user", StringType()),
    StructField("dst_user", StringType()),
    StructField("src_computer", StringType()),
    StructField("dst_computer", StringType()),
    StructField("auth_type", StringType()),
    StructField("logon_type", StringType()),
    StructField("auth_orientation", StringType()),
    StructField("outcome", StringType()),
])


def _events(seed=11, n=1500):
    rng = random.Random(seed)
    t, out = 0, []
    while len(out) < n:
        t += rng.choice([0, 1, 7, 90, 400])
        u = f"U{rng.randint(1, 30)}@DOM1"
        out.append({
            "time": t, "src_user": u, "dst_user": u,
            "src_computer": f"C{rng.randint(1, 6)}", "dst_computer": f"C{rng.randint(7, 40)}",
            "auth_type": "NTLM", "logon_type": "Network", "auth_orientation": "LogOn",
            "outcome": "Fail" if rng.random() < 0.05 else "Success",
        })
    # Noise the candidate filter must drop: a machine account and a local logon.
    out.append({**out[-1], "src_user": "C9$@DOM1"})
    out.append({**out[-1], "src_user": "U1@DOM1", "dst_computer": out[-1]["src_computer"]})
    return out


def _fitted(events):
    df = pd.DataFrame(events)
    fit = df[df.time < protocol.FIT_END]
    names = {n: i for i, n in enumerate(sorted(set(df.src_user)), start=1)}
    hosts = {h: i for i, h in
             enumerate(sorted(set(df.src_computer) | set(df.dst_computer)), start=1)}
    t, u = fit.time.tolist(), [names[x] for x in fit.src_user]
    s, d = [hosts[x] for x in fit.src_computer], [hosts[x] for x in fit.dst_computer]
    ctx = FitContext.build(t, u, s, d)
    eng = FeatureEngine(ctx)
    rows = eng.push(t, u, s, d, [0] * len(t)) + eng.flush()
    X = np.array([r[4:] for r in rows if r[0] >= protocol.WARMUP_END], dtype=int)
    model = HistogramModel.fit(X, HEADLINE, fit_max_time=int(fit.time.max()))
    return ctx, model


def test_streamed_scores_match_the_batch_run(spark, tmp_path):
    events = _events()
    ctx, model = _fitted(events)
    src = tmp_path / "src"
    src.mkdir()
    for i, chunk in enumerate(np.array_split(np.arange(len(events)), 6)):
        (src / f"part-{i}.json").write_text(
            "\n".join(json.dumps(events[j]) for j in chunk) + "\n")
    # One event from long ago, arriving last. It must be dropped, never scored.
    late = {**events[10], "dst_computer": "C99"}
    (src / "part-9.json").write_text(json.dumps(late) + "\n")
    # The file source reads in modification-time order, and files written in the same instant
    # come out in any order. Spaced times make the arrival order the one written above.
    for k, path in enumerate(sorted(src.glob("part-*.json"))):
        os.utime(path, (1_700_000_000 + k * 10, 1_700_000_000 + k * 10))

    scorer = GraphScorer(ctx, model, threshold=0.0)
    stream = spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(src))
    query = (candidate_events(stream).writeStream
             .foreachBatch(graph_sink(scorer, tmp_path / "sink"))
             .option("checkpointLocation", str(tmp_path / "ckpt"))
             .trigger(availableNow=True).start())
    query.awaitTermination(timeout=180)
    query.stop()
    tail = scorer.close()
    streamed = pd.concat([pd.read_parquet(p) for p in sorted((tmp_path / "sink").glob("*"))]
                         + [tail])

    ref = GraphScorer(ctx, model, threshold=0.0)
    ref.users, ref.hosts = scorer.users, scorer.hosts
    frame = pd.DataFrame(events)
    frame = frame[frame.src_user.str.startswith("U")
                  & (frame.src_computer != frame.dst_computer)]
    frame = frame.assign(fail=frame.outcome == "Fail")
    batch = pd.concat([ref.process(frame), ref.close()])

    key = ["time", "uid", "sid", "did"]
    a = streamed.sort_values(key).reset_index(drop=True)
    b = batch.sort_values(key).reset_index(drop=True)
    assert len(a) == len(b) > 1000
    pd.testing.assert_frame_equal(a, b, check_dtype=False)
    assert scorer.engine.late == 1


def test_verifier_passes_on_the_streamed_input():
    events = _events(seed=5, n=800)
    ids: dict[str, int] = {}
    rows = [
        (e["time"], ids.setdefault(e["src_user"], len(ids)),
         ids.setdefault(e["src_computer"], len(ids)), ids.setdefault(e["dst_computer"], len(ids)),
         int(e["outcome"] == "Fail"))
        for e in events if e["src_user"].startswith("U") and e["src_computer"] != e["dst_computer"]
    ]
    report = verify_point_in_time(rows, FeatureEngine, FitContext.empty(), cuts=15)
    assert report["passed"], report
