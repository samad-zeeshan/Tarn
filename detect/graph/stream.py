"""Score micro-batches of named events with the frozen graph detector, one second at a time.

The Spark job hands each micro-batch here. Names become the same integer ids the batch run
used, and names the batch run never saw get fresh ids above the known range.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from detect.graph.features import FEATURES, FeatureEngine, FitContext
from detect.graph.model import HEADLINE, HistogramModel

COLUMNS = ["time", "src_user", "src_computer", "dst_computer", "fail"]


class IdMap:
    def __init__(self, names: dict[str, int]):
        self.ids = dict(names)
        self.names = {v: k for k, v in names.items()}
        self.next = max(names.values(), default=0) + 1

    def get(self, name: str) -> int:
        i = self.ids.get(name)
        if i is None:
            i = self.ids[name] = self.next
            self.names[i] = name
            self.next += 1
        return i

    @classmethod
    def load(cls, path: Path, col: str) -> IdMap:
        t = pq.read_table(path).to_pydict()
        return cls(dict(zip(t["name"], t[col], strict=True)))


class GraphScorer:
    def __init__(self, ctx: FitContext, model: HistogramModel, threshold: float,
                 users: IdMap | None = None, hosts: IdMap | None = None):
        self.engine = FeatureEngine(ctx)
        self.model = model
        self.threshold = threshold
        self.users = users or IdMap({})
        self.hosts = hosts or IdMap({})
        self.scored = 0

    @classmethod
    def load(cls, work: Path, out: Path) -> GraphScorer:
        manifest = json.loads((out / "manifest.json").read_text())
        return cls(
            FitContext.from_json(json.loads((out / "context.json").read_text())),
            HistogramModel.from_json(json.loads((out / f"model_{HEADLINE}.json").read_text())),
            manifest["alert_threshold"],
            IdMap.load(work / "users.parquet", "uid"),
            IdMap.load(work / "hosts.parquet", "hid"),
        )

    def _frame(self, rows: list[tuple]) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=["time", "uid", "sid", "did", *FEATURES, "score", "alert"])
        arr = np.array(rows, dtype=np.int64)
        feats = arr[:, 4:].astype(np.int8)
        df = pd.DataFrame(arr[:, :4], columns=["time", "uid", "sid", "did"])
        for i, name in enumerate(FEATURES):
            df[name] = feats[:, i]
        df["score"] = self.model.score(feats)
        df["alert"] = df["score"] >= self.threshold
        self.scored += len(df)
        return df

    def process(self, batch: pd.DataFrame) -> pd.DataFrame:
        """Score a micro-batch. Events in its last second wait for the next batch."""
        batch = batch.sort_values("time", kind="stable")
        rows = self.engine.push(
            batch["time"].astype(int).tolist(),
            [self.users.get(x) for x in batch["src_user"]],
            [self.hosts.get(x) for x in batch["src_computer"]],
            [self.hosts.get(x) for x in batch["dst_computer"]],
            batch["fail"].astype(int).tolist(),
        )
        return self._frame(rows)

    def close(self) -> pd.DataFrame:
        return self._frame(self.engine.flush())
