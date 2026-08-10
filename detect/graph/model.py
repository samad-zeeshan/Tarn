"""A histogram outlier model over the graph features, with v1's rules injected as extra inputs.

The score is a sum of per-feature surprises, so each alert's explanation adds up to its score
exactly.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import numpy as np

from detect.graph.features import BINS, FEATURES
from eval import protocol

GRAPH = ("edge_new", "src_new", "gap", "burst_1h", "src_new_users_24h", "dst_hub", "hv_delta")
RULES = ("r_new_hosts_today", "r_fail_1h", "r_off_hours")

# The headline detector was fixed before any test-period score was looked at. Picking the best
# of several variants after seeing the labels would be model selection on the test set.
GROUPS = {
    "graph": GRAPH,
    "graph_plus_rules": GRAPH + RULES,
    "rules_only": RULES,
}
HEADLINE = "graph_plus_rules"


@dataclass
class HistogramModel:
    group: str
    columns: tuple[int, ...]
    log_probs: list[np.ndarray]
    fit_rows: int
    fit_max_time: int

    @classmethod
    def fit(cls, X: np.ndarray, group: str, fit_max_time: int) -> HistogramModel:
        protocol.assert_fit_window({"fit_max_time": fit_max_time})
        cols = tuple(FEATURES.index(c) for c in GROUPS[group])
        log_probs = []
        for c in cols:
            counts = np.bincount(X[:, c], minlength=BINS[c]).astype(float)
            # Add-one smoothing, so a value never seen on day 0 gets a large but finite
            # surprise instead of an infinite one that would tie every such event.
            log_probs.append(np.log((counts + 1.0) / (counts.sum() + BINS[c])))
        return cls(group, cols, log_probs, int(len(X)), int(fit_max_time))

    def contributions(self, X: np.ndarray) -> np.ndarray:
        return np.stack(
            [-lp[X[:, c]] for c, lp in zip(self.columns, self.log_probs, strict=True)], axis=1
        )

    def score(self, X: np.ndarray) -> np.ndarray:
        return self.contributions(X).sum(axis=1)

    def to_json(self) -> dict:
        return {
            "group": self.group,
            "features": [FEATURES[c] for c in self.columns],
            "fit_rows": self.fit_rows,
            "fit_max_time": self.fit_max_time,
            "log_probs": [lp.round(6).tolist() for lp in self.log_probs],
        }

    @classmethod
    def from_json(cls, blob: dict) -> HistogramModel:
        cols = tuple(FEATURES.index(c) for c in blob["features"])
        return cls(blob["group"], cols, [np.array(lp) for lp in blob["log_probs"]],
                   blob["fit_rows"], blob["fit_max_time"])

    def save(self, path) -> None:
        with open(path, "w") as fh:
            json.dump(self.to_json(), fh)
