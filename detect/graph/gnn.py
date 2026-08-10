"""A LightGCN link predictor, fitted on the day-0 User to Computer graph, as the GNN comparison.

A login is anomalous when the model thinks that user and that computer should not be linked.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from eval import protocol


class LightGCN:
    """Two propagation layers over a symmetric normalised bipartite graph, trained with BPR."""

    def __init__(self, user_index, host_index, final, fit_max_time, edges):
        self.user_index = user_index
        self.host_index = host_index
        self.final = final
        self.fit_max_time = fit_max_time
        self.edges = edges

    @staticmethod
    def _propagate(x, rows, cols, w, n):
        out = np.zeros_like(x)
        for k in range(x.shape[1]):
            out[:, k] = np.bincount(rows, weights=w * x[cols, k], minlength=n)
        return out

    @classmethod
    def fit(cls, users, hosts, fit_max_time: int, dim: int = 32, epochs: int = 200,
            lr: float = 0.05, reg: float = 1e-4, seed: int = 0) -> LightGCN:
        protocol.assert_fit_window({"fit_max_time": fit_max_time})
        pairs = np.unique(np.stack([users, hosts], axis=1), axis=0)
        uids, u_local = np.unique(pairs[:, 0], return_inverse=True)
        hids, h_local = np.unique(pairs[:, 1], return_inverse=True)
        nu, nh = len(uids), len(hids)
        n = nu + hids.size
        a = u_local
        b = h_local + nu
        rows = np.concatenate([a, b])
        cols = np.concatenate([b, a])
        deg = np.bincount(rows, minlength=n).astype(float)
        w = 1.0 / np.sqrt(deg[rows] * deg[cols])

        rng = np.random.default_rng(seed)
        emb = rng.normal(0, 0.1, size=(n, dim))

        def forward(e):
            l1 = cls._propagate(e, rows, cols, w, n)
            l2 = cls._propagate(l1, rows, cols, w, n)
            return (e + l1 + l2) / 3

        # Adam, because plain SGD on BPR needs a learning-rate search, and a search would
        # have nothing but day 0 to be tuned on.
        m = np.zeros_like(emb)
        v = np.zeros_like(emb)
        for step in range(1, epochs + 1):
            f = forward(emb)
            idx = rng.integers(0, len(a), size=len(a))
            pu, pp = a[idx], b[idx]
            pn = rng.integers(0, nh, size=len(a)) + nu
            x = (f[pu] * (f[pp] - f[pn])).sum(axis=1)
            g = -1.0 / (1.0 + np.exp(x)) / len(a)
            grad_f = np.zeros_like(f)
            np.add.at(grad_f, pu, g[:, None] * (f[pp] - f[pn]))
            np.add.at(grad_f, pp, g[:, None] * f[pu])
            np.add.at(grad_f, pn, -g[:, None] * f[pu])
            # The propagation matrix is symmetric, so the gradient flows back through it
            # with the same operator.
            g1 = cls._propagate(grad_f, rows, cols, w, n)
            g2 = cls._propagate(g1, rows, cols, w, n)
            grad = (grad_f + g1 + g2) / 3 + reg * emb
            m = 0.9 * m + 0.1 * grad
            v = 0.999 * v + 0.001 * grad * grad
            emb -= lr * (m / (1 - 0.9**step)) / (np.sqrt(v / (1 - 0.999**step)) + 1e-8)
        final = forward(emb)
        return cls(
            {int(x): i for i, x in enumerate(uids)},
            {int(x): i + nu for i, x in enumerate(hids)},
            final.astype(np.float32),
            int(fit_max_time),
            int(len(pairs)),
        )

    def score_raw(self, users, hosts) -> np.ndarray:
        """Negative affinity, with inf for a user or computer that day 0 never saw."""
        users = np.asarray(users, dtype=np.int64)
        hosts = np.asarray(hosts, dtype=np.int64)
        ulut = np.full(max(int(users.max()), max(self.user_index)) + 1, -1)
        ulut[list(self.user_index)] = list(self.user_index.values())
        hlut = np.full(max(int(hosts.max()), max(self.host_index)) + 1, -1)
        hlut[list(self.host_index)] = list(self.host_index.values())
        ui, hi = ulut[users], hlut[hosts]
        known = (ui >= 0) & (hi >= 0)
        out = np.full(len(users), np.inf, dtype=np.float32)
        out[known] = -(self.final[ui[known]] * self.final[hi[known]]).sum(axis=1)
        return out

    def score(self, users, hosts) -> np.ndarray:
        """Like score_raw, but unknown nodes rank just above the most anomalous known pair."""
        raw = self.score_raw(users, hosts)
        finite = raw[np.isfinite(raw)]
        top = float(finite.max()) + 1.0 if finite.size else 1.0
        return np.where(np.isfinite(raw), raw, top).astype(np.float32)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--work", required=True)
    ap.add_argument("--scores", required=True, help="scores.parquet from detect/graph/run.py")
    ap.add_argument("--out", required=True)
    ap.add_argument("--epochs", type=int, default=200)
    args = ap.parse_args()

    fit = pq.read_table(Path(args.work) / "candidates.parquet",
                        filters=[("time", "<", protocol.FIT_END)], columns=["time", "uid", "did"])
    t0 = time.perf_counter()
    model = LightGCN.fit(fit["uid"].to_numpy(), fit["did"].to_numpy(),
                         fit_max_time=int(fit["time"].to_numpy().max()), epochs=args.epochs)
    fit_seconds = time.perf_counter() - t0

    src = pq.ParquetFile(args.scores)
    parts = []
    for batch in src.iter_batches(batch_size=5_000_000, columns=["time", "uid", "sid", "did"]):
        raw = model.score_raw(batch.column("uid").to_numpy(), batch.column("did").to_numpy())
        parts.append(pa.table({"time": batch.column("time"), "uid": batch.column("uid"),
                               "sid": batch.column("sid"), "did": batch.column("did"),
                               "score_gnn": raw}))
    table = pa.concat_tables(parts)
    finite = table["score_gnn"].to_numpy()
    top = float(finite[np.isfinite(finite)].max()) + 1.0
    table = table.set_column(4, "score_gnn", pa.array(np.where(np.isfinite(finite), finite, top)
                                                      .astype(np.float32)))
    Path(args.out).mkdir(parents=True, exist_ok=True)
    pq.write_table(table, Path(args.out) / "scores.parquet", compression="zstd")
    manifest = {"model": "LightGCN, 2 layers, 32 dims, BPR", "fit_edges": model.edges,
                "fit_max_time": model.fit_max_time, "fit_seconds": round(fit_seconds, 1),
                "events_scored": table.num_rows,
                "unknown_node_events": int((~np.isfinite(finite)).sum())}
    protocol.assert_fit_window(manifest)
    (Path(args.out) / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
