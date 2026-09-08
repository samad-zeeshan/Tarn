"""Replay a stream and fail if any feature changes when the future is cut off.

KONTOGRAPH (arXiv 2608.22389) caught point-in-time bugs that review had missed by perturbing
the future. This does the same with truncation at random cut points.
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

import duckdb

from detect.graph.features import FeatureEngine, FitContext


def _features(engine_cls, ctx, events):
    cols = list(map(list, zip(*events, strict=True))) if events else [[], [], [], [], []]
    eng = engine_cls(ctx)
    eng.prepare(*cols)
    rows = eng.push(*cols) + eng.flush()
    return {r[:4]: r[4:] for r in rows}


def verify_point_in_time(events, engine_cls, ctx, cuts: int = 20, seed: int = 0) -> dict:
    """events must be sorted by time. The report says passed=False on any mismatch."""
    full = _features(engine_cls, ctx, events)
    times = sorted({e[0] for e in events})
    rng = random.Random(seed)
    picked = sorted(rng.sample(times, min(cuts, len(times))))
    mismatches = 0
    checked = 0
    examples = []
    for cut in picked:
        # Everything up to and including the cut second survives. A feature of an event at or
        # before the cut that differs from the full run must have read something later.
        head = [e for e in events if e[0] <= cut]
        for key, feats in _features(engine_cls, ctx, head).items():
            checked += 1
            if full.get(key) != feats:
                mismatches += 1
                if len(examples) < 5:
                    examples.append({"event": list(key), "truncated": list(feats),
                                     "full": list(full.get(key, ()))})
    return {
        "passed": mismatches == 0 and checked > 0,
        "cuts": len(picked),
        "features_checked": checked,
        "mismatches": mismatches,
        "examples": examples,
    }


class LeakyControl(FeatureEngine):
    """A negative control. Its burst feature counts new hosts in the next hour as well as the last.

    The verifier must fail on this engine, on the same data it passes the real one on, or the
    pass means nothing.
    """

    def prepare(self, t, u, s, d, f):
        seen, firsts = set(), {}
        for ti, ui, di in zip(t, u, d, strict=True):
            if (ui, di) not in seen:
                seen.add((ui, di))
                firsts.setdefault(ui, []).append(ti)
        self._firsts = firsts

    def _window(self, store, key, t, width):
        if store is not self.user_new_edges:
            return super()._window(store, key, t, width)
        return sum(1 for x in self._firsts.get(key, ()) if t - width < x <= t + width)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--work", required=True, help="detect/graph/extract.py output")
    ap.add_argument("--scores", required=True, help="detect/graph/run.py output")
    ap.add_argument("--accounts", type=int, default=300)
    ap.add_argument("--days", type=int, default=10)
    ap.add_argument("--cuts", type=int, default=25)
    ap.add_argument("--data-label", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    # A seeded subset of accounts keeps each replay small enough to rerun at every cut. Their
    # logins still form a real, time-ordered stream from the lake.
    rows = duckdb.connect().execute(f"""
        with pick as (
            select uid from (select distinct uid from
                             read_parquet('{(Path(args.work) / 'candidates.parquet').as_posix()}'))
            order by hash(uid + 20260925) limit {args.accounts}
        )
        select time, uid, sid, did, fail::int
        from read_parquet('{(Path(args.work) / 'candidates.parquet').as_posix()}')
        where uid in (select uid from pick) and time < {args.days} * 86400
        order by time, uid, sid, did
    """).fetchall()
    ctx = FitContext.from_json(json.loads((Path(args.scores) / "context.json").read_text()))
    real = verify_point_in_time(rows, FeatureEngine, ctx, cuts=args.cuts)
    leak = verify_point_in_time(rows, LeakyControl, ctx, cuts=args.cuts)
    report = {
        "data": args.data_label,
        "stream": {"accounts": args.accounts, "days": args.days, "events": len(rows)},
        "detector": {k: v for k, v in real.items() if k != "examples"},
        "leaky_control": {k: v for k, v in leak.items() if k != "examples"},
    }
    Path(args.out).write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return 0 if real["passed"] and not leak["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
