"""Replay a stream and fail if any feature changes when the future is cut off.

KONTOGRAPH (arXiv 2608.22389) caught point-in-time bugs that review had missed by perturbing
the future. This does the same with truncation at random cut points.
"""

from __future__ import annotations

import random


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
