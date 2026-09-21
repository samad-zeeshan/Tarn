"""Build site/data/night.json: one day of the committed slice as a user-to-computer graph, with the analyst's alerts.

Reads only committed files, so anyone with the repo can rebuild it byte for byte.
"""

from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path

import numpy as np

REPO = Path(__file__).resolve().parent.parent
SAMPLE = REPO / "data" / "sample"
RUNS = REPO / "analyst" / "runs"
OUT = REPO / "site" / "data" / "night.json"

# Day 1 is the only day where the committed slice, the red team and the agent's finished
# benchmark runs all overlap: 10 attack logins and 11 alerts the agent read with graph tools.
DAY = 1
SEED = 20150101
DAY_S = 86_400


def read_logins(day0: int) -> list[tuple[int, str, str, str]]:
    rows = []
    with gzip.open(SAMPLE / "auth_sample.csv.gz", "rt", newline="") as fh:
        for r in csv.DictReader(fh):
            t = int(r["time"])
            # People only, and only logins that cross machines. A login from a computer to itself
            # draws a loop, and machine accounts ($) are the background hum, not the night shift.
            if not (day0 <= t < day0 + DAY_S) or not r["src_user"].startswith("U"):
                continue
            if r["src_computer"] == r["dst_computer"]:
                continue
            rows.append((t - day0, r["src_user"], r["src_computer"], r["dst_computer"]))
    return rows


def read_attacks(day0: int) -> list[tuple[int, str, str, str]]:
    with gzip.open(SAMPLE / "redteam_sample.csv.gz", "rt", newline="") as fh:
        return [(int(r["time"]) - day0, r["user"], r["src_computer"], r["dst_computer"])
                for r in csv.DictReader(fh) if day0 <= int(r["time"]) < day0 + DAY_S]


def read_alerts(day0: int) -> list[dict]:
    items = {i["alert_id"]: i for i in json.loads((RUNS / "benchmark.json").read_text())["items"]}
    runs = {}
    for arm in ("graph", "nograph"):
        for line in (RUNS / f"runs_{arm}.jsonl").read_text().splitlines():
            r = json.loads(line)
            runs[(arm, r["alert_id"])] = r
    out = []
    for (arm, aid), r in runs.items():
        a = items[aid]["alert"]
        if arm != "graph" or a["day"] != DAY:
            continue
        v = r["verdict"]
        other = runs.get(("nograph", aid))
        out.append({
            "id": aid, "t": a["time"] - day0, "account": a["account"],
            "source": a["source"], "destination": a["destination"],
            "score": round(a["score"], 2), "reason": a["contributions"][0]["meaning"],
            "attack": bool(items[aid]["truth"]["is_attack"]),
            "call": v["verdict"], "confidence": v["confidence"], "why": v["reason"],
            "path": v.get("path", []), "tools": r["tool_calls"], "tokens": r["tokens"],
            "call_without_tools": other["verdict"]["verdict"] if other else None,
        })
    return sorted(out, key=lambda a: a["t"])


def layout(n: int, edges: np.ndarray, weights: np.ndarray, iters: int = 420) -> np.ndarray:
    """Fruchterman-Reingold with a weak pull to the centre, seeded so the picture never reshuffles."""
    rng = np.random.default_rng(SEED)
    pos = rng.uniform(-1, 1, (n, 2)).astype(np.float64)
    k = np.sqrt(4.0 / n)
    temp = 0.12
    for i in range(iters):
        disp = np.zeros_like(pos)
        # Repulsion in row blocks, so a 2,600 node graph fits in memory as float64.
        for s in range(0, n, 512):
            d = pos[s:s + 512, None, :] - pos[None, :, :]
            dist2 = (d ** 2).sum(-1) + 1e-6
            disp[s:s + 512] += (d * (k * k / dist2)[..., None]).sum(1)
        d = pos[edges[:, 0]] - pos[edges[:, 1]]
        dist = np.sqrt((d ** 2).sum(-1)) + 1e-9
        # Log weight: a pair that logged in forty times sits closer, but does not collapse.
        pull = (d * (dist * (1 + np.log1p(weights)) / k)[:, None])
        np.add.at(disp, edges[:, 0], -pull)
        np.add.at(disp, edges[:, 1], pull)
        disp -= pos * 0.9 * n * k * 0.02
        length = np.sqrt((disp ** 2).sum(-1)) + 1e-9
        pos += disp / length[:, None] * np.minimum(length, temp)[:, None]
        temp = max(0.004, temp * 0.992) if i > 40 else temp
    lo, hi = pos.min(0), pos.max(0)
    return (pos - lo) / (hi - lo).max()


def build() -> dict:
    day0 = DAY * DAY_S
    logins = read_logins(day0)
    attacks = read_attacks(day0)
    alerts = read_alerts(day0)

    index: dict[str, int] = {}
    kind: list[str] = []

    def node(name: str, k: str) -> int:
        if name not in index:
            index[name] = len(kind)
            kind.append(k)
        return index[name]

    edge_of: dict[tuple[int, int], int] = {}
    edge_list: list[list[int]] = []
    events: list[list[int]] = []

    def edge(a: int, b: int) -> int:
        key = (a, b)
        if key not in edge_of:
            edge_of[key] = len(edge_list)
            edge_list.append([a, b, 0])
        return edge_of[key]

    for t, user, src, dst in sorted(logins):
        u, s, d = node(user, "u"), node(src, "c"), node(dst, "c")
        edge(u, s)
        e = edge(u, d)
        edge_list[e][2] += 1
        events.append([t, e])

    attack_out = []
    for t, user, src, dst in sorted(attacks):
        u, s, d = node(user, "u"), node(src, "c"), node(dst, "c")
        edge(u, s)
        attack_out.append({"t": t, "account": user, "source": src, "destination": dst,
                           "edge": edge(u, d), "src_node": s, "dst_node": d})
    for a in alerts:
        u, s, d = node(a["account"], "u"), node(a["source"], "c"), node(a["destination"], "c")
        edge(u, s)
        a.update(edge=edge(u, d), src_node=s, dst_node=d, account_node=u)

    e = np.array([[a, b] for a, b, _ in edge_list], dtype=np.int64)
    w = np.array([c for *_, c in edge_list], dtype=np.float64)
    pos = layout(len(kind), e, w)

    return {
        "what": f"Day {DAY} of the committed slice: every person-to-computer login that crosses "
                "machines, the red team's logins that day, and the alerts the analyst agent read.",
        "day": DAY,
        "day_seconds": DAY_S,
        "sample_rate": json.loads((SAMPLE / "manifest.json").read_text())["selection"]["modulus"],
        "logins_drawn": len(logins),
        "names": list(index),
        "kind": "".join(kind),
        "x": [round(float(v), 4) for v in pos[:, 0]],
        "y": [round(float(v), 4) for v in pos[:, 1]],
        "edges": edge_list,
        "events": events,
        "attacks": attack_out,
        "alerts": alerts,
    }


def main() -> int:
    OUT.write_text(json.dumps(build(), separators=(",", ":")) + "\n")
    print(f"wrote {OUT} ({OUT.stat().st_size / 1e3:.0f} kB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
