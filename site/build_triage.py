"""Build site/data/triage.json: two days of alerts, one explained alert, and the agent's verdicts.

Every number comes from eval/results or a recorded run. The page computes nothing new.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
RESULTS = REPO / "eval" / "results"
RUNS = REPO / "analyst" / "runs"
NORMAL_DAY = 40
ATTACK_DAY = 8


def _key(a: dict) -> tuple:
    return (a["time"], a["account"], a["source"], a["destination"])


def build(alerts_path: Path, work: Path) -> dict:
    labels = {
        tuple(r) for r in duckdb.connect().execute(
            f"select time, src_user, src_computer, dst_computer "
            f"from read_parquet('{(work / 'labels.parquet').as_posix()}')").fetchall()
    }
    alerts = [json.loads(x) for x in alerts_path.read_text().splitlines()]
    det = json.loads((RESULTS / "detectors.json").read_text())
    analyst = json.loads((RESULTS / "analyst.json").read_text())

    bench = json.loads((RUNS / "benchmark.json").read_text())
    runs = {}
    for name in ("graph", "nograph"):
        path = RUNS / f"runs_{name}.jsonl"
        if path.exists():
            for line in path.read_text().splitlines():
                r = json.loads(line)
                runs[(name, r["alert_id"])] = {k: r[k] for k in (
                    "verdict", "tool_calls", "tools_used", "hallucinated_calls", "tokens")}

    def day(d: int) -> dict:
        rows = [a for a in alerts if a["day"] == d]
        attack_events = sum(1 for lb in labels if lb[0] // 86_400 == d)
        return {
            "day": d,
            "attack_events_that_day": attack_events,
            "alerts": [{
                "alert_id": a["alert_id"], "time": a["time"], "account": a["account"],
                "source": a["source"], "destination": a["destination"],
                "score": round(a["score"], 2), "is_attack": _key(a) in labels,
                "top_reason": a["contributions"][0]["meaning"],
            } for a in rows],
        }

    attack = day(ATTACK_DAY)
    hit = next((a for a in alerts if a["day"] == ATTACK_DAY and _key(a) in labels), None)
    # The verdict view shows a real attack both agents finished, preferring one the agent with
    # graph tools got right, so the page shows the difference the tools make when there is one.
    done = [i for i in bench["items"] if i["truth"]["is_attack"]
            and ("graph", i["alert_id"]) in runs and ("nograph", i["alert_id"]) in runs]
    done.sort(key=lambda i: runs[("graph", i["alert_id"])]["verdict"]["verdict"] != "true_positive")
    pick = done[0] if done else None
    verdicts = {name: runs.get((name, pick["alert_id"])) if pick else None
                for name in ("graph", "nograph")}
    if pick:
        a = pick["alert"]
        verdicts["alert"] = {k: a[k] for k in ("account", "source", "destination", "day")}
        verdicts["truth"] = pick["truth"]

    g = det["graph"]
    v1 = det["v1_protocol"]["detectors"]
    return {
        "data": det["data"],
        "labels": det["labels"],
        "budget_per_day": det["protocol"]["budget_per_day"],
        "triage_minutes": det["protocol"]["triage_minutes_per_alert"],
        "compare": {
            "v1_best": v1["any_of_four"]["budget"],
            "headline": g["graph_plus_rules"]["identity_day"]["budget"],
            "graph_only": g["graph"]["identity_day"]["budget"],
            "gnn": g["gnn"]["identity_day"]["budget"],
        },
        "normal_day": day(NORMAL_DAY),
        "attack_day": attack,
        "explained": hit,
        "verdicts": verdicts,
        "analyst": {
            "model": analyst["model"],
            "size": analyst["benchmark_size"],
            "scored": analyst["alerts_scored"],
            "with": analyst["with_graph_tools"],
            "without": analyst["without_graph_tools"],
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--alerts", required=True, help="alerts.jsonl from detect/graph/explain.py")
    ap.add_argument("--work", required=True, help="detect/graph/extract.py output")
    args = ap.parse_args()
    out = REPO / "site" / "data" / "triage.json"
    out.write_text(json.dumps(build(Path(args.alerts), Path(args.work)), indent=1) + "\n")
    print(f"wrote {out} ({out.stat().st_size / 1e3:.0f} kB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
