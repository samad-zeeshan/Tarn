"""Attach to each alert the subgraph and the feature values that made it fire.

The per-feature surprises add up to the alert's score exactly, the pass-through property
arXiv 2608.15559 asks of an explanation layer. The subgraph only holds logins from before
the alert, so it shows what a live analyst could have seen.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from detect.graph.features import FEATURES, HV_CAP, FitContext
from detect.graph.model import HEADLINE, HistogramModel
from eval import protocol
from eval.score import top_candidates_sql, top_per_day

NEIGHBOURS = 8


def describe(name: str, value: int) -> str:
    """Plain words for one feature value, used on the site and in the analyst's prompt."""
    approx = 0 if value == 0 else 2 ** (value - 1)
    if name == "edge_new":
        return "first login by this account to this computer" if value else "has been here before"
    if name == "src_new":
        return "first login by this account from this source" if value else "known source"
    if name == "gap":
        return "never reached before" if value == 0 else f"last reached about {approx}s earlier"
    if name == "burst_1h":
        return f"about {approx} new computers for this account in the past hour"
    if name == "src_new_users_24h":
        return f"about {approx} accounts used this source for the first time in the past day"
    if name == "dst_hub":
        return ("computer unseen on day 0" if value == 0
                else f"about {approx} accounts used this computer on day 0")
    if name == "hv_delta":
        hops = value - 3
        if hops > 0:
            return f"{hops} hops closer to a high-value host"
        return "no closer to a high-value host" if hops == 0 else f"{-hops} hops further away"
    if name == "r_new_hosts_today":
        return "five or more new computers today (v1 rule Q3)" if value else "rule Q3 quiet"
    if name == "r_fail_1h":
        return "five or more failed logins in the hour (v1 rule Q4)" if value else "rule Q4 quiet"
    if name == "r_off_hours":
        return "inside the quiet hours (v1 rule Q2)" if value else "normal hours"
    raise KeyError(name)


def contributions(model: HistogramModel, row: pd.Series) -> list[dict]:
    X = np.array([[int(row[f]) for f in FEATURES]], dtype=int)
    parts = model.contributions(X)[0]
    names = [FEATURES[c] for c in model.columns]
    out = [
        {"feature": n, "value": int(row[n]), "meaning": describe(n, int(row[n])),
         "surprise": float(p)}
        for n, p in zip(names, parts, strict=True)
    ]
    return sorted(out, key=lambda x: -x["surprise"])


def subgraphs(con: duckdb.DuckDBPyConnection, candidates: str, alerts: pd.DataFrame) -> dict:
    """For each alert, the account's other logins and the source host's other accounts in the
    24 hours before it. One range join for all alerts instead of one query per alert."""
    con.register("_alerts", alerts[["alert_id", "time", "uid", "sid"]])
    user_edges = con.execute(f"""
        select a.alert_id, c.time, c.sid, c.did from _alerts a
        join read_parquet('{candidates}') c
          on c.uid = a.uid and c.time < a.time and c.time >= a.time - 86400
        qualify row_number() over (partition by a.alert_id order by c.time desc) <= {NEIGHBOURS}
    """).df()
    src_users = con.execute(f"""
        select a.alert_id, c.time, c.uid, c.did from _alerts a
        join read_parquet('{candidates}') c
          on c.sid = a.sid and c.uid <> a.uid and c.time < a.time and c.time >= a.time - 86400
        qualify row_number() over (partition by a.alert_id order by c.time desc) <= {NEIGHBOURS}
    """).df()
    out: dict[int, dict] = {int(i): {"account_logins": [], "source_accounts": []}
                            for i in alerts["alert_id"]}
    for r in user_edges.itertuples():
        out[int(r.alert_id)]["account_logins"].append(
            {"time": int(r.time), "sid": int(r.sid), "did": int(r.did)})
    for r in src_users.itertuples():
        out[int(r.alert_id)]["source_accounts"].append(
            {"time": int(r.time), "uid": int(r.uid), "did": int(r.did)})
    return out


def explain(con, candidates: str, alerts: pd.DataFrame, model: HistogramModel,
            users: dict[int, str], hosts: dict[int, str], hv_dist: dict[int, int]) -> list[dict]:
    """alerts needs alert_id, time, uid, sid, did, the feature columns and score."""
    graphs = subgraphs(con, candidates, alerts)
    out = []
    for _, row in alerts.iterrows():
        g = graphs[int(row["alert_id"])]
        parts = contributions(model, row)
        out.append({
            "alert_id": int(row["alert_id"]),
            "time": int(row["time"]),
            "day": int(row["time"]) // 86_400,
            "account": users[int(row["uid"])],
            "source": hosts[int(row["sid"])],
            "destination": hosts[int(row["did"])],
            "score": float(row["score"]),
            "contributions": parts,
            "explained_score": float(sum(p["surprise"] for p in parts)),
            "subgraph": {
                "account_logins": [
                    {"seconds_before": int(row["time"]) - e["time"], "source": hosts[e["sid"]],
                     "destination": hosts[e["did"]]} for e in g["account_logins"]],
                "source_accounts": [
                    {"seconds_before": int(row["time"]) - e["time"], "account": users[e["uid"]],
                     "destination": hosts[e["did"]]} for e in g["source_accounts"]],
                "hops_to_high_value": {
                    "source": hv_dist.get(int(row["sid"]), HV_CAP),
                    "destination": hv_dist.get(int(row["did"]), HV_CAP),
                },
            },
        })
    return out


def build_alerts(work: Path, out: Path, budget: int = protocol.BUDGET_PER_DAY) -> list[dict]:
    """The headline detector's top `budget` logins per day, each with its explanation."""
    con = duckdb.connect()
    scores = (out / "scores.parquet").as_posix()
    top = top_per_day(con.execute(top_candidates_sql(scores, f"score_{HEADLINE}", budget)).df(),
                      budget)
    top = top.sort_values(["time", "uid", "sid", "did"]).reset_index(drop=True)
    top["alert_id"] = np.arange(1, len(top) + 1)
    model = HistogramModel.from_json(json.loads((out / f"model_{HEADLINE}.json").read_text()))
    ctx = FitContext.from_json(json.loads((out / "context.json").read_text()))
    users = dict(zip(*pq.read_table(work / "users.parquet").to_pydict().values(), strict=True))
    hosts = dict(zip(*pq.read_table(work / "hosts.parquet").to_pydict().values(), strict=True))
    return explain(con, (work / "candidates.parquet").as_posix(), top, model, users, hosts,
                   ctx.hv_dist)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--work", required=True, help="detect/graph/extract.py output")
    ap.add_argument("--scores", required=True, help="detect/graph/run.py output")
    ap.add_argument("--budget", type=int, default=protocol.BUDGET_PER_DAY)
    ap.add_argument("--name", default="alerts.jsonl")
    args = ap.parse_args()
    alerts = build_alerts(Path(args.work), Path(args.scores), args.budget)
    path = Path(args.scores) / args.name
    with path.open("w") as fh:
        for a in alerts:
            fh.write(json.dumps(a) + "\n")
    print(f"{len(alerts):,} alerts, every one explained -> {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
