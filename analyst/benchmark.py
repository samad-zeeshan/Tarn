"""Build the analyst benchmark, run the agent on it, and score the committed runs.

The answers are computed by code from the red-team labels, never by a model, the shape of
the Era by Eon benchmark (arXiv 2609.30055). The agent never sees the labels.
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

import numpy as np

from eval import protocol

REPO = Path(__file__).resolve().parent.parent
BENCH_DIR = REPO / "analyst" / "runs"
ACCEPT_AT = 0.8
SIZE = 600
SEED = 20260925


def ground_truth(alert: dict, labels: list[dict]) -> dict:
    """Is the alert one of the labelled events, and if so which account, host and path."""
    hit = any(
        lb["time"] == alert["time"] and lb["src_user"] == alert["account"]
        and lb["src_computer"] == alert["source"] and lb["dst_computer"] == alert["destination"]
        for lb in labels
    )
    if not hit:
        return {"is_attack": False}
    # The hidden fact: other accounts the attacker had already used from the same foothold in
    # the day before. Nothing in the alert states it, the logins and other alerts imply it.
    linked = sorted({
        lb["src_user"] for lb in labels
        if lb["src_computer"] == alert["source"] and lb["src_user"] != alert["account"]
        and alert["time"] - 86_400 <= lb["time"] < alert["time"]
    })
    return {"is_attack": True, "account": alert["account"], "launch_host": alert["source"],
            "destination": alert["destination"], "linked_accounts": linked}


def pick(alerts: list[dict], labels: list[dict], size: int = SIZE, seed: int = SEED) -> list:
    """Every attack alert in the stream, topped up with a seeded sample of false alarms."""
    truths = [(a, ground_truth(a, labels)) for a in alerts]
    attacks = [x for x in truths if x[1]["is_attack"]]
    benign = [x for x in truths if not x[1]["is_attack"]]
    rng = random.Random(seed)
    chosen = attacks + rng.sample(benign, max(0, min(len(benign), size - len(attacks))))
    return sorted(chosen, key=lambda x: x[0]["alert_id"])


def _path_ok(truth: dict, v: dict) -> bool:
    path = v.get("path") or []
    return (v.get("launch_host") == truth["launch_host"] and len(path) >= 2
            and path[0] == truth["launch_host"] and path[-1] == truth["destination"])


def _ece(conf: np.ndarray, correct: np.ndarray, bins: int = 10) -> tuple[float, list[dict]]:
    edges = np.linspace(0, 1, bins + 1)
    idx = np.clip(np.digitize(conf, edges[1:-1]), 0, bins - 1)
    ece, curve = 0.0, []
    for b in range(bins):
        m = idx == b
        if not m.any():
            continue
        gap = abs(conf[m].mean() - correct[m].mean())
        ece += m.mean() * gap
        curve.append({"bin": [round(edges[b], 1), round(edges[b + 1], 1)], "n": int(m.sum()),
                      "confidence": float(conf[m].mean()), "accuracy": float(correct[m].mean())})
    return float(ece), curve


def score_runs(runs: list[dict], accept_at: float = ACCEPT_AT, stream: dict | None = None) -> dict:
    """Accuracy, path correctness, calibration, tool use, escalation and the cascade."""
    n = len(runs)
    is_attack = np.array([r["truth"]["is_attack"] for r in runs])
    verdict = [r["verdict"]["verdict"] for r in runs]
    conf = np.array([r["verdict"]["confidence"] for r in runs], dtype=float)
    decided = np.array([v != "needs_human" for v in verdict])
    says_attack = np.array([v == "true_positive" for v in verdict])
    correct = says_attack == is_attack
    ece, curve = _ece(conf[decided], correct[decided].astype(float))

    accepted = decided & (conf >= accept_at)
    escalated = ~accepted
    attacks = int(is_attack.sum())
    benign = n - attacks
    path_ok = [_path_ok(r["truth"], r["verdict"]) for r in runs if r["truth"]["is_attack"]]
    linked = []
    for r in runs:
        want = set(r["truth"].get("linked_accounts") or [])
        if r["truth"]["is_attack"] and want:
            linked.append(len(want & set(r["verdict"].get("linked_accounts") or [])) / len(want))

    esc_attack = float(escalated[is_attack].mean()) if attacks else 0.0
    esc_benign = float(escalated[~is_attack].mean()) if benign else 0.0
    stream = stream or {"attack": attacks, "benign": benign}
    out = {
        "alerts": n,
        "attack_alerts": attacks,
        "benign_alerts": benign,
        "decided": int(decided.sum()),
        "accuracy_on_decided": float(correct[decided].mean()) if decided.any() else 0.0,
        "attack_recall_on_decided": float(says_attack[is_attack & decided].mean())
        if (is_attack & decided).any() else 0.0,
        "escalation_rate": float((~decided).mean()),
        "attacks_closed_as_benign": int((accepted & is_attack & ~says_attack).sum()),
        "path_correct_on_attacks": float(np.mean(path_ok)) if path_ok else 0.0,
        "linked_account_recall": float(np.mean(linked)) if linked else None,
        "calibration": {"ece": ece, "curve": curve},
        "tool_calls_per_alert": float(np.mean([r["tool_calls"] for r in runs])),
        "hallucinated_calls": int(sum(r["hallucinated_calls"] for r in runs)),
        "tokens_per_alert": float(np.mean([r["tokens"] for r in runs])),
        "cascade": {
            "accept_at": accept_at,
            "accepted": int(accepted.sum()),
            "escalated": int(escalated.sum()),
            "escalated_attack_share": esc_attack,
            "escalated_benign_share": esc_benign,
            # An attack survives the cascade if the agent confidently called it an attack or
            # handed it to a person. Only a confident "benign" loses it.
            "attack_recall": float((escalated | says_attack)[is_attack].mean()) if attacks
            else 0.0,
        },
        "workload": {
            "alerts_read_without_agent": stream["attack"] + stream["benign"],
            "alerts_read_with_agent": stream["attack"] * esc_attack + stream["benign"] * esc_benign,
            "triage_minutes_per_alert": protocol.TRIAGE_MINUTES,
        },
    }
    w = out["workload"]
    w["analyst_hours_without_agent"] = w["alerts_read_without_agent"] * protocol.TRIAGE_MINUTES / 60
    w["analyst_hours_with_agent"] = w["alerts_read_with_agent"] * protocol.TRIAGE_MINUTES / 60
    return out


def run(alerts_path: Path, work: Path, model: str, out: Path, graph_tools: bool,
        limit: int | None = None) -> None:
    from analyst.agent import Agent, LMStudioLLM
    from analyst.tools import ToolStore

    alerts = [json.loads(x) for x in alerts_path.read_text().splitlines()]
    bench = json.loads((out.parent / "benchmark.json").read_text())
    chosen = {b["alert_id"] for b in bench["items"]}
    by_id = {a["alert_id"]: a for a in alerts}
    store = ToolStore.from_work(work, alerts)
    agent = Agent(LMStudioLLM(model), graph_tools=graph_tools)
    done = set()
    if out.exists():
        done = {json.loads(x)["alert_id"] for x in out.read_text().splitlines() if x.strip()}
    todo = [i for i in sorted(chosen) if i not in done][:limit]
    with out.open("a") as fh:
        for k, aid in enumerate(todo, 1):
            r = agent.triage(by_id[aid], store)
            r["model"] = model
            r["graph_tools"] = graph_tools
            fh.write(json.dumps(r) + "\n")
            fh.flush()
            if k % 20 == 0:
                print(f"  {k}/{len(todo)} {r['seconds']}s", flush=True)


def build(alerts_path: Path, work: Path, out: Path) -> dict:
    import duckdb

    alerts = [json.loads(x) for x in alerts_path.read_text().splitlines()]
    labels = duckdb.connect().execute(
        f"select time, src_user, src_computer, dst_computer from "
        f"read_parquet('{(work / 'labels.parquet').as_posix()}')").df().to_dict("records")
    chosen = pick(alerts, labels)
    stream_attacks = sum(1 for a in alerts if ground_truth(a, labels)["is_attack"])
    bench = {
        "size": len(chosen),
        "seed": SEED,
        "stream": {"attack": stream_attacks, "benign": len(alerts) - stream_attacks},
        "items": [{"alert_id": a["alert_id"], "truth": t, "alert": a} for a, t in chosen],
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "benchmark.json").write_text(json.dumps(bench, indent=1) + "\n")
    return bench


def score(out_dir: Path, result: Path, data_label: str, model: str) -> dict:
    bench = json.loads((out_dir / "benchmark.json").read_text())
    truth = {b["alert_id"]: b["truth"] for b in bench["items"]}
    results = {"data": data_label, "model": model, "benchmark_size": bench["size"],
               "alert_stream": bench["stream"], "accept_at": ACCEPT_AT}
    arms = {}
    for name, fname in (("with_graph_tools", "runs_graph.jsonl"),
                        ("without_graph_tools", "runs_nograph.jsonl")):
        path = out_dir / fname
        runs = [json.loads(x) for x in path.read_text().splitlines() if x.strip()]             if path.exists() else []
        arms[name] = {r["alert_id"]: r for r in runs if r["alert_id"] in truth}
    # Both arms are scored on the same alerts, the ones each has finished. Otherwise a run that
    # stopped early would be compared against a different, larger set of alerts.
    common = sorted(set.intersection(*(set(a) for a in arms.values()))) if all(arms.values())         else []
    results["alerts_scored"] = len(common)
    results["complete"] = len(common) == bench["size"]
    for name, runs in arms.items():
        picked = [{**runs[i], "truth": truth[i]} for i in common]
        results[name] = score_runs(picked, stream=bench["stream"]) if picked else None
    result.write_text(json.dumps(results, indent=2) + "\n")
    return results


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("step", choices=["build", "run", "score"])
    ap.add_argument("--alerts", default=None, help="alerts.jsonl from detect/graph/explain.py")
    ap.add_argument("--work", default=None)
    ap.add_argument("--model", default="qwen/qwen3.5-9b")
    ap.add_argument("--no-graph-tools", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--data-label", default="")
    ap.add_argument("--out", default=str(BENCH_DIR))
    ap.add_argument("--result", default=str(REPO / "eval" / "results" / "analyst.json"))
    args = ap.parse_args()
    out = Path(args.out)
    if args.step == "build":
        b = build(Path(args.alerts), Path(args.work), out)
        print(f"{b['size']} alerts, {sum(i['truth']['is_attack'] for i in b['items'])} attacks")
    elif args.step == "run":
        name = "runs_nograph.jsonl" if args.no_graph_tools else "runs_graph.jsonl"
        run(Path(args.alerts), Path(args.work), args.model, out / name,
            graph_tools=not args.no_graph_tools, limit=args.limit)
    else:
        print(json.dumps(score(out, Path(args.result), args.data_label, args.model), indent=2)[:3000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
