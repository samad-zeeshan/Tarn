"""Render the README's results tables from eval/results, or fail if the README has drifted.

Each table sits between <!-- results:NAME --> and <!-- /results --> markers.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
README = REPO / "README.md"
RESULTS = REPO / "eval" / "results"


def _load(name: str) -> dict:
    return json.loads((RESULTS / f"{name}.json").read_text())


def _pct(x: float) -> str:
    return f"{100 * x:.1f}%"


def _n(x) -> str:
    return f"{int(round(x)):,}"


def v1_table(det: dict) -> str:
    pub = det["v1_published"]
    prot = det["v1_protocol"]["detectors"]
    total = det["v1_protocol"]["redteam_identity_days_in_test"]
    names = [("q3_new_paths", "Q3 new access paths"), ("q1_fanout", "Q1 fan-out spike"),
             ("q4_failures", "Q4 failure spike"), ("q2_off_hours", "Q2 off-hours"),
             ("any_of_four", "Any of the four"), ("two_or_more", "Two or more")]
    rows = ["| v1 rule | v1 README: caught, alerts | fair protocol: caught, alerts | "
            "attack logins covered |", "|---|---|---|---|"]
    for key, label in names:
        p, q = pub[key], prot[key]["threshold"]
        rows.append(f"| {label} | {p['caught']} of {p['total']}, {_n(p['alerts'])} | "
                    f"{q['identity_day']['caught']} of {total}, {_n(q['identity_day']['alerts'])} | "
                    f"{q['events_covered']} of {det['labels']['in_log_test_window']} |")
    return "\n".join(rows)


def budget_table(det: dict) -> str:
    g = det["graph"]
    total_days = det["labels"]["redteam_identity_days_test_window"]
    total_ev = det["labels"]["in_log_test_window"]
    rows = ["| detector, 100 alerts a day | attack account-days caught | attack logins covered | "
            "single-login alerts: attack logins caught | average precision |",
            "|---|---|---|---|---|"]
    v1 = det["v1_protocol"]["detectors"]["any_of_four"]["budget"]
    rows.append(f"| v1, rules ranked by how many fired | {v1['identity_days_caught']} of "
                f"{total_days} | {v1['events_covered']} of {total_ev} | n/a | n/a |")
    for key, label in (("graph_plus_rules", "v2 graph detector (fixed in advance)"),
                       ("graph", "v2 without the v1 rules"),
                       ("gnn", "LightGCN graph neural network"),
                       ("rules_only", "v1 rules alone, as login features")):
        d = g[key]
        b = d["identity_day"]["budget"]
        e = d["event"]
        rows.append(f"| {label} | {b['identity_days_caught']} of {total_days} | "
                    f"{b['events_covered']} of {total_ev} | {e['budget']['caught']} of {total_ev} | "
                    f"{e['average_precision']:.5f} |")
    return "\n".join(rows)


def verifier_table(ver: dict) -> str:
    d, c = ver["detector"], ver["leaky_control"]
    return "\n".join([
        "| engine | features checked | mismatches | verdict |", "|---|---|---|---|",
        f"| v2 feature engine | {_n(d['features_checked'])} | {_n(d['mismatches'])} | "
        f"{'pass' if d['passed'] else 'FAIL'} |",
        f"| control that looks an hour ahead | {_n(c['features_checked'])} | "
        f"{_n(c['mismatches'])} | {'pass' if c['passed'] else 'fails, as it should'} |",
    ])


def analyst_table(an: dict, days: int) -> str:
    w, wo = an["with_graph_tools"], an["without_graph_tools"]
    if not w or not wo:
        return "Not run."

    def row(label, f):
        return f"| {label} | {f(w)} | {f(wo)} |"

    def hours(r):
        wl = r["workload"]
        return (f"{wl['analyst_hours_without_agent'] / days:.0f} to "
                f"{wl['analyst_hours_with_agent'] / days:.0f}")

    return "\n".join([
        f"| {an['alerts_scored']} of {an['benchmark_size']} alerts scored, "
        f"{w['attack_alerts']} of them attacks | with graph tools | without |", "|---|---|---|",
        row("right, when it decided", lambda r: _pct(r["accuracy_on_decided"])),
        row("handed to a person", lambda r: _pct(r["escalation_rate"])),
        row("attacks closed as false alarms at confidence 0.8 or more",
            lambda r: str(r["attacks_closed_as_benign"])),
        row("launch host and path right, on attacks", lambda r: _pct(r["path_correct_on_attacks"])),
        row("calibration error (ECE, lower is better)", lambda r: f"{r['calibration']['ece']:.2f}"),
        row("tool calls, invented calls refused, tokens per alert",
            lambda r: f"{r['tool_calls_per_alert']:.1f}, {r['hallucinated_calls']}, "
                      f"{_n(r['tokens_per_alert'])}"),
        row("analyst hours a day for the 1,000-a-day feed, before and after the agent", hours),
        row("attack alerts still called attacks or passed to a person",
            lambda r: _pct(r["cascade"]["attack_recall"])),
    ])


def render_all() -> dict[str, str]:
    det = _load("detectors")
    return {
        "v1": v1_table(det),
        "budget": budget_table(det),
        "verifier": verifier_table(_load("verifier")),
        "analyst": analyst_table(_load("analyst"), det["protocol"]["test_days"]),
    }


# The body is matched lazily up to the first closing marker, so an empty block cannot swallow
# the prose between it and the next table.
PATTERN = re.compile(r"(<!-- results:(\w+) -->\n)(.*?)(<!-- /results -->)", re.S)


def apply(text: str, tables: dict[str, str]) -> str:
    return PATTERN.sub(lambda m: m.group(1) + tables[m.group(2)] + "\n" + m.group(4), text)


def check() -> list[str]:
    text = README.read_text(encoding="utf-8")
    tables = render_all()
    found = {m.group(2) for m in PATTERN.finditer(text)}
    problems = [f"README has no results:{k} block" for k in tables if k not in found]
    if apply(text, tables) != text:
        problems.append("README tables differ from eval/results, run python eval/readme.py")
    return problems


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()
    if args.check:
        problems = check()
        for p in problems:
            print(p)
        return 1 if problems else 0
    README.write_text(apply(README.read_text(encoding="utf-8"), render_all()), encoding="utf-8")
    print("README tables regenerated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
