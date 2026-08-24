"""The analyst agent: closed-world tool registry, point-in-time tools, the loop, and its scoring.

No model is called here. A scripted model plays the agent's side so CI stays offline.
"""

from __future__ import annotations

import json

import numpy as np
import pytest

from analyst.agent import Agent, ScriptedLLM, parse_reply
from analyst.benchmark import ground_truth, score_runs
from analyst.registry import GRAPH_TOOLS, Registry
from analyst.tools import ToolStore

# Accounts 1-3, hosts 10-14. Account 1 walks 10 -> 11 -> 12. Account 2 appears on host 10
# later, and one login happens after the alert time so the tools must never show it.
EVENTS = [
    (100, 1, 10, 11), (200, 1, 11, 12), (300, 2, 10, 13), (400, 3, 14, 13), (900, 1, 10, 14),
]
USERS = {1: "U1@D", 2: "U2@D", 3: "U3@D"}
HOSTS = {10: "C10", 11: "C11", 12: "C12", 13: "C13", 14: "C14"}
ALERTS = [
    {"alert_id": 1, "time": 300, "account": "U2@D", "source": "C10", "destination": "C13",
     "score": 20.0},
    {"alert_id": 2, "time": 250, "account": "U1@D", "source": "C10", "destination": "C11",
     "score": 15.0},
]


@pytest.fixture()
def store():
    t, u, s, d = (np.array(c) for c in zip(*EVENTS, strict=True))
    return ToolStore.from_arrays(t, u, s, d, USERS, HOSTS, ALERTS)


def test_tools_never_see_past_the_alert(store):
    reg = Registry(store, as_of=500)
    hist = reg.call("recent_auth_history", {"account": "U1@D"})
    assert [h["destination"] for h in hist["result"]["logins"]] == ["C12", "C11"]
    reached = reg.call("hosts_reached", {"account": "U1@D"})
    assert "C14" not in [h["host"] for h in reached["result"]["hosts"]]


def test_path_to_uses_only_edges_that_existed(store):
    early = Registry(store, as_of=150).call("path_to", {"source": "C10", "destination": "C12"})
    assert early["result"]["path"] is None
    later = Registry(store, as_of=500).call("path_to", {"source": "C10", "destination": "C12"})
    assert later["result"]["path"] == ["C10", "C11", "C12"]


def test_similar_alerts_only_shows_earlier_alerts_on_the_same_source(store):
    got = Registry(store, as_of=300, alert_id=1).call("similar_alerts", {"host": "C10"})
    assert [a["account"] for a in got["result"]["alerts"]] == ["U1@D"]


def test_unknown_tool_is_counted_and_never_executed(store):
    reg = Registry(store, as_of=500)
    got = reg.call("delete_account", {"account": "U1@D"})
    assert got["error"].startswith("unknown tool")
    assert reg.hallucinations == [{"kind": "unknown_tool", "tool": "delete_account"}]
    assert reg.executed == []


def test_bad_arguments_are_rejected_before_execution(store):
    reg = Registry(store, as_of=500)
    reg.call("who_is", {"acount": "U1@D"})
    reg.call("who_is", {"account": 7})
    kinds = [h["kind"] for h in reg.hallucinations]
    assert kinds == ["bad_arguments", "bad_arguments"]
    assert reg.executed == []


def test_ablation_registry_has_no_graph_tools(store):
    reg = Registry(store, as_of=500, graph_tools=False)
    assert not set(reg.names()) & set(GRAPH_TOOLS)
    reg.call("path_to", {"source": "C10", "destination": "C12"})
    assert reg.hallucinations[0]["kind"] == "unknown_tool"


def test_reply_parser_finds_the_json_object():
    assert parse_reply('Sure. {"tool": "who_is", "args": {"account": "U1@D"}}')["tool"] == "who_is"
    assert parse_reply("no json here") is None


def test_agent_loop_runs_tools_then_returns_a_verdict(store):
    script = [
        '{"tool": "similar_alerts", "args": {"host": "C10"}}',
        '{"tool": "made_up_tool", "args": {}}',
        json.dumps({"verdict": "true_positive", "confidence": 0.9, "account": "U2@D",
                    "launch_host": "C10", "path": ["C10", "C13"], "linked_accounts": ["U1@D"],
                    "reason": "shared source"}),
    ]
    run = Agent(ScriptedLLM(script)).triage(ALERTS[0], store)
    assert run["verdict"]["verdict"] == "true_positive"
    assert run["tool_calls"] == 2
    assert run["hallucinated_calls"] == 1
    assert run["executed_calls"] == 1


def test_agent_that_never_answers_escalates(store):
    run = Agent(ScriptedLLM(['{"tool": "who_is", "args": {"account": "U2@D"}}'] * 20),
                max_steps=3).triage(ALERTS[0], store)
    assert run["verdict"]["verdict"] == "needs_human"
    assert run["verdict"]["confidence"] == 0.0


def test_ground_truth_is_computed_from_labels():
    labels = [
        {"time": 100, "src_user": "U9@D", "src_computer": "C10", "dst_computer": "C11"},
        {"time": 300, "src_user": "U2@D", "src_computer": "C10", "dst_computer": "C13"},
    ]
    truth = ground_truth(ALERTS[0], labels)
    assert truth == {"is_attack": True, "account": "U2@D", "launch_host": "C10",
                     "destination": "C13", "linked_accounts": ["U9@D"]}
    assert ground_truth(ALERTS[1], labels)["is_attack"] is False


def test_scoring_accuracy_calibration_and_cascade():
    runs = [
        {"truth": {"is_attack": True, "launch_host": "C1", "destination": "C2",
                   "linked_accounts": ["U5"]},
         "verdict": {"verdict": "true_positive", "confidence": 0.9, "launch_host": "C1",
                     "path": ["C1", "C2"], "linked_accounts": ["U5"]},
         "tool_calls": 2, "hallucinated_calls": 0, "tokens": 100},
        {"truth": {"is_attack": False},
         "verdict": {"verdict": "false_positive", "confidence": 0.95},
         "tool_calls": 1, "hallucinated_calls": 1, "tokens": 50},
        {"truth": {"is_attack": False},
         "verdict": {"verdict": "true_positive", "confidence": 0.6},
         "tool_calls": 3, "hallucinated_calls": 0, "tokens": 70},
        {"truth": {"is_attack": True, "launch_host": "C1", "destination": "C3",
                   "linked_accounts": []},
         "verdict": {"verdict": "needs_human", "confidence": 0.2},
         "tool_calls": 0, "hallucinated_calls": 0, "tokens": 30},
    ]
    s = score_runs(runs, accept_at=0.8, stream={"attack": 10, "benign": 990})
    assert s["decided"] == 3
    assert s["accuracy_on_decided"] == pytest.approx(2 / 3)
    assert s["escalation_rate"] == pytest.approx(1 / 4)
    assert s["path_correct_on_attacks"] == pytest.approx(1 / 2)
    assert s["attacks_closed_as_benign"] == 0
    assert s["hallucinated_calls"] == 1
    # The 0.6 verdict and the needs_human one go to a person, the other two are accepted.
    assert s["cascade"]["escalated"] == 2
    # Attacks: one accepted as an attack, one escalated. Benign: one closed, one escalated.
    assert s["cascade"]["attack_recall"] == pytest.approx(1.0)
    assert s["workload"]["alerts_read_without_agent"] == 1000
    assert s["workload"]["alerts_read_with_agent"] == pytest.approx(10 * 0.5 + 990 * 0.5)
