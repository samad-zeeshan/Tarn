"""The analyst agent: reads one alert and its explanation, calls tools, returns a verdict.

The model speaks plain JSON rather than a native tool-calling API. That is the surface where
arXiv 2609.19425 saw most invented tools, so the registry has something real to catch.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

from analyst.registry import Registry
from analyst.tools import ToolStore

VERDICTS = ("true_positive", "false_positive", "needs_human")

SYSTEM = """You are a security analyst triaging login alerts from a lateral-movement detector.
Most alerts are false alarms. Some are an attacker using stolen accounts from a foothold host.
You cannot see the network yourself. Ask the tools, which answer as of the alert time.

Tools:
{tools}

Reply with exactly one JSON object and nothing else. To call a tool:
{{"tool": "<name>", "args": {{...}}}}
When you are done, give the verdict:
{{"verdict": "true_positive" | "false_positive" | "needs_human", "confidence": <0 to 1>,
 "account": "<compromised account or empty>", "launch_host": "<host the attacker worked from>",
 "path": ["<launch host>", "...", "<destination>"], "linked_accounts": ["<other accounts
 used from the same launch host>"], "reason": "<one sentence>"}}
Confidence is your probability that the verdict is right. Use needs_human when unsure.
You may call at most {steps} tools."""


def parse_reply(text: str) -> dict | None:
    """The first balanced JSON object in the reply, or None."""
    if not text:
        return None
    start = text.find("{")
    while start != -1:
        depth = 0
        for i in range(start, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        obj = json.loads(text[start:i + 1])
                    except json.JSONDecodeError:
                        break
                    return obj if isinstance(obj, dict) else None
        start = text.find("{", start + 1)
    return None


class ScriptedLLM:
    """Plays back fixed replies. Used by the tests so CI never needs a model."""

    def __init__(self, replies: list[str]):
        self.replies = list(replies)
        self.i = 0

    def chat(self, messages: list[dict]) -> tuple[str, int]:
        reply = self.replies[min(self.i, len(self.replies) - 1)]
        self.i += 1
        return reply, 0


class LMStudioLLM:
    """A local model behind LM Studio's OpenAI-compatible endpoint, reasoning switched off."""

    def __init__(self, model: str, url: str = "http://localhost:1234/v1/chat/completions"):
        self.model = model
        self.url = url

    def chat(self, messages: list[dict]) -> tuple[str, int]:
        body = {
            "model": self.model, "messages": messages, "temperature": 0,
            "max_tokens": 400,
            # Qwen 3.5 ignores enable_thinking in LM Studio but honours this. Without it every
            # turn spends its whole token budget thinking and returns an empty reply.
            "reasoning_effort": "none",
        }
        req = urllib.request.Request(self.url, data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
        # LM Studio answers 500 while it swaps models in and out of memory. Waiting it out is
        # fine for a benchmark that already takes hours, and a run that never recovers still
        # fails loudly after twenty minutes.
        for attempt in range(40):
            try:
                with urllib.request.urlopen(req, timeout=300) as r:
                    out = json.load(r)
                break
            except (urllib.error.URLError, TimeoutError):
                if attempt == 39:
                    raise
                time.sleep(30)
        return out["choices"][0]["message"].get("content") or "", out["usage"]["total_tokens"]


def alert_prompt(alert: dict) -> str:
    parts = [f"- {c['feature']}: {c['meaning']} (surprise {c['surprise']:.1f})"
             for c in alert.get("contributions", [])[:6]]
    return (
        f"Alert {alert['alert_id']}: account {alert['account']} logged into "
        f"{alert['destination']} from {alert['source']}, day {alert['time'] // 86_400}, "
        f"score {alert['score']:.1f}.\nWhy it fired:\n" + "\n".join(parts)
    )


def _normalise(v: dict | None) -> dict:
    if not v or v.get("verdict") not in VERDICTS:
        return {"verdict": "needs_human", "confidence": 0.0, "reason": "no valid verdict"}
    try:
        conf = min(1.0, max(0.0, float(v.get("confidence", 0.0))))
    except (TypeError, ValueError):
        conf = 0.0
    path = v.get("path") if isinstance(v.get("path"), list) else []
    linked = v.get("linked_accounts") if isinstance(v.get("linked_accounts"), list) else []
    return {
        "verdict": v["verdict"], "confidence": conf,
        "account": str(v.get("account") or ""), "launch_host": str(v.get("launch_host") or ""),
        "path": [str(p) for p in path], "linked_accounts": [str(a) for a in linked],
        "reason": str(v.get("reason") or "")[:300],
    }


class Agent:
    def __init__(self, llm, max_steps: int = 6, graph_tools: bool = True):
        self.llm = llm
        self.max_steps = max_steps
        self.graph_tools = graph_tools

    def triage(self, alert: dict, store: ToolStore) -> dict:
        reg = Registry(store, as_of=alert["time"], alert_id=alert["alert_id"],
                       graph_tools=self.graph_tools)
        messages = [
            {"role": "system", "content": SYSTEM.format(tools=reg.describe(),
                                                        steps=self.max_steps)},
            {"role": "user", "content": alert_prompt(alert)},
        ]
        tokens, calls, transcript = 0, 0, []
        verdict = None
        t0 = time.perf_counter()
        # max_steps tool turns, one turn to answer, and one more after being told the tools
        # are spent. Without that last turn a model that asks for a seventh tool never answers.
        for _ in range(self.max_steps + 2):
            reply, used = self.llm.chat(messages)
            tokens += used
            obj = parse_reply(reply)
            transcript.append(reply)
            messages.append({"role": "assistant", "content": reply})
            if obj and "verdict" in obj:
                verdict = obj
                break
            if obj and "tool" in obj and calls < self.max_steps:
                calls += 1
                result = reg.call(obj.get("tool"), obj.get("args", {}))
                messages.append({"role": "user", "content": json.dumps(result)[:3000]})
                continue
            if calls >= self.max_steps:
                messages.append({"role": "user", "content": "No tools left. Give the verdict."})
            else:
                messages.append({"role": "user", "content": "Reply with one JSON object."})
        return {
            "alert_id": alert["alert_id"],
            "verdict": _normalise(verdict),
            "tool_calls": calls,
            "executed_calls": len(reg.executed),
            "tools_used": reg.executed,
            "hallucinated_calls": len(reg.hallucinations),
            "hallucinations": reg.hallucinations,
            "tokens": tokens,
            "seconds": round(time.perf_counter() - t0, 2),
            "transcript": transcript,
        }
