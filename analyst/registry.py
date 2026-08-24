"""The closed-world tool registry. A call runs only if the tool exists and its arguments fit.

Every other call is recorded as a hallucination and never executed, the resolver arXiv
2609.19425 places in front of any other check.
"""

from __future__ import annotations

from analyst.tools import ToolStore

# name -> (required args, optional args, description). Argument types are all checked below.
SPECS = {
    "who_is": ({"account": str}, {}, "profile of an account from its logins before the alert"),
    "recent_auth_history": ({"account": str}, {"limit": int},
                            "the account's latest logins before the alert, newest first"),
    "hosts_reached": ({"account": str}, {"hours": int},
                      "computers the account reached in the past hours, and which were new"),
    "path_to": ({"source": str, "destination": str}, {},
                "shortest chain of logins from one computer to another, as of the alert"),
    "blast_radius": ({"host": str}, {}, "computers reachable from a host in one and two hops"),
    "similar_alerts": ({"host": str}, {}, "other alerts from the same source in the past day"),
}
GRAPH_TOOLS = ("hosts_reached", "path_to", "blast_radius", "similar_alerts")


class Registry:
    def __init__(self, store: ToolStore, as_of: int, alert_id: int | None = None,
                 graph_tools: bool = True):
        self.store = store
        self.as_of = as_of
        self.alert_id = alert_id
        self.allowed = {k: v for k, v in SPECS.items() if graph_tools or k not in GRAPH_TOOLS}
        self.hallucinations: list[dict] = []
        self.executed: list[str] = []

    def names(self) -> list[str]:
        return list(self.allowed)

    def describe(self) -> str:
        lines = []
        for name, (req, opt, text) in self.allowed.items():
            args = [f"{a}: {t.__name__}" for a, t in req.items()]
            args += [f"{a}: {t.__name__} (optional)" for a, t in opt.items()]
            lines.append(f"- {name}({', '.join(args)}): {text}")
        return "\n".join(lines)

    def _check(self, name: str, args) -> str | None:
        if name not in self.allowed:
            return "unknown_tool"
        req, opt, _ = self.allowed[name]
        if not isinstance(args, dict):
            return "bad_arguments"
        if set(req) - set(args) or set(args) - set(req) - set(opt):
            return "bad_arguments"
        for key, value in args.items():
            want = req.get(key) or opt.get(key)
            # bool is a subclass of int in Python, so it has to be refused by name.
            if not isinstance(value, want) or isinstance(value, bool):
                return "bad_arguments"
        return None

    def call(self, name, args) -> dict:
        problem = self._check(name, args)
        if problem:
            self.hallucinations.append({"kind": problem, "tool": name})
            label = "unknown tool" if problem == "unknown_tool" else "arguments do not match"
            return {"error": f"{label}: {name}. Available: {', '.join(self.allowed)}"}
        self.executed.append(name)
        fn = getattr(self.store, name)
        # The alert time comes from the registry, never from the model, so no argument the
        # model invents can make a tool read the future.
        extra = {"alert_id": self.alert_id} if name == "similar_alerts" else {}
        return {"result": fn(**args, as_of=self.as_of, **extra)}
