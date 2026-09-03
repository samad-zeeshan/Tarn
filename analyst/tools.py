"""The analyst's tools: account history and graph questions, answered by code as of the alert.

Topology is computed here and never left to the model, the split SENTINEL-RL (arXiv
2609.04159) argues for. Every tool takes the alert time and ignores anything after it.
"""

from __future__ import annotations

from collections import deque
from pathlib import Path

import duckdb
import numpy as np

DAY = 86_400
MAX_ROWS = 20


class ToolStore:
    """Logins sorted by account and time, the host graph with first-seen times, and the alerts."""

    def __init__(self, t, u, s, d, users, hosts, alerts, edge_src, edge_dst, edge_first):
        self.t, self.u, self.s, self.d = t, u, s, d
        self.users = users
        self.hosts = hosts
        self.user_ids = {v: k for k, v in users.items()}
        # Small models often drop the domain ("U12" for "U12@DOM1"). A bare name that maps to
        # exactly one account is resolved, an ambiguous one is not.
        bare: dict[str, list] = {}
        for name, uid in self.user_ids.items():
            bare.setdefault(name.split("@")[0], []).append(uid)
        self.bare_ids = {k: v[0] for k, v in bare.items() if len(v) == 1}
        self.host_ids = {v: k for k, v in hosts.items()}
        self.alerts = sorted(alerts, key=lambda a: a["time"])
        self.edge_src, self.edge_dst, self.edge_first = edge_src, edge_dst, edge_first
        bounds = np.flatnonzero(np.diff(u)) + 1
        starts = np.concatenate([[0], bounds])
        ends = np.concatenate([bounds, [len(u)]])
        self.span = {int(u[a]): (int(a), int(b)) for a, b in zip(starts, ends, strict=True)
                     if b > a}

    @classmethod
    def from_arrays(cls, t, u, s, d, users, hosts, alerts):
        order = np.lexsort((t, u))
        t, u, s, d = t[order], u[order], s[order], d[order]
        pairs = {}
        for si, di, ti in zip(s.tolist(), d.tolist(), t.tolist(), strict=True):
            key = (si, di)
            if key not in pairs or ti < pairs[key]:
                pairs[key] = ti
        es = np.array([k[0] for k in pairs], dtype=np.int64)
        ed = np.array([k[1] for k in pairs], dtype=np.int64)
        ef = np.array(list(pairs.values()), dtype=np.int64)
        return cls(t, u, s, d, users, hosts, alerts, es, ed, ef)

    @classmethod
    def from_work(cls, work: Path, alerts: list[dict]) -> ToolStore:
        con = duckdb.connect()
        cand = (work / "candidates.parquet").as_posix()
        ev = con.execute(f"select time, uid, sid, did from read_parquet('{cand}') "
                         "order by uid, time").fetchnumpy()
        g = con.execute(f"select sid, did, min(time) as first from read_parquet('{cand}') "
                        "group by all").fetchnumpy()
        users = dict(con.execute(f"select uid, name from '{(work / 'users.parquet').as_posix()}'")
                     .fetchall())
        hosts = dict(con.execute(f"select hid, name from '{(work / 'hosts.parquet').as_posix()}'")
                     .fetchall())
        return cls(ev["time"], ev["uid"], ev["sid"], ev["did"], users, hosts, alerts,
                   g["sid"], g["did"], g["first"])

    def resolve(self, account: str) -> int | None:
        uid = self.user_ids.get(account)
        return uid if uid is not None else self.bare_ids.get(account.split("@")[0])

    def _logins(self, account: str, as_of: int, since: int | None = None):
        uid = self.resolve(account)
        if uid is None or uid not in self.span:
            return None
        a, b = self.span[uid]
        # Strictly before the alert second, which matches the feature engine's view of history.
        hi = a + int(np.searchsorted(self.t[a:b], as_of, side="left"))
        lo = a if since is None else a + int(np.searchsorted(self.t[a:b], since, side="left"))
        return lo, hi, a

    def who_is(self, account: str, as_of: int) -> dict:
        span = self._logins(account, as_of)
        if span is None or span[1] == span[2]:
            return {"account": account, "known": False}
        lo, hi, _ = span
        t, s, d = self.t[lo:hi], self.s[lo:hi], self.d[lo:hi]
        src, counts = np.unique(s, return_counts=True)
        usual = [self.hosts[int(h)] for h in src[np.argsort(-counts)][:3]]
        return {
            "account": account,
            "known": True,
            "first_seen_days_before": round((as_of - int(t[0])) / DAY, 2),
            "active_days": int(len(np.unique(t // DAY))),
            "logins": int(hi - lo),
            "distinct_sources": int(len(src)),
            "distinct_destinations": int(len(np.unique(d))),
            "usual_sources": usual,
        }

    def recent_auth_history(self, account: str, as_of: int, limit: int = 10) -> dict:
        span = self._logins(account, as_of)
        if span is None:
            return {"account": account, "logins": []}
        lo, hi, a = span
        limit = max(1, min(int(limit), MAX_ROWS))
        rows = []
        for i in range(hi - 1, max(lo, hi - limit) - 1, -1):
            earlier = self.d[a:i] == self.d[i]
            rows.append({
                "seconds_before": int(as_of - self.t[i]),
                "source": self.hosts[int(self.s[i])],
                "destination": self.hosts[int(self.d[i])],
                "first_time_here": bool(not earlier.any()),
            })
        return {"account": account, "logins": rows}

    def hosts_reached(self, account: str, as_of: int, hours: int = 24) -> dict:
        span = self._logins(account, as_of, since=as_of - int(hours) * 3600)
        if span is None:
            return {"account": account, "hosts": []}
        lo, hi, a = span
        before = set(self.d[a:lo].tolist())
        dst, first = np.unique(self.d[lo:hi], return_index=True)
        order = np.argsort(first)
        return {
            "account": account,
            "hours": int(hours),
            "hosts": [{"host": self.hosts[int(h)], "new": int(h) not in before}
                      for h in dst[order][:MAX_ROWS]],
            "total": int(len(dst)),
        }

    def _adjacency(self, as_of: int) -> dict[int, list[int]]:
        live = self.edge_first < as_of
        adj: dict[int, list[int]] = {}
        for a, b in zip(self.edge_src[live].tolist(), self.edge_dst[live].tolist(), strict=True):
            adj.setdefault(a, []).append(b)
        return adj

    def path_to(self, source: str, destination: str, as_of: int) -> dict:
        s, d = self.host_ids.get(source), self.host_ids.get(destination)
        out = {"source": source, "destination": destination, "path": None}
        if s is None or d is None:
            return out
        adj = self._adjacency(as_of)
        prev = {s: None}
        q = deque([s])
        while q:
            h = q.popleft()
            if h == d:
                path = []
                while h is not None:
                    path.append(self.hosts[h])
                    h = prev[h]
                out["path"] = path[::-1]
                out["hops"] = len(path) - 1
                return out
            for n in adj.get(h, ()):
                if n not in prev:
                    prev[n] = h
                    q.append(n)
        return out

    def blast_radius(self, host: str, as_of: int) -> dict:
        h = self.host_ids.get(host)
        if h is None:
            return {"host": host, "known": False}
        adj = self._adjacency(as_of)
        one = set(adj.get(h, ()))
        two = set()
        for n in one:
            two.update(adj.get(n, ()))
        two -= one | {h}
        return {"host": host, "known": True, "one_hop": len(one), "two_hops": len(two),
                "hosts_in_graph": len(adj)}

    def similar_alerts(self, host: str, as_of: int, alert_id: int | None = None) -> dict:
        rows = [
            {"account": a["account"], "destination": a["destination"],
             "seconds_before": as_of - a["time"], "score": round(a["score"], 2)}
            for a in self.alerts
            if a["source"] == host and as_of - DAY <= a["time"] < as_of
            and a["alert_id"] != alert_id
        ]
        return {"host": host, "count": len(rows),
                "accounts": sorted({r["account"] for r in rows})[:MAX_ROWS],
                "alerts": rows[-MAX_ROWS:]}
