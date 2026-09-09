"""Point-in-time features over the User to Computer authentication graph.

One implementation serves the batch run, the streaming job and the verifier, so there is no
second copy of a feature that can drift from the first.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from eval import protocol

# What each value means in plain words is in detect/graph/explain.py, next to the alert text
# that uses it. The r_ features are v1's rules restated so they only look backwards.
FEATURES = (
    "edge_new",
    "src_new",
    "gap",
    "burst_1h",
    "src_new_users_24h",
    "dst_hub",
    "hv_delta",
    "r_new_hosts_today",
    "r_fail_1h",
    "r_off_hours",
)
BINS = (2, 2, 24, 16, 16, 20, 7, 2, 2, 2)

# Host ids are packed into one integer key with the user id. LANL has under 30,000 computers,
# so 2**18 leaves room and keeps dict keys as plain ints, which is most of the speed.
HOST_BITS = 1 << 18
HV_TOP_SHARE = 0.01
HV_CAP = 4
DAY = protocol.SECONDS_PER_DAY
HOUR = 3600


def bucket(x: int, cap: int = 15) -> int:
    return 0 if x <= 0 else min(x.bit_length(), cap)


@dataclass
class FitContext:
    """Statistics learned once from the fit window and then frozen."""

    dst_users: dict[int, int] = field(default_factory=dict)
    hv_dist: dict[int, int] = field(default_factory=dict)
    off_band: frozenset[int] = frozenset()
    fit_max_time: int = -1

    @classmethod
    def empty(cls) -> FitContext:
        return cls()

    def to_json(self) -> dict:
        return {
            "dst_users": {str(k): v for k, v in self.dst_users.items()},
            "hv_dist": {str(k): v for k, v in self.hv_dist.items()},
            "off_band": sorted(self.off_band),
            "fit_max_time": self.fit_max_time,
        }

    @classmethod
    def from_json(cls, blob: dict) -> FitContext:
        return cls(
            {int(k): v for k, v in blob["dst_users"].items()},
            {int(k): v for k, v in blob["hv_dist"].items()},
            frozenset(blob["off_band"]),
            blob["fit_max_time"],
        )

    @classmethod
    def build(cls, t, u, s, d, f=None, off_band=(), fit_end: int = protocol.FIT_END):
        users_per_dst: dict[int, set] = {}
        adj: dict[int, set] = {}
        fit_max = -1
        for ti, ui, si, di in zip(t, u, s, d, strict=True):
            if ti >= fit_end:
                continue
            fit_max = max(fit_max, ti)
            users_per_dst.setdefault(di, set()).add(ui)
            adj.setdefault(si, set()).add(di)
            adj.setdefault(di, set()).add(si)
        dst_users = {h: len(us) for h, us in users_per_dst.items()}

        # High-value means the busiest one percent of destinations, the hubs every account
        # passes through. Hop counts to them come from the fit-window graph only, so no edge
        # the attacker created can shorten a path the detector already knows.
        ranked = sorted(dst_users, key=lambda h: (-dst_users[h], h))
        hv = ranked[: max(1, int(len(ranked) * HV_TOP_SHARE))] if ranked else []
        dist = {h: 0 for h in hv}
        frontier = list(hv)
        for depth in range(1, HV_CAP):
            nxt = []
            for h in frontier:
                for n in adj.get(h, ()):
                    if n not in dist:
                        dist[n] = depth
                        nxt.append(n)
            frontier = nxt
        return cls(dst_users, dist, frozenset(off_band), fit_max)


class FeatureEngine:
    """Consumes events in time order and emits each event's features from strictly earlier state.

    Events that share a second are scored together against the state before that second.
    """

    def __init__(self, ctx: FitContext):
        self.ctx = ctx
        self.ud_last: dict[int, int] = {}
        self.us_seen: set[int] = set()
        self.user_new_edges: dict[int, deque] = {}
        self.user_fails: dict[int, deque] = {}
        self.src_new_users: dict[int, deque] = {}
        self.user_today: dict[int, list] = {}
        self.pending: list[tuple] = []
        self.watermark = -1
        self.late = 0

    def prepare(self, t, u, s, d, f) -> None:
        """Hook for the verifier. The real engine never looks ahead, so this does nothing."""

    def _today_new(self, u: int, t: int) -> int:
        rec = self.user_today.get(u)
        return rec[1] if rec is not None and rec[0] == t // DAY else 0

    def _window(self, store: dict, key: int, t: int, width: int) -> int:
        q = store.get(key)
        if not q:
            return 0
        lo = t - width
        while q and q[0] <= lo:
            q.popleft()
        return len(q)

    def _features(self, t, u, s, d) -> tuple:
        ctx = self.ctx
        last = self.ud_last.get(u * HOST_BITS + d)
        gap = 0 if last is None else 1 + min((t - last).bit_length() - 1, 22)
        ds = ctx.hv_dist.get(s, HV_CAP)
        dd = ctx.hv_dist.get(d, HV_CAP)
        return (
            int(last is None),
            int((u * HOST_BITS + s) not in self.us_seen),
            gap,
            bucket(self._window(self.user_new_edges, u, t, HOUR)),
            bucket(self._window(self.src_new_users, s, t, DAY)),
            min(ctx.dst_users.get(d, 0).bit_length(), 19),
            max(-3, min(3, ds - dd)) + 3,
            int(self._today_new(u, t) >= 5),
            int(self._window(self.user_fails, u, t, HOUR) >= 5),
            int(((t % DAY) // HOUR) in ctx.off_band),
        )

    def _apply(self, t, u, s, d, fail) -> None:
        ud = u * HOST_BITS + d
        if ud not in self.ud_last:
            self.user_new_edges.setdefault(u, deque()).append(t)
            rec = self.user_today.get(u)
            if rec is None or rec[0] != t // DAY:
                self.user_today[u] = [t // DAY, 1]
            else:
                rec[1] += 1
        self.ud_last[ud] = t
        us = u * HOST_BITS + s
        if us not in self.us_seen:
            self.us_seen.add(us)
            self.src_new_users.setdefault(s, deque()).append(t)
        if fail:
            self.user_fails.setdefault(u, deque()).append(t)

    def _close_second(self) -> list[tuple]:
        # The same login can be logged more than once in a second. The batch extract keeps one
        # row per (time, user, source, destination), so the stream must too or the two disagree.
        merged: dict[tuple, int] = {}
        for t, u, s, d, fail in self.pending:
            key = (t, u, s, d)
            merged[key] = max(merged.get(key, 0), int(fail))
        group = [(*k, f) for k, f in merged.items()]
        self.pending = []
        rows = [(t, u, s, d, *self._features(t, u, s, d)) for t, u, s, d, _ in group]
        for ev in group:
            self._apply(*ev)
        return rows

    def push(self, t, u, s, d, f) -> list[tuple]:
        """Feed events sorted by time. Returns rows for every second that is now complete."""
        out: list[tuple] = []
        for ev in zip(t, u, s, d, f, strict=True):
            ti = ev[0]
            if ti <= self.watermark:
                # Late event. Scoring it now would use state from its future, so it is
                # counted and dropped, the way the v1 streaming job drops late windows.
                self.late += 1
                continue
            if self.pending and ti != self.pending[0][0]:
                self.watermark = self.pending[0][0]
                out.extend(self._close_second())
            self.pending.append(ev)
        return out

    def flush(self) -> list[tuple]:
        if not self.pending:
            return []
        self.watermark = self.pending[0][0]
        return self._close_second()
