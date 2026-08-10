"""Point-in-time graph features, the histogram model, explanations and the leakage verifier.

The verifier tests matter most. A feature that peeks at the future inflates every number
downstream, and the only defence is a check that fails when one does.
"""

from __future__ import annotations

import random

import numpy as np
import pytest

from detect.graph.features import FEATURES, FeatureEngine, FitContext
from detect.graph.model import GROUPS, HistogramModel
from detect.graph.verify import verify_point_in_time
from eval import protocol

H = 3600


def _run(events, ctx=None):
    eng = FeatureEngine(ctx or FitContext.empty())
    rows = eng.push(*map(list, zip(*events, strict=True))) + eng.flush()
    return {(r[0], r[1], r[2], r[3]): r[4:] for r in rows}


def _f(row, name):
    return row[FEATURES.index(name)]


def test_first_edge_is_new_and_repeat_is_not():
    got = _run([(100, 1, 10, 20, 0), (200, 1, 10, 20, 0)])
    assert _f(got[(100, 1, 10, 20)], "edge_new") == 1
    assert _f(got[(200, 1, 10, 20)], "edge_new") == 0


def test_events_in_the_same_second_do_not_see_each_other():
    # Two logins by U1 in the same second. Neither may count the other as history, because a
    # live system has no order inside one second of the LANL clock.
    got = _run([(100, 1, 10, 20, 0), (100, 1, 10, 21, 0), (101, 1, 10, 22, 0)])
    assert _f(got[(100, 1, 10, 20)], "burst_1h") == 0
    assert _f(got[(100, 1, 10, 21)], "burst_1h") == 0
    # One second later both are history: two new edges in the past hour, log2 bucket 2.
    assert _f(got[(101, 1, 10, 22)], "burst_1h") == 2


def test_burst_window_forgets_after_an_hour():
    got = _run([(0, 1, 10, 20, 0), (H - 1, 1, 10, 21, 0), (H + 1, 1, 10, 22, 0)])
    assert _f(got[(H - 1, 1, 10, 21)], "burst_1h") == 1
    # The edge at t=0 has left the window. Only the one at H-1 remains.
    assert _f(got[(H + 1, 1, 10, 22)], "burst_1h") == 1


def test_new_users_on_a_source_host_are_counted_over_a_day():
    events = [(10 * i, u, 50, 60 + u, 0) for i, u in enumerate(range(1, 6))]
    got = _run(events)
    # The fifth user from host 50 sees four users who were new on that host before it.
    assert _f(got[(40, 5, 50, 65)], "src_new_users_24h") == 3  # log2 bucket of 4


def test_rules_are_point_in_time_versions_of_v1():
    events = [(i, 1, 10, 100 + i, 0) for i in range(6)]
    got = _run(events)
    # Q3 in v1 fires on five new hosts in a day. Here it only fires once five are in the past.
    assert _f(got[(4, 1, 10, 104)], "r_new_hosts_today") == 0
    assert _f(got[(5, 1, 10, 105)], "r_new_hosts_today") == 1


def test_fit_context_is_built_only_from_the_fit_window():
    events = [(10, 1, 10, 20, 0), (protocol.FIT_END + 5, 2, 11, 21, 0)]
    ctx = FitContext.build(*map(list, zip(*events, strict=True)))
    assert ctx.fit_max_time < protocol.FIT_END
    assert 21 not in ctx.dst_users


def _random_stream(seed: int, n: int = 600):
    rng = random.Random(seed)
    t = 0
    out = []
    for _ in range(n):
        t += rng.choice([0, 0, 1, 5, 60, 900])
        out.append((t, rng.randint(1, 8), rng.randint(10, 14), rng.randint(20, 40),
                    int(rng.random() < 0.1)))
    return out


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_verifier_passes_the_real_engine(seed):
    report = verify_point_in_time(_random_stream(seed), FeatureEngine, FitContext.empty(),
                                  cuts=12, seed=seed)
    assert report["passed"], report


class LeakyEngine(FeatureEngine):
    """Counts the user's new hosts for the whole day, including ones that have not happened."""

    def __init__(self, ctx, future=None):
        super().__init__(ctx)
        self._future = future

    def prepare(self, t, u, s, d, f):
        seen, per_day = set(), {}
        for ti, ui, di in zip(t, u, d, strict=True):
            if (ui, di) not in seen:
                seen.add((ui, di))
                per_day[(ui, ti // 86_400)] = per_day.get((ui, ti // 86_400), 0) + 1
        self._future = per_day

    def _today_new(self, u, t):
        return self._future.get((u, t // 86_400), 0)


def test_verifier_catches_a_feature_that_reads_the_future():
    report = verify_point_in_time(_random_stream(4), LeakyEngine, FitContext.empty(),
                                  cuts=12, seed=4)
    assert not report["passed"]
    assert report["mismatches"] > 0


def test_histogram_contributions_add_up_to_the_score_exactly():
    rng = np.random.default_rng(0)
    fit = rng.integers(0, 3, size=(500, len(FEATURES)))
    model = HistogramModel.fit(fit, group="graph_plus_rules", fit_max_time=100)
    test = rng.integers(0, 3, size=(50, len(FEATURES)))
    scores = model.score(test)
    parts = model.contributions(test)
    assert np.allclose(parts.sum(axis=1), scores, rtol=0, atol=1e-12)


def test_rare_values_score_higher_than_common_ones():
    fit = np.zeros((1000, len(FEATURES)), dtype=int)
    fit[:10, FEATURES.index("edge_new")] = 1
    model = HistogramModel.fit(fit, group="graph", fit_max_time=100)
    common = np.zeros((1, len(FEATURES)), dtype=int)
    rare = common.copy()
    rare[0, FEATURES.index("edge_new")] = 1
    assert model.score(rare)[0] > model.score(common)[0]


def test_model_refuses_a_fit_that_reaches_into_the_attack_period():
    fit = np.zeros((10, len(FEATURES)), dtype=int)
    with pytest.raises(protocol.ProtocolViolation):
        HistogramModel.fit(fit, group="graph", fit_max_time=protocol.FIT_END + 1)


def test_groups_only_name_real_features_and_no_labels():
    for cols in GROUPS.values():
        assert set(cols) <= set(FEATURES)
    protocol.assert_no_label_columns(FEATURES)
