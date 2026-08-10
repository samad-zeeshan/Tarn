"""The graph neural network baseline: it must learn the fit graph and never see the test period."""

from __future__ import annotations

import numpy as np
import pytest

from detect.graph.gnn import LightGCN
from eval import protocol


def _two_communities():
    # Users 0-9 log into hosts 0-4, users 10-19 into hosts 5-9. An edge across the two groups
    # is exactly what a link predictor should find surprising.
    users, hosts = [], []
    for u in range(20):
        base = 0 if u < 10 else 5
        for h in range(base, base + 5):
            users.append(u)
            hosts.append(h)
    return np.array(users), np.array(hosts)


def test_cross_community_edges_score_as_more_anomalous():
    u, h = _two_communities()
    model = LightGCN.fit(u, h, fit_max_time=100, dim=8, epochs=150, seed=0)
    inside = model.score(np.array([1, 12]), np.array([2, 7]))
    across = model.score(np.array([1, 12]), np.array([7, 2]))
    assert across.min() > inside.max()


def test_unknown_nodes_get_the_highest_score():
    u, h = _two_communities()
    model = LightGCN.fit(u, h, fit_max_time=100, dim=8, epochs=50, seed=0)
    known = model.score(np.array([1]), np.array([2]))
    unknown = model.score(np.array([999]), np.array([2]))
    assert unknown[0] > known[0]


def test_same_seed_same_scores():
    u, h = _two_communities()
    a = LightGCN.fit(u, h, fit_max_time=100, dim=8, epochs=30, seed=3)
    b = LightGCN.fit(u, h, fit_max_time=100, dim=8, epochs=30, seed=3)
    assert np.array_equal(a.score(u, h), b.score(u, h))


def test_refuses_to_fit_on_the_attack_period():
    u, h = _two_communities()
    with pytest.raises(protocol.ProtocolViolation):
        LightGCN.fit(u, h, fit_max_time=protocol.FIT_END, dim=8, epochs=1)
