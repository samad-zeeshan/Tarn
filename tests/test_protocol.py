"""Tests for the fair evaluation protocol and the scorer.

Every expected number here is worked out by hand from fixtures small enough to check by eye.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from eval import protocol
from eval.score import (
    average_precision,
    identity_day_rollup,
    recall_at_daily_budget,
    threshold_metrics,
    workload,
)


def test_fit_window_guard_rejects_attack_period_data():
    protocol.assert_fit_window({"fit_max_time": protocol.FIT_END - 1})
    with pytest.raises(protocol.ProtocolViolation):
        protocol.assert_fit_window({"fit_max_time": protocol.FIT_END})
    with pytest.raises(protocol.ProtocolViolation):
        protocol.assert_fit_window({})


def test_fit_window_ends_before_the_first_labelled_event():
    # The first red-team row in the LANL file is at t=150885. The fit window must close before
    # it, or the protocol would be training on the attack.
    assert protocol.FIT_END <= 150_885
    assert protocol.TEST_START >= protocol.FIT_END


def test_feature_columns_may_not_look_like_labels():
    protocol.assert_no_label_columns(["edge_new", "burst_1h", "dst_hub"])
    with pytest.raises(protocol.ProtocolViolation):
        protocol.assert_no_label_columns(["edge_new", "is_attack"])
    with pytest.raises(protocol.ProtocolViolation):
        protocol.assert_no_label_columns(["redteam_user_seen"])


def test_label_policy_collapses_duplicates_and_counts_missing_rows():
    redteam = pd.DataFrame(
        {
            "time": [10, 10, 20, 30],
            "user": ["U1@D", "U1@D", "U2@D", "U3@D"],
            "src_computer": ["C1", "C1", "C1", "C9"],
            "dst_computer": ["C2", "C2", "C3", "C4"],
        }
    )
    events = pd.DataFrame(
        {
            "time": [10, 20, 25],
            "src_user": ["U1@D", "U2@D", "U3@D"],
            "src_computer": ["C1", "C1", "C9"],
            "dst_computer": ["C2", "C3", "C4"],
        }
    )
    labels, stats = protocol.apply_label_policy(redteam, events)
    assert stats == {"rows": 4, "distinct": 3, "in_log": 2, "not_in_log": 1}
    assert len(labels) == 2


def test_average_precision_by_hand():
    # Ranked: P, N, P, N. Precision at each positive: 1/1 and 2/3. AP = (1 + 2/3) / 2.
    scores = np.array([0.9, 0.8, 0.7, 0.1])
    labels = np.array([1, 0, 1, 0])
    assert average_precision(scores, labels, total_positives=2) == pytest.approx((1 + 2 / 3) / 2)
    # A positive that was never scored still counts in the denominator.
    assert average_precision(scores, labels, total_positives=4) == pytest.approx((1 + 2 / 3) / 4)


def test_recall_at_daily_budget_takes_top_k_per_day():
    df = pd.DataFrame(
        {
            "day": [1, 1, 1, 2, 2],
            "score": [0.9, 0.5, 0.8, 0.1, 0.2],
            "positives": [0, 1, 1, 1, 0],
            "key": ["a", "b", "c", "d", "e"],
        }
    )
    # Budget 1 per day: day 1 keeps score 0.9 (0 positives), day 2 keeps 0.2 (0 positives).
    got = recall_at_daily_budget(df, budget=1, total_positives=3)
    assert got["caught"] == 0
    assert got["alerts"] == 2
    # Budget 2 per day: day 1 keeps 0.9 and 0.8 (1 positive), day 2 keeps both (1 positive).
    got = recall_at_daily_budget(df, budget=2, total_positives=3)
    assert got["caught"] == 2
    assert got["recall"] == pytest.approx(2 / 3)


def test_budget_ties_break_the_same_way_every_run():
    df = pd.DataFrame(
        {"day": [1] * 6, "score": [1.0] * 6, "positives": [1, 0, 0, 0, 0, 1],
         "key": list("abcdef")}
    )
    first = recall_at_daily_budget(df, budget=3, total_positives=2)
    shuffled = recall_at_daily_budget(df.sample(frac=1, random_state=7), budget=3,
                                      total_positives=2)
    assert first == shuffled


def test_identity_day_rollup_keeps_the_max_event_score():
    events = pd.DataFrame(
        {
            "src_user": ["U1", "U1", "U2"],
            "day": [3, 3, 3],
            "score": [0.2, 0.7, 0.4],
            "is_attack": [1, 0, 0],
        }
    )
    got = identity_day_rollup(events).set_index("src_user")
    assert got.loc["U1", "score"] == pytest.approx(0.7)
    assert got.loc["U1", "positives"] == 1
    assert got.loc["U2", "positives"] == 0


def test_threshold_metrics_and_workload():
    m = threshold_metrics(alerts=200, caught=5, total_positives=10, population=10_000)
    assert m["recall"] == pytest.approx(0.5)
    assert m["precision"] == pytest.approx(0.025)
    assert m["lift"] == pytest.approx(0.025 / (10 / 10_000))

    w = workload(alerts=570, days=57, caught_events=12)
    # 10 alerts a day at five minutes each is 50 minutes of reading a day.
    assert w["alerts_per_day"] == pytest.approx(10.0)
    assert w["analyst_hours_per_day"] == pytest.approx(10 * protocol.TRIAGE_MINUTES / 60)
    total_hours = 570 * protocol.TRIAGE_MINUTES / 60
    assert w["attack_events_per_analyst_hour"] == pytest.approx(12 / total_hours)
