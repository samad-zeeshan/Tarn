"""
Feature construction, and the two guards that make the vector results worth anything.

The first is that no label may get into a vector. The second is that a lookalike search must not
be allowed to return the thing you searched with.
"""

from __future__ import annotations

import numpy as np
import pytest

from pipeline.embed import DIM, FEATURE_NAMES, build_features, standardize_and_normalize
from vector.search import anomaly_scores, build_index, leave_one_out_retrieval, score_as_detector


def test_no_label_ever_reaches_the_vector():
    """The whole approach is worthless if the answer key is one of the dimensions."""
    # It would be very easy to add is_redteam_day to the feature list, get a perfect score, and
    # not notice. This test is cheap and it is the only thing standing between here and that.
    banned = ("redteam", "attack", "compromis", "label", "is_bad", "malicious")
    for name in FEATURE_NAMES:
        low = name.lower()
        for word in banned:
            assert word not in low, f"feature {name!r} looks like it came from the answer key"


def test_feature_names_are_unique_and_match_the_declared_size():
    assert len(FEATURE_NAMES) == DIM
    assert len(set(FEATURE_NAMES)) == DIM


def test_features_are_shares_and_volumes(spark, sample_lake, tmp_path):
    """Shares are shares. If they do not sum to one, a category is being dropped on the floor."""
    rollup_path = str(tmp_path / "rollup")
    from pipeline.rollup import build_rollup

    lake_root = sample_lake.rsplit("/", 1)[0]
    build_rollup(spark, sample_lake, f"{lake_root}/redteam", []).write.mode("overwrite").parquet(
        rollup_path
    )

    feats = build_features(spark, sample_lake, rollup_path)
    row = feats.limit(1).collect()[0]

    hour = sum(row[f"hour_{h:02d}_share"] for h in range(24))
    assert hour == pytest.approx(1.0, abs=1e-6)

    auth = sum(row[c] for c in FEATURE_NAMES if c.startswith("auth_type_"))
    assert auth == pytest.approx(1.0, abs=1e-6), "an authentication type is being lost"

    logon = sum(row[c] for c in FEATURE_NAMES if c.startswith("logon_type_"))
    assert logon == pytest.approx(1.0, abs=1e-6), "a logon type is being lost"

    orient = sum(row[c] for c in FEATURE_NAMES if c.startswith("orientation_"))
    assert orient == pytest.approx(1.0, abs=1e-6), "an orientation is being lost"

    assert 0.0 <= row["failure_ratio"] <= 1.0
    assert row["is_machine"] in (0.0, 1.0)


def test_vectors_come_out_unit_length(spark, sample_lake, tmp_path):
    """Distance is only a measure of shape if every vector has the same length."""
    rollup_path = str(tmp_path / "rollup2")
    from pipeline.rollup import build_rollup

    lake_root = sample_lake.rsplit("/", 1)[0]
    build_rollup(spark, sample_lake, f"{lake_root}/redteam", []).write.mode("overwrite").parquet(
        rollup_path
    )

    feats = build_features(spark, sample_lake, rollup_path)
    vectors, stats = standardize_and_normalize(feats)

    rows = vectors.limit(200).collect()
    for r in rows:
        v = np.asarray(r["vector"], dtype=np.float64)
        assert len(v) == DIM
        assert np.isfinite(v).all(), "a NaN got into the index and every distance is now poison"
        assert np.linalg.norm(v) == pytest.approx(1.0, abs=1e-4)

    # A dimension with no spread would divide by zero. The code substitutes one, and it had
    # better still be doing that.
    assert all(s["stddev"] != 0 for s in stats.values())


def _toy_space(seed: int = 0):
    """A tight crowd, plus ten loners scattered on their own.

    The vectors have to be built ON the unit sphere, not built anywhere and normalised
    afterwards. Normalising a cluster that sits far from the origin squashes it into a TIGHT
    group of near-identical directions, which is the opposite of an outlier. The first version of
    this fixture did exactly that and the test failed, correctly.
    """
    rng = np.random.default_rng(seed)

    crowd = np.zeros((500, 8), dtype=np.float32)
    crowd[:, 0] = 1.0
    crowd += rng.normal(0, 0.02, size=crowd.shape).astype(np.float32)

    # Each loner points somewhere random, so it is far from the crowd AND far from the others.
    odd = rng.normal(0, 1.0, size=(10, 8)).astype(np.float32)

    mat = np.vstack([crowd, odd])
    mat /= np.linalg.norm(mat, axis=1, keepdims=True)
    labels = np.zeros(len(mat), dtype=bool)
    labels[500:] = True
    return np.ascontiguousarray(mat), labels


def test_anomaly_score_puts_the_outliers_on_top():
    mat, labels = _toy_space()
    index = build_index(mat, m=16)
    scores = anomaly_scores(index, mat, k=5)

    ranked = np.argsort(-scores)
    top = labels[ranked[:10]]
    assert top.sum() >= 8, "the planted outliers did not rise to the top of the ranking"


def test_a_lookalike_search_cannot_return_itself():
    """Every query is already a member of the index it is searching.

    Leave the query in its own results and it scores a guaranteed hit against its own row, which
    would make a completely useless index look accurate. This is the easiest way to fake a good
    result here, so it gets a test of its own.

    The attack points here are hidden INSIDE the crowd, drawn from the same distribution, so they
    look exactly like ordinary behaviour. An honest search finds almost no attack neighbours for
    them. A search that returns the query itself scores at least 1.0 every single time, because
    the query is an attack day by definition. So anything below 1.0 can only come from the query
    having been removed.
    """
    rng = np.random.default_rng(7)
    mat = np.zeros((510, 8), dtype=np.float32)
    mat[:, 0] = 1.0
    mat += rng.normal(0, 0.02, size=mat.shape).astype(np.float32)
    mat /= np.linalg.norm(mat, axis=1, keepdims=True)
    mat = np.ascontiguousarray(mat)

    labels = np.zeros(len(mat), dtype=bool)
    labels[rng.choice(len(mat), size=10, replace=False)] = True

    index = build_index(mat, m=16)
    got = leave_one_out_retrieval(index, mat, labels, k=5)

    assert got["self_excluded"] is True
    assert got["queries"] == 10
    assert got["mean_attack_days_in_top_k"] < 1.0, (
        "every query found at least one attack day among neighbours that look like nothing at "
        "all, which means it found itself"
    )


def test_detector_refuses_a_budget_bigger_than_the_population():
    """Flagging everybody is not a detection result, and must not be reported as one."""
    mat, labels = _toy_space()
    index = build_index(mat, m=16)
    scores = anomaly_scores(index, mat, k=5)

    budgets = {
        "absurd": {"alerts": 10_000, "caught": 10},
        "sane": {"alerts": 20, "caught": 4},
    }
    got = score_as_detector(scores, labels, budgets)
    absurd = next(g for g in got if g["matched_against"] == "absurd")
    sane = next(g for g in got if g["matched_against"] == "sane")

    assert "skipped" in absurd
    assert "recall_pct" not in absurd

    assert sane["vector_search_caught"] > 0
    # The head-to-head only means anything if it also carries what the rule managed on the same
    # money, so that number has to survive the trip.
    assert sane["the_rule_caught"] == 4
    assert sane["winner"] in ("the simple rule", "vector search", "tie")
