"""The committed analyst results must be exactly what scoring the committed agent runs gives."""

from __future__ import annotations

import json

import pytest

from analyst.benchmark import BENCH_DIR, score
from eval.readme import RESULTS


def test_analyst_results_rescore_from_the_committed_runs(tmp_path):
    committed = json.loads((RESULTS / "analyst.json").read_text())
    if not (BENCH_DIR / "runs_graph.jsonl").exists():
        pytest.skip("no committed agent runs")
    got = score(BENCH_DIR, tmp_path / "analyst.json", committed["data"], committed["model"])
    assert got == committed


def test_benchmark_is_big_enough_and_mixed():
    bench = json.loads((BENCH_DIR / "benchmark.json").read_text())
    attacks = sum(1 for i in bench["items"] if i["truth"]["is_attack"])
    assert bench["size"] >= 500
    assert 0 < attacks < bench["size"]


def test_every_committed_alert_is_explained():
    bench = json.loads((BENCH_DIR / "benchmark.json").read_text())
    for item in bench["items"]:
        a = item["alert"]
        assert a["contributions"], a["alert_id"]
        assert abs(sum(c["surprise"] for c in a["contributions"]) - a["score"]) < 1e-3
