"""Every number in the README's results tables must match eval/results exactly."""

from __future__ import annotations

from eval.readme import PATTERN, README, apply, check, render_all


def test_readme_tables_match_the_results_files():
    assert check() == []


def test_a_changed_number_is_caught():
    text = README.read_text(encoding="utf-8")
    tables = render_all()
    first = next(PATTERN.finditer(text)).group(2)
    tables[first] = tables[first].replace("|", "| 999", 1)
    assert apply(text, tables) != text


def test_readme_is_short_and_says_no_scale_claim():
    text = README.read_text(encoding="utf-8")
    assert len(text.splitlines()) < 120
    assert "at scale" not in text.lower()
