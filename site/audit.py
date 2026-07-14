"""
Enforce the site's evidence contract before it deploys.

No hard-coded metric in the HTML, every data-bench binding resolves, no external assets, and
the payload budgets hold.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SITE = REPO / "site"

# A "metric" is a number that looks measured. Small integers like viewBox coords, years and font
# weights are not, so these patterns only fire on shapes that carry units or digit grouping,
# which is what a fabricated measurement looks like.
METRIC_PATTERNS = [
    (r"\b\d{1,3}(?:,\d{3})+\b", "digit-grouped figure (e.g. 1,051,430,459)"),
    (r"\b\d+(?:\.\d+)?\s?ms\b", "duration in ms"),
    (r"\b\d+(?:\.\d+)?\s?s\b(?!\w)", "duration in seconds"),
    (r"\b\d+(?:\.\d+)?\s?×\b", "speedup"),
    (r"\b\d+(?:\.\d+)?%", "percentage"),
    (r"\b\d+(?:\.\d+)?\s?(?:GB|MB)\b", "size"),
]

# Prose that legitimately holds a figure because it describes the dataset or a config constant
# rather than reporting a measurement. Each entry is here on purpose.
ALLOWED_SUBSTRINGS = [
    "1.05 billion",
    "749",
    "9-to-5", "nine-to-five",
    "2015",
    "1-minute", "1-min",
    "COUNT(DISTINCT",
    "100 MB", "150 MB", "35 MB", "20 MB",
    "6 hops", "3 hops", "1 hop", "≤6 hops",
    "30-day", "24-hour",
    "2×2",
]


def check_hardcoded_metrics(errors: list[str]) -> None:
    html = (SITE / "index.html").read_text(encoding="utf-8")

    # Strip the things that legitimately hold numbers: SVG geometry, the bindings themselves,
    # inline styles and comments.
    stripped = re.sub(r"<svg.*?</svg>", "", html, flags=re.S)
    stripped = re.sub(r'data-bench="[^"]*"', "", stripped)
    stripped = re.sub(r"<style.*?</style>", "", stripped, flags=re.S)
    stripped = re.sub(r"style=\"[^\"]*\"", "", stripped)
    stripped = re.sub(r"<!--.*?-->", "", stripped, flags=re.S)

    for pattern, what in METRIC_PATTERNS:
        for m in re.finditer(pattern, stripped):
            start = max(0, m.start() - 60)
            context = stripped[start : m.end() + 30].replace("\n", " ").strip()
            if any(a in context for a in ALLOWED_SUBSTRINGS):
                continue
            errors.append(
                f"index.html hard-codes a {what}: '{m.group()}'\n"
                f"      context: ...{context}...\n"
                f"      Metrics must come from bench/*.json via a data-bench binding."
            )


def check_bench_bindings(errors: list[str]) -> None:
    bench_path = SITE / "data" / "bench.json"
    if not bench_path.exists():
        errors.append("site/data/bench.json missing, run `python site/build_payloads.py`")
        return

    bench = json.loads(bench_path.read_text())
    html = (SITE / "index.html").read_text(encoding="utf-8")

    # A binding that points at a path which no longer exists renders as an em-dash on a live
    # site. Silent, and exactly the kind of rot this catches.
    for match in re.finditer(r'data-bench="([^"|]+)(?:\|[^"]*)?"', html):
        path = match.group(1)
        node = bench
        for key in path.split("."):
            if not isinstance(node, dict) or key not in node:
                errors.append(
                    f'data-bench="{path}" does not resolve in bench.json (broke at "{key}"). '
                    "It would render as an em-dash on the live site."
                )
                node = None
                break
            node = node[key]


def check_budgets(errors: list[str]) -> None:
    files = [p for p in SITE.rglob("*") if p.is_file() and ".git" not in p.parts]
    total = sum(p.stat().st_size for p in files)

    for p in files:
        if p.stat().st_size > 100e6:
            errors.append(
                f"{p.relative_to(REPO)} is {p.stat().st_size / 1e6:.1f} MB, over GitHub's 100 MB file limit"
            )
    if total > 150e6:
        errors.append(f"site payload is {total / 1e6:.1f} MB, over the 150 MB budget")

    print(f"  payload: {total / 1e6:.1f} MB total, largest file "
          f"{max(p.stat().st_size for p in files) / 1e6:.1f} MB")


def check_no_external_calls(errors: list[str]) -> None:
    """Assets must be vendored, or the demo dies the day a CDN moves."""
    asset_ref = re.compile(
        r"""(?:src|href)\s*=\s*["']https?://|url\(\s*["']?https?://|import\s+.*?["']https?://""",
        re.I,
    )
    for path in [SITE / "index.html", SITE / "styles.css", SITE / "app.js",
                 SITE / "charts.js", SITE / "graph.js", SITE / "fonts.css"]:
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            errors.append(f"{path.name} is not valid UTF-8 ({exc}), it will mojibake when served")
            continue
        for m in asset_ref.finditer(text):
            line = text[: m.start()].count("\n") + 1
            snippet = text[max(0, m.start() - 30) : m.end() + 50]
            # An <a href="https://..."> in prose is a link, not a fetched asset.
            if re.search(r"<a\s[^>]*href", snippet, re.I):
                continue
            errors.append(
                f"{path.name}:{line} references an external asset, everything must be vendored:\n"
                f"      {snippet.strip()[:90]}"
            )


def main() -> int:
    print("[audit] site evidence contract")
    errors: list[str] = []

    check_hardcoded_metrics(errors)
    check_bench_bindings(errors)
    check_no_external_calls(errors)
    check_budgets(errors)

    if errors:
        print(f"\n  FAILED, {len(errors)} problem(s):\n")
        for e in errors:
            print(f"  x {e}")
        return 1

    print("  ok  no hard-coded metrics in index.html")
    print("  ok  every data-bench binding resolves against bench.json")
    print("  ok  no external asset references")
    print("  ok  payload budgets hold")
    return 0


if __name__ == "__main__":
    sys.exit(main())
