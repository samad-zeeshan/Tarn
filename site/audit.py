"""Stage 5 evidence contract, enforced.

DIRECTIVE §5 Stage 5 says the site must render every panel from committed artifacts —
"grep-able: no hard-coded metric that differs from its bench/ source" — and must hold its
payload budgets. A promise like that decays the moment someone types a number into the HTML
to fix a layout. So it is a test, and it runs in the Pages workflow before deploy.

Checks:
  1. NO HARD-CODED METRICS. index.html must not contain a number that looks like a measurement
     (a long digit-grouped figure, a duration in ms/s, a percentage, a speedup). Numbers belong
     in bench/*.json and reach the page through data-bench bindings or the chart code.
  2. EVERY data-bench BINDING RESOLVES. A binding pointing at a path that does not exist in
     bench.json renders as an em-dash on a live site — silent, and exactly the kind of rot this
     catches.
  3. PAYLOAD BUDGETS. Total site < 150 MB, every file < GitHub's 100 MB hard limit.
  4. NO EXTERNAL NETWORK CALLS. No http(s):// asset URLs in the HTML/CSS/JS — the demo must run
     with the network cut after load. (Links in prose are fine; asset references are not.)

    python site/audit.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SITE = REPO / "site"

# A "metric" is a number that looks measured. Small integers (viewBox coords, years, section
# numbers, font weights) are not — this pattern deliberately only fires on shapes that carry
# units or grouping, which is what a fabricated measurement looks like.
METRIC_PATTERNS = [
    (r"\b\d{1,3}(?:,\d{3})+\b", "digit-grouped figure (e.g. 1,051,430,459)"),
    (r"\b\d+(?:\.\d+)?\s?ms\b", "duration in ms"),
    (r"\b\d+(?:\.\d+)?\s?s\b(?!\w)", "duration in seconds"),
    (r"\b\d+(?:\.\d+)?\s?×\b", "speedup"),
    (r"\b\d+(?:\.\d+)?%", "percentage"),
    (r"\b\d+(?:\.\d+)?\s?(?:GB|MB)\b", "size"),
]

# Prose that legitimately contains a figure because it is describing the DATA SET or a config
# constant, not reporting a measurement. Each one is here on purpose.
ALLOWED_SUBSTRINGS = [
    "1.05 billion",           # the meta description; the on-page figure is data-bench bound
    "749",                    # the label count, quoted in explanatory prose
    "9-to-5", "nine-to-five",
    "2015",                   # the dataset's year
    "1-minute", "1-min",      # the streaming window, a config value not a measurement
    "COUNT(DISTINCT",
    "100 MB", "150 MB", "35 MB", "20 MB",   # payload budgets, stated as budgets
    "6 hops", "3 hops", "1 hop", "≤6 hops",
    "30-day", "24-hour",
    "2×2",                    # the experiment's shape, not a speedup
]


def check_hardcoded_metrics(errors: list[str]) -> None:
    html = (SITE / "index.html").read_text(encoding="utf-8")

    # Strip the things that legitimately hold numbers: SVG geometry, the data-bench bindings
    # themselves, and inline style/script blocks.
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
                f"      context: …{context}…\n"
                f"      Metrics must come from bench/*.json via a data-bench binding."
            )


def check_bench_bindings(errors: list[str]) -> None:
    bench_path = SITE / "data" / "bench.json"
    if not bench_path.exists():
        errors.append("site/data/bench.json missing — run `python site/build_payloads.py`")
        return

    bench = json.loads(bench_path.read_text())
    html = (SITE / "index.html").read_text(encoding="utf-8")

    for match in re.finditer(r'data-bench="([^"|]+)(?:\|[^"]*)?"', html):
        path = match.group(1)
        node = bench
        for key in path.split("."):
            if not isinstance(node, dict) or key not in node:
                errors.append(
                    f"data-bench=\"{path}\" does not resolve in bench.json "
                    f"(broke at '{key}') — it would render as an em-dash on the live site."
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
                f"{p.relative_to(REPO)} is {p.stat().st_size / 1e6:.1f} MB — over GitHub's 100 MB file limit"
            )
    if total > 150e6:
        errors.append(f"site payload is {total / 1e6:.1f} MB — over the 150 MB budget")

    print(f"  payload: {total / 1e6:.1f} MB total, largest file "
          f"{max(p.stat().st_size for p in files) / 1e6:.1f} MB")


def check_no_external_calls(errors: list[str]) -> None:
    """Assets must be vendored. An external font or CDN script would break the 'no network
    calls after load' guarantee — and would mean the demo dies the day a CDN moves."""
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
            # A served asset that is not valid UTF-8 will render as mojibake in the browser,
            # so this is a real finding, not a reason for the audit to fall over.
            errors.append(f"{path.name} is not valid UTF-8 ({exc}) — it will mojibake when served")
            continue
        for m in asset_ref.finditer(text):
            line = text[: m.start()].count("\n") + 1
            # <a href="https://…"> in prose is a link, not a fetched asset.
            snippet = text[max(0, m.start() - 30) : m.end() + 50]
            if re.search(r"<a\s[^>]*href", snippet, re.I):
                continue
            errors.append(
                f"{path.name}:{line} references an external asset — everything must be vendored:\n"
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
        print(f"\n  FAILED — {len(errors)} problem(s):\n")
        for e in errors:
            print(f"  ✗ {e}")
        return 1

    print("  ✓ no hard-coded metrics in index.html")
    print("  ✓ every data-bench binding resolves against bench.json")
    print("  ✓ no external asset references (nothing fetched after load)")
    print("  ✓ payload budgets hold")
    return 0


if __name__ == "__main__":
    sys.exit(main())
