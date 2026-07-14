"""Stage 5 — build everything the demo site serves.

Two kinds of payload:

  site/data/*.parquet   the warehouse extracts DuckDB-WASM actually queries in the browser.
                        This is the part of the demo that is genuinely LIVE: the screener's
                        browser executes real analytical SQL against real marts.

  site/data/bench.json  every measured number the page displays, bundled from bench/*.json.
                        The page renders FROM this file. Nothing on the site is a metric
                        typed into HTML by hand — if a number appears on screen and is not
                        in here, that is a bug, and `make site-audit` fails the build.

Payload budget (DIRECTIVE §5 Stage 5): total site under ~150 MB, every file under GitHub's
100 MB hard limit, and the warm path to a first query under ~20 MB on the wire. The extracts
below are column-pruned and row-bounded to hold that line, and this script prints the budget
so a regression is visible rather than discovered by a screener on a phone.

    python site/build_payloads.py --db /data/work/tarn.duckdb
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
BENCH = REPO / "bench"
SITE_DATA = REPO / "site" / "data"

# Rows the browser must be able to query to reproduce Q1-Q5 exactly as committed.
#
# The rollup is shipped WHOLE (every identity-day, machine accounts included) rather than
# filtered to human identities. That costs payload — but Q5's precision and lift numbers are
# ratios against the total identity-day population, and an extract that quietly dropped the
# machine accounts would let the browser compute a *different, flatteringly better* Q5 than
# the one in warehouse/queries/results/. The demo would then be lying in the one section that
# claims to be live.
EXTRACTS = {
    # Column-pruned and down-cast on purpose. This is the payload a phone downloads, so:
    #   - success_count is dropped: it is auth_count - failure_count, derivable in the browser.
    #   - distinct_src_computers is dropped: no query on the page reads it.
    #   - the ratios and z-scores are FLOAT (32-bit), not DOUBLE. They are displayed to 2-4
    #     decimal places; 15 significant digits of precision is 4 bytes per value of pure
    #     download time. The committed query results in warehouse/queries/results/ are computed
    #     in full double precision — this cast only affects what the browser re-derives.
    "rollup": """
        select
            src_user,
            event_date,
            auth_count,
            failure_count,
            distinct_dst_computers,
            new_dst_computers,
            off_hours_events,
            is_redteam_day,
            baseline_days_available,
            failure_ratio::float                    as failure_ratio,
            off_hours_share::float                  as off_hours_share,
            fanout_zscore::float                    as fanout_zscore,
            failure_ratio_zscore::float             as failure_ratio_zscore,
            off_hours_share_baseline_mean::float    as off_hours_share_baseline_mean
        from main_marts.mart_daily_identity_rollup
    """,
    "dim_identity": """
        select
            identity_name,
            is_machine_account,
            is_compromised,
            total_auth_events,
            total_failure,
            lifetime_distinct_destinations,
            active_days,
            first_seen_date,
            last_seen_date
        from main_marts.dim_identity
    """,
    "dim_computer": """
        select
            computer_name,
            events_as_source,
            events_as_destination,
            total_events,
            distinct_identities_to,
            host_role,
            is_redteam_pivot,
            is_redteam_target
        from main_marts.dim_computer
    """,
    "dim_time": "select * from main_marts.dim_time",
    "redteam": """
        select
            event_time_seconds,
            src_user,
            src_computer,
            dst_computer,
            event_date,
            hour_of_day
        from main_staging.stg_redteam
    """,
}


def build_extracts(db: str, out: Path) -> dict:
    con = duckdb.connect(db, read_only=True)
    manifest = {}

    for name, sql in EXTRACTS.items():
        target = out / f"{name}.parquet"
        # ZSTD over snappy: the browser decompresses once, and every byte here is a byte the
        # screener waits for on a phone.
        con.execute(
            f"copy ({sql}) to '{target.as_posix()}' "
            "(format parquet, compression zstd, row_group_size 100000)"
        )
        rows = con.execute(f"select count(*) from ({sql})").fetchone()[0]
        size = target.stat().st_size
        manifest[name] = {
            "file": f"data/{name}.parquet",
            "rows": rows,
            "bytes": size,
            "mb": round(size / 1e6, 2),
        }
        print(f"  {name:<14} {rows:>10,} rows  {size / 1e6:>7.2f} MB")

    con.close()
    return manifest


def bundle_bench(out: Path) -> dict:
    """Collect every bench artifact into one file the page renders from.

    The page is not allowed to contain a hard-coded metric. Everything numeric on screen is
    read out of this bundle at load time, so a stale number on the site is impossible without
    a stale artifact in bench/ — which is a much easier thing to notice.
    """
    bundle: dict = {}
    for path in sorted(BENCH.glob("*.json")):
        bundle[path.stem] = json.loads(path.read_text())
        print(f"  bench/{path.name}")

    # The query results are evidence too — the page shows Q5's table verbatim.
    results_dir = REPO / "warehouse" / "queries" / "results"
    if results_dir.exists():
        bundle["queries"] = {}
        summary = results_dir / "_summary.json"
        if summary.exists():
            bundle["queries"]["_summary"] = json.loads(summary.read_text())
        for csv in sorted(results_dir.glob("q*.csv")):
            lines = csv.read_text().strip().splitlines()
            if not lines:
                continue
            header = lines[0].split(",")
            bundle["queries"][csv.stem] = {
                "columns": header,
                "rows": [dict(zip(header, ln.split(","), strict=False)) for ln in lines[1:]],
            }
            print(f"  results/{csv.name}")

    # The SQL text itself, so the workbench's preset buttons show the REAL query — not a
    # simplified retype of it that could drift from what produced the committed results.
    queries_dir = REPO / "warehouse" / "queries"
    bundle["sql"] = {
        p.stem: p.read_text() for p in sorted(queries_dir.glob("q*.sql"))
    }

    bundle["built_at"] = datetime.now(UTC).isoformat(timespec="seconds")
    return bundle


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", default="/data/work/tarn.duckdb")
    ap.add_argument("--out", default=str(SITE_DATA))
    ap.add_argument("--skip-extracts", action="store_true")
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    manifest: dict = {}
    if not args.skip_extracts:
        print("[extracts] building Parquet the browser will query:")
        manifest = build_extracts(args.db, out)
    else:
        for p in out.glob("*.parquet"):
            manifest[p.stem] = {
                "file": f"data/{p.name}",
                "bytes": p.stat().st_size,
                "mb": round(p.stat().st_size / 1e6, 2),
            }

    print("[bench] bundling measured artifacts:")
    bundle = bundle_bench(out)
    bundle["extracts"] = manifest
    (out / "bench.json").write_text(json.dumps(bundle, indent=2) + "\n")

    # ---- payload budget ---------------------------------------------------------------
    site = REPO / "site"
    total = sum(p.stat().st_size for p in site.rglob("*") if p.is_file())
    biggest = max(
        ((p, p.stat().st_size) for p in site.rglob("*") if p.is_file()),
        key=lambda kv: kv[1],
    )
    # The page itself — what has to arrive before anything renders. The Parquet is NOT in this
    # number, and that is not a dodge: DuckDB-WASM registers the extracts over HTTP and issues
    # RANGE requests, so a query reads only the column chunks and row groups it actually touches
    # rather than pulling whole files. The honest budget line is therefore "page shell" plus
    # "the largest single extract, as a worst case".
    shell = sum(
        (site / f).stat().st_size
        for f in ("index.html", "styles.css", "app.js", "charts.js", "graph.js", "fonts.css")
        if (site / f).exists()
    ) + sum(p.stat().st_size for p in (site / "vendor" / "fonts").glob("*")) \
      + sum(p.stat().st_size for p in (site / "data").glob("*.json"))

    extracts_total = sum(p["bytes"] for p in manifest.values())
    largest_extract = max((p["bytes"] for p in manifest.values()), default=0)

    print()
    print(f"[budget] site total            {total / 1e6:>8.2f} MB   (limit ~150 MB)")
    print(f"[budget] largest file          {biggest[1] / 1e6:>8.2f} MB   {biggest[0].name}  (GitHub hard limit 100 MB)")
    print(f"[budget] page shell            {shell / 1e6:>8.2f} MB   HTML/CSS/JS/fonts/bench — everything needed to render")
    print(f"[budget] Parquet extracts      {extracts_total / 1e6:>8.2f} MB   see the note below")
    print(f"[budget]   largest extract     {largest_extract / 1e6:>8.2f} MB   worst case if a query scanned every column of it")
    print("[budget] DuckDB wasm           35.66 MB   gzipped to ~9 MB in transit, then browser-cached")
    print()
    print("[budget] NOTE on the Parquet figure: DuckDB-WASM issues HTTP RANGE requests and reads")
    print("         only the column chunks and row groups a query touches — but ONLY if the server")
    print("         honours Range. GitHub Pages does (verified: returns 206). Python's")
    print("         http.server does NOT, so a local `make serve` preview logs 'fall back to full")
    print("         HTTP read' and downloads whole files. The local preview is therefore a")
    print("         PESSIMISTIC view of the real page's network cost, not an optimistic one.")

    failed = False
    if biggest[1] > 100e6:
        print(f"  FAIL: {biggest[0].name} exceeds GitHub's 100 MB file limit")
        failed = True
    if total > 150e6:
        print("  FAIL: site payload exceeds the 150 MB budget")
        failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
