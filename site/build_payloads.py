"""
Build everything the demo site serves: the Parquet the browser queries, and the bench bundle.

The page renders every number from bench.json. If a figure is on screen and not in an
artifact, site/audit.py fails the build.
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

# The rollup ships whole, every identity-day including machine accounts. That costs payload, but
# Q5's precision and lift are ratios against the total identity-day population, so an extract
# that quietly dropped the machine accounts would let the browser compute a flatteringly better
# Q5 than the one committed in warehouse/queries/results/.
EXTRACTS = {
    # Column pruned and down-cast on purpose, since this is what a phone downloads. success_count
    # is derivable from auth_count minus failure_count, distinct_src_computers is read by no
    # query on the page, and the ratios are displayed to four decimals so 32-bit floats are
    # plenty. The committed query results are still computed in full double precision.
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
    # The vectors the browser searches. Every attack day, plus a random sample of ordinary ones,
    # because all 1.6 million would be 333 MB.
    #
    # This makes attack days about forty times commoner in the browser than they are in reality,
    # so the panel deliberately does NOT report a hit rate. It shows you what the search returns
    # and lets you look. The measured hit rate comes from the full index and is stated separately.
    "vectors": """
        select
            v.src_user,
            v.event_date,
            v.vector,
            s.is_attack,
            s.anomaly_score
        from read_parquet('/data/lake/vectors/**/*.parquet') v
        join read_parquet('/data/lake/vector_scores.parquet') s
          on v.src_user = s.src_user and v.event_date = s.event_date
        where s.is_attack
           or hash(v.src_user || v.event_date::varchar) % 64 = 0
    """,
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
        # ZSTD over snappy. The browser decompresses once, and every byte here is a byte the
        # reader waits for on a phone.
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
    """Collect every bench artifact into one file the page renders from."""
    bundle: dict = {}
    for path in sorted(BENCH.glob("*.json")):
        bundle[path.stem] = json.loads(path.read_text())
        print(f"  bench/{path.name}")

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

    # Ship the SQL text itself so the preset buttons show the real query, not a simplified
    # retype of it that could drift from what produced the committed results.
    queries_dir = REPO / "warehouse" / "queries"
    bundle["sql"] = {
        p.stem: p.read_text() for p in sorted(queries_dir.glob("q*.sql"))
    }

    bundle["built_at"] = datetime.now(UTC).isoformat(timespec="seconds")
    return bundle


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
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

    site = REPO / "site"
    total = sum(p.stat().st_size for p in site.rglob("*") if p.is_file())
    biggest = max(
        ((p, p.stat().st_size) for p in site.rglob("*") if p.is_file()),
        key=lambda kv: kv[1],
    )

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
    print(f"[budget] largest file          {biggest[1] / 1e6:>8.2f} MB   {biggest[0].name}  (GitHub limit 100 MB)")
    print(f"[budget] page shell            {shell / 1e6:>8.2f} MB   everything needed to render")
    print(f"[budget] Parquet extracts      {extracts_total / 1e6:>8.2f} MB   see the note below")
    print(f"[budget]   largest extract     {largest_extract / 1e6:>8.2f} MB   worst case if a query scanned every column")
    print("[budget] DuckDB wasm           35.66 MB   gzipped to about 9 MB in transit, then cached")
    print()
    print("[budget] The Parquet figure is not a download figure. DuckDB-WASM issues HTTP RANGE")
    print("         requests and reads only the column chunks a query touches, but only if the")
    print("         server honours Range. GitHub Pages does. Python's http.server does not, so a")
    print("         local `make serve` preview logs 'fall back to full HTTP read' and pulls whole")
    print("         files. The local preview is a pessimistic view of the real cost, not an")
    print("         optimistic one.")

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
