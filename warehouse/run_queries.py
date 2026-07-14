"""
Run the five showcase queries and commit their results.

A query with no committed result is an assertion, not evidence.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import UTC, datetime
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
QUERY_DIR = REPO / "warehouse" / "queries"
RESULT_DIR = QUERY_DIR / "results"

# dbt-duckdb materializes into <target_schema>_<custom_schema>, so the marts land in main_marts
# and not marts. Kept in one place because the demo site substitutes the same placeholders
# against a completely different schema layout.
MARTS_SCHEMA = "main_marts"

TABLES = {
    "ROLLUP": f"{MARTS_SCHEMA}.mart_daily_identity_rollup",
    "FACT": f"{MARTS_SCHEMA}.fact_auth_event",
    "DIM_IDENTITY": f"{MARTS_SCHEMA}.dim_identity",
    "DIM_COMPUTER": f"{MARTS_SCHEMA}.dim_computer",
    "DIM_TIME": f"{MARTS_SCHEMA}.dim_time",
}


def render(sql: str, tables: dict[str, str] = TABLES) -> str:
    """Substitute the {{TABLE}} placeholders. An unknown one is an error, not a no-op."""
    def sub(match: re.Match) -> str:
        name = match.group(1).strip()
        if name not in tables:
            raise KeyError(f"unknown table placeholder {{{{{name}}}}}")
        return tables[name]

    return re.sub(r"\{\{(\w+)\}\}", sub, sql)


def title_of(sql: str) -> str:
    for line in sql.splitlines():
        if line.startswith("--"):
            return line.lstrip("- ").strip()
    return ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--db", default="/data/work/tarn.duckdb")
    ap.add_argument("--out", default=str(RESULT_DIR))
    ap.add_argument("--max-rows", type=int, default=100)
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(args.db, read_only=True)
    summary = {
        "warehouse": args.db,
        "run_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "queries": [],
    }

    for path in sorted(QUERY_DIR.glob("q*.sql")):
        sql = path.read_text()
        rendered = render(sql)

        t0 = time.perf_counter()
        df = con.execute(rendered).df()
        elapsed = round(time.perf_counter() - t0, 3)

        csv_path = out / f"{path.stem}.csv"
        df.head(args.max_rows).to_csv(csv_path, index=False)

        summary["queries"].append(
            {
                "id": path.stem,
                "title": title_of(sql),
                "sql": f"warehouse/queries/{path.name}",
                "result": f"warehouse/queries/results/{csv_path.name}",
                "rows_returned": len(df),
                "rows_committed": min(len(df), args.max_rows),
                "seconds": elapsed,
            }
        )
        print(f"[{path.stem}] {len(df):>5} rows in {elapsed:>6.3f}s -> {csv_path.name}")

    (out / "_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"\nwrote {out / '_summary.json'}")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
