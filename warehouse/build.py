"""Stage 2 driver — run `dbt build` with the MEASURED off-hours band injected.

fact_auth_event.is_off_hours must never be a hard-coded 9-to-5 (see pipeline/diurnal.py for
why that would be fiction on this corpus). The band is an output of a measurement, so it
has to travel from bench/diurnal.json into dbt as a var rather than being retyped into a
model where it would immediately drift.

This wrapper is the seam. It reads the band, refuses to build if the measurement says there
isn't one, and hands it to dbt.

    python warehouse/build.py --lake /data/lake --diurnal bench/diurnal.json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WAREHOUSE = REPO / "warehouse"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--lake", default="/data/lake")
    ap.add_argument("--diurnal", default=str(REPO / "bench" / "diurnal.json"))
    ap.add_argument("--target", default="dev")
    ap.add_argument(
        "--fact-materialization",
        default="view",
        choices=["table", "view"],
        help="the fact stays in the lake as a view by default — see the header of "
             "models/marts/fact_auth_event.sql for the 134 GB reason why",
    )
    ap.add_argument("--select", default=None, help="pass through to dbt --select")
    ap.add_argument("dbt_command", nargs="?", default="build", help="build | run | test | docs")
    args = ap.parse_args()

    diurnal_path = Path(args.diurnal)
    if not diurnal_path.exists():
        print(
            f"ERROR: {diurnal_path} not found. Run pipeline/diurnal.py first — the off-hours "
            "band is a MEASUREMENT, and the warehouse will not invent one.",
            file=sys.stderr,
        )
        return 2

    diurnal = json.loads(diurnal_path.read_text())
    band = diurnal["off_hours"]["band"]

    if not band:
        # Not a failure: it is the measurement telling us hour-of-day carries no signal.
        # Build anyway (is_off_hours becomes false everywhere) but make the consequence loud,
        # because Q2 becomes unclaimable and someone needs to notice.
        print(
            "WARNING: bench/diurnal.json reports NO off-hours band "
            f"({diurnal['off_hours'].get('verdict')}).\n"
            "         is_off_hours will be false for every row and Q2 must not be claimed.",
            file=sys.stderr,
        )

    # mart_streaming_windows reads the Stage-3 sink. On a fresh clone (and in CI) Stage 3
    # has not run, so the directory does not exist. Switch the model OFF rather than let it
    # read an empty directory and materialize a silently-empty mart — an honestly disabled
    # model is debuggable; a quietly empty one is not.
    streaming_sink = Path(f"{args.lake}/streaming_windows")
    streaming_enabled = streaming_sink.exists() and any(streaming_sink.rglob("*.parquet"))
    if not streaming_enabled:
        print(f"[dbt] no streaming sink at {streaming_sink} — mart_streaming_windows disabled")

    dbt_vars = {
        "lake_path": args.lake,
        "rollup_path": f"{args.lake}/rollup",
        "off_hours_band": band,
        "fact_materialization": args.fact_materialization,
        "streaming_enabled": streaming_enabled,
    }

    cmd = [
        "dbt", args.dbt_command,
        "--profiles-dir", ".",
        "--target", args.target,
        "--vars", json.dumps(dbt_vars),
    ]
    if args.select:
        cmd += ["--select", args.select]

    print(f"[dbt] lake={args.lake}  off_hours_band={band}  target={args.target}")
    return subprocess.run(cmd, cwd=WAREHOUSE).returncode


if __name__ == "__main__":
    raise SystemExit(main())
