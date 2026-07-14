"""
Run dbt with the measured off-hours band injected as a var.
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
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--lake", default="/data/lake")
    ap.add_argument("--diurnal", default=str(REPO / "bench" / "diurnal.json"))
    ap.add_argument("--target", default="dev")
    ap.add_argument("--fact-materialization", default="view", choices=["table", "view"])
    ap.add_argument("--select", default=None)
    ap.add_argument("dbt_command", nargs="?", default="build")
    args = ap.parse_args()

    # The band is an output of a measurement, so it travels from bench/diurnal.json into dbt as
    # a var. Retyping it into a model is how it silently drifts from what was measured.
    diurnal_path = Path(args.diurnal)
    if not diurnal_path.exists():
        print(
            f"ERROR: {diurnal_path} not found. Run pipeline/diurnal.py first. The off-hours band "
            "is a measurement and the warehouse will not invent one.",
            file=sys.stderr,
        )
        return 2

    diurnal = json.loads(diurnal_path.read_text())
    band = diurnal["off_hours"]["band"]

    if not band:
        # Not a failure. It is the measurement saying hour of day carries no signal. Build
        # anyway, but make the consequence loud, because Q2 becomes unclaimable.
        print(
            "WARNING: bench/diurnal.json reports NO off-hours band "
            f"({diurnal['off_hours'].get('verdict')}).\n"
            "         is_off_hours will be false for every row and Q2 must not be claimed.",
            file=sys.stderr,
        )

    # On a fresh clone, and in CI, Stage 3 has not run and the streaming sink does not exist.
    # Switch the model off rather than let it read an empty directory and quietly materialize an
    # empty mart. An honestly disabled model is debuggable. A quietly empty one is not.
    streaming_sink = Path(f"{args.lake}/streaming_windows")
    streaming_enabled = streaming_sink.exists() and any(streaming_sink.rglob("*.parquet"))
    if not streaming_enabled:
        print(f"[dbt] no streaming sink at {streaming_sink}, mart_streaming_windows disabled")

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
