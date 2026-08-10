"""Pull the candidate events out of the lake, or the committed CSV slice, sorted by time.

Users and hosts become small integer ids so the feature engine can key its state on plain ints.
Labels go to a separate file that the detector never opens.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

from eval import protocol

# The raw CSV slice has none of the lake's derived columns, so they are rebuilt here with the
# same rules pipeline/common.py applies: '?' is LANL's null, and a '$@' marks a machine account.
CSV_VIEW = """
    create or replace view auth as
    select
        time::int as time,
        nullif(src_user, '?') as src_user,
        nullif(src_computer, '?') as src_computer,
        nullif(dst_computer, '?') as dst_computer,
        nullif(auth_orientation, '?') as auth_orientation,
        nullif(outcome, '?') as outcome,
        contains(src_user, '$@') as src_is_machine
    from read_csv('{path}', header = true, all_varchar = true)
"""
LAKE_VIEW = """
    create or replace view auth as
    select time, src_user, src_computer, dst_computer, auth_orientation, outcome, src_is_machine
    from read_parquet('{path}/auth/*/*.parquet', hive_partitioning = true)
"""


def extract(out: Path, lake: str | None = None, sample_auth: str | None = None,
            redteam: str | None = None, threads: int = 0) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    if threads:
        con.execute(f"set threads = {threads}")
    con.execute("set preserve_insertion_order = false")
    if lake:
        con.execute(LAKE_VIEW.format(path=Path(lake).as_posix()))
        rt_src = f"read_parquet('{Path(lake).as_posix()}/redteam/*.parquet')"
    else:
        con.execute(CSV_VIEW.format(path=Path(sample_auth).as_posix()))
        rt_src = f"read_csv('{Path(redteam).as_posix()}', header = true)"

    con.execute(f"""
        create temp table cand as
        select time, src_user, src_computer, dst_computer, bool_or(outcome = 'Fail') as fail
        from auth where {protocol.CANDIDATE_FILTER}
        group by all
    """)
    con.execute("""
        create temp table users as
        select row_number() over (order by src_user)::int as uid, src_user as name
        from (select distinct src_user from cand)
    """)
    con.execute("""
        create temp table hosts as
        select row_number() over (order by h)::int as hid, h as name
        from (select src_computer as h from cand union select dst_computer from cand)
    """)
    con.execute(f"""
        copy (
            select c.time, u.uid, hs.hid as sid, hd.hid as did, c.fail
            from cand c
            join users u on u.name = c.src_user
            join hosts hs on hs.name = c.src_computer
            join hosts hd on hd.name = c.dst_computer
            order by c.time, u.uid, hs.hid, hd.hid
        ) to '{(out / "candidates.parquet").as_posix()}' (format parquet, compression zstd)
    """)
    con.execute(f"copy users to '{(out / 'users.parquet').as_posix()}' (format parquet)")
    con.execute(f"copy hosts to '{(out / 'hosts.parquet').as_posix()}' (format parquet)")

    # The denominator is every labelled event present anywhere in the authentication log, not
    # just in the candidates. A label the candidate filter throws away is a miss, not a
    # smaller denominator.
    con.execute(f"create temp table rt as select * from {rt_src}")
    con.execute(f"copy (select time, \"user\" from rt) to "
                f"'{(out / 'redteam.parquet').as_posix()}' (format parquet)")
    labels, stats = protocol.apply_label_policy("rt", "auth", con=con)
    con.register("labels_df", labels)
    in_cand = con.execute("""
        select count(*) from labels_df l join cand c using (time, src_user, src_computer,
        dst_computer)
    """).fetchone()[0]
    stats["in_candidates"] = int(in_cand)
    con.execute(f"""
        copy (
            select l.time, l.src_user, l.src_computer, l.dst_computer,
                   u.uid, hs.hid as sid, hd.hid as did
            from labels_df l
            left join users u on u.name = l.src_user
            left join hosts hs on hs.name = l.src_computer
            left join hosts hd on hd.name = l.dst_computer
        ) to '{(out / "labels.parquet").as_posix()}' (format parquet)
    """)
    counts = con.execute(f"""
        select count(*), sum((time < {protocol.FIT_END})::int), max(time),
               (select count(*) from users), (select count(*) from hosts)
        from cand
    """).fetchone()
    report = {
        "source": lake or sample_auth,
        "candidates": int(counts[0]),
        "fit_window_candidates": int(counts[1]),
        "max_time": int(counts[2]),
        "users": int(counts[3]),
        "hosts": int(counts[4]),
        "labels": stats,
    }
    (out / "extract.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--out", required=True)
    ap.add_argument("--lake", default=None)
    ap.add_argument("--sample-auth", default="data/sample/auth_sample.csv.gz")
    ap.add_argument("--sample-redteam", default="data/sample/redteam_sample.csv.gz")
    ap.add_argument("--threads", type=int, default=0)
    args = ap.parse_args()
    report = extract(Path(args.out), lake=args.lake, sample_auth=args.sample_auth,
                     redteam=args.sample_redteam, threads=args.threads)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
