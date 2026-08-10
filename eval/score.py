"""Score every detector under the fair protocol and write eval/results/detectors.json.

Event level and identity-day level, at v1's native thresholds and at a fixed daily alert budget.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from eval import protocol

REPO = Path(__file__).resolve().parent.parent
V1_PUBLISHED = REPO / "warehouse" / "queries" / "results" / "q5_redteam_enrichment.csv"


def _tiebreak(keys) -> np.ndarray:
    # Ties are common, since v1 rules and the histogram model both give many rows the same
    # score. pandas' hash is deterministic, so top-k is identical on every run and machine.
    return pd.util.hash_pandas_object(pd.Series(keys), index=False).to_numpy()


def _rank(scores, keys) -> np.ndarray:
    return np.lexsort((_tiebreak(keys), -np.asarray(scores, dtype=float)))


def average_precision(scores, labels, total_positives: int, keys=None) -> float:
    """Area under the precision-recall curve, with unscored positives counted as misses."""
    if total_positives == 0 or len(scores) == 0:
        return 0.0
    keys = np.arange(len(scores)) if keys is None else keys
    hits = np.asarray(labels, dtype=float)[_rank(scores, keys)]
    precision_at = np.cumsum(hits) / np.arange(1, len(hits) + 1)
    return float((precision_at * hits).sum() / total_positives)


def pr_curve(scores, labels, total_positives: int, keys=None, points: int = 40) -> list[dict]:
    """A precision-recall curve sampled at log-spaced alert counts."""
    n = len(scores)
    if n == 0:
        return []
    keys = np.arange(n) if keys is None else keys
    cum = np.cumsum(np.asarray(labels, dtype=float)[_rank(scores, keys)])
    ks = np.unique(np.geomspace(1, n, num=points).astype(int))
    return [
        {"alerts": int(k), "recall": float(cum[k - 1] / total_positives),
         "precision": float(cum[k - 1] / k)}
        for k in ks
    ]


def top_per_day(df: pd.DataFrame, budget: int) -> pd.DataFrame:
    """The `budget` highest-scoring rows of each day. df needs day, score and key."""
    ranked = df.assign(_tb=_tiebreak(df["key"]))
    ranked = ranked.sort_values(["day", "score", "_tb"], ascending=[True, False, True])
    return ranked.groupby("day", sort=False).head(budget).drop(columns="_tb")


def top_candidates_sql(scores: str, col: str, budget: int) -> str:
    """Rows that can make a day's top `budget`: at or above that day's cut score, ties included.

    Only these come back to pandas, where top_per_day breaks the ties.
    """
    return f"""
        with k as (
            select time // 86400 as day, {col} as score,
                   row_number() over (partition by time // 86400 order by {col} desc) as rk
            from read_parquet('{scores}')
        ), cut as (select day, min(score) as c from k where rk <= {budget} group by day)
        select s.*, s.time // 86400 as day, s.{col} as score,
               concat_ws(':', s.time, s.uid, s.sid, s.did) as key
        from read_parquet('{scores}') s
        join cut on cut.day = s.time // 86400 and s.{col} >= cut.c
    """


def recall_at_daily_budget(df: pd.DataFrame, budget: int, total_positives: int) -> dict:
    """Keep the top `budget` rows per day by score and count the positives they cover.

    df needs day, score, positives (attack events covered by that alert) and key.
    """
    top = top_per_day(df, budget)
    caught = int(top["positives"].sum())
    return {
        "budget_per_day": budget,
        "alerts": int(len(top)),
        "caught": caught,
        "recall": caught / total_positives if total_positives else 0.0,
    }


def identity_day_rollup(events: pd.DataFrame) -> pd.DataFrame:
    """One alert per account per day, scored by its most suspicious event."""
    return (
        events.groupby(["src_user", "day"], as_index=False)
        .agg(score=("score", "max"), positives=("is_attack", "sum"))
    )


def threshold_metrics(alerts: int, caught: int, total_positives: int, population: int) -> dict:
    precision = caught / alerts if alerts else 0.0
    base_rate = total_positives / population if population else 0.0
    return {
        "alerts": int(alerts),
        "caught": int(caught),
        "recall": caught / total_positives if total_positives else 0.0,
        "precision": precision,
        "lift": precision / base_rate if base_rate else 0.0,
    }


def workload(alerts: int, days: int, caught_events: int) -> dict:
    hours = alerts * protocol.TRIAGE_MINUTES / 60
    return {
        "alerts_per_day": alerts / days,
        "analyst_hours_per_day": hours / days,
        "attack_events_per_analyst_hour": caught_events / hours if hours else 0.0,
    }


# v1's four rules, restated over the protocol's test window. Thresholds are v1's own constants.
# Q2's band is the one thing v1 fitted on data, and v1 fitted it on all 58 days. Here it is
# re-derived from day 0, since the attack days are not allowed to shape it.
V1_RULES = """
    create or replace temp table v1 as
    with base as (
        select r.src_user, (r.event_date - date '2015-01-01')::int as day, r.is_redteam_day,
               r.auth_count, r.failure_count, r.new_dst_computers, r.fanout_zscore,
               r.failure_ratio_zscore, r.off_hours_share, r.off_hours_share_baseline_mean,
               r.baseline_days_available,
               coalesce(o.off_events, 0) / r.auth_count as off_share_fit
        from w.main_marts.mart_daily_identity_rollup r
        left join off_fit o using (src_user, event_date)
    ),
    with_baseline as (
        select *, avg(off_share_fit) over (
            partition by src_user order by day rows between 30 preceding and 1 preceding
        ) as off_base_fit
        from base
    )
    select *,
        coalesce(fanout_zscore > 3, false) as q1,
        coalesce(baseline_days_available >= 3 and off_base_fit < 0.05
                 and off_share_fit > 0.25 and auth_count >= 10, false) as q2,
        coalesce(baseline_days_available >= 3 and off_hours_share_baseline_mean < 0.05
                 and off_hours_share > 0.25 and auth_count >= 10, false) as q2_v1_band,
        new_dst_computers >= 5 as q3,
        coalesce(failure_ratio_zscore > 3 and failure_count >= 5, false) as q4
    from with_baseline
"""

V1_DETECTORS = {
    "q3_new_paths": ("q3", "new_dst_computers"),
    "q1_fanout": ("q1", "coalesce(fanout_zscore, -1e9)"),
    "q4_failures": ("q4", "case when failure_count >= 5 then coalesce(failure_ratio_zscore, -1e9) "
                          "else -1e9 end"),
    "q2_off_hours": ("q2", "case when q2 then off_share_fit - off_base_fit else -1e9 end"),
    "any_of_four": ("(q1 or q2 or q3 or q4)", "q1::int + q2::int + q3::int + q4::int"),
    "two_or_more": ("(q1::int + q2::int + q3::int + q4::int) >= 2",
                    "q1::int + q2::int + q3::int + q4::int"),
}


def _published_v1() -> dict:
    if not V1_PUBLISHED.exists():
        return {}
    df = pd.read_csv(V1_PUBLISHED)
    names = {
        "ANY of Q1-Q4": "any_of_four", "Q3 new access paths": "q3_new_paths",
        "TWO OR MORE of Q1-Q4": "two_or_more", "Q1 fan-out spike": "q1_fanout",
        "Q4 failure-ratio spike": "q4_failures", "Q2 off-hours": "q2_off_hours",
    }
    out = {}
    for _, r in df.iterrows():
        key = next(v for k, v in names.items() if r["detector"].startswith(k))
        out[key] = {"caught": int(r["redteam_days_caught"]), "total": int(r["redteam_days_total"]),
                    "recall": float(r["recall_pct"]) / 100, "alerts": int(r["alerts_raised"]),
                    "precision": float(r["precision_pct"]) / 100}
    return out


def score_v1(con, lake: str, labels: pd.DataFrame, total_events: int, days: int) -> dict:
    from pipeline.diurnal import derive_off_hours

    con.execute(f"""
        create or replace view lake as
        select src_user, event_date, day_index, hour_of_day, src_is_machine
        from read_parquet('{Path(lake).as_posix()}/auth/*/*.parquet', hive_partitioning = true)
    """)
    hours = con.execute("""
        select hour_of_day, count(*) from lake
        where day_index = 0 and not src_is_machine group by 1
    """).fetchall()
    band = derive_off_hours({int(h): int(c) for h, c in hours}, near_min_pct=0.15).get("band", [])
    band_sql = ", ".join(str(h) for h in band) or "-1"
    con.execute(f"""
        create or replace temp table off_fit as
        select src_user, event_date, count(*) as off_events from lake
        where src_user is not null and hour_of_day in ({band_sql}) group by all
    """)
    con.execute(V1_RULES)

    con.register("labels_df", labels)
    con.execute("""
        create or replace temp table label_days as
        select src_user, (time // 86400)::int as day, count(*) as n from labels_df group by all
    """)
    population = con.execute("select count(*) from v1 where day >= 1").fetchone()[0]
    rt_days = con.execute("select sum(is_redteam_day::int) from v1 where day >= 1").fetchone()[0]

    results = {}
    for name, (flag, score) in V1_DETECTORS.items():
        caught, alerts, events = con.execute(f"""
            select sum((({flag}) and is_redteam_day)::int), sum(({flag})::int),
                   coalesce(sum(case when ({flag}) then l.n end), 0)
            from v1 left join label_days l using (src_user, day) where v1.day >= 1
        """).fetchone()
        df = con.execute(f"""
            select day, src_user as key, ({score})::double as score,
                   is_redteam_day::int as rt_day, coalesce(l.n, 0) as positives
            from v1 left join label_days l using (src_user, day)
            where v1.day >= 1 and ({score}) > -1e9 and ({score}) > 0
        """).df()
        b_days = recall_at_daily_budget(df.assign(positives=df["rt_day"]),
                                        protocol.BUDGET_PER_DAY, int(rt_days))
        b_events = recall_at_daily_budget(df, protocol.BUDGET_PER_DAY, total_events)
        results[name] = {
            "threshold": {
                "identity_day": threshold_metrics(alerts or 0, caught or 0, int(rt_days),
                                                  population),
                "events_covered": int(events),
                "event_recall": int(events) / total_events,
                "workload": workload(alerts or 0, days, int(events)),
            },
            "budget": {
                "identity_day_recall": b_days["recall"],
                "identity_days_caught": b_days["caught"],
                "events_covered": b_events["caught"],
                "event_recall": b_events["recall"],
                "alerts": b_days["alerts"],
                "workload": workload(b_days["alerts"], days, b_events["caught"]),
            },
        }

    # The same rules over all 58 days with v1's own band must give back v1's published table.
    # If they do not, the re-scoring above is scoring something else.
    repro = {}
    for name, flag in (("any_of_four", "(q1 or q2_v1_band or q3 or q4)"),
                       ("q2_off_hours", "q2_v1_band"), ("q3_new_paths", "q3")):
        c, a = con.execute(f"select sum(({flag} and is_redteam_day)::int), sum(({flag})::int) "
                           "from v1").fetchone()
        repro[name] = {"caught": int(c), "alerts": int(a)}
    return {
        "off_hours_band_fit_window": band,
        "identity_days_in_test": int(population),
        "redteam_identity_days_in_test": int(rt_days),
        "detectors": results,
        "reproduced_all_days_v1_band": repro,
    }


def score_graph(con, scores: str, labels: pd.DataFrame, rt_days: pd.DataFrame,
                total_events: int, days: int, variants: list[str]) -> dict:
    con.execute(f"create or replace view s as select * from read_parquet('{scores}')")
    con.register("labels_df", labels)
    con.register("rt_days_df", rt_days)
    out = {}
    for v in variants:
        col = f"score_{v}"
        ev = con.execute(f"""
            select s.time, s.uid, s.sid, s.did, s.{col} as score,
                   (l.time is not null)::int as y
            from s left join labels_df l
              on l.time = s.time and l.uid = s.uid and l.sid = s.sid and l.did = s.did
        """).fetchnumpy()
        keys = ev["time"].astype(np.int64) * (1 << 40) + ev["uid"].astype(np.int64) * (1 << 20) \
            + ev["did"].astype(np.int64) + ev["sid"].astype(np.int64) * 7
        ap = average_precision(ev["score"], ev["y"], total_events, keys=keys)
        curve = pr_curve(ev["score"], ev["y"], total_events, keys=keys)
        del ev, keys

        top_events = con.execute(f"""
            select t.day, t.score, t.key, (l.time is not null)::int as positives
            from ({top_candidates_sql(scores, col, protocol.BUDGET_PER_DAY)}) t
            left join labels_df l
              on l.time = t.time and l.uid = t.uid and l.sid = t.sid and l.did = t.did
        """).df()
        b_ev = recall_at_daily_budget(top_events, protocol.BUDGET_PER_DAY, total_events)

        idd = con.execute(f"""
            with d as (
                select s.uid, s.time // 86400 as day, max({col}) as score,
                       count(l.time) as positives
                from s left join labels_df l
                  on l.time = s.time and l.uid = s.uid and l.sid = s.sid and l.did = s.did
                group by all
            )
            select d.day, concat_ws(':', d.uid, d.day) as key, d.score, d.positives,
                   (r.uid is not null)::int as rt_day
            from d left join rt_days_df r on r.uid = d.uid and r.day = d.day
        """).df()
        n_rt_days = int(len(rt_days))
        b_days = recall_at_daily_budget(idd.assign(positives=idd["rt_day"]),
                                        protocol.BUDGET_PER_DAY, n_rt_days)
        b_idev = recall_at_daily_budget(idd, protocol.BUDGET_PER_DAY, total_events)
        out[v] = {
            "event": {
                "average_precision": ap,
                "pr_curve": curve,
                "budget": {**b_ev, "workload": workload(b_ev["alerts"], days, b_ev["caught"])},
            },
            "identity_day": {
                "budget": {
                    "identity_day_recall": b_days["recall"],
                    "identity_days_caught": b_days["caught"],
                    "events_covered": b_idev["caught"],
                    "event_recall": b_idev["recall"],
                    "alerts": b_days["alerts"],
                    "workload": workload(b_days["alerts"], days, b_idev["caught"]),
                },
            },
        }
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--work", required=True, help="directory written by detect/graph/extract.py")
    ap.add_argument("--scores", required=True, help="directory written by detect/graph/run.py")
    ap.add_argument("--warehouse", required=True, help="v1 DuckDB warehouse")
    ap.add_argument("--lake", required=True)
    ap.add_argument("--extra", nargs="*", default=[], help="extra scores.parquet files, name=path")
    ap.add_argument("--data-label", required=True, help="which data these numbers came from")
    ap.add_argument("--out", default=str(REPO / "eval" / "results" / "detectors.json"))
    args = ap.parse_args()

    from detect.graph.model import GROUPS, HEADLINE

    work = Path(args.work)
    extract = json.loads((work / "extract.json").read_text())
    manifest = json.loads((Path(args.scores) / "manifest.json").read_text())
    protocol.assert_fit_window(manifest)

    con = duckdb.connect()
    con.execute(f"attach '{Path(args.warehouse).as_posix()}' as w (read_only)")
    labels = con.execute(f"select * from read_parquet('{(work / 'labels.parquet').as_posix()}')"
                         f" where time >= {protocol.TEST_START}").df()
    rt_days = con.execute(f"""
        select distinct u.uid, (r.time // 86400)::int as day
        from read_parquet('{(work / 'redteam.parquet').as_posix()}') r
        join read_parquet('{(work / 'users.parquet').as_posix()}') u on u.name = r."user"
        where r.time >= {protocol.TEST_START}
    """).df()
    total = int(len(labels))
    days = int(extract["max_time"] // protocol.SECONDS_PER_DAY)

    v1 = score_v1(con, args.lake, labels, total, days)
    scores_path = (Path(args.scores) / "scores.parquet").as_posix()
    graph = score_graph(con, scores_path, labels, rt_days, total, days, list(GROUPS))
    for spec in args.extra:
        name, path = spec.split("=", 1)
        graph.update(score_graph(con, Path(path).as_posix(), labels, rt_days, total, days,
                                 [name]))

    result = {
        "data": args.data_label,
        "protocol": {
            "fit_window_seconds": [0, protocol.FIT_END],
            "histogram_fit_seconds": [protocol.WARMUP_END, protocol.FIT_END],
            "test_window_days": [1, days],
            "test_days": days,
            "triage_minutes_per_alert": protocol.TRIAGE_MINUTES,
            "budget_per_day": protocol.BUDGET_PER_DAY,
            "headline_detector": HEADLINE,
        },
        "labels": {**extract["labels"], "in_log_test_window": total,
                   "redteam_identity_days_test_window": int(len(rt_days))},
        "candidates": extract["candidates"],
        "events_scored": manifest["events_scored"],
        "detector_run": manifest,
        "v1_published": _published_v1(),
        "v1_protocol": v1,
        "graph": graph,
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(result, indent=2) + "\n")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
