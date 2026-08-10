"""The fair evaluation protocol every detector in Tarn is scored under.

Follows arXiv 2607.29390: a temporal split, no attack-period data in any fitted statistic,
event-level labels with a stated policy, and workload counted in analyst time.
"""

from __future__ import annotations

import duckdb
import pandas as pd

SECONDS_PER_DAY = 86_400

# Day 0 is the only stretch of the corpus with no red-team activity. The first labelled row is
# at t=150885, so anything a detector fits (histograms, embeddings, the off-hours band) must be
# learned from t < 86400 and then frozen.
FIT_END = SECONDS_PER_DAY
TEST_START = SECONDS_PER_DAY
# Everything in the fit window starts from an empty graph, so the first twelve hours only warm
# the state up and the histograms are fitted on the second twelve.
WARMUP_END = SECONDS_PER_DAY // 2

# Five minutes to read one alert is a stated assumption, not a measurement. Every workload
# number scales linearly with it.
TRIAGE_MINUTES = 5
BUDGET_PER_DAY = 100

# Which events a lateral-movement detector may see, chosen without looking at the labels:
# remote logons by human accounts. Keeping only NTLM because the red team used NTLM is the
# label-informed preprocessing 2607.29390 warns about, so it is not done here.
CANDIDATE_FILTER = """
    auth_orientation = 'LogOn'
    and src_user like 'U%'
    and not coalesce(src_is_machine, false)
    and src_computer is not null
    and dst_computer is not null
    and src_computer <> dst_computer
"""

BANNED_FEATURE_WORDS = ("attack", "redteam", "red_team", "label", "compromis", "malicious")


class ProtocolViolation(AssertionError):
    """Raised when a detector breaks the protocol. Scoring stops instead of printing a number."""


def assert_fit_window(manifest: dict) -> None:
    """Fail unless the detector declares that every fitted statistic came from before FIT_END."""
    fit_max = manifest.get("fit_max_time")
    if fit_max is None:
        raise ProtocolViolation("detector manifest does not declare fit_max_time")
    if fit_max >= FIT_END:
        raise ProtocolViolation(
            f"detector fitted on t={fit_max}, the fit window closes at t={FIT_END}"
        )


def assert_no_label_columns(columns) -> None:
    for name in columns:
        low = name.lower()
        for word in BANNED_FEATURE_WORDS:
            if word in low:
                raise ProtocolViolation(f"feature {name!r} looks like it came from the labels")


def apply_label_policy(redteam, events, con=None) -> tuple[pd.DataFrame, dict]:
    """Turn red-team rows into event labels and count how many survive each step.

    redteam and events are DataFrames, or table names on `con`. The LANL file has 749 rows but
    715 distinct events, and 14 of those never appear in the authentication log.
    """
    con = con or duckdb.connect()
    if isinstance(redteam, pd.DataFrame):
        con.register("_rt", redteam)
        redteam = "_rt"
    if isinstance(events, pd.DataFrame):
        con.register("_ev", events)
        events = "_ev"
    con.execute(f"""
        create or replace temp table _labels as
        with _distinct as (
            select distinct time, "user" as src_user, src_computer, dst_computer from {redteam}
        )
        select x.* from _distinct x
        where exists (
            select 1 from {events} e
            where e.time = x.time and e.src_user = x.src_user
              and e.src_computer = x.src_computer and e.dst_computer = x.dst_computer
        )
    """)
    rows = con.execute(f"select count(*) from {redteam}").fetchone()[0]
    distinct = con.execute(
        f'select count(*) from (select distinct time, "user", src_computer, dst_computer '
        f"from {redteam})"
    ).fetchone()[0]
    labels = con.execute("select * from _labels order by time").df()
    stats = {
        "rows": int(rows),
        "distinct": int(distinct),
        "in_log": int(len(labels)),
        "not_in_log": int(distinct - len(labels)),
    }
    return labels, stats
