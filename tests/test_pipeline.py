"""
Parsing, sessionization, and rollup correctness.

The rollup tests check numbers against expectations worked out by hand from a fixture small
enough to verify by eye. A rollup that runs and returns wrong counts is worse than one that crashes.
"""

from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from pipeline.common import AUTH_SCHEMA, derive_columns
from pipeline.diurnal import derive_off_hours
from pipeline.rollup import build_rollup
from pipeline.sessionize import build_lake, build_sessions


def test_derive_columns_maps_lanl_conventions(spark):
    raw = spark.createDataFrame(
        [
            # t=0 -> anchor day 0, hour 0
            (0, "U1@DOM1", "U1@DOM1", "C1", "C2", "Kerberos", "Network", "LogOn", "Success"),
            # t=90000 -> day 1, hour 1 (90000 = 86400 + 3600)
            (90_000, "C5$@DOM1", "SYSTEM@C5", "C5", "C5", "?", "Service", "LogOn", "Fail"),
        ],
        schema=AUTH_SCHEMA,
    )
    got = derive_columns(raw).orderBy("time").collect()

    assert got[0]["day_index"] == 0
    assert got[0]["hour_of_day"] == 0
    assert got[0]["event_date"] == date(2015, 1, 1)
    assert got[0]["is_success"] is True
    assert got[0]["is_failure"] is False
    assert got[0]["src_user_name"] == "U1"
    assert got[0]["src_domain"] == "DOM1"
    assert got[0]["src_is_machine"] is False

    assert got[1]["day_index"] == 1
    assert got[1]["hour_of_day"] == 1
    assert got[1]["event_date"] == date(2015, 1, 2)
    assert got[1]["is_failure"] is True
    # LANL's '?' null marker must become a real NULL, or COUNT/DISTINCT silently count it.
    assert got[1]["auth_type"] is None
    # Trailing '$' marks a machine account.
    assert got[1]["src_is_machine"] is True


def test_question_mark_becomes_null_not_literal(spark):
    raw = spark.createDataFrame(
        [(1, "?", "?", "?", "?", "?", "?", "?", "?")], schema=AUTH_SCHEMA
    )
    got = derive_columns(raw).collect()[0]
    for col in ("src_user", "dst_user", "src_computer", "dst_computer", "auth_type"):
        assert got[col] is None, f"{col} kept the literal '?'"


def test_lake_is_partitioned_by_date_and_preserves_row_count(spark, sample_auth_path, tmp_path):
    out = str(tmp_path / "lake")
    stats = build_lake(spark, sample_auth_path, out, coalesce=0)

    # The committed slice is 8 days, so the lake must have 8 date partitions.
    assert stats["partitions"] == 8
    assert stats["rows"] == 99_434, "CI slice row count changed, regenerate data/sample/"

    written = spark.read.parquet(out)
    for col in ("event_date", "day_index", "hour_of_day", "is_success", "src_is_machine"):
        assert col in written.columns


def test_lake_round_trips_without_losing_events(spark, sample_auth_path, tmp_path):
    """Reading the lake back must yield exactly what went in, with no silent DROPMALFORMED loss."""
    out = str(tmp_path / "lake")
    build_lake(spark, sample_auth_path, out, coalesce=0)

    raw_count = (
        spark.read.option("header", "true").csv(sample_auth_path).count()
    )
    lake_count = spark.read.parquet(out).count()
    assert lake_count == raw_count


def test_sessions_split_on_idle_gap(spark, tmp_path):
    """Three events: two within the gap, one after it. Expect exactly two sessions."""
    lake = str(tmp_path / "lake")
    rows = [
        # (user, host) does 2 events 60s apart -> one session
        (100, "U1@DOM1", "U1@DOM1", "C1", "C9", "Kerberos", "Network", "LogOn", "Success"),
        (160, "U1@DOM1", "U1@DOM1", "C1", "C9", "Kerberos", "Network", "LogOn", "Success"),
        # then goes quiet for 2h (7200s > 1800s gap) -> a second session
        (7_360, "U1@DOM1", "U1@DOM1", "C1", "C9", "Kerberos", "Network", "LogOn", "Success"),
    ]
    derive_columns(spark.createDataFrame(rows, schema=AUTH_SCHEMA)).write.mode(
        "overwrite"
    ).partitionBy("event_date").parquet(lake)

    stats = build_sessions(spark, lake, str(tmp_path / "sessions"), idle_gap=1800)
    assert stats["sessions"] == 2

    got = spark.read.parquet(str(tmp_path / "sessions")).orderBy("session_start").collect()
    assert got[0]["event_count"] == 2
    assert got[0]["duration_seconds"] == 60
    assert got[1]["event_count"] == 1
    assert got[1]["duration_seconds"] == 0


def test_session_boundary_is_exclusive_at_exactly_the_gap(spark, tmp_path):
    """A gap of exactly idle_gap must not split. The rule is `> gap`, and an off-by-one here
    would silently reshape every session in the corpus."""
    lake = str(tmp_path / "lake")
    rows = [
        (100, "U1@DOM1", "U1@DOM1", "C1", "C9", "Kerberos", "Network", "LogOn", "Success"),
        (1_900, "U1@DOM1", "U1@DOM1", "C1", "C9", "Kerberos", "Network", "LogOn", "Success"),
    ]
    derive_columns(spark.createDataFrame(rows, schema=AUTH_SCHEMA)).write.mode(
        "overwrite"
    ).partitionBy("event_date").parquet(lake)

    stats = build_sessions(spark, lake, str(tmp_path / "sessions"), idle_gap=1800)
    assert stats["sessions"] == 1, "gap of exactly idle_gap must not open a new session"


@pytest.fixture(scope="module")
def toy_lake(spark, tmp_path_factory):
    """A fixture small enough to verify by eye.

    U1 day 0: 4 events, 3 success 1 fail, hits C9 and C8, one of them at hour 2.
    U1 day 1: 2 events, hits C9 (already seen) and C7 (new).
    U2 day 0: 1 event, hits C9.
    """
    base = tmp_path_factory.mktemp("toy")
    lake = str(base / "auth")
    rt = str(base / "redteam")

    D0, D1 = 0, 86_400
    rows = [
        # U1 day 0, hour 10 (36000s), hour 2 (7200s = off-hours)
        (D0 + 36_000, "U1@DOM1", "x@DOM1", "C1", "C9", "Kerberos", "Network", "LogOn", "Success"),
        (D0 + 36_060, "U1@DOM1", "x@DOM1", "C1", "C8", "Kerberos", "Network", "LogOn", "Success"),
        (D0 + 36_120, "U1@DOM1", "x@DOM1", "C1", "C9", "Kerberos", "Network", "LogOn", "Fail"),
        (D0 + 7_200, "U1@DOM1", "x@DOM1", "C1", "C9", "Kerberos", "Network", "LogOn", "Success"),
        # U1 day 1, C9 is old, C7 is new
        (D1 + 36_000, "U1@DOM1", "x@DOM1", "C1", "C9", "Kerberos", "Network", "LogOn", "Success"),
        (D1 + 36_060, "U1@DOM1", "x@DOM1", "C1", "C7", "Kerberos", "Network", "LogOn", "Success"),
        # U2 day 0
        (D0 + 36_000, "U2@DOM1", "x@DOM1", "C2", "C9", "Kerberos", "Network", "LogOn", "Success"),
    ]
    derive_columns(spark.createDataFrame(rows, schema=AUTH_SCHEMA)).write.mode(
        "overwrite"
    ).partitionBy("event_date").parquet(lake)

    # Label U1's day-1 activity as a compromise.
    spark.createDataFrame(
        [Row(time=D1 + 36_060, user="U1@DOM1", src_computer="C1", dst_computer="C7",
             day_index=1, event_date=date(2015, 1, 2), hour_of_day=10)]
    ).write.mode("overwrite").parquet(rt)

    return lake, rt


def test_rollup_matches_hand_computed_values(spark, toy_lake):
    lake, rt = toy_lake
    got = {
        (r["src_user"], str(r["event_date"])): r
        for r in build_rollup(spark, lake, rt, off_hours_band=[0, 1, 2, 3]).collect()
    }

    u1d0 = got[("U1@DOM1", "2015-01-01")]
    assert u1d0["auth_count"] == 4
    assert u1d0["success_count"] == 3
    assert u1d0["failure_count"] == 1
    assert u1d0["failure_ratio"] == pytest.approx(0.25)
    assert u1d0["distinct_dst_computers"] == 2       # C9, C8
    assert u1d0["distinct_src_computers"] == 1       # C1
    assert u1d0["new_dst_computers"] == 2            # both first seen today
    assert u1d0["off_hours_events"] == 1             # the 07:00->02:00 one
    assert u1d0["off_hours_share"] == pytest.approx(0.25)
    assert u1d0["is_redteam_day"] is False

    u1d1 = got[("U1@DOM1", "2015-01-02")]
    assert u1d1["auth_count"] == 2
    assert u1d1["distinct_dst_computers"] == 2       # C9, C7
    # THE POINT OF THE FIXTURE: C9 was seen on day 0, so only C7 is new.
    assert u1d1["new_dst_computers"] == 1
    assert u1d1["is_redteam_day"] is True

    u2d0 = got[("U2@DOM1", "2015-01-01")]
    assert u2d0["auth_count"] == 1
    assert u2d0["new_dst_computers"] == 1


@pytest.mark.parametrize(
    "broadcast,dedup",
    [(False, False), (True, False), (False, True), (True, True)],
)
def test_optimization_variants_produce_identical_results(spark, toy_lake, broadcast, dedup):
    """The 2x2 must change execution strategy only.

    If a variant computed a different answer the benchmark would be measuring the speed of
    being wrong. This is the test that makes the speedup claim honest.
    """
    lake, rt = toy_lake
    cols = [
        "src_user", "event_date", "auth_count", "success_count", "failure_count",
        "distinct_dst_computers", "distinct_src_computers", "new_dst_computers",
        "off_hours_events", "is_redteam_day",
    ]

    reference = (
        build_rollup(spark, lake, rt, [0, 1, 2, 3], broadcast_redteam=False, dedup_distincts=False)
        .select(*cols).orderBy("src_user", "event_date").collect()
    )
    variant = (
        build_rollup(spark, lake, rt, [0, 1, 2, 3], broadcast_redteam=broadcast, dedup_distincts=dedup)
        .select(*cols).orderBy("src_user", "event_date").collect()
    )
    assert variant == reference


def test_rollup_is_deterministic(spark, toy_lake):
    """Same input, same code, same answer."""
    lake, rt = toy_lake
    runs = [
        build_rollup(spark, lake, rt, [0, 1, 2, 3])
        .orderBy("src_user", "event_date")
        .agg(F.sum("auth_count"), F.sum("distinct_dst_computers"), F.sum("new_dst_computers"))
        .collect()
        for _ in range(2)
    ]
    assert runs[0] == runs[1]


def test_redteam_label_does_not_smear_across_the_identity(spark, toy_lake):
    """U1 is compromised on day 1 only, so day 0 must not be flagged.

    Joining the label on user alone would mark every day of a compromised account and quietly
    inflate every recall number in Q5.
    """
    lake, rt = toy_lake
    rows = {
        (r["src_user"], str(r["event_date"])): r["is_redteam_day"]
        for r in build_rollup(spark, lake, rt, []).collect()
    }
    assert rows[("U1@DOM1", "2015-01-01")] is False
    assert rows[("U1@DOM1", "2015-01-02")] is True


def test_off_hours_band_found_in_a_clear_trough():
    # Quiet 22-05, busy 06-21.
    counts = {h: (100 if h in (22, 23, 0, 1, 2, 3, 4, 5) else 500) for h in range(24)}
    got = derive_off_hours(counts, near_min_pct=0.15)
    assert got["band"] == [0, 1, 2, 3, 4, 5, 22, 23]
    assert got["band_hours"] == 8
    assert got["peak_to_trough_ratio"] == 5.0


def test_off_hours_refuses_to_guess_on_a_flat_curve():
    """A flat curve means hour of day carries no signal, so the band must come back empty
    rather than picking an arbitrary 8 hours. This is the guard that stopped the project from
    claiming an off-hours signal the data does not support."""
    counts = {h: 1000 for h in range(24)}
    got = derive_off_hours(counts, near_min_pct=0.15)
    assert got["band"] == []
    assert "MUST NOT be claimed" in got["verdict"]


def test_off_hours_band_wraps_midnight():
    counts = {h: (100 if h in (23, 0, 1) else 400) for h in range(24)}
    got = derive_off_hours(counts, near_min_pct=0.15)
    assert got["band"] == [0, 1, 23]
    assert got["band_start_hour"] == 23  # the run starts before midnight and wraps
