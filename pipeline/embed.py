"""
Turn each person-day into a vector describing how that person behaved that day.

The vector is built only from behaviour. Nothing derived from the answer key goes into it, or
the search would be finding the labels rather than the attack.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.common import spark_session

# The vocabularies are taken from the corpus itself, not guessed. Anything outside them lands in
# the "other" slot, so the shares always sum to one and a rare value cannot silently vanish.
AUTH_TYPES = ["Kerberos", "Negotiate", "NTLM", "MICROSOFT"]
LOGON_TYPES = [
    "Network", "Service", "Unlock", "Interactive", "Batch", "NewCredentials", "NetworkCleartext",
]
ORIENTATIONS = ["LogOn", "LogOff", "TGS", "TGT", "AuthMap", "ScreenLock", "ScreenUnlock"]

# The order is fixed and written into the artifact. A vector whose dimensions mean different
# things on different runs is not a vector, it is a bug that compiles.
FEATURE_NAMES = (
    ["log_auth_count", "log_failure_count", "log_distinct_dst", "log_distinct_src", "log_new_dst"]
    + ["failure_ratio"]
    + [f"hour_{h:02d}_share" for h in range(24)]
    + [f"auth_type_{t.lower()}_share" for t in AUTH_TYPES]
    + ["auth_type_other_share"]
    + [f"logon_type_{t.lower()}_share" for t in LOGON_TYPES]
    + ["logon_type_other_share"]
    + [f"orientation_{t.lower()}_share" for t in ORIENTATIONS]
    + ["orientation_other_share"]
    + ["is_machine"]
)
DIM = len(FEATURE_NAMES)


def build_features(spark: SparkSession, lake: str, rollup: str) -> DataFrame:
    """One row per person per day, with the raw (unscaled) features."""
    events = spark.read.parquet(lake).filter(F.col("src_user").isNotNull())

    def share(col: str, vocab: list[str], prefix: str):
        """Turn a category column into one share per known value, plus an 'other' share."""
        # Startswith, not equality, because the corpus truncates some of the longer type names at
        # several different lengths. Matching those exactly would scatter one behaviour across
        # four dimensions.
        cols = []
        for v in vocab:
            cols.append(
                F.sum(F.when(F.col(col).startswith(v), 1).otherwise(0)).alias(f"{prefix}_{v.lower()}")
            )
        known = None
        for v in vocab:
            cond = F.col(col).startswith(v)
            known = cond if known is None else (known | cond)
        cols.append(F.sum(F.when(known, 0).otherwise(1)).alias(f"{prefix}_other"))
        return cols

    hour_cols = [
        F.sum(F.when(F.col("hour_of_day") == h, 1).otherwise(0)).alias(f"hour_{h:02d}")
        for h in range(24)
    ]

    daily = events.groupBy("src_user", "event_date").agg(
        F.count("*").alias("auth_count"),
        F.sum(F.col("is_failure").cast("int")).alias("failure_count"),
        F.countDistinct("dst_computer").alias("distinct_dst"),
        F.countDistinct("src_computer").alias("distinct_src"),
        F.max(F.col("src_is_machine").cast("int")).alias("is_machine"),
        *hour_cols,
        *share("auth_type", AUTH_TYPES, "at"),
        *share("logon_type", LOGON_TYPES, "lt"),
        *share("auth_orientation", ORIENTATIONS, "or"),
    )

    # new_dst comes from the rollup rather than being recomputed. It is the one feature that needs
    # the person's whole history, and Spark already paid for that once.
    new_dst = spark.read.parquet(rollup).select(
        "src_user", "event_date", F.col("new_dst_computers").alias("new_dst")
    )
    daily = daily.join(new_dst, ["src_user", "event_date"], "left").fillna({"new_dst": 0})

    n = F.col("auth_count")
    cols = [
        F.col("src_user"),
        F.col("event_date"),
        # Counts are log scaled. Raw, one service account with a million logins would dominate
        # every distance in the space and everybody else would look identical to each other.
        F.log1p("auth_count").alias("log_auth_count"),
        F.log1p("failure_count").alias("log_failure_count"),
        F.log1p("distinct_dst").alias("log_distinct_dst"),
        F.log1p("distinct_src").alias("log_distinct_src"),
        F.log1p("new_dst").alias("log_new_dst"),
        (F.col("failure_count") / n).alias("failure_ratio"),
    ]
    cols += [(F.col(f"hour_{h:02d}") / n).alias(f"hour_{h:02d}_share") for h in range(24)]
    cols += [(F.col(f"at_{t.lower()}") / n).alias(f"auth_type_{t.lower()}_share") for t in AUTH_TYPES]
    cols += [(F.col("at_other") / n).alias("auth_type_other_share")]
    cols += [(F.col(f"lt_{t.lower()}") / n).alias(f"logon_type_{t.lower()}_share") for t in LOGON_TYPES]
    cols += [(F.col("lt_other") / n).alias("logon_type_other_share")]
    cols += [(F.col(f"or_{t.lower()}") / n).alias(f"orientation_{t.lower()}_share") for t in ORIENTATIONS]
    cols += [(F.col("or_other") / n).alias("orientation_other_share")]
    cols += [F.col("is_machine").cast("double").alias("is_machine")]

    return daily.select(*cols)


def standardize_and_normalize(features: DataFrame) -> tuple[DataFrame, dict]:
    """Put every dimension on the same footing, then scale each vector to unit length."""
    # Two steps, and both are needed. The dimensions have wildly different natural ranges, so
    # without the first one a log count of 12 would drown a share of 0.03 and the distance would
    # be measuring volume and nothing else. Unit length then makes the distance a pure measure of
    # SHAPE, which is what "does this day look like that day" is supposed to mean.
    stats_row = features.agg(
        *[F.mean(c).alias(f"m_{c}") for c in FEATURE_NAMES],
        *[F.stddev_pop(c).alias(f"s_{c}") for c in FEATURE_NAMES],
    ).collect()[0]

    stats = {
        c: {
            "mean": float(stats_row[f"m_{c}"] or 0.0),
            # A dimension with no variance carries no information, and dividing by its zero
            # deviation would produce NaN and poison every distance in the index.
            "stddev": float(stats_row[f"s_{c}"] or 0.0) or 1.0,
        }
        for c in FEATURE_NAMES
    }

    z = [
        ((F.col(c) - F.lit(stats[c]["mean"])) / F.lit(stats[c]["stddev"])).alias(f"z_{c}")
        for c in FEATURE_NAMES
    ]
    zdf = features.select("src_user", "event_date", *z)

    norm = F.sqrt(sum(F.pow(F.col(f"z_{c}"), 2) for c in FEATURE_NAMES))
    zdf = zdf.withColumn("_norm", F.when(norm > 0, norm).otherwise(F.lit(1.0)))

    vector = F.array(*[(F.col(f"z_{c}") / F.col("_norm")).cast("float") for c in FEATURE_NAMES])
    return zdf.select("src_user", "event_date", vector.alias("vector")), stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--lake", default="/data/lake/auth")
    ap.add_argument("--rollup", default="/data/lake/rollup")
    ap.add_argument("--output", default="/data/lake/vectors")
    ap.add_argument("--stats-out", default="bench/embedding.json")
    args = ap.parse_args()

    spark = spark_session("embed")

    t0 = time.perf_counter()
    features = build_features(spark, args.lake, args.rollup).cache()
    rows = features.count()

    vectors, stats = standardize_and_normalize(features)
    vectors.write.mode("overwrite").parquet(args.output)
    elapsed = round(time.perf_counter() - t0, 1)

    payload = {
        "what": (
            "One vector per person per day, describing how that person behaved that day. Used by "
            "the nearest-neighbour search in vector/."
        ),
        "no_leakage": (
            "The vector is built from behaviour only. Nothing derived from the published attack "
            "list is in it, and neither is anything derived from the four warning signs. If the "
            "labels were in here, the search would be finding the answer key rather than the "
            "attack, and every number that came out of it would be worthless."
        ),
        "dimensions": DIM,
        "feature_names": FEATURE_NAMES,
        "rows": rows,
        "scaling": (
            "Each dimension is centred and divided by its standard deviation over the whole "
            "population, then each vector is scaled to unit length. The first step stops the log "
            "counts from drowning the shares. The second makes distance a measure of shape rather "
            "than of volume."
        ),
        "feature_stats": stats,
        "output": args.output,
        "seconds": elapsed,
    }
    Path(args.stats_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.stats_out).write_text(json.dumps(payload, indent=2) + "\n")

    print(f"[embed] {rows:,} person-days as {DIM}-dimensional vectors in {elapsed}s")
    print(f"[embed] wrote {args.output} and {args.stats_out}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
