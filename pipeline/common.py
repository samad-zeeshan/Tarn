"""Shared Spark plumbing, schema, and time semantics for every Tarn batch job.

TIME SEMANTICS — read this before trusting any hour-of-day column
-----------------------------------------------------------------
LANL publishes `time` as *seconds elapsed since the start of collection*, not as a
wall-clock timestamp. There is no absolute date anywhere in the corpus. That has two
consequences Tarn has to be honest about:

1. Calendar dates are *anchored*, not observed. We anchor t=0 to ANCHOR_EPOCH purely so
   the lake has a sane partition key and the warehouse has a real date dimension. The
   anchor is arbitrary and is recorded in bench/dataset.json. Day-of-week is therefore
   meaningless and Tarn never uses it.

2. Hour-of-day IS meaningful, but only as an offset: hour = (t mod 86400) // 3600. It is
   an honest hour-of-day *if and only if* collection began near local midnight. We do not
   assume that — pipeline/diurnal.py measures the actual volume-by-hour curve and derives
   the off-hours band from the trough in the data. Q2 ("off-hours share") consumes that
   measured band, never a hard-coded 9-to-5.
"""

from __future__ import annotations

import os
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Arbitrary anchor: LANL gives relative seconds only. Documented in bench/dataset.json.
ANCHOR_EPOCH = date(2015, 1, 1)
SECONDS_PER_DAY = 86_400

AUTH_SCHEMA = StructType(
    [
        StructField("time", IntegerType(), nullable=False),
        StructField("src_user", StringType()),
        StructField("dst_user", StringType()),
        StructField("src_computer", StringType()),
        StructField("dst_computer", StringType()),
        StructField("auth_type", StringType()),
        StructField("logon_type", StringType()),
        StructField("auth_orientation", StringType()),
        StructField("outcome", StringType()),
    ]
)

REDTEAM_SCHEMA = StructType(
    [
        StructField("time", IntegerType(), nullable=False),
        StructField("user", StringType()),
        StructField("src_computer", StringType()),
        StructField("dst_computer", StringType()),
    ]
)


def spark_session(app: str, shuffle_partitions: int | None = None, **conf: str) -> SparkSession:
    """Build a local Spark session.

    Defaults are tuned for the 6-core / 32 GB dev box this project was measured on; every
    bench artifact records the effective config it ran under, so nothing here is a hidden
    variable in a benchmark.
    """
    builder = (
        SparkSession.builder.appName(f"tarn-{app}")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "10g"))
        .config("spark.sql.session.timeZone", "UTC")
        # Parquet + snappy: the lake is read far more often than written.
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.files.maxPartitionBytes", "128m")
        .config("spark.local.dir", os.environ.get("SPARK_LOCAL_DIR", "/data/work/spark-tmp"))
    )
    if shuffle_partitions is not None:
        builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    for key, value in conf.items():
        builder = builder.config(key.replace("__", "."), value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_auth_raw(spark: SparkSession, path: str) -> DataFrame:
    """Read raw LANL auth events.

    Handles both shapes we ever feed it: the headerless 7.2 GB auth.txt.gz, and the
    committed CI slice (data/sample/auth_sample.csv.gz), which carries a header. Reading
    with an explicit schema (never inferSchema) keeps this a single pass and keeps the
    column types stable between the two.
    """
    has_header = "sample" in os.path.basename(path)
    return (
        spark.read.option("header", str(has_header).lower())
        .option("mode", "DROPMALFORMED")
        .schema(AUTH_SCHEMA)
        .csv(path)
    )


def read_redteam(spark: SparkSession, path: str) -> DataFrame:
    has_header = "sample" in os.path.basename(path)
    return (
        spark.read.option("header", str(has_header).lower())
        .schema(REDTEAM_SCHEMA)
        .csv(path)
    )


def read_redteam_any(spark: SparkSession, path: str) -> DataFrame:
    """Read the red-team labels from EITHER the raw .gz/.csv or the Parquet lake copy.

    Stage 1 lands redteam in the lake as Parquet, but the jobs can also be pointed straight
    at the raw file. Having each caller guess the format is how a label silently fails to
    join and every downstream recall number quietly becomes zero — which is exactly the bug
    tests/test_pipeline.py::test_redteam_label_does_not_smear_across_the_identity caught.
    So the sniff lives here, once, and every caller gets the same four columns back.
    """
    if path.endswith((".gz", ".csv", ".txt")):
        df = read_redteam(spark, path)
    else:
        df = spark.read.parquet(path)
    return df.select("time", "user", "src_computer", "dst_computer")


def derive_columns(df: DataFrame) -> DataFrame:
    """Add the derived columns the lake is partitioned and analysed on.

    `?` is LANL's null marker; it becomes a real NULL here so downstream COUNT/DISTINCT
    behave. Machine accounts (trailing `$`) are flagged rather than dropped: they are the
    bulk of the traffic and excluding them is an analytical choice each query makes for
    itself, not something the lake should decide.
    """
    q = lambda c: F.when(F.col(c) == "?", None).otherwise(F.col(c))  # noqa: E731

    return (
        df.select(
            F.col("time").cast("int").alias("time"),
            q("src_user").alias("src_user"),
            q("dst_user").alias("dst_user"),
            q("src_computer").alias("src_computer"),
            q("dst_computer").alias("dst_computer"),
            q("auth_type").alias("auth_type"),
            q("logon_type").alias("logon_type"),
            q("auth_orientation").alias("auth_orientation"),
            q("outcome").alias("outcome"),
        )
        .withColumn("day_index", (F.col("time") / F.lit(SECONDS_PER_DAY)).cast("int"))
        .withColumn("hour_of_day", ((F.col("time") % F.lit(SECONDS_PER_DAY)) / 3600).cast("int"))
        .withColumn(
            "event_date",
            F.date_add(F.lit(ANCHOR_EPOCH).cast("date"), F.col("day_index")),
        )
        .withColumn(
            "event_ts",
            (F.unix_timestamp(F.lit(ANCHOR_EPOCH).cast("timestamp")) + F.col("time")).cast(
                "timestamp"
            ),
        )
        .withColumn("is_success", F.col("outcome") == F.lit("Success"))
        .withColumn("is_failure", F.col("outcome") == F.lit("Fail"))
        # U123@DOM1 -> user U123, domain DOM1. Machine accounts end in `$`.
        .withColumn("src_user_name", F.split(F.col("src_user"), "@").getItem(0))
        .withColumn("src_domain", F.split(F.col("src_user"), "@").getItem(1))
        .withColumn("src_is_machine", F.col("src_user").rlike(r"\$@"))
    )


def redteam_keys(redteam: DataFrame) -> DataFrame:
    """The join key that marks an auth row as a labelled compromise event.

    LANL's redteam.txt identifies a compromise by (time, user, src_computer,
    dst_computer) — the same tuple, exactly, that appears in auth.txt. Joining on all
    four is what makes the label trustworthy; joining on the user alone would smear the
    label across that user's entire benign history.
    """
    return redteam.select(
        F.col("time").alias("rt_time"),
        F.col("user").alias("rt_user"),
        F.col("src_computer").alias("rt_src"),
        F.col("dst_computer").alias("rt_dst"),
    ).distinct()
