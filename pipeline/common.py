"""
Shared Spark session, schema, and the derived columns every batch job depends on.

LANL publishes `time` as seconds since collection started, not wall clock, so read the
note on ANCHOR_EPOCH before trusting any date or hour column.
"""

from __future__ import annotations

import os
import tempfile
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# There is no absolute date anywhere in the corpus. We anchor t=0 here purely so the lake has
# a partition key and the warehouse has a date dimension. The anchor is arbitrary, which is
# why nothing in this project ever uses day of week. Hour of day is different: (t mod 86400)
# is a real offset, and pipeline/diurnal.py measures the volume curve to prove it carries a
# signal rather than assuming it does.
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
    """Build a local Spark session. Every bench artifact records the config it ran under."""
    # Scratch space defaults to the system temp dir, which exists everywhere. It used to default
    # to /data/work/spark-tmp, which only exists because docker-compose mounts it there, and that
    # was a container-shaped assumption baked into a library. Anywhere without that mount, Spark
    # could not create the directory and the JVM died on startup, taking every test with it.
    #
    # docker-compose still points SPARK_LOCAL_DIR at the data volume, because a billion-row
    # shuffle spills far more than a container's own writable layer wants to hold.
    local_dir = os.environ.get("SPARK_LOCAL_DIR") or tempfile.gettempdir()

    builder = (
        SparkSession.builder.appName(f"tarn-{app}")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "10g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.files.maxPartitionBytes", "128m")
        .config("spark.local.dir", local_dir)
    )
    if shuffle_partitions is not None:
        builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    for key, value in conf.items():
        builder = builder.config(key.replace("__", "."), value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_auth_raw(spark: SparkSession, path: str) -> DataFrame:
    """Read raw auth events. Handles the headerless 7.2 GB file and the CI slice, which has a header."""
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
    """Read the labels from either the raw .gz or the Parquet copy in the lake."""
    # Letting each caller guess the format is how a label silently fails to join and every
    # recall number downstream quietly becomes zero. The sniff lives here once.
    if path.endswith((".gz", ".csv", ".txt")):
        df = read_redteam(spark, path)
    else:
        df = spark.read.parquet(path)
    return df.select("time", "user", "src_computer", "dst_computer")


def derive_columns(df: DataFrame) -> DataFrame:
    """Add the derived columns the lake is partitioned and analysed on."""
    q = lambda c: F.when(F.col(c) == "?", None).otherwise(F.col(c))  # noqa: E731

    return (
        df.select(
            F.col("time").cast("int").alias("time"),
            # '?' is LANL's null marker. It has to become a real NULL or COUNT and DISTINCT
            # will happily count it as a value.
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
        .withColumn("src_user_name", F.split(F.col("src_user"), "@").getItem(0))
        .withColumn("src_domain", F.split(F.col("src_user"), "@").getItem(1))
        # Machine accounts end in $. They are most of the traffic, so they are flagged and
        # kept, not filtered. Each query decides for itself whether to exclude them.
        .withColumn("src_is_machine", F.col("src_user").rlike(r"\$@"))
    )
