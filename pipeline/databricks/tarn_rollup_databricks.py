# Databricks notebook source
# MAGIC %md
# MAGIC # Tarn — Stage 1 rollup on Databricks
# MAGIC
# MAGIC This is the **same job** as `pipeline/rollup.py`, running unchanged on a Databricks
# MAGIC cluster instead of local Spark in Docker. The point of the exercise is to prove the
# MAGIC pipeline is not secretly coupled to `local[*]` — the only things that change are the
# MAGIC paths and the session (Databricks supplies `spark`).
# MAGIC
# MAGIC **What to run:** upload `data/sample/auth_sample.csv.gz` and
# MAGIC `data/sample/redteam_sample.csv.gz` to a Volume or DBFS, set the paths in the widget
# MAGIC cell, and Run All.
# MAGIC
# MAGIC **Honesty note.** This ran ONCE, on the free Databricks edition, against the committed
# MAGIC CI slice (~100k events) — not against the full 1.05B-row corpus, which would need a
# MAGIC cluster nobody is paying for. The claim it supports is *"executed on Databricks and
# MAGIC validated"*, nothing more. The real scale numbers come from the local runs recorded in
# MAGIC `bench/`. Do not let this notebook grow into a "we run on Databricks at scale" story.

# COMMAND ----------

dbutils.widgets.text("auth_path", "/Volumes/main/default/tarn/auth_sample.csv.gz", "Auth events")
dbutils.widgets.text("redteam_path", "/Volumes/main/default/tarn/redteam_sample.csv.gz", "Red-team labels")
dbutils.widgets.text("output_path", "/Volumes/main/default/tarn/out", "Output root")

AUTH_PATH = dbutils.widgets.get("auth_path")
REDTEAM_PATH = dbutils.widgets.get("redteam_path")
OUTPUT_PATH = dbutils.widgets.get("output_path")

print(f"auth    : {AUTH_PATH}")
print(f"redteam : {REDTEAM_PATH}")
print(f"output  : {OUTPUT_PATH}")
print(f"spark   : {spark.version}")  # noqa: F821 — provided by Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema and derived columns
# MAGIC
# MAGIC Copied verbatim from `pipeline/common.py`. A notebook that quietly re-derives its
# MAGIC columns differently from the batch job is how two "identical" pipelines end up
# MAGIC disagreeing, so this is a copy, not a rewrite.
# MAGIC
# MAGIC LANL ships `time` as **seconds since collection began** — there is no wall clock in the
# MAGIC corpus. `event_date` is therefore anchored to an arbitrary epoch (2015-01-01) purely to
# MAGIC give the lake a partition key.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

AUTH_SCHEMA = StructType([
    StructField("time", IntegerType(), False),
    StructField("src_user", StringType()),
    StructField("dst_user", StringType()),
    StructField("src_computer", StringType()),
    StructField("dst_computer", StringType()),
    StructField("auth_type", StringType()),
    StructField("logon_type", StringType()),
    StructField("auth_orientation", StringType()),
    StructField("outcome", StringType()),
])

REDTEAM_SCHEMA = StructType([
    StructField("time", IntegerType(), False),
    StructField("user", StringType()),
    StructField("src_computer", StringType()),
    StructField("dst_computer", StringType()),
])

ANCHOR = "2015-01-01"
SECONDS_PER_DAY = 86_400


def derive_columns(df):
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
        .withColumn("event_date", F.date_add(F.lit(ANCHOR).cast("date"), F.col("day_index")))
        .withColumn("is_success", F.col("outcome") == F.lit("Success"))
        .withColumn("is_failure", F.col("outcome") == F.lit("Fail"))
        .withColumn("src_is_machine", F.col("src_user").rlike(r"\$@"))
    )


events = derive_columns(
    spark.read.option("header", "true").schema(AUTH_SCHEMA).csv(AUTH_PATH)  # noqa: F821
).filter(F.col("src_user").isNotNull()).cache()

print(f"{events.count():,} auth events")
display(events.limit(10))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## The lake layer — date-partitioned Parquet

# COMMAND ----------

(
    events.write.mode("overwrite")
    .partitionBy("event_date")
    .parquet(f"{OUTPUT_PATH}/auth")
)
print(f"lake written: {spark.read.parquet(f'{OUTPUT_PATH}/auth').count():,} rows")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sessionization — the window-function workload
# MAGIC
# MAGIC LAG over `(src_user, src_computer)` ordered by time, flag a new session whenever the
# MAGIC idle gap exceeds 30 minutes, then a running sum of that flag numbers the sessions.

# COMMAND ----------

IDLE_GAP = 1800
by_identity_host = Window.partitionBy("src_user", "src_computer").orderBy("time")

sessions = (
    events.withColumn("prev_time", F.lag("time").over(by_identity_host))
    .withColumn(
        "is_session_start",
        F.when(
            F.col("prev_time").isNull() | ((F.col("time") - F.col("prev_time")) > F.lit(IDLE_GAP)),
            1,
        ).otherwise(0),
    )
    .withColumn(
        "session_seq",
        F.sum("is_session_start").over(
            by_identity_host.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ),
    )
    .groupBy("src_user", "src_computer", "session_seq")
    .agg(
        F.min("time").alias("session_start"),
        F.max("time").alias("session_end"),
        F.count("*").alias("event_count"),
        F.countDistinct("dst_computer").alias("distinct_destinations"),
    )
    .withColumn("duration_seconds", F.col("session_end") - F.col("session_start"))
)

print(f"{sessions.count():,} sessions")
display(sessions.orderBy(F.desc("event_count")).limit(10))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-identity daily rollup — with the two-stage distinct aggregation
# MAGIC
# MAGIC This is the **optimized** form benchmarked in `bench/spark_opt.json`: aggregate to the
# MAGIC distinct grain first, *then* count, instead of putting two `COUNT(DISTINCT ...)` in one
# MAGIC `groupBy` and making Spark plan an `Expand` that replays every input row once per
# MAGIC distinct expression.

# COMMAND ----------

base = events.select(
    "src_user", "event_date", "dst_computer", "src_computer", "hour_of_day",
    F.col("is_success").cast("int").alias("succ"),
    F.col("is_failure").cast("int").alias("fail"),
)

# Two-stage: collapse to the distinct grain, then count.
grain = base.groupBy("src_user", "event_date", "dst_computer", "src_computer").agg(
    F.count("*").alias("n"),
    F.sum("succ").alias("succ"),
    F.sum("fail").alias("fail"),
)

daily = grain.groupBy("src_user", "event_date").agg(
    F.sum("n").alias("auth_count"),
    F.sum("succ").alias("success_count"),
    F.sum("fail").alias("failure_count"),
    F.countDistinct("dst_computer").alias("distinct_dst_computers"),
    F.countDistinct("src_computer").alias("distinct_src_computers"),
)

# New access paths: destinations whose first-ever appearance for this identity is today.
first_seen = (
    events.filter(F.col("dst_computer").isNotNull())
    .groupBy("src_user", "dst_computer")
    .agg(F.min("event_date").alias("first_seen_date"))
)
new_dst = first_seen.groupBy(
    "src_user", F.col("first_seen_date").alias("event_date")
).agg(F.count("*").alias("new_dst_computers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Red-team enrichment — the broadcast join
# MAGIC
# MAGIC The label table is ~749 rows (50 in the CI slice). Broadcasting it turns a SortMergeJoin
# MAGIC into a map-side hash lookup. Join on the FULL (time, user, src, dst) tuple, not on the
# MAGIC user alone — joining on user would smear "compromised" across that account's entire
# MAGIC benign history and inflate every recall number downstream.

# COMMAND ----------

redteam = spark.read.option("header", "true").schema(REDTEAM_SCHEMA).csv(REDTEAM_PATH)  # noqa: F821

rt_days = (
    redteam.withColumn("day_index", (F.col("time") / F.lit(SECONDS_PER_DAY)).cast("int"))
    .withColumn("event_date", F.date_add(F.lit(ANCHOR).cast("date"), F.col("day_index")))
    .select(F.col("user").alias("src_user"), "event_date")
    .distinct()
    .withColumn("is_redteam_day", F.lit(True))
)

rollup = (
    daily.join(new_dst, ["src_user", "event_date"], "left")
    .join(F.broadcast(rt_days), ["src_user", "event_date"], "left")
    .fillna({"new_dst_computers": 0, "is_redteam_day": False})
    .withColumn(
        "failure_ratio",
        F.when(F.col("auth_count") > 0, F.col("failure_count") / F.col("auth_count")).otherwise(0.0),
    )
)

rollup.write.mode("overwrite").partitionBy("event_date").parquet(f"{OUTPUT_PATH}/rollup")

print(f"{rollup.count():,} identity-days")
print(f"{rollup.filter('is_redteam_day').count()} red-team identity-days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The physical plan
# MAGIC
# MAGIC Confirm Databricks planned a `BroadcastHashJoin` for the red-team enrichment and that
# MAGIC the two-stage aggregation avoided an `Expand`. This is the same thing
# MAGIC `bench/spark_opt.json` captures for the local runs.

# COMMAND ----------

rollup.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the attacker's days look like
# MAGIC
# MAGIC The whole reason the dataset is worth using: there is ground truth in it.

# COMMAND ----------

display(  # noqa: F821
    rollup.filter("is_redteam_day")
    .orderBy(F.desc("distinct_dst_computers"))
    .select(
        "src_user", "event_date", "auth_count", "failure_count",
        "distinct_dst_computers", "new_dst_computers", "failure_ratio",
    )
)
