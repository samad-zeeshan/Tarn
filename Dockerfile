# Tarn runtime, one image runs every stage (Spark, dbt, streaming, graph, tests).
#
# Spark runs in Docker, never on the host. This machine is
# Windows 11 with Python 3.14 + Java 25, neither of which PySpark 3.5 supports; this
# image pins the combination Spark is actually tested against.
FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_LOCAL_IP=127.0.0.1 \
    # Keep Ivy's resolver cache inside the image so streaming runs need no network.
    IVY_HOME=/opt/ivy

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Pre-resolve the Kafka source connector so `spark-submit --packages` is a cache hit at
# run time (Stage 3). Without this every streaming run re-hits Maven Central.
RUN mkdir -p ${IVY_HOME} && \
    python - <<'PY'
import subprocess, pyspark, os
spark_home = os.path.dirname(pyspark.__file__)
pkg = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
subprocess.run(
    [os.path.join(spark_home, "bin", "spark-submit"),
     "--packages", pkg,
     f"--conf", f"spark.jars.ivy={os.environ['IVY_HOME']}",
     "--master", "local[1]",
     "/dev/null"],
    check=False,
)
PY

ENV PYTHONPATH=/work

CMD ["bash"]
