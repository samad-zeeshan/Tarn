# Measurement environment

Every number in `bench/*.json` was produced on this machine, in this container. DIRECTIVE
rule 2: provenance travels with every number — so if you are reading a figure from this
project anywhere else, this is the box it came from.

## Host

| | |
|---|---|
| CPU | AMD Ryzen 5 7600X3D — 6 cores / 12 threads, 4.10 GHz base |
| RAM | 31.2 GB |
| OS | Windows 11 Pro 26200 |
| Storage | NVMe SSD; raw corpus at `C:\data\tarn\` — **outside the OneDrive tree** (rule 7: OneDrive churning multi-GB files corrupts sync) |
| Container runtime | Docker Desktop 29.5.3, WSL2 backend (kernel 6.18.33.2-microsoft-standard-WSL2) |

## Container (`tarn:local`, built from `Dockerfile`)

| | |
|---|---|
| Base | `python:3.11-slim-bookworm` |
| Python | 3.11.15 |
| Java | OpenJDK 17.0.19 (Temurin) |
| Spark | PySpark 3.5.3 |
| DuckDB | 1.1.3 |
| dbt | dbt-core 1.8.9 / dbt-duckdb 1.8.4 |
| Neo4j | 5.26-community (separate container, 2 GB heap) |
| Redpanda | v24.2.7 (separate container, 1 core / 1 GB) |
| CPUs visible to the container | 12 |
| RAM visible to the container | 15.2 GB (WSL2's default half-of-host allocation) |

**Note the RAM discrepancy, because it matters for reading the Spark numbers:** the host has
31.2 GB but WSL2 hands the container 15.2 GB by default. Spark's driver is configured for 10 GB
(`spark.driver.memory`), which fits inside that with headroom. Nothing here was tuned against the
host's full memory, and no benchmark result should be read as "what this CPU can do" — only as
"what this configuration did".

## Why Spark runs in Docker and not on the host

The host has **Python 3.14 and Java 25**. PySpark 3.5 supports neither (Python ≤3.11 in
practice, Java 8/11/17). Native Windows Spark also needs `winutils.exe` hacks and produces
timings that are not representative. So the container pins the combination Spark is actually
tested against — this is DIRECTIVE rule 8, and it is the reason there is a Dockerfile at all.

## Reading the benchmarks

- **`spark_opt.json`** — median of 5 timed runs per variant after 1 untimed warm-up, all four
  variants on an identical slice in the same Spark session, `spark.sql.shuffle.partitions` held
  constant. Raw per-run timings are in the artifact; the median is what gets quoted.
- **`streaming_lag.json`** — an accelerated replay from a local file into a single-broker
  Redpanda on this same laptop. No network hop, no broker cluster, no competing load. It is a
  measurement of this pipeline's processing latency, not of a production system's.
- **`graph_stats.json`** — single-run Cypher timings against a warm page cache. Indicative, not
  a benchmark; treat them as "this is not slow", not as "this is fast".
- **`dataset.json`** — the row counts are *counted*, by walking all 7.2 GB. They are not the
  figures quoted in the LANL paper.
