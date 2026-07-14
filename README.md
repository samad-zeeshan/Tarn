# Tarn

**A small identity-security data lakehouse.** 1.05 billion real authentication events →
PySpark batch lake → dbt/DuckDB star schema → Spark Structured Streaming → Neo4j
privilege-path graph. The demo runs real SQL in your browser.

### → **[Open the demo](https://samad-zeeshan.github.io/tarn/)**

The SQL workbench on that page is genuinely live: DuckDB is compiled to WebAssembly and
executes analytical queries against the real warehouse extracts, in your browser. Everything
else on the page — the Spark benchmark, the streaming lag, the graph timings — is *recorded
evidence*, replayed from the committed artifacts in [`bench/`](bench/), and the page says so
in a visible provenance footer.

---

## The story

The corpus is LANL's *Comprehensive, Multi-Source Cyber-Security Events* (Kent, 2015): 58
days of real, anonymized enterprise authentication telemetry from Los Alamos National
Laboratory. Crucially, it ships with **ground truth** — 749 labelled red-team compromise
events, covering 104 identities that were actually taken over.

That changes what a data project can be. Every layer here answers *"would this have caught
the attacker?"* rather than *"how many rows are there?"*

```
LANL auth events  (1,051,430,459 rows, 58 days, real + anonymized)
        │
        ▼
[Stage 1: PySpark]      parse → date-partitioned Parquet lake
        │               sessionization + per-identity daily rollups (window functions)
        │               one MEASURED optimization, honestly reported
        ▼
[Stage 2: Lakehouse]    dbt → DuckDB star schema
        │               fact_auth_event + dim_identity / dim_computer / dim_time
        │               5 analytical queries, incl. the honest detection scoring
        ▼
[Stage 3: Streaming]    Redpanda (Kafka API) replay → Spark Structured Streaming
        │               1-min tumbling windows + watermark → Parquet → back into the warehouse
        ▼
[Stage 4: Graph]        Neo4j: (:User)-[:AUTH]->(:Computer)
        │               shortest privilege paths, blast radius, the red team's real subgraph
        ▼
[Stage 5: Demo]         GitHub Pages: DuckDB-WASM live SQL, benchmark charts, path explorer
```

---

## Measured numbers

Every figure below is produced by committed code and read from a committed artifact. None is
estimated, and none is rounded up. (`bench/env.md` records the machine.)

| What | Value | Artifact |
|---|---|---|
| Authentication events processed | **1,051,430,459** | [`bench/dataset.json`](bench/dataset.json) |
| Unparseable rows | **0** | [`bench/dataset.json`](bench/dataset.json) |
| Span | 58 days | [`bench/dataset.json`](bench/dataset.json) |
| Labelled red-team events (ground truth) | 749, over 104 identities, from 4 pivot hosts | [`bench/dataset.json`](bench/dataset.json) |
| Lake build (1.05B rows → 58 date-partitioned Parquet dirs) | 1,343 s | [`bench/lake_build.json`](bench/lake_build.json) |
| Per-identity daily rollups | 1,604,500 identity-days over 80,553 identities | [`bench/rollup.json`](bench/rollup.json) |
| Human accounts, hour-of-day peak:trough | 1.79× (peak 09:00, trough 03:00) | [`bench/diurnal.json`](bench/diurnal.json) |
| Machine accounts, hour-of-day peak:trough | 1.28× — essentially flat | [`bench/diurnal.json`](bench/diurnal.json) |
| Spark rollup optimization (113.7M-row slice, median of 5) | **50.70 s → 39.38 s, 1.29× (22.3% faster)**, output verified identical across all 4 variants | [`bench/spark_opt.json`](bench/spark_opt.json) |
| …and the optimization that lost first | 40.52 s → 52.64 s (1.30× **slower**) | [`bench/spark_opt_rejected.md`](bench/spark_opt_rejected.md) |
| Streaming throughput (accelerated local replay) | 1,481,012 events → 250,183 windows, **7,139 events/s sustained** | [`bench/streaming_lag.json`](bench/streaming_lag.json) |
| Streaming end-to-end lag | **p50 12.5 s · p95 15.6 s · p99 16.3 s** (event time ran 72.9× wall clock) | [`bench/streaming_lag.json`](bench/streaming_lag.json) |
| Privilege graph (30-day window: the whole campaign) | 72,751 identities · 14,540 hosts · **600,145 AUTH edges** · 437 labelled | [`bench/graph_stats.json`](bench/graph_stats.json) |
| Shortest privilege paths | 40/40 pairs connected; 2–4 hops; median query **2.5 ms** | [`bench/graph_stats.json`](bench/graph_stats.json) |
| Warehouse tests | **54 dbt checks green** — every FK verified across all 1.05B fact rows | CI + `dbt build` |

---

## Four things this project got wrong first, and kept

A portfolio repo that only shows the happy path is not showing you engineering. These are in
the git history on purpose.

**0. The fact table tried to eat 134 GB.** `fact_auth_event` was first materialized as a real
table. It was killed at 35 GB and still growing, and the arithmetic says why: the fact carries
four md5 surrogate keys, and an md5 is a unique 32-character string, so it does not compress —
4 keys × 32 bytes × 1.05e9 rows ≈ **134 GB of key data**, to duplicate a 9.9 GB Parquet lake
sitting on the same disk. So the fact stays in the lake and the warehouse serves it as a
**view**: DuckDB reads the Parquet in place with projection and predicate pushdown, the date
partitioning prunes files, and the dimensions and aggregate marts — small, read constantly —
are the things that get materialized. That is not a concession; it is the lakehouse pattern.
The `relationships` tests still run against all 1.05 billion rows.

**1. The first optimization lost.** The obvious hypothesis was that the rollup's bottleneck was
the `Expand` operator Spark plans behind two `COUNT(DISTINCT)`s in a single `groupBy`. Measured
on 113.7M rows, the textbook rewrite was *20% slower* than doing nothing — collapsing to the
distinct grain adds a shuffle, and it spent the savings buying it. The real bottleneck was
duller: the job scanned the event set **twice**, once for the daily aggregation and once inside
`first_seen_destinations()`. The fix was not "dedupe first" but **share the grain** — both
aggregations are reachable from the same distinct `(identity, date, dst, src)` tuples, so
materialize it once and let both read it. That won: **50.70s → 39.38s, 1.29× (22.3% faster)**,
with output checksummed identical across all four variants. The whole null result is kept in
[`bench/spark_opt_rejected.md`](bench/spark_opt_rejected.md).

**2. "Off-hours" nearly became fiction.** LANL publishes `time` as *seconds since collection
started* — there is no wall clock anywhere in the corpus, so "this login happened at 3am" is
not a fact you can read out of it. The first attempt to derive an off-hours band found a
nearly flat curve and **refused to emit one**. That refusal was correct: machine accounts
(`C1065$@DOM1`) authenticate around the clock and are the bulk of the traffic, flattening the
aggregate curve to 1.46×. Split them out and the human cycle is unmistakable (1.79×, trough at
03:00). The band the warehouse actually uses is *derived from that measurement*
([`pipeline/diurnal.py`](pipeline/diurnal.py) → [`bench/diurnal.json`](bench/diurnal.json)) and
injected into dbt as a var — never typed into a model as an assumed nine-to-five.

**3. The red-team label almost got smeared.** Joining LANL's ground truth on `user` alone
would mark *every* day of a compromised account as compromised, silently inflating every
recall number in Q5. The label is joined on the full `(time, user, src_computer,
dst_computer)` tuple, and there is a test that fails if that ever regresses
([`tests/test_pipeline.py::test_redteam_label_does_not_smear_across_the_identity`](tests/test_pipeline.py)).

---

## Q5: would the analytics have caught the attacker?

The query the whole warehouse exists for, and the only honest way to answer it is as a
detection evaluation — with the **misses** and the **alert volume**, not just the hits. Each of
Q1–Q4 is scored as a detector against the labelled red-team events across all **1,604,500
identity-days**, of which **181** were genuinely compromised (a 0.011% base rate).

| detector | caught | missed | recall | alerts to triage | precision | lift vs random |
|---|---|---|---|---|---|---|
| Q3 — new access paths (≥5 new hosts/day) | 56 | 125 | 30.9% | 33,319 | 0.168% | **14.9×** |
| Q1 — fan-out spike (z>3 vs own baseline) | 20 | 161 | 11.0% | 21,136 | 0.095% | 8.4× |
| Q4 — failure-ratio spike (z>3) | 11 | 170 | 6.1% | 5,752 | 0.191% | 17.0× |
| **Q2 — off-hours vs own baseline** | **0** | **181** | **0.0%** | 6,379 | 0.000% | **0.0×** |
| ANY of Q1–Q4 | 62 | 119 | 34.3% | 63,891 | 0.097% | 8.6× |
| **TWO OR MORE of Q1–Q4** | 22 | 159 | 12.2% | **2,599** | **0.846%** | **75.0×** |

Three things worth saying out loud:

1. **Q2 caught nothing. Zero of 181.** The off-hours signal is the piece of this project that
   took the most care to derive honestly — measuring the diurnal curve, splitting machine from
   human accounts, refusing to assume a nine-to-five — and it turns out to be **useless** for
   catching this red team. That is the result. It is in the table.
2. **Even the best combination misses two-thirds of the attack.** "ANY of Q1–Q4" reaches 34.3%
   recall and raises 63,891 alerts doing it. A detector with good recall and sixty thousand
   alerts is not a detector; it is a denial-of-service attack on a SOC.
3. **The actually useful configuration is the strictest one.** Requiring **two or more**
   independent signals cuts alerts from 63,891 to 2,599 and lifts precision to 75× random — at
   the cost of recall falling to 12.2%. That trade is the entire job, and here it is measured
   rather than asserted.

Full results: [`warehouse/queries/results/q5_redteam_enrichment.csv`](warehouse/queries/results/).

---

## Quickstart

Spark runs in Docker, never on the host — the host here has Python 3.14 and Java 25, and
PySpark 3.5 supports neither.

```bash
docker compose up -d          # tarn (Spark/dbt/DuckDB) + Redpanda + Neo4j
make test                     # pytest: pipeline, streaming (watermark + checkpoint), graph
```

To rebuild everything from the raw corpus you need the data. LANL serves it behind a
"data fence" — fill in the form at <https://csr.lanl.gov/data/cyber1/>, which reveals URLs of
the form `/data-fence/<TOKEN>/cyber1/auth.txt.gz`:

```bash
export TARN_LANL_TOKEN='<token from the form>'
make fetch                    # auth.txt.gz (7.2 GB) + redteam.txt.gz -> C:\data\tarn\raw
make describe                 # count the corpus -> bench/dataset.json
make sample                   # cut the deterministic ~100k-event CI slice

make lake                     # 1.05B rows -> Parquet + the diurnal measurement
make rollup                   # per-identity daily rollups
make bench                    # the 2x2 optimization benchmark
make warehouse                # dbt build + tests + the 5 showcase queries
make stream                   # Redpanda replay + Structured Streaming + lag probe
make graph                    # load Neo4j, run the Cypher, export the paths
make site                     # build the demo payloads, then audit them
```

Without the corpus, everything still runs against the committed CI slice in
[`data/sample/`](data/sample/) — that is exactly what CI does on every push.

---

## Honest limitations

- **Not a production system.** One laptop, plus one run on a free Databricks workspace. Never
  "operates at scale", never "billions per day". The honest claim is the row count actually
  processed, in batch, and that number is large enough not to need inflating.
- **The streaming figures are an accelerated local replay** from a file into a single-broker
  Redpanda on the same machine as the Spark driver — no network hop, no broker cluster, no
  competing load. They measure this pipeline's processing latency, not a production system's.
  The lag is dominated by the watermark hold *by design*; a sub-second figure would mean the
  watermark wasn't working.
- **The dataset is 2015 and anonymized.** Users and hosts are pseudonyms (`U292@DOM1`, `C1065`).
- **Calendar dates are anchored, not observed.** LANL ships relative seconds; `pipeline/common.py`
  anchors t=0 to 2015-01-01 so the lake has a partition key. Day-of-week would be fiction, so
  `dim_time` deliberately does not expose one.
- **The demo's SQL is live; nothing else on the page is.** The Spark, streaming, and graph
  panels are recorded evidence, labelled as such on the page itself.
- **The Databricks run is a validation, not a scale claim** — it executes the same job on the
  ~100k-event CI slice on a free workspace. See [`pipeline/databricks/`](pipeline/databricks/).

---

## Layout

```
data/fetch.py            download behind the LANL data fence · checksum · deterministic CI slice
data/sample/             ~100k committed events + all in-window red-team rows + manifest (seeded)
pipeline/                Stage 1 — sessionize.py · diurnal.py · rollup.py · optimize_bench.py
pipeline/databricks/     the same rollup, as a Databricks notebook
warehouse/               Stage 2 — dbt project (star schema), 5 queries + committed results
streaming/               Stage 3 — replay_producer.py · stream_job.py · lag_probe.py
graph/                   Stage 4 — load_neo4j.py · queries.cypher · export_paths.py
bench/                   every measured number lives here, with its provenance. env.md = the machine.
site/                    Stage 5 — the demo (DuckDB-WASM, hand-rolled SVG charts, force-directed graph)
tests/                   pytest across every stage
```

## Data

A. D. Kent, *Comprehensive, Multi-Source Cyber-Security Events*, Los Alamos National
Laboratory (2015). CC0. <https://csr.lanl.gov/data/cyber1/>

```
@misc{kent-2015-cyberdata1,
  author    = {Alexander D. Kent},
  title     = {{Comprehensive, Multi-Source Cyber-Security Events}},
  year      = {2015},
  howpublished = {Los Alamos National Laboratory},
  doi       = {10.17021/1179829}
}
```
