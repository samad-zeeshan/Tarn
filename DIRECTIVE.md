# TARN — build directive

**A small identity-security data lakehouse: raw authentication events → Spark → star-schema warehouse → streaming rollups → privilege-path graph, with a beautiful in-browser demo on GitHub Pages.**

This file is the complete build order. A Claude Code session opened in this directory should be told: *"Build Tarn per DIRECTIVE.md, stage by stage, stopping at each STAGE GATE for Samad's confirmation."* A human following it alone should read Ground Rules first and skip nothing marked MUST.

---

## 0. Why this project exists (read before building anything)

The `/tailor beyondtrust1` run (2026-07-13, resume repo `jobs/beyondtrust1/`) ended `MAXIMIZED — READY FOR APPROVAL` at a flat 6/10 across four blind grades. The score is capped by exactly five JD requirements that no fact in `facts/master-facts.md` supports:

| JD requirement (BeyondTrust Associate Data Engineer) | JD tier | Tarn stage that flips it |
|---|---|---|
| Distributed processing framework experience | **Required** | Stage 1 |
| Spark ("Spark experience is needed") | **Required** in prose / Highly Preferred | Stage 1 |
| Data warehousing for analytics use cases | **Required** | Stage 2 |
| Realtime processing | Highly Preferred | Stage 3 |
| Graph data stores | Ideal | Stage 4 |
| Databricks | a plus | Stage 1 (one cloud run) |
| "Optimize data workloads at a software level by improving processing efficiency" | responsibility | Stage 1 (the measured optimization) |

These are not this-job-only gaps: every data-engineering JD Samad targets will ask for the first three. Tarn is one repo that closes all of them honestly, plus a demo a screener can open and click.

**The project is worthless to the pipeline unless it ends in claimable facts.** The resume repo's Law 1 (facts law) means nothing ships on a CV without a fact ID backed by evidence that survives a screening question. Every stage below therefore ends with an **EVIDENCE CONTRACT**: the exact committed artifacts that let Samad flip the corresponding draft fact (§9) to VERIFIED. Build to the contract, not past it.

**Name.** Working name **Tarn** (a tarn is a small mountain lake — a small data lake). Fits the existing roster's naming register (Kvasir, Fulcrum, Docket). If Samad renames it, do so before the first push and record the canonical spelling in §9.

---

## 1. Ground rules (MUST — these mirror the resume pipeline's laws)

1. **Every number is measured, never estimated.** Row counts, runtimes, lag, path lengths — all come from committed artifacts (`bench/*.json`, query result files, CI logs). If a number wasn't measured, it doesn't exist. No rounding up: `1.05B` stays `1,051,430,459` in the artifact even if prose says "over a billion".
2. **Provenance travels with every number.** Each benchmark artifact records: hardware (CPU, RAM), OS/environment (WSL2/Docker/Databricks), Spark version, dataset slice (exact row count, date range), run count, and whether the figure is a median, mean, or single run. The Docket KI-4 lesson: three unlabelled numbers get confused every time; label them at birth.
3. **No production cosplay.** Tarn runs on a laptop and once on a free Databricks workspace. Never write "in production", "operates at scale", "billions of events per day". Claimable verbs: *built, processed, measured, modelled, optimized, deployed to Databricks and validated*. The honest scale claim is the actual row count processed, which (with the LANL dataset) is genuinely large — no inflation needed.
4. **Determinism.** Every sample, split, and synthetic fallback uses a committed seed. Every "before/after" optimization comparison runs on the identical slice, same machine, ≥3 runs, report the **median**.
5. **The demo lies about nothing.** The GitHub Pages site executes real SQL in the browser (DuckDB-WASM) against real pipeline outputs — that part is genuinely live. The Spark runs, streaming lag, and Neo4j paths are **recorded evidence**, replayed. The site says so, on-page, in a visible "what's live vs. recorded" note (the Docket KI-5 precedent). Any resume variant links it as **`Demo`**, never `Live Demo`.
6. **Solo authorship.** One committer (`samad-zeeshan`), linear history, meaningful commit messages per stage. The inventory scanner reads git logs for ownership.
7. **Keep raw data out of git and out of OneDrive sync.** `data/raw/` is gitignored AND lives outside the OneDrive tree (e.g. `C:\data\tarn\` or the WSL filesystem) with a symlink or config pointer. OneDrive churning multi-GB files corrupts sync and the resume pipeline's portfolio hash. Only `data/sample/` (the small committed CI slice) lives in the repo.
8. **Spark runs in WSL2 or Docker, never native Windows.** Native Windows Spark needs winutils hacks and produces unrepresentative timings. Record which environment in every bench artifact (rule 2).

---

## 2. Architecture (the one-paragraph story the demo must tell)

```
LANL auth events (~1.05B rows, 58 days, anonymized real network)
        │
        ▼
[Stage 1: PySpark batch]  sessionize + per-identity daily rollups
        │                 one MEASURED optimization (before/after)
        ▼
[Stage 2: Lakehouse]      partitioned Parquet → DuckDB star schema via dbt
        │                 fact_auth_event + dim_identity/dim_computer/dim_time
        ▼
[Stage 3: Streaming]      Kafka(Redpanda) replay → Spark Structured Streaming
        │                 1-min tumbling windows + watermark → warehouse sink
        ▼
[Stage 4: Graph]          Neo4j: (:User)-[:AUTH]->(:Computer)
        │                 shortest privilege-escalation paths; red-team overlay
        ▼
[Stage 5: Demo site]      GitHub Pages: DuckDB-WASM live SQL, benchmark charts,
                          interactive path explorer, honest provenance footer
```

The through-line for a screener: *"I took a billion real authentication events, built the batch, warehouse, streaming, and graph layers a security data platform needs, measured everything, and you can query the results in your browser right now."* That maps one-to-one onto BeyondTrust's "data lake … security analysis … identity security use cases … Paths to Privilege".

---

## 3. Dataset (Stage 0 decides this once; everything downstream depends on it)

**Primary: LANL "Comprehensive, Multi-Source Cyber-Security Events"** (A. Kent, Los Alamos National Laboratory, 2015; `csr.lanl.gov`). Use:
- `auth.txt.gz` — ~1.05 billion authentication events over 58 days: `time, src_user@domain, dst_user@domain, src_computer, dst_computer, auth_type, logon_type, auth_orientation, success/failure`. Real (anonymized) enterprise auth — the exact shape of identity-security telemetry.
- `redteam.txt.gz` — the labelled red-team compromise events (a few hundred rows). **This is the treasure**: it lets every layer answer "find the attacker" instead of "count the rows".

Working slice policy: full download if disk allows (~15GB compressed); otherwise the **first 14 days** (still hundreds of millions of rows — an honest "hundreds of millions" claim). Record the exact slice row count in `bench/dataset.json`.

**Fallback (if LANL is unreachable):** CERT Insider Threat Test Dataset r4.2 (CMU SEI, `logon.csv` + `device.csv`), or — last resort — a committed synthetic generator (`data/generate.py`, seeded, modelled on the LANL schema, clearly labelled synthetic everywhere it appears). A fallback changes no stage below, only the scale claims.

**Committed CI slice:** `data/sample/` — ~100k events + all red-team rows that intersect it, chosen deterministically (seed in the script). Small enough for GitHub Actions, real enough for tests.

---

## 4. Repository layout

```
tarn/
  DIRECTIVE.md              ← this file (keep; it documents intent)
  README.md                 ← written last (§8 spec)
  .gitignore                ← data/raw, warehouse/*.duckdb, .venv, checkpoints
  data/
    sample/                 ← committed CI slice + manifest with seed
    fetch.py                ← download + integrity-check + slice scripts
  pipeline/                 ← PySpark jobs (Stage 1)
    sessionize.py  rollup.py  common.py
  warehouse/                ← dbt project (Stage 2)
    models/staging/  models/marts/  tests/
    queries/                ← the 5 showcase analytical queries + committed results
  streaming/                ← Stage 3
    replay_producer.py  stream_job.py  lag_probe.py
  graph/                    ← Stage 4
    load_neo4j.py  queries.cypher  export_paths.py
  bench/                    ← ALL measured numbers live here, committed
    dataset.json  spark_opt.json  streaming_lag.json  graph_stats.json  env.md
  site/                     ← Stage 5 demo (built with ui-ux-pro-max)
  tests/                    ← pytest across all stages
  .github/workflows/
    ci.yml                  ← lint + pytest + small-slice pipeline + dbt build
    pages.yml               ← build site/ → deploy to GitHub Pages on push to main
  docker-compose.yml        ← redpanda + neo4j for stages 3–4
```

---

## 5. Stages

Each stage ends with a **STAGE GATE**: tests green, evidence contract satisfied, one commit (or small series) pushed, and — when Claude is driving — a stop for Samad to confirm before the next stage. Do not start Stage N+1 with Stage N's contract unmet.

### Stage 0 — Scaffold and data (≈ half a day)

Repo init, layout above, `.gitignore`, `docker-compose.yml`, Python env (`pyspark`, `duckdb`, `dbt-duckdb`, `pytest`, pinned in `requirements.txt` — record exact versions). `data/fetch.py` downloads, checksums, and slices the dataset; generates `data/sample/`. `bench/dataset.json` records source, slice bounds, exact row counts, checksum. `bench/env.md` records the machine and environment.

**EVIDENCE CONTRACT:** `bench/dataset.json` + `bench/env.md` + committed `data/sample/` with its manifest.

### Stage 1 — PySpark batch + the measured optimization (≈ 1 day) — **the Required-flipper**

1. `pipeline/sessionize.py`: read the raw slice into Spark (WSL2/Docker, `local[*]`), parse, write **partitioned Parquet** (partition by event date) — this is the lake layer.
2. `pipeline/rollup.py`: per-identity daily rollups using **window functions** — distinct destination computers (fan-out), failure ratio, first-seen destinations per user per day, off-hours activity share. Join red-team labels onto the rollups.
3. **The optimization (MUST, and MUST be honest):** pick exactly one real bottleneck and fix it — the natural candidate is the rollup-to-redteam join (**broadcast join** vs. shuffle join) or shuffle-partition tuning for the window stage. Run before and after on the identical slice, same machine, ≥3 runs each, commit medians + all raw timings + the two physical plans (`df.explain()` output) to `bench/spark_opt.json`. If the "optimization" doesn't actually win, say so in the artifact and try a different one — a fabricated delta is worse than no delta.
4. **Databricks run (Samad, personally):** create a free Databricks workspace (Free Edition, formerly Community Edition), upload the CI slice or a mid-size slice, run the same rollup notebook once end to end, export the notebook (`.ipynb` or `.dbc`) into the repo under `pipeline/databricks/`. This single artifact is what makes "Databricks" a true word.
5. Tests: schema assertions on Parquet output; rollup correctness on `data/sample/` against hand-computed expectations; determinism test.

**EVIDENCE CONTRACT:** partitioned-Parquet lake produced by committed code · `bench/spark_opt.json` with before/after medians, raw runs, plans, environment · the committed Databricks notebook export · green tests.

### Stage 2 — Star-schema warehouse with dbt (≈ half a day) — **flips "data warehousing for analytics"**

1. dbt project (`dbt-duckdb`): staging models over the Parquet lake → marts: **`fact_auth_event`** (grain: one auth event) + **`dim_identity`**, **`dim_computer`**, **`dim_time`**, plus a `mart_daily_identity_rollup`. This is deliberate dimensional modelling — document the grain and each dimension's SCD stance (type 1 is fine; say so) in the model YAML.
2. dbt tests: `unique`, `not_null`, `relationships` on every key. All green in CI.
3. **The 5 showcase analytical queries** (`warehouse/queries/`, each with committed result set + one-line finding):
   - Q1: top-N identities by destination fan-out, week over week (lateral-movement precursor)
   - Q2: off-hours authentication share per identity vs. their own baseline
   - Q3: first-time user→computer edges per day (new-access-path rate)
   - Q4: failure-ratio spike detection per identity (windowed z-score in SQL)
   - Q5: **red-team enrichment** — how the labelled compromise events rank under Q1–Q4 (i.e., "would these analytics have surfaced the attacker?"). Report the honest answer, including misses.
4. Tests: dbt build green; query results reproducible from the committed lake sample in CI.

**EVIDENCE CONTRACT:** dbt project with passing tests in CI · schema docs stating grain/dimensions · 5 committed queries with committed results and findings, including Q5's honest hit/miss.

### Stage 3 — Streaming (≈ 1 day) — **flips "realtime processing"**

1. `docker-compose.yml` brings up **Redpanda** (Kafka-compatible, single binary — less yak than full Kafka).
2. `streaming/replay_producer.py`: replays auth events from file into a topic at an accelerated, configurable rate (e.g. 5k events/s), preserving event-time.
3. `streaming/stream_job.py`: **Spark Structured Streaming** — 1-minute tumbling windows per identity (auth counts, failure counts, distinct destinations) with a **watermark** for late data, checkpointed, sinking to Parquet that dbt picks up as a streaming mart.
4. `streaming/lag_probe.py`: measures end-to-end lag (event-time → sink commit-time) across a sustained run; commit p50/p95 + throughput + run duration + config to `bench/streaming_lag.json` (rule 2 provenance).
5. Tests: windowing correctness on the sample (including a deliberately late event that the watermark handles); checkpoint-recovery test (kill and resume, no duplicate windows).

**EVIDENCE CONTRACT:** `bench/streaming_lag.json` (p50/p95 lag, throughput, duration, config, environment) · checkpoint-recovery test green · the streaming mart queryable in the warehouse.

### Stage 4 — Privilege-path graph in Neo4j (≈ half a day) — **flips "graph data stores" and is the BeyondTrust bullseye**

1. Neo4j in `docker-compose.yml`. `graph/load_neo4j.py` loads a bounded window (e.g. the red-team days) as `(:User)-[:AUTH {count, first_seen, success_ratio}]->(:Computer)`.
2. `graph/queries.cypher` (committed, each with a comment stating what it answers):
   - shortest path between two identities via shared computers (**the "Paths to Privilege" query** — BeyondTrust's own tagline)
   - blast radius: computers reachable from a compromised identity within k hops
   - the red-team account's actual movement subgraph vs. its 30-day baseline
3. `graph/export_paths.py`: exports the showcase paths/subgraphs + `bench/graph_stats.json` (node/edge counts, path lengths, query timings) as JSON for the demo site.
4. Tests: loader idempotency; path query correctness on a hand-built toy graph.

**EVIDENCE CONTRACT:** committed Cypher + loader · `bench/graph_stats.json` · exported path JSON that the demo renders · tests green.

### Stage 5 — The demo site (≈ 1 day) — **GitHub Pages, and it must be beautiful**

**MUST: before writing a single line of site code, invoke the `ui-ux-pro-max` skill** (`ui-ux-pro-max:ui-ux-pro-max`; use its style/palette/font-pairing system to pick one coherent direction — brief: *professional data-platform aesthetic, dark-mode-first with a light theme, restrained, fast; the wow comes from real interactivity, not decoration*). **And before building any chart, invoke the `dataviz` skill** and follow its palette/mark rules so the benchmark and rollup charts read as one system.

Site (`site/`, static, deployed by `pages.yml` to `https://samad-zeeshan.github.io/tarn/`):

1. **Hero** — one sentence, the architecture story of §2, a single clean pipeline diagram, and the honest scale line ("N,NNN,NNN,NNN authentication events processed" — the real number from `bench/dataset.json`, rendered from that file, not hard-coded).
2. **Live SQL workbench** — **DuckDB-WASM** (vendored, no CDN dependency) over committed Parquet extracts of the marts (keep total site payload under ~150MB; every individual file under GitHub's 100MB hard limit; target <20MB warm-path for first query). Preset buttons for Q1–Q5 plus a free-form SQL box. This is the section that makes the demo *genuinely* live: the screener's browser is really executing analytical SQL against the real warehouse extract.
3. **The optimization story** — before/after Spark benchmark as a chart (dataviz rules), the two physical plans side by side in collapsible panels, and the provenance line (machine, slice, median-of-N) directly under the chart.
4. **Streaming panel** — the measured lag distribution and throughput, clearly labelled *recorded from a local run*, with the window/watermark config shown.
5. **Path explorer** — interactive rendering of the exported Neo4j subgraphs (self-contained force-directed canvas/SVG; vendored lib or hand-rolled): pick two identities → see the shortest privilege path; toggle the red-team overlay. Caption it with BeyondTrust-adjacent framing ("paths to privilege") without naming the company.
6. **Provenance footer (MUST, rule 5)** — "What's live: the SQL in section 2 runs in your browser via DuckDB-WASM. What's recorded: Spark runs, streaming lag, and graph timings were measured on [env] on [date]; artifacts in `/bench`. Dataset: LANL 2015, anonymized. Nothing here is a production system."
7. Responsive, keyboard-accessible, no external network calls after load, favicon, `<title>`. Lighthouse performance ≥90 on desktop.

**EVIDENCE CONTRACT:** Pages deploy green from `pages.yml` on push to `main` · the URL returns 200 · every panel renders from committed artifacts (grep-able: no hard-coded metric that differs from its `bench/` source) · payload budgets met.

### CI (`ci.yml`, built incrementally from Stage 0)

Lint (ruff) → pytest (all stages' tests) → small-slice pipeline run (Spark `local[*]` on `data/sample/`) → dbt build with tests → site build. Green CI is itself a claimable practice signal (every existing top project — Docket 111, Tally 254+31 — earns points for exactly this).

---

## 6. What Samad must do personally (Claude cannot do these)

1. Approve the name (or rename before first push).
2. Create the GitHub repo (public) and the free Databricks workspace; run the Stage-1 notebook there once.
3. Skim each stage's bench artifact at its STAGE GATE — these become *your* numbers in a screen; if you can't narrate one, flag it before it's committed.
4. After Stage 5: open the demo on your phone and one desktop browser; click every preset query.
5. Convert the draft facts (§9) in `facts/master-facts.md` — status VERIFIED only after you've run/read the evidence yourself.
6. Rerun `/tailor beyondtrust1` (and any other data-JD variant). Expect the five MISSING requirements to flip and the lineup to rescore with Tarn as a lead candidate.

---

## 7. Timeline

Roughly four focused days end to end with Claude driving: Stage 0+1 in one weekend day, Stage 2 a half day, Stage 3 one day, Stage 4 a half day, Stage 5 one day. Stages 1–2 alone already flip all three **Required** items — if time is short, ship 0–2 + a minimal demo (hero + SQL workbench + optimization story), and add 3–4 after.

---

## 8. README spec (write it last)

Lead with the demo link and one screenshot. Then: the §2 story, quickstart (`docker compose up`, `make pipeline`, `make site`), the measured-numbers table (each row citing its `bench/` artifact), the honest-limitations section (single machine, replayed streaming, dataset is 2015 and anonymized), and the layout map. No badge walls, no marketing adjectives the bench files can't back.

---

## 9. Fact conversion package (draft claims — Samad flips to VERIFIED after evidence review)

Numbering continues from F-020 (F-018 is reserved for job-fit). `[M]` = fill from the named bench artifact; never from memory.

**F-021 — Tarn: Spark / distributed processing (proposed)**
claim: Built a PySpark batch layer over [M: dataset.json rows] real anonymized enterprise authentication events (LANL 2015): sessionization and per-identity daily rollups with window functions, written as date-partitioned Parquet. Optimized the [join/shuffle] stage — [M: spark_opt.json before] → [M: after] median over [M: n] runs ([M: %] faster), physical plans committed. Also executed on Databricks (free workspace), notebook committed.
evidence: repo `pipeline/`, `bench/spark_opt.json`, `pipeline/databricks/`, CI run. tags: data-eng, spark, cloud.

**F-022 — Tarn: dimensional modelling / warehousing (proposed)**
claim: Modelled the lake into a DuckDB star schema with dbt — `fact_auth_event` plus identity/computer/time dimensions, documented grain, dbt `unique`/`not_null`/`relationships` tests in CI — and shipped five analytical queries (lateral-movement fan-out, off-hours baselines, new-access-path rate, failure spikes, red-team enrichment) with committed results.
evidence: `warehouse/` dbt project, `warehouse/queries/` + results, CI. tags: data-eng, warehousing, sql, dbt.

**F-023 — Tarn: stream processing (proposed)**
claim: Built a Spark Structured Streaming job consuming replayed auth events from Kafka-compatible Redpanda — 1-minute tumbling windows per identity with watermarked late-data handling and checkpoint recovery — sustaining [M: streaming_lag.json throughput] with [M: p50] / [M: p95] end-to-end lag, measured over [M: duration].
evidence: `streaming/`, `bench/streaming_lag.json`, recovery test. tags: data-eng, streaming, kafka, spark.

**F-024 — Tarn: graph data store (proposed)**
claim: Loaded identity→computer authentication edges into Neo4j and answered privilege-path questions in Cypher: shortest user-to-user privilege paths via shared computers, k-hop blast radius from a compromised identity, and the labelled red-team account's movement subgraph vs. its baseline. Stats and exports committed; rendered interactively in the public demo.
evidence: `graph/`, `bench/graph_stats.json`, demo path explorer. tags: data-eng, graph, security.

**HONESTY GUARDS to record with all four:** never "production", never "billions per day" (the honest claim is the measured row count, processed in batch); streaming lag figures are from an accelerated local replay, not a live feed; the demo's SQL is live in-browser, everything else on the page is recorded evidence — resume link label is `Demo`. Demo URL joins the KI-1 link-curl list once live.

---

## 10. Kickoff line

Open Claude Code in `portfolio/tarn/` and say:

> **Build Tarn per DIRECTIVE.md. Start at Stage 0. Stop at every STAGE GATE and show me the evidence contract before continuing. Use WSL2 or Docker for all Spark runs. Do not write the demo site without invoking the ui-ux-pro-max skill first, and do not write a chart without the dataviz skill.**
