# Every stage is one target. All heavy work runs inside the `tarn` container, because Spark
# does not run on this host's Python 3.14 and Java 25.
#
#   make up          bring the stack up (builds the image on first run)
#   make fetch       download + verify LANL auth/redteam into /data/raw (needs TARN_LANL_TOKEN)
#   make sample      cut the deterministic committed CI slice into data/sample/
#   make describe    count the full corpus -> bench/dataset.json
#
#   make lake        Stage 1a, raw .gz -> date-partitioned Parquet lake (+ diurnal measurement)
#   make rollup      Stage 1b, per-identity daily rollups
#   make bench       Stage 1c, the measured 2x2 optimization -> bench/spark_opt.json
#
#   make warehouse   Stage 2, dbt star schema + tests + the 5 showcase queries
#   make stream      Stage 3, Redpanda replay + Structured Streaming + lag probe
#   make graph       Stage 4, load Neo4j, run Cypher, export paths
#   make vectors     Stage 4b, embed each person-day, index it, score the search honestly
#   make site        Stage 5, build the demo payloads, then audit them
#
#   make v2          graph detector, explanations, fair scoring and the verifier on the lake
#   make analyst     run the analyst benchmark against a local LM Studio model, then score it
#
#   make all         stages 1-5 end to end (assumes `make fetch` has run)
#   make test        pytest across every stage
#   make lint        ruff
#   make serve       preview the site at http://localhost:8080

SHELL := /bin/bash
DC    := docker compose
EXEC  := $(DC) exec -T tarn

LAKE      ?= /data/lake
RAW       ?= /data/raw
DUCKDB    ?= /data/work/tarn.duckdb
BENCH_RUNS ?= 5

.DEFAULT_GOAL := help

.PHONY: help up down build shell fetch sample describe lake rollup bench warehouse \
        stream graph site audit all test lint serve clean

help:
	@grep -E '^#   make' $(MAKEFILE_LIST) | sed 's/^#   //'

build:
	$(DC) build tarn

up:
	$(DC) up -d
	@$(DC) ps

down:
	$(DC) down

shell:
	$(DC) exec tarn bash

# ---- Stage 0 -----------------------------------------------------------------
fetch:
	$(EXEC) python data/fetch.py download
	$(EXEC) python data/fetch.py verify

sample:
	$(EXEC) python data/fetch.py sample

describe:
	$(EXEC) python data/fetch.py describe

# ---- Stage 1 -----------------------------------------------------------------
lake:
	$(EXEC) python pipeline/sessionize.py \
		--input $(RAW)/auth.txt.gz --output $(LAKE)/auth \
		--redteam $(RAW)/redteam.txt.gz --stats-out bench/lake_build.json
	$(EXEC) python pipeline/diurnal.py --lake $(LAKE)/auth --out bench/diurnal.json

rollup:
	$(EXEC) python pipeline/rollup.py \
		--lake $(LAKE)/auth --output $(LAKE)/rollup \
		--redteam $(LAKE)/redteam --diurnal bench/diurnal.json

bench:
	$(EXEC) python pipeline/optimize_bench.py \
		--lake $(LAKE)/auth --redteam $(LAKE)/redteam \
		--runs $(BENCH_RUNS) --out bench/spark_opt.json

# ---- Stage 2 -----------------------------------------------------------------
warehouse:
	$(EXEC) python warehouse/build.py --lake $(LAKE) --diurnal bench/diurnal.json
	$(EXEC) python warehouse/run_queries.py --db $(DUCKDB)

# ---- Stage 3 -----------------------------------------------------------------
stream:
	$(EXEC) python streaming/run_stage3.py --fresh --rate 5000 --duration 300 --lake $(LAKE)/auth

# ---- Stage 4 -----------------------------------------------------------------
graph:
	$(EXEC) python graph/load_neo4j.py --lake $(LAKE)/auth --redteam $(LAKE)/redteam --wipe
	$(EXEC) python graph/export_paths.py

# ---- Stage 4b ----------------------------------------------------------------
vectors:
	$(EXEC) python pipeline/embed.py --lake $(LAKE)/auth --rollup $(LAKE)/rollup --output $(LAKE)/vectors --stats-out bench/embedding.json
	$(EXEC) python vector/search.py --vectors $(LAKE)/vectors --rollup $(LAKE)/rollup --scores-out $(LAKE)/vector_scores --out bench/vector_eval.json

# ---- v2 ------------------------------------------------------------------------
V2 ?= /data/work/v2

v2:
	$(EXEC) python detect/graph/extract.py --lake $(LAKE) --out $(V2)
	$(EXEC) python detect/graph/run.py --work $(V2) --out $(V2)/out
	$(EXEC) python detect/graph/gnn.py --work $(V2) --scores $(V2)/out/scores.parquet --out $(V2)/gnn
	$(EXEC) python detect/graph/explain.py --work $(V2) --scores $(V2)/out
	$(EXEC) python detect/graph/explain.py --work $(V2) --scores $(V2)/out --budget 1000 --name alerts_wide.jsonl
	$(EXEC) python eval/score.py --work $(V2) --scores $(V2)/out --warehouse $(DUCKDB) --lake $(LAKE) 		--extra gnn=$(V2)/gnn/scores.parquet --data-label "full LANL auth log"
	$(EXEC) python detect/graph/verify.py --work $(V2) --scores $(V2)/out 		--data-label "full LANL auth log" --out eval/results/verifier.json

# The model runs on the host in LM Studio, so these run on the host too.
analyst:
	python analyst/benchmark.py build --alerts $(V2)/out/alerts_wide.jsonl --work $(V2)
	python analyst/benchmark.py run --alerts $(V2)/out/alerts_wide.jsonl --work $(V2)
	python analyst/benchmark.py run --no-graph-tools --alerts $(V2)/out/alerts_wide.jsonl --work $(V2)
	python analyst/benchmark.py score --data-label "full LANL auth log"
	python eval/readme.py

# ---- Stage 5 -----------------------------------------------------------------
site:
	$(EXEC) python site/build_payloads.py --db $(DUCKDB)
	$(EXEC) python site/audit.py

audit:
	$(EXEC) python site/audit.py

serve:
	@echo "http://localhost:8080"
	@cd site && python -m http.server 8080

all: lake rollup bench warehouse stream graph vectors site

# ---- quality -----------------------------------------------------------------
test:
	$(EXEC) python -m pytest tests/ -q

lint:
	$(EXEC) ruff check .

clean:
	$(DC) down -v
	rm -rf warehouse/target warehouse/logs
