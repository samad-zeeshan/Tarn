# Tarn — every stage is one target. All heavy work runs inside the `tarn` container
# (DIRECTIVE rule 8: Spark never runs on native Windows).
#
#   make up          bring the stack up (builds the image on first run)
#   make fetch       download + verify LANL auth/redteam into /data/raw (needs TARN_LANL_TOKEN)
#   make sample      cut the deterministic committed CI slice into data/sample/
#   make describe    count the full corpus -> bench/dataset.json
#
#   make lake        Stage 1a — raw .gz -> date-partitioned Parquet lake (+ diurnal measurement)
#   make rollup      Stage 1b — per-identity daily rollups
#   make bench       Stage 1c — the measured 2x2 optimization -> bench/spark_opt.json
#
#   make warehouse   Stage 2  — dbt star schema + tests + the 5 showcase queries
#   make stream      Stage 3  — Redpanda replay + Structured Streaming + lag probe
#   make graph       Stage 4  — load Neo4j, run Cypher, export paths
#   make site        Stage 5  — build the demo payloads, then audit them
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

# ---- Stage 5 -----------------------------------------------------------------
site:
	$(EXEC) python site/build_payloads.py --db $(DUCKDB)
	$(EXEC) python site/audit.py

audit:
	$(EXEC) python site/audit.py

serve:
	@echo "http://localhost:8080"
	@cd site && python -m http.server 8080

all: lake rollup bench warehouse stream graph site

# ---- quality -----------------------------------------------------------------
test:
	$(EXEC) python -m pytest tests/ -q

lint:
	$(EXEC) ruff check .

clean:
	$(DC) down -v
	rm -rf warehouse/target warehouse/logs
