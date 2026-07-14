"""Stage 4a — load the identity->computer authentication graph into Neo4j.

The graph is the access surface: every distinct (identity, destination host) pair that ever
authenticated in the loaded window becomes an edge. Two identities are connected when they
share a host, and THAT is what makes privilege paths computable — an attacker who owns U1
and finds that U1 and U2 both touch C7 has a route from U1 to whatever U2 can reach.

    (:User {name, is_machine, is_compromised})
        -[:AUTH {count, first_seen, last_seen, success_ratio, is_redteam}]->
    (:Computer {name, is_redteam_pivot, is_redteam_target})

Edges are AGGREGATED, not one-per-event: a billion events collapse to the distinct pairs,
which is the only way this fits in a laptop Neo4j and also the only shape the path queries
want (an edge means "this identity can reach this host", and repeating it a million times
adds nothing).

The window is bounded on purpose — default days 0-30, which contains the entire red-team
campaign (its labelled events run from day 1.7 to day 29.6). Loading all 58 days would add
edges that no query in queries.cypher asks about.

    python graph/load_neo4j.py --lake /data/lake/auth --start-day 0 --end-day 30
"""

from __future__ import annotations

import argparse
import os
import time

from neo4j import GraphDatabase
from pyspark.sql import functions as F

from pipeline.common import spark_session

BATCH = 10_000

SCHEMA_CYPHER = [
    "CREATE CONSTRAINT user_name IF NOT EXISTS FOR (u:User) REQUIRE u.name IS UNIQUE",
    "CREATE CONSTRAINT computer_name IF NOT EXISTS FOR (c:Computer) REQUIRE c.name IS UNIQUE",
    "CREATE INDEX user_compromised IF NOT EXISTS FOR (u:User) ON (u.is_compromised)",
    "CREATE INDEX computer_pivot IF NOT EXISTS FOR (c:Computer) ON (c.is_redteam_pivot)",
]


def build_edges(spark, lake: str, redteam_path: str, start_day: int, end_day: int):
    """Aggregate the event stream into the distinct access edges of the window."""
    events = (
        spark.read.parquet(lake)
        .filter((F.col("day_index") >= start_day) & (F.col("day_index") < end_day))
        .filter(F.col("src_user").isNotNull() & F.col("dst_computer").isNotNull())
    )

    redteam = spark.read.parquet(redteam_path).filter(
        (F.col("day_index") >= start_day) & (F.col("day_index") < end_day)
    )

    # Which (user, host) pairs are labelled compromises? Mark the EDGE, so the demo can
    # overlay the attacker's real route on top of the benign graph.
    rt_edges = (
        redteam.select(
            F.col("user").alias("src_user"),
            F.col("dst_computer").alias("dst_computer"),
        )
        .distinct()
        .withColumn("is_redteam", F.lit(True))
    )

    edges = (
        events.groupBy("src_user", "dst_computer")
        .agg(
            F.count("*").alias("count"),
            F.min("time").alias("first_seen"),
            F.max("time").alias("last_seen"),
            F.sum(F.col("is_success").cast("int")).alias("successes"),
        )
        .withColumn(
            "success_ratio",
            F.round(F.col("successes") / F.col("count"), 4),
        )
        .join(F.broadcast(rt_edges), ["src_user", "dst_computer"], "left")
        .fillna({"is_redteam": False})
    )

    compromised = redteam.select(F.col("user").alias("src_user")).distinct()
    pivots = redteam.select(F.col("src_computer").alias("name")).distinct()
    targets = redteam.select(F.col("dst_computer").alias("name")).distinct()

    users = (
        edges.select("src_user")
        .distinct()
        .join(compromised.withColumn("is_compromised", F.lit(True)), ["src_user"], "left")
        .fillna({"is_compromised": False})
        .withColumn("is_machine", F.col("src_user").rlike(r"\$@"))
        .select(
            F.col("src_user").alias("name"), "is_compromised", "is_machine"
        )
    )

    computers = (
        edges.select(F.col("dst_computer").alias("name"))
        .distinct()
        .join(pivots.withColumn("is_redteam_pivot", F.lit(True)), ["name"], "left")
        .join(targets.withColumn("is_redteam_target", F.lit(True)), ["name"], "left")
        .fillna({"is_redteam_pivot": False, "is_redteam_target": False})
    )

    return users, computers, edges


def load(driver, users, computers, edges) -> dict:
    """Write nodes then edges in batches. Idempotent: MERGE everywhere, so re-running
    converges rather than duplicating (tests/test_graph.py asserts this)."""
    stats = {}

    with driver.session() as session:
        for stmt in SCHEMA_CYPHER:
            session.run(stmt)

        t0 = time.perf_counter()
        n = 0
        batch = []
        for row in users.toLocalIterator():
            batch.append(
                {"name": row["name"], "is_compromised": row["is_compromised"],
                 "is_machine": row["is_machine"]}
            )
            if len(batch) >= BATCH:
                session.run(
                    "UNWIND $rows AS r MERGE (u:User {name: r.name}) "
                    "SET u.is_compromised = r.is_compromised, u.is_machine = r.is_machine",
                    rows=batch,
                )
                n += len(batch)
                batch = []
        if batch:
            session.run(
                "UNWIND $rows AS r MERGE (u:User {name: r.name}) "
                "SET u.is_compromised = r.is_compromised, u.is_machine = r.is_machine",
                rows=batch,
            )
            n += len(batch)
        stats["users"] = n
        stats["users_seconds"] = round(time.perf_counter() - t0, 1)
        print(f"[neo4j] {n:,} User nodes ({stats['users_seconds']}s)")

        t0 = time.perf_counter()
        n = 0
        batch = []
        for row in computers.toLocalIterator():
            batch.append(
                {"name": row["name"], "is_redteam_pivot": row["is_redteam_pivot"],
                 "is_redteam_target": row["is_redteam_target"]}
            )
            if len(batch) >= BATCH:
                session.run(
                    "UNWIND $rows AS r MERGE (c:Computer {name: r.name}) "
                    "SET c.is_redteam_pivot = r.is_redteam_pivot, "
                    "    c.is_redteam_target = r.is_redteam_target",
                    rows=batch,
                )
                n += len(batch)
                batch = []
        if batch:
            session.run(
                "UNWIND $rows AS r MERGE (c:Computer {name: r.name}) "
                "SET c.is_redteam_pivot = r.is_redteam_pivot, "
                "    c.is_redteam_target = r.is_redteam_target",
                rows=batch,
            )
            n += len(batch)
        stats["computers"] = n
        stats["computers_seconds"] = round(time.perf_counter() - t0, 1)
        print(f"[neo4j] {n:,} Computer nodes ({stats['computers_seconds']}s)")

        t0 = time.perf_counter()
        n = 0
        batch = []
        edge_cypher = (
            "UNWIND $rows AS r "
            "MATCH (u:User {name: r.src_user}) "
            "MATCH (c:Computer {name: r.dst_computer}) "
            "MERGE (u)-[a:AUTH]->(c) "
            "SET a.count = r.count, a.first_seen = r.first_seen, a.last_seen = r.last_seen, "
            "    a.success_ratio = r.success_ratio, a.is_redteam = r.is_redteam"
        )
        for row in edges.toLocalIterator():
            batch.append(
                {
                    "src_user": row["src_user"],
                    "dst_computer": row["dst_computer"],
                    "count": int(row["count"]),
                    "first_seen": int(row["first_seen"]),
                    "last_seen": int(row["last_seen"]),
                    "success_ratio": float(row["success_ratio"]),
                    "is_redteam": bool(row["is_redteam"]),
                }
            )
            if len(batch) >= BATCH:
                session.run(edge_cypher, rows=batch)
                n += len(batch)
                batch = []
                if n % 100_000 == 0:
                    print(f"  {n:,} edges...")
        if batch:
            session.run(edge_cypher, rows=batch)
            n += len(batch)
        stats["edges"] = n
        stats["edges_seconds"] = round(time.perf_counter() - t0, 1)
        print(f"[neo4j] {n:,} AUTH edges ({stats['edges_seconds']}s)")

    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--lake", default="/data/lake/auth")
    ap.add_argument("--redteam", default="/data/lake/redteam")
    ap.add_argument("--start-day", type=int, default=0)
    ap.add_argument("--end-day", type=int, default=30)
    ap.add_argument("--uri", default=os.environ.get("NEO4J_URI", "bolt://neo4j:7687"))
    ap.add_argument("--user", default=os.environ.get("NEO4J_USER", "neo4j"))
    ap.add_argument("--password", default=os.environ.get("NEO4J_PASSWORD", "tarnlocal1"))
    ap.add_argument("--wipe", action="store_true", help="delete all nodes first")
    args = ap.parse_args()

    spark = spark_session("graph-load")
    users, computers, edges = build_edges(
        spark, args.lake, args.redteam, args.start_day, args.end_day
    )

    # Materialize once — these are read three times each by toLocalIterator otherwise.
    users = users.cache()
    computers = computers.cache()
    edges = edges.cache()
    print(f"[graph] window days {args.start_day}-{args.end_day}: "
          f"{users.count():,} users, {computers.count():,} computers, {edges.count():,} edges")

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    if args.wipe:
        with driver.session() as s:
            s.run("MATCH (n) DETACH DELETE n")
        print("[neo4j] wiped")

    stats = load(driver, users, computers, edges)
    stats["window_days"] = [args.start_day, args.end_day]
    print(f"[graph] loaded: {stats}")

    driver.close()
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
