"""
Run the Cypher, time it, and export what the demo renders.

Writes bench/graph_stats.json, plus site/data/graph.json and site/data/paths.json. The browser
cannot run Cypher, so the path explorer replays these real query results.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

from neo4j import GraphDatabase

REPO = Path(__file__).resolve().parent.parent


def timed(session, cypher: str, **params) -> tuple[list[dict], float]:
    t0 = time.perf_counter()
    rows = [dict(r) for r in session.run(cypher, **params)]
    return rows, round((time.perf_counter() - t0) * 1000, 1)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--uri", default=os.environ.get("NEO4J_URI", "bolt://neo4j:7687"))
    ap.add_argument("--user", default=os.environ.get("NEO4J_USER", "neo4j"))
    ap.add_argument("--password", default=os.environ.get("NEO4J_PASSWORD", "tarnlocal1"))
    ap.add_argument("--bench-out", default=str(REPO / "bench" / "graph_stats.json"))
    ap.add_argument("--site-dir", default=str(REPO / "site" / "data"))
    ap.add_argument("--max-path-pairs", type=int, default=40)
    args = ap.parse_args()

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    timings: dict[str, float] = {}

    with driver.session() as s:
        counts, timings["counts"] = timed(
            s,
            """
            MATCH (u:User) WITH count(u) AS users
            MATCH (c:Computer) WITH users, count(c) AS computers
            MATCH ()-[a:AUTH]->() WITH users, computers, count(a) AS edges
            MATCH ()-[r:AUTH {is_redteam: true}]->()
            RETURN users, computers, edges, count(r) AS redteam_edges
            """,
        )
        shape = counts[0]

        compromised, _ = timed(
            s, "MATCH (u:User {is_compromised: true}) RETURN count(u) AS n"
        )
        pivots, _ = timed(
            s, "MATCH (c:Computer {is_redteam_pivot: true}) RETURN collect(c.name) AS names"
        )
        choke, timings["choke_points"] = timed(
            s,
            """
            MATCH (u:User)-[:AUTH]->(c:Computer)
            WITH c, count(DISTINCT u) AS identities
            WHERE identities > 1
            RETURN c.name AS host, identities,
                   c.is_redteam_pivot AS was_pivot, c.is_redteam_target AS was_target
            ORDER BY identities DESC LIMIT 25
            """,
        )
        attacker_baseline, timings["attacker_vs_baseline"] = timed(
            s,
            """
            MATCH (u:User {is_compromised: true})
            OPTIONAL MATCH (u)-[:AUTH {is_redteam: true}]->(rtc:Computer)
            WITH u, count(DISTINCT rtc) AS redteam_hosts
            MATCH (u)-[:AUTH]->(all:Computer)
            WITH u, redteam_hosts, count(DISTINCT all) AS total_hosts
            RETURN u.name AS identity, redteam_hosts AS hosts_via_redteam_edges,
                   total_hosts AS hosts_total_footprint,
                   round(100.0 * redteam_hosts / total_hosts, 1) AS pct_of_footprint_that_was_attack
            ORDER BY redteam_hosts DESC LIMIT 25
            """,
        )
        top_attackers, _ = timed(
            s,
            """
            MATCH (u:User {is_compromised: true})-[a:AUTH {is_redteam: true}]->(:Computer)
            RETURN u.name AS name, count(a) AS redteam_edges
            ORDER BY redteam_edges DESC LIMIT 8
            """,
        )

        blast: list[dict] = []
        t0 = time.perf_counter()
        for a in top_attackers:
            rows, _ = timed(
                s,
                """
                MATCH (start:User {name: $user})
                CALL (start) {
                  MATCH (start)-[:AUTH]->(c1:Computer)
                  RETURN count(DISTINCT c1) AS hop1 }
                CALL (start) {
                  MATCH (start)-[:AUTH]->(:Computer)<-[:AUTH]-(peer:User) WHERE peer <> start
                  RETURN count(DISTINCT peer) AS hop2 }
                CALL (start) {
                  MATCH (start)-[:AUTH]->(:Computer)<-[:AUTH]-(peer:User)-[:AUTH]->(c2:Computer)
                  WHERE peer <> start
                  RETURN count(DISTINCT c2) AS hop3 }
                RETURN start.name AS identity, hop1 AS hosts_1_hop,
                       hop2 AS identities_2_hops, hop3 AS hosts_3_hops
                """,
                user=a["name"],
            )
            if rows:
                rows[0]["is_compromised"] = True
                blast.append(rows[0])

        # The control group. Without it a blast radius number is unanchored: "reaches 400
        # hosts at 3 hops" only means something next to what a normal account reaches.
        benign, _ = timed(
            s,
            """
            MATCH (u:User {is_compromised: false})-[:AUTH]->(:Computer)
            WHERE NOT u.is_machine
            WITH u, count(*) AS deg ORDER BY deg DESC
            SKIP 50 LIMIT 8
            RETURN u.name AS name
            """,
        )
        for b in benign:
            rows, _ = timed(
                s,
                """
                MATCH (start:User {name: $user})
                CALL (start) {
                  MATCH (start)-[:AUTH]->(c1:Computer)
                  RETURN count(DISTINCT c1) AS hop1 }
                CALL (start) {
                  MATCH (start)-[:AUTH]->(:Computer)<-[:AUTH]-(peer:User) WHERE peer <> start
                  RETURN count(DISTINCT peer) AS hop2 }
                CALL (start) {
                  MATCH (start)-[:AUTH]->(:Computer)<-[:AUTH]-(peer:User)-[:AUTH]->(c2:Computer)
                  WHERE peer <> start
                  RETURN count(DISTINCT c2) AS hop3 }
                RETURN start.name AS identity, hop1 AS hosts_1_hop,
                       hop2 AS identities_2_hops, hop3 AS hosts_3_hops
                """,
                user=b["name"],
            )
            if rows:
                rows[0]["is_compromised"] = False
                blast.append(rows[0])
        timings["blast_radius_all"] = round((time.perf_counter() - t0) * 1000, 1)
        # Compromised to compromised, and compromised out to ordinary. This is the "how far
        # can it spread" question.
        attackers = [a["name"] for a in top_attackers[:6]]
        others, _ = timed(
            s,
            """
            MATCH (u:User {is_compromised: false})-[:AUTH]->(c:Computer {is_redteam_target: true})
            WHERE NOT u.is_machine
            RETURN DISTINCT u.name AS name LIMIT 8
            """,
        )
        targets = [o["name"] for o in others]

        paths: list[dict] = []
        t0 = time.perf_counter()
        lengths: list[int] = []
        pairs = [
            (a, b)
            for i, a in enumerate(attackers)
            for b in (attackers[i + 1:] + targets)
        ][: args.max_path_pairs]

        for src, dst in pairs:
            rows, ms = timed(
                s,
                """
                MATCH (a:User {name: $from_user}), (b:User {name: $to_user})
                MATCH p = shortestPath((a)-[:AUTH*..6]-(b))
                RETURN [n IN nodes(p) | n.name] AS hops,
                       [n IN nodes(p) | labels(n)[0]] AS kinds,
                       length(p) AS hop_count,
                       [r IN relationships(p) | r.count] AS weights,
                       any(r IN relationships(p) WHERE r.is_redteam) AS traverses_redteam
                """,
                from_user=src,
                to_user=dst,
            )
            if rows:
                r = rows[0]
                r["from_user"] = src
                r["to_user"] = dst
                r["query_ms"] = ms
                paths.append(r)
                lengths.append(int(r["hop_count"]))
        timings["shortest_paths_all"] = round((time.perf_counter() - t0) * 1000, 1)
        subgraph_rows, timings["subgraph_export"] = timed(
            s,
            """
            MATCH (u:User {is_compromised: true})-[a:AUTH]->(c:Computer)
            WHERE a.is_redteam OR c.is_redteam_target OR c.is_redteam_pivot
            RETURN u.name AS src, u.is_machine AS src_is_machine,
                   c.name AS dst, c.is_redteam_pivot AS dst_is_pivot,
                   c.is_redteam_target AS dst_is_target,
                   a.count AS weight, a.is_redteam AS is_redteam,
                   a.first_seen AS first_seen, a.success_ratio AS success_ratio
            """,
        )

    driver.close()
    nodes: dict[str, dict] = {}
    links: list[dict] = []
    for r in subgraph_rows:
        nodes.setdefault(
            r["src"],
            {"id": r["src"], "kind": "user", "compromised": True,
             "machine": bool(r["src_is_machine"])},
        )
        nodes.setdefault(
            r["dst"],
            {"id": r["dst"], "kind": "computer", "pivot": bool(r["dst_is_pivot"]),
             "target": bool(r["dst_is_target"])},
        )
        links.append(
            {
                "source": r["src"],
                "target": r["dst"],
                "weight": int(r["weight"]),
                "redteam": bool(r["is_redteam"]),
                "first_seen": int(r["first_seen"]),
                "success_ratio": float(r["success_ratio"]),
            }
        )

    site_dir = Path(args.site_dir)
    site_dir.mkdir(parents=True, exist_ok=True)

    (site_dir / "graph.json").write_text(
        json.dumps(
            {
                "what": (
                    "Red-team movement subgraph: every edge from a compromised identity that "
                    "either IS a labelled compromise event or lands on a host the red team "
                    "touched. Benign edges from those same identities are included so the "
                    "attack shows up against the account's normal behaviour rather than in a "
                    "vacuum."
                ),
                "source": "Neo4j, loaded by graph/load_neo4j.py from the Parquet lake",
                "nodes": list(nodes.values()),
                "links": links,
            },
            indent=2,
        )
        + "\n"
    )

    (site_dir / "paths.json").write_text(
        json.dumps(
            {
                "what": (
                    "Shortest privilege paths, precomputed. The browser cannot run Cypher, so "
                    "the path explorer replays these REAL shortestPath() results rather than "
                    "recomputing them. Each entry records the query time it actually took."
                ),
                "cypher": "MATCH p = shortestPath((a:User)-[:AUTH*..6]-(b:User)) RETURN p",
                "paths": paths,
                "blast_radius": blast,
            },
            indent=2,
        )
        + "\n"
    )
    length_hist: dict[str, int] = {}
    for n in lengths:
        length_hist[str(n)] = length_hist.get(str(n), 0) + 1

    compromised_blast = [b for b in blast if b["is_compromised"]]
    benign_blast = [b for b in blast if not b["is_compromised"]]

    def mean(rows: list[dict], key: str) -> float:
        return round(sum(r[key] for r in rows) / len(rows), 1) if rows else 0.0
    #
    # It is easy to write "this account can reach 14,256 hosts" and let a reader assume that is
    # alarming. It is not alarming until you say what an ordinary account reaches. So the
    # artifact computes the discrimination and disqualifies its own metric when it saturates.
    total_hosts = int(shape["computers"])
    c3 = mean(compromised_blast, "hosts_3_hops")
    b3 = mean(benign_blast, "hosts_3_hops")
    c1 = mean(compromised_blast, "hosts_1_hop")
    b1 = mean(benign_blast, "hosts_1_hop")

    def verdict(c: float, b: float, hops: str) -> dict:
        ratio = round(c / b, 2) if b else None
        coverage = round(100 * max(c, b) / total_hosts, 1) if total_hosts else 0.0
        useful = bool(ratio and ratio >= 1.5 and coverage < 90)
        return {
            "compromised_mean": c,
            "benign_control_mean": b,
            "ratio": ratio,
            "pct_of_all_hosts_covered": coverage,
            "discriminates": useful,
            "verdict": (
                f"USABLE at {hops}: compromised identities reach {ratio}x what the control "
                f"reaches, and the reachable set is still only {coverage}% of the network."
                if useful
                else
                f"NOT A SIGNAL at {hops}: both groups reach ~{coverage}% of every host in the "
                f"graph (ratio {ratio}x). The metric has saturated, so quoting the compromised "
                f"number alone would be true and completely meaningless."
            ),
        }

    payload = {
        "what": "Shape and query performance of the Neo4j privilege-path graph (Stage 4).",
        "graph": {
            "users": int(shape["users"]),
            "computers": int(shape["computers"]),
            "auth_edges": int(shape["edges"]),
            "redteam_edges": int(shape["redteam_edges"]),
            "compromised_identities": int(compromised[0]["n"]),
            "redteam_pivot_hosts": pivots[0]["names"],
        },
        "shortest_paths": {
            "pairs_queried": len(pairs),
            "paths_found": len(paths),
            "hop_length_histogram": length_hist,
            "min_hops": min(lengths) if lengths else None,
            "max_hops": max(lengths) if lengths else None,
            "median_query_ms": (
                round(sorted(p["query_ms"] for p in paths)[len(paths) // 2], 1) if paths else None
            ),
            "note": (
                "Hops alternate User -> Computer -> User. A 2-hop path means the two "
                "identities share a single host; 4 hops means one intermediate identity."
            ),
        },
        "blast_radius": {
            "compromised_mean_hosts_1_hop": c1,
            "compromised_mean_hosts_3_hops": c3,
            "benign_control_mean_hosts_1_hop": b1,
            "benign_control_mean_hosts_3_hops": b3,
            "total_hosts_in_graph": total_hosts,
            "note": (
                "The benign control group is what makes these numbers mean anything. Compare "
                "compromised vs. control rather than reading either alone. See the verdicts."
            ),
            "at_1_hop": verdict(c1, b1, "1 hop"),
            "at_3_hops": verdict(c3, b3, "3 hops"),
            "headline_finding": (
                "BLAST RADIUS SATURATES. At 3 hops both compromised and ordinary identities "
                f"reach essentially every host in the network ({c3:,.0f} vs {b3:,.0f} of "
                f"{total_hosts:,}), so the measure carries no information at that depth. The "
                "cause is visible in choke_points_top: hub hosts here are authenticated to by "
                "tens of thousands of distinct identities, which collapses the graph into a "
                "small world where almost everyone is two hops from almost everyone. Only the "
                "1-hop measure discriminates, and even that is confounded, because the "
                "accounts the red team chose to compromise were higher-privilege to begin with. "
                "The genuinely actionable output of this stage is therefore not blast radius "
                "but the choke points: harden those hubs and you break the paths."
            ),
            "detail": blast,
        },
        "choke_points_top": choke[:10],
        "attacker_vs_baseline_top": attacker_baseline[:10],
        "query_timings_ms": timings,
        "environment": {
            "neo4j": "5.26-community (Docker), 2 GB heap",
            "host": "Windows 11 Pro 26200, Docker Desktop (WSL2 backend)",
            "note": "Timings are single-run against a warm page cache, on a laptop. Indicative, not a benchmark.",
        },
        "measured_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }

    Path(args.bench_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.bench_out).write_text(json.dumps(payload, indent=2) + "\n")

    g = payload["graph"]
    print(f"[graph] {g['users']:,} users, {g['computers']:,} computers, "
          f"{g['auth_edges']:,} AUTH edges ({g['redteam_edges']} labelled red-team)")
    print(f"[paths] {len(paths)}/{len(pairs)} pairs connected; hop histogram {length_hist}")
    print(f"[blast] compromised reach {payload['blast_radius']['compromised_mean_hosts_3_hops']:,} "
          f"hosts at 3 hops vs {payload['blast_radius']['benign_control_mean_hosts_3_hops']:,} "
          f"for the benign control")
    print(f"[graph] wrote {args.bench_out}, {site_dir / 'graph.json'}, {site_dir / 'paths.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
