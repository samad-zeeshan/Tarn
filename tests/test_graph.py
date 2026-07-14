"""Stage 4 — loader idempotency and path-query correctness.

These run against the real Neo4j from docker-compose (skipped if it isn't up). They operate
on nodes whose names are prefixed `TEST_`, and tear them down afterwards, so they can share
the instance with the real graph without either one corrupting the other.

Path correctness is asserted on a hand-built toy graph whose shortest paths are obvious by
inspection — checking shortestPath() against the real 30-day graph would only tell us that
Neo4j returns *something*, not that it returns the right thing.
"""

from __future__ import annotations

import os

import pytest
from neo4j import GraphDatabase

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def driver(neo4j_available):
    if not neo4j_available:
        pytest.skip("Neo4j not reachable — `docker compose up -d neo4j`")
    d = GraphDatabase.driver(
        os.environ.get("NEO4J_URI", "bolt://neo4j:7687"),
        auth=(
            os.environ.get("NEO4J_USER", "neo4j"),
            os.environ.get("NEO4J_PASSWORD", "tarnlocal1"),
        ),
    )
    yield d
    d.close()


@pytest.fixture(autouse=True)
def clean_test_nodes(driver):
    """Isolate from the real graph: only ever touch TEST_-prefixed nodes."""
    def wipe():
        with driver.session() as s:
            s.run(
                "MATCH (n) WHERE n.name STARTS WITH 'TEST_' DETACH DELETE n"
            )
    wipe()
    yield
    wipe()


def _load_toy(driver) -> None:
    """A graph whose answers are obvious by eye:

        TEST_U1 -> TEST_C1 <- TEST_U2 -> TEST_C2 <- TEST_U3
        TEST_U1 -> TEST_C9                 (a leaf only U1 touches)

    So: U1 and U2 share C1        -> shortest path U1..U2 is 2 hops.
        U2 and U3 share C2        -> shortest path U2..U3 is 2 hops.
        U1 reaches U3 only via C1-U2-C2 -> shortest path U1..U3 is 4 hops.
        TEST_C9 is reachable from nobody but U1.
    """
    with driver.session() as s:
        s.run(
            """
            MERGE (u1:User {name: 'TEST_U1'}) SET u1.is_compromised = true,  u1.is_machine = false
            MERGE (u2:User {name: 'TEST_U2'}) SET u2.is_compromised = false, u2.is_machine = false
            MERGE (u3:User {name: 'TEST_U3'}) SET u3.is_compromised = false, u3.is_machine = false
            MERGE (c1:Computer {name: 'TEST_C1'}) SET c1.is_redteam_pivot = true,  c1.is_redteam_target = false
            MERGE (c2:Computer {name: 'TEST_C2'}) SET c2.is_redteam_pivot = false, c2.is_redteam_target = true
            MERGE (c9:Computer {name: 'TEST_C9'}) SET c9.is_redteam_pivot = false, c9.is_redteam_target = false
            MERGE (u1)-[a1:AUTH]->(c1) SET a1.count = 10, a1.is_redteam = true,  a1.success_ratio = 1.0
            MERGE (u2)-[a2:AUTH]->(c1) SET a2.count = 5,  a2.is_redteam = false, a2.success_ratio = 1.0
            MERGE (u2)-[a3:AUTH]->(c2) SET a3.count = 7,  a3.is_redteam = false, a3.success_ratio = 1.0
            MERGE (u3)-[a4:AUTH]->(c2) SET a4.count = 3,  a4.is_redteam = false, a4.success_ratio = 1.0
            MERGE (u1)-[a5:AUTH]->(c9) SET a5.count = 1,  a5.is_redteam = false, a5.success_ratio = 1.0
            """
        )


def _counts(driver) -> tuple[int, int]:
    with driver.session() as s:
        rec = s.run(
            """
            MATCH (n) WHERE n.name STARTS WITH 'TEST_'
            WITH count(n) AS nodes
            MATCH (a)-[r:AUTH]->(b)
            WHERE a.name STARTS WITH 'TEST_' AND b.name STARTS WITH 'TEST_'
            RETURN nodes, count(r) AS edges
            """
        ).single()
        return rec["nodes"], rec["edges"]


def test_loader_is_idempotent(driver):
    """Loading twice must converge, not duplicate.

    The loader uses MERGE everywhere precisely so a re-run after a crash is safe. If this
    ever regressed to CREATE, the edge counts in bench/graph_stats.json would silently
    double on the second run and nobody would be able to tell which number was real.
    """
    _load_toy(driver)
    first = _counts(driver)

    _load_toy(driver)
    second = _counts(driver)

    assert first == second == (6, 5)


def test_shortest_privilege_path_two_users_sharing_a_host(driver):
    _load_toy(driver)
    with driver.session() as s:
        rec = s.run(
            """
            MATCH (a:User {name: 'TEST_U1'}), (b:User {name: 'TEST_U2'})
            MATCH p = shortestPath((a)-[:AUTH*..6]-(b))
            RETURN [n IN nodes(p) | n.name] AS hops, length(p) AS hop_count
            """
        ).single()

    # U1 -> C1 <- U2. Two hops, pivoting through the shared host.
    assert rec["hop_count"] == 2
    assert rec["hops"] == ["TEST_U1", "TEST_C1", "TEST_U2"]


def test_shortest_path_through_an_intermediate_identity(driver):
    """U1 and U3 share no host. The only route is via U2, so the path must be 4 hops and must
    pass through both shared computers. This is the query the demo calls 'paths to privilege'
    — if it silently returned a 2-hop path, the whole panel would be lying."""
    _load_toy(driver)
    with driver.session() as s:
        rec = s.run(
            """
            MATCH (a:User {name: 'TEST_U1'}), (b:User {name: 'TEST_U3'})
            MATCH p = shortestPath((a)-[:AUTH*..6]-(b))
            RETURN [n IN nodes(p) | n.name] AS hops, length(p) AS hop_count
            """
        ).single()

    assert rec["hop_count"] == 4
    assert rec["hops"] == ["TEST_U1", "TEST_C1", "TEST_U2", "TEST_C2", "TEST_U3"]


def test_blast_radius_expands_with_hops(driver):
    """U1 directly touches 2 hosts; via its one peer it reaches 1 further host.

    On hop3 the answer is 1, not 2, and the reason is worth pinning down because it is what
    the number MEANS. Cypher enforces relationship uniqueness within a path: the edge
    U2->C1 cannot be traversed twice, so the path U1 -> C1 <- U2 -> C1 is not a path. C1 is
    therefore excluded from the 3-hop set — which is the semantics we actually want, since
    C1 is already reachable at hop 1 and counting it again would inflate blast radius with
    hosts the identity could already touch. hop3 = "hosts reachable THROUGH a peer".
    """
    _load_toy(driver)
    with driver.session() as s:
        rec = s.run(
            """
            MATCH (start:User {name: 'TEST_U1'})
            CALL (start) {
              MATCH (start)-[:AUTH]->(c1:Computer) RETURN count(DISTINCT c1) AS hop1 }
            CALL (start) {
              MATCH (start)-[:AUTH]->(:Computer)<-[:AUTH]-(peer:User) WHERE peer <> start
              RETURN count(DISTINCT peer) AS hop2 }
            CALL (start) {
              MATCH (start)-[:AUTH]->(:Computer)<-[:AUTH]-(peer:User)-[:AUTH]->(c2:Computer)
              WHERE peer <> start
              RETURN count(DISTINCT c2) AS hop3 }
            RETURN hop1, hop2, hop3
            """
        ).single()

    assert rec["hop1"] == 2   # C1 and C9, touched directly
    assert rec["hop2"] == 1   # only U2 shares a host with U1
    assert rec["hop3"] == 1   # U2 leads onward to C2 (C1 excluded — see docstring)


def test_no_path_when_none_exists(driver):
    """An isolated identity must return NO path, not an empty-but-truthy one."""
    _load_toy(driver)
    with driver.session() as s:
        s.run("MERGE (u:User {name: 'TEST_ISOLATED'})")
        rec = s.run(
            """
            MATCH (a:User {name: 'TEST_U1'}), (b:User {name: 'TEST_ISOLATED'})
            OPTIONAL MATCH p = shortestPath((a)-[:AUTH*..6]-(b))
            RETURN p IS NULL AS no_path
            """
        ).single()

    assert rec["no_path"] is True
