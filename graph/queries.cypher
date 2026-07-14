// Tarn — Stage 4. Privilege-path queries over the identity->computer authentication graph.
//
// The graph:  (:User)-[:AUTH {count, first_seen, last_seen, success_ratio, is_redteam}]->(:Computer)
//
// The central idea: two identities are LINKED when they share a host. An attacker holding
// U1 who notices that U1 and U2 both authenticate to C7 has a candidate route from U1 to
// everything U2 can reach. Chain those hops and you have a path to privilege. Every query
// below is a variation on walking that alternating User -> Computer <- User structure.
//
// Run:  cat graph/queries.cypher | docker compose exec -T neo4j cypher-shell -u neo4j -p tarnlocal1


// ---------------------------------------------------------------------------------------
// Q-G1 — THE PATHS-TO-PRIVILEGE QUERY.
// "Given a compromised identity, what is the shortest authentication path from it to a
//  high-value identity, and which hosts does that path pivot through?"
//
// Undirected traversal is correct here even though AUTH is a directed edge: an attacker on
// a shared host does not care which direction the original authentication went — the host
// is a meeting point either way. Bounding at 6 hops (3 user-to-user hops) keeps it from
// exploring the whole graph; anything longer is not an attack path, it is a rumour.
// ---------------------------------------------------------------------------------------
MATCH (attacker:User {name: $from_user}), (target:User {name: $to_user})
MATCH path = shortestPath((attacker)-[:AUTH*..6]-(target))
RETURN
  [n IN nodes(path) | coalesce(n.name, '?')]                    AS hops,
  length(path)                                                  AS hop_count,
  [n IN nodes(path) WHERE n:Computer | n.name]                  AS pivot_hosts,
  [r IN relationships(path) | r.count]                          AS edge_weights,
  any(r IN relationships(path) WHERE r.is_redteam)              AS traverses_redteam_edge;


// ---------------------------------------------------------------------------------------
// Q-G2 — BLAST RADIUS.
// "If this identity is compromised, what can be reached from it within k hops, and how does
//  that compare to a normal user?"
//
// Reported as a curve (1..k hops), not a single number, because the shape is the finding:
// a workstation user's reachable set saturates quickly, while an account that touches a
// shared server explodes at hop 2. That explosion IS the privilege risk.
//
// NOTE ON WHAT hop3_hosts COUNTS. Cypher enforces relationship uniqueness within a path, so
// an edge cannot be traversed twice: the walk start -> C -> peer -> C is not a path, and C
// is therefore excluded from the 3-hop set. That is the semantics we want — hop3 means
// "hosts reachable THROUGH a peer", not "every host seen along the way". Hosts the identity
// already touches directly are counted at hop 1 and would otherwise be double-counted here,
// silently inflating the blast radius.
// ---------------------------------------------------------------------------------------
MATCH (start:User {name: $user})
CALL (start) {
  MATCH (start)-[:AUTH]->(c1:Computer)
  RETURN count(DISTINCT c1) AS hop1_hosts, collect(DISTINCT c1.name)[..25] AS hop1_sample
}
CALL (start) {
  MATCH (start)-[:AUTH]->(:Computer)<-[:AUTH]-(peer:User)
  WHERE peer <> start
  RETURN count(DISTINCT peer) AS hop2_peers
}
CALL (start) {
  MATCH (start)-[:AUTH]->(:Computer)<-[:AUTH]-(peer:User)-[:AUTH]->(c2:Computer)
  WHERE peer <> start
  RETURN count(DISTINCT c2) AS hop3_hosts
}
RETURN
  start.name              AS identity,
  start.is_compromised    AS is_compromised,
  hop1_hosts              AS hosts_directly_reachable,
  hop2_peers              AS identities_sharing_a_host,
  hop3_hosts              AS hosts_reachable_at_3_hops,
  hop1_sample             AS sample_of_direct_hosts;


// ---------------------------------------------------------------------------------------
// Q-G3 — THE RED TEAM'S ACTUAL MOVEMENT SUBGRAPH.
// "What did the attacker really touch, and how much of it was new?"
//
// This is the query the whole dataset exists for. It returns only the edges LANL labelled
// as compromise events, so it is ground truth, not inference.
// ---------------------------------------------------------------------------------------
MATCH (u:User {is_compromised: true})-[a:AUTH {is_redteam: true}]->(c:Computer)
RETURN
  u.name          AS attacker_identity,
  c.name          AS compromised_host,
  a.count         AS auth_events_on_edge,
  a.success_ratio AS success_ratio,
  a.first_seen    AS first_seen_seconds
ORDER BY a.first_seen;


// ---------------------------------------------------------------------------------------
// Q-G4 — THE ATTACKER vs. ITS OWN BASELINE.
// "Did the compromised accounts behave differently from how they normally behave, and from
//  how everyone else behaves?"
//
// The honest comparison. For each compromised identity: how many hosts it reached on
// red-team edges vs. how many it reaches in total (its benign footprint). An account whose
// red-team fan-out is a small fraction of its normal fan-out is one a fan-out detector was
// never going to catch — and saying so is the point of Q5 in the warehouse.
// ---------------------------------------------------------------------------------------
MATCH (u:User {is_compromised: true})
OPTIONAL MATCH (u)-[rt:AUTH {is_redteam: true}]->(rtc:Computer)
WITH u, count(DISTINCT rtc) AS redteam_hosts
MATCH (u)-[:AUTH]->(all:Computer)
WITH u, redteam_hosts, count(DISTINCT all) AS total_hosts
RETURN
  u.name                                              AS identity,
  redteam_hosts                                       AS hosts_via_redteam_edges,
  total_hosts                                         AS hosts_total_footprint,
  round(100.0 * redteam_hosts / total_hosts, 1)       AS pct_of_footprint_that_was_attack
ORDER BY redteam_hosts DESC
LIMIT 25;


// ---------------------------------------------------------------------------------------
// Q-G5 — CHOKE POINTS.
// "Which hosts, if hardened, would break the most privilege paths?"
//
// Degree centrality as a defensive prioritisation: hosts touched by many distinct
// identities are the joints the whole path graph articulates around. This is the query that
// turns the analysis into an action item.
// ---------------------------------------------------------------------------------------
MATCH (u:User)-[:AUTH]->(c:Computer)
WITH c, count(DISTINCT u) AS distinct_identities
WHERE distinct_identities > 1
RETURN
  c.name                  AS host,
  distinct_identities     AS identities_authenticating_here,
  c.is_redteam_pivot      AS was_redteam_pivot,
  c.is_redteam_target     AS was_redteam_target
ORDER BY distinct_identities DESC
LIMIT 25;
