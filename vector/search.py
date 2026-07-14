"""
Nearest-neighbour search over the person-day vectors, and an honest score for what it is worth.

Two experiments, because they answer different questions and only one of them is a detector.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import faiss
import numpy as np

REPO = Path(__file__).resolve().parent.parent


def load_vectors(vectors_path: str, rollup_path: str):
    """Read the vectors and attach the answer key, which is used to SCORE and never to search."""
    con = duckdb.connect()
    rows = con.execute(
        f"""
        select v.src_user, v.event_date, v.vector, coalesce(r.is_redteam_day, false) as is_attack
        from read_parquet('{vectors_path}/**/*.parquet') v
        left join read_parquet('{rollup_path}/**/*.parquet', hive_partitioning=1) r
          on v.src_user = r.src_user and v.event_date = r.event_date
        order by v.src_user, v.event_date
        """
    ).arrow()
    con.close()

    ids = np.arange(rows.num_rows, dtype=np.int64)
    users = rows.column("src_user").to_pylist()
    dates = [str(d) for d in rows.column("event_date").to_pylist()]
    labels = np.array(rows.column("is_attack").to_pylist(), dtype=bool)

    mat = np.vstack([np.asarray(v, dtype=np.float32) for v in rows.column("vector").to_pylist()])
    return ids, users, dates, labels, np.ascontiguousarray(mat)


def build_index(mat: np.ndarray, m: int = 32, ef_construction: int = 80, ef_search: int = 64):
    """An HNSW index. Exact search over this many vectors is not affordable."""
    index = faiss.IndexHNSWFlat(mat.shape[1], m)
    index.hnsw.efConstruction = ef_construction
    index.hnsw.efSearch = ef_search

    # Built on ONE thread, on purpose. HNSW inserts in parallel by default, and the graph it ends
    # up with depends on the order the threads got there, so the same input produced 4.48% on one
    # run and 4.64% on the next. That is a small difference and it would have been easy to shrug
    # at, but a number that changes when nothing changed is not a measurement. Single threaded
    # insertion costs about three minutes here and makes the result reproducible.
    #
    # Only the build is serialised. Searching a finished graph is deterministic either way, so
    # the threads come straight back afterwards.
    threads = faiss.omp_get_max_threads()
    faiss.omp_set_num_threads(1)
    try:
        index.add(mat)
    finally:
        faiss.omp_set_num_threads(threads)
    return index


def anomaly_scores(index, mat: np.ndarray, k: int) -> np.ndarray:
    """How far is each person-day from the crowd?

    The score is the mean distance to its k nearest neighbours, with itself dropped. A day that
    sits in a dense cluster of ordinary behaviour scores low. A day that sits on its own scores
    high. This uses no labels at all, which is what makes it a fair detector rather than a
    lookup.
    """
    dist, _ = index.search(mat, k + 1)
    # Column 0 is the point itself at distance zero, so it goes.
    return dist[:, 1:].mean(axis=1)


def score_as_detector(scores: np.ndarray, labels: np.ndarray, budgets: dict[str, dict]) -> list[dict]:
    """Score the anomaly ranking at the SAME alert budgets the other warning signs spent.

    Any ranking can be made to look good by picking a threshold after seeing the answers. So no
    threshold is picked here. For each existing warning sign, take the exact number of alerts it
    raised, give that budget to the vector ranking, and ask what it catches for the money. Same
    budget, same population, head to head.
    """
    order = np.argsort(-scores)
    total_attack = int(labels.sum())
    n = len(labels)
    base_rate = total_attack / n

    out = []
    for name, spec in budgets.items():
        budget = int(spec["alerts"])

        # A budget bigger than the population is not a budget, it is flagging everybody. It can
        # only happen on the small test slice, and it must not be reported as a result.
        if budget >= n:
            out.append({"matched_against": name, "alerts": budget, "skipped":
                        "budget exceeds the population, so this comparison is meaningless here"})
            continue

        caught = int(labels[order[:budget]].sum())
        precision = caught / budget if budget else 0.0
        theirs = int(spec["caught"])
        out.append(
            {
                "matched_against": name,
                "alerts": budget,
                "the_rule_caught": theirs,
                "vector_search_caught": caught,
                "winner": "the simple rule" if theirs > caught
                          else ("vector search" if caught > theirs else "tie"),
                "missed": total_attack - caught,
                "recall_pct": round(100 * caught / total_attack, 1) if total_attack else 0.0,
                "precision_pct": round(100 * precision, 3),
                "lift_over_random": round(precision / base_rate, 1) if base_rate else None,
            }
        )
    return out


def leave_one_out_retrieval(index, mat, labels, k: int) -> dict:
    """Given one confirmed attack day, does the index surface the others?

    This is NOT a detector. It cannot start without already knowing about one attack, so it
    cannot find the first one. It is a threat-hunting question: an analyst confirms a single bad
    day and asks the index to show them everything that looks like it.

    Each attack day queries the index, its own row is removed from the answer, and we count how
    many of the k it returns are also attack days.
    """
    attack_idx = np.flatnonzero(labels)
    if len(attack_idx) == 0:
        return {}

    dist, idx = index.search(mat[attack_idx], k + 1)

    hits = []
    for row, self_id in zip(idx, attack_idx, strict=True):
        neighbours = [i for i in row if i != self_id][:k]
        hits.append(int(labels[neighbours].sum()))

    hits = np.array(hits)
    base_rate = labels.sum() / len(labels)
    precision = hits.mean() / k

    return {
        "k": k,
        "queries": int(len(attack_idx)),
        "mean_attack_days_in_top_k": round(float(hits.mean()), 2),
        "precision_at_k_pct": round(100 * float(precision), 2),
        "queries_that_surfaced_at_least_one": int((hits > 0).sum()),
        "queries_that_surfaced_nothing": int((hits == 0).sum()),
        "base_rate_pct": round(100 * float(base_rate), 4),
        "lift_over_random": round(float(precision / base_rate), 1) if base_rate else None,
        "self_excluded": True,
    }


def load_alert_budgets() -> dict[str, dict]:
    """Read what each existing warning sign spent, and what it caught for the money."""
    # Read from the committed results rather than typed in here. A number copied by hand drifts
    # the first time the query is re-run, and the whole comparison rests on these being the
    # budgets those signs really spent.
    import csv

    path = REPO / "warehouse" / "queries" / "results" / "q5_redteam_enrichment.csv"
    out: dict[str, dict] = {}
    with path.open() as fh:
        for row in csv.DictReader(fh):
            alerts = int(float(row["alerts_raised"]))
            if alerts <= 0:
                continue
            out[row["detector"]] = {
                "alerts": alerts,
                "caught": int(float(row["redteam_days_caught"])),
            }
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--vectors", default="/data/lake/vectors")
    ap.add_argument("--rollup", default="/data/lake/rollup")
    ap.add_argument("--scores-out", default="/data/lake/vector_scores")
    ap.add_argument("--out", default="bench/vector_eval.json")
    ap.add_argument("--k", type=int, default=20)
    args = ap.parse_args()

    print("[vector] loading")
    t0 = time.perf_counter()
    ids, users, dates, labels, mat = load_vectors(args.vectors, args.rollup)
    load_s = round(time.perf_counter() - t0, 1)
    print(f"[vector] {len(ids):,} person-days, {mat.shape[1]} dimensions, "
          f"{int(labels.sum())} of them attack days ({load_s}s)")

    t0 = time.perf_counter()
    index = build_index(mat)
    build_s = round(time.perf_counter() - t0, 1)
    print(f"[vector] index built in {build_s}s")

    t0 = time.perf_counter()
    scores = anomaly_scores(index, mat, args.k)
    search_s = round(time.perf_counter() - t0, 1)
    print(f"[vector] {len(ids):,} nearest-neighbour searches in {search_s}s "
          f"({1000 * search_s / len(ids):.3f} ms each)")

    budgets = load_alert_budgets()
    detector = score_as_detector(scores, labels, budgets)
    retrieval = leave_one_out_retrieval(index, mat, labels, k=10)

    # Straight through Arrow. Passing 1.6M rows as SQL parameters would build the whole thing as
    # Python lists first and spend more memory on the handoff than on the index.
    import pyarrow as pa

    scored = pa.table(
        {
            "src_user": pa.array(users),
            "event_date": pa.array(dates),
            "anomaly_score": pa.array(scores.astype("float64")),
            "is_attack": pa.array(labels),
        }
    )
    con = duckdb.connect()
    con.register("scored", scored)
    con.execute(
        f"copy (select src_user, event_date::DATE as event_date, anomaly_score, is_attack "
        f"from scored) to '{args.scores_out}.parquet' (format parquet, compression zstd)"
    )
    con.close()

    payload = {
        "what": (
            "Nearest-neighbour search over one vector per person per day, and an honest account of "
            "what it is worth."
        ),
        "two_experiments": {
            "detector": (
                "Unsupervised. Rank every person-day by how far it sits from its neighbours, with "
                "no labels involved at all, and read the top of that ranking as alerts. This is "
                "directly comparable to the four warning signs, so it is scored at exactly the "
                "alert budgets those signs actually spent."
            ),
            "retrieval": (
                "Given one confirmed attack day, find the others that look like it. This is NOT a "
                "detector. It cannot start until somebody has already found the first bad day, so "
                "it can never find that one. It is a hunting tool, and it is scored as one."
            ),
        },
        "population": {
            "person_days": int(len(ids)),
            "attack_days": int(labels.sum()),
            "base_rate_pct": round(100 * float(labels.sum() / len(labels)), 4),
        },
        "index": {
            "library": f"faiss {faiss.__version__}",
            "type": "HNSW, 32 links per node",
            "deterministic": (
                "The graph is built on a single thread. Parallel insertion makes the result depend "
                "on thread timing, and the same input gave 4.48% and then 4.64% on two runs. "
                "Serialising the build costs about three minutes and makes the number repeatable."
            ),
            "dimensions": int(mat.shape[1]),
            "build_seconds": build_s,
            "search_seconds_all": search_s,
            "search_ms_each": round(1000 * search_s / len(ids), 3),
            "note": (
                "Approximate, not exact. Exact search over this many vectors would be about 2.5e12 "
                "distance computations. The index trades a little accuracy for that, which is the "
                "entire reason vector databases exist."
            ),
        },
        "detector_at_matched_alert_budgets": detector,
        "retrieval_from_one_known_attack_day": retrieval,
        "measured_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(payload, indent=2) + "\n")

    print()
    print("head to head, at the same alert budget:")
    print(f"  {'budget taken from':<44}{'alerts':>8}{'rule':>7}{'vectors':>9}   winner")
    for d in detector:
        if "skipped" in d:
            continue
        print(f"  {d['matched_against']:<44}{d['alerts']:>8,}{d['the_rule_caught']:>7}"
              f"{d['vector_search_caught']:>9}   {d['winner']}")
    print()
    if retrieval:
        print(f"as a hunting tool, from one known attack day (k={retrieval['k']}):")
        print(f"  {retrieval['precision_at_k_pct']}% of the neighbours it returns are also attack "
              f"days, which is {retrieval['lift_over_random']}x the base rate")
        print(f"  {retrieval['queries_that_surfaced_at_least_one']} of {retrieval['queries']} "
              f"attack days surfaced at least one other")
    print()
    print(f"[vector] wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
