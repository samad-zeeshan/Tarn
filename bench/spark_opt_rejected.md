# The optimization that didn't work

DIRECTIVE rule 1 says every number is measured. This file exists because the corollary is that
**measurements that came out badly still count**. The first optimization attempted here lost,
and deleting it would leave `spark_opt.json` looking like the answer was obvious.

## The hypothesis

The Stage-1 rollup computes two `COUNT(DISTINCT ...)` inside a single `groupBy`:

```python
daily = base.groupBy("src_user", "event_date").agg(
    F.count("*").alias("auth_count"),
    ...
    F.countDistinct("dst_computer").alias("distinct_dst_computers"),
    F.countDistinct("src_computer").alias("distinct_src_computers"),
)
```

Spark plans multiple distinct aggregations with an **`Expand`** operator: every input row is
replayed once per distinct expression before it can shuffle, so the shuffle carries a multiple
of the input. On ~114M rows that is a lot of extra rows crossing the network. The textbook fix
is to collapse to the distinct grain first and then count over it.

## The measurement

Slice: **113,699,303 rows**, 7 dates (2015-01-01 → 2015-01-07). Median of 5 timed runs after
1 warm-up, all four variants in one Spark session, `spark.sql.shuffle.partitions=24`.

| variant | median | vs baseline |
|---|---|---|
| baseline — two scans, Expand, SortMergeJoin | **40.52 s** | — |
| broadcast join only | 61.14 s | **1.51× slower** |
| two-stage distinct aggregation only | 52.64 s | **1.30× slower** |
| both | 50.29 s | **1.24× slower** |

Raw run timings:

```
baseline        [36.415, 36.272, 40.522, 41.612, 57.333]
broadcast_only  [65.432, 57.233, 61.135, 63.504, 57.512]
dedup_only      [58.156, 52.597, 52.636, 53.191, 50.741]
both            [49.258, 50.709, 50.678, 50.294, 48.821]
```

**Every variant was slower than doing nothing.** The harness printed
`NO WIN: baseline was fastest. Recorded honestly; do not claim a speedup.` and exited.

## Why it lost

Collapsing to the distinct grain **adds a shuffle**. It buys a cheaper wide shuffle (no Expand
row multiplication) and then immediately spends the savings on the extra one. Net: worse.

Meanwhile the actual bottleneck was sitting in plain sight and had nothing to do with `Expand`.
The rollup scanned the event set **twice** — once for the daily aggregation, and once more
inside `first_seen_destinations()` to compute new-access-path counts. Two full scans of 114M
rows, two wide shuffles of the same data.

## What worked instead

Not "dedupe first" but **share the grain**. Both consumers only ever need the distinct
`(identity, date, dst_computer, src_computer)` tuples:

- `daily` groups that grain by `(identity, date)`
- `first_seen` groups the **same** grain by `(identity, dst_computer)` taking `min(date)`

So materialize the grain once, `persist(MEMORY_AND_DISK)`, and let both read it — one scan and
one wide shuffle where there were two of each. Eliminating the `Expand` then comes along for
free, because `COUNT(DISTINCT)` over an already-deduplicated grain is a plain count.

Result: **50.70 s → 39.38 s, 1.29× (22.3% faster)**, output verified identical. See
[`spark_opt.json`](spark_opt.json).

Spark cannot do this rewrite on its own: it has no way to know that two separately-expressed
aggregations are both reachable from a common grain. That is what makes it a genuine
software-level change rather than a config flag.

## The lesson worth keeping

Also worth recording: **the same baseline code measured 40.52 s in this run and 50.70 s in the
run that produced `spark_opt.json`** — ~25% drift between Spark sessions from JIT warmth, OS
page cache, and GC state. That is precisely why the final benchmark times all four variants
back-to-back in a single session and reports a within-session ratio. Cross-run absolute
comparisons on a laptop are not meaningful, and any project quoting one is not measuring what
it thinks it is.
