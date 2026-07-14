# Tarn

A small data platform for identity security, built on a billion real authentication events.

Raw events go through Spark, into a DuckDB warehouse, through a streaming job, and into a Neo4j
graph. The demo site runs real SQL in your browser.

### [Open the demo](https://samad-zeeshan.github.io/Tarn/)

---

## What this is

The data is LANL's authentication log: 58 days of real (anonymized) login activity from Los
Alamos National Laboratory. About a billion events.

The important part is that it comes with an answer key. A red team ran an attack during those
58 days, and LANL published the list of exactly which 749 events were the attack.

So every part of this project can ask a better question than "how many rows are there". It can
ask: **would this have caught the attacker?**

Mostly, the answer is no. That is written down below, and on the site.

---

## The pipeline

```
LANL auth events        1,051,430,459 rows, 58 days, real and anonymized
        |
        v
PySpark                 parse into a date-partitioned Parquet lake
                        build per-identity daily rollups
                        one optimization, measured properly
        |
        v
dbt + DuckDB            star schema: a fact table plus identity, computer and time dimensions
                        five analytical queries with their results committed
        |
        v
Spark Streaming         replay events through Redpanda (Kafka)
                        1-minute windows per user, watermarked, checkpointed
        |
        v
Neo4j                   who can reach whom, and how fast an attacker spreads
        |
        v
Vector search           each person-day as 52 numbers, indexed for nearest neighbours
                        "find me more days that look like this one"
        |
        v
Demo site               DuckDB compiled to WebAssembly. The SQL is really running,
                        and so is the vector search.
```

---

## The numbers

Everything here was measured. Nothing is estimated or rounded up. The machine it ran on is in
`bench/env.json`.

| | |
|---|---|
| Authentication events | **1,051,430,459** (counted, not quoted from the paper) |
| Bad rows | 0 |
| Time span | 58 days |
| The attack | 749 labelled events, 104 compromised accounts, 4 launch hosts |
| Building the lake | 1,343 seconds for all 1.05 billion rows |
| Daily rollups | 1,604,500 identity-days across 80,553 accounts |
| Spark optimization | 50.70s to 39.38s, **1.29x faster**, output verified identical |
| Streaming | 7,139 events/sec sustained, lag p50 12.5s, p95 15.6s |
| Graph | 72,751 accounts, 14,540 hosts, 600,145 edges |
| Warehouse tests | 54 dbt checks, all passing, every key checked across all 1.05 billion rows |
| Vector index | 1,604,500 person-days as 52 numbers each, searchable in 0.005 ms |

Each number has an artifact behind it in `bench/`.

---

## Would the analytics have caught the attacker?

This is the question the whole warehouse exists to answer, and the honest answer is not
flattering.

Four detectors were run across all 1,604,500 identity-days. Of those, 181 were days when an
account was genuinely compromised. That is a base rate of about 0.011%.

| detector | caught | missed | recall | alerts to triage | precision | lift vs random |
|---|---|---|---|---|---|---|
| Q3: new access paths | 56 | 125 | 30.9% | 33,319 | 0.168% | 14.9x |
| Q1: fan-out spike | 20 | 161 | 11.0% | 21,136 | 0.095% | 8.4x |
| Q4: failure spike | 11 | 170 | 6.1% | 5,752 | 0.191% | 17.0x |
| **Q2: off-hours** | **0** | **181** | **0.0%** | 6,379 | 0.000% | **0.0x** |
| Any of the four | 62 | 119 | 34.3% | 63,891 | 0.097% | 8.6x |
| **Two or more of the four** | 22 | 159 | 12.2% | **2,599** | **0.846%** | **75.0x** |

Three things worth saying plainly:

**Q2 caught nothing. Zero out of 181.** The off-hours detector is the piece of this project that
took the most care to build honestly. It measures the network's actual daily rhythm instead of
assuming office hours. And it turned out to be useless against this attacker. That is the
result. It is in the table.

**Even the best combination misses two thirds of the attack.** "Any of the four" catches 34% and
raises 63,891 alerts doing it. A detector with decent recall and sixty thousand alerts is not a
detector. It is a denial of service attack on whoever has to read them.

**The only usable setting is the strictest one.** Requiring two independent signals cuts alerts
from 63,891 down to 2,599 and makes each alert 75 times more likely to be real. The cost is
recall dropping to 12%. That trade-off is the actual job, and here it is measured instead of
asserted.

---

## Vector search: bad at finding the attack, very good at growing it

Every person-day is turned into 52 numbers describing how that person behaved. How much they
did, how often they failed, what hours they were active, what kinds of login they used. Similar
behaviour ends up with similar numbers, so you can ask for the nearest matches, the way an image
search finds similar photos.

Nothing from the answer key goes into those numbers. There is a test that fails if it ever does.

**As a detector on its own, it lost.** Told to go and find unusual behaviour with no other help,
and given the exact same number of people to accuse that each simple rule was given:

| same budget as | people accused | the rule caught | vector search caught | winner |
|---|---|---|---|---|
| any of the four | 63,891 | 62 | 10 | the rule |
| new access paths | 33,319 | 56 | 5 | the rule |
| two or more | 2,599 | 22 | 1 | the rule |
| sudden fan-out | 21,136 | 20 | 3 | the rule |
| failed logins | 5,752 | 11 | 3 | the rule |
| odd hours | 6,379 | 0 | 3 | vector search |

It loses five out of six, and the one it wins is against the rule that caught nothing at all.
Being unusual and being an attacker are not the same thing. Most unusual days are just somebody
having an unusual day.

**As a way of growing an attack you have already found, it is excellent.** Hand it one confirmed
bad day and ask for lookalikes, and **4.59% of what comes back is also part of the attack. That
is 406 times better than picking days at random.** 45 of the 181 attack days surface at least one
other.

It cannot find the first one for you. It can find the rest. That is a hunting tool, not a
detector, and the difference matters enough to be measured rather than blurred.

The index is built on a single thread on purpose. Building it in parallel made the same input
give 4.48% on one run and 4.64% on the next, and a number that changes when nothing changed is
not a measurement.

You can run the search yourself on the demo. It executes in your browser in about 30
milliseconds.

---

## Four things I got wrong

A project that only shows the parts that worked is not showing you engineering. These are all in
the git history.

**1. The fact table nearly ate 134 GB.**

I built it the textbook way, with four md5 hash keys on every row. Then I did the arithmetic. An
md5 is a 32-character string and it is unique per row, so it does not compress. Four of them,
times a billion rows, is about 134 GB of pure key data. To duplicate a 9.9 GB Parquet lake that
was already sitting on the same disk.

I killed the build at 35 GB. The fact table is now a view over the Parquet, which is what a
lakehouse is supposed to be. The dimensions, which are small and read constantly, are still real
tables. Every foreign key is still tested across all 1.05 billion rows.

**2. The first optimization made things slower.**

The obvious bottleneck in the rollup is the `Expand` step Spark uses behind two
`COUNT(DISTINCT)` calls in one `groupBy`. So I rewrote it the textbook way. It was 30% slower.
Collapsing to a distinct grain adds a shuffle, and it spent the savings paying for it.

The real bottleneck was duller. The job was reading the events twice, once for the daily counts
and once to work out first-seen destinations. The fix was to build the shared grain once and let
both steps read it. That won: 50.70s down to 39.38s.

The failed attempt is still in `bench/spark_opt_rejected.json`, with its numbers.

**3. "Off-hours" nearly became fiction.**

LANL does not publish clock times. It publishes seconds since the log started. So "this login
happened at 3am" is not something you can read out of the data.

My first attempt to find a quiet period found an almost flat curve and refused to produce an
answer. That refusal was correct. Machine accounts (backup agents, service accounts) log in
around the clock, and they are most of the traffic. They were flattening the curve.

Separate them out and the human pattern is obvious: a clear dip overnight, a peak mid-morning.
The quiet band the warehouse uses comes from that measurement. It is never a hard-coded nine to
five.

And then, as the table above shows, it caught nothing anyway.

**4. Blast radius turned out to be a meaningless number.**

The graph can tell you how many machines a compromised account can reach. The answer is 14,256.

That sounds alarming until you run the same query on an ordinary account, which reaches 14,253.
Out of 14,540 machines total. Both of them reach basically the entire network.

The reason shows up in the choke points. One host, C1065, is logged into by 28,483 different
accounts. Hubs that big collapse the whole network into a small world where everyone is two hops
from everyone.

So "this attacker can reach 14,256 machines" is completely true and completely useless. The
export script now works this out for itself and writes the warning into the artifact, so the
number cannot be quoted on its own. The finding that actually matters is the choke points:
harden those few hubs and you break the paths.

---

## Running it

Spark runs in Docker, not on the host. The host here has Python 3.14 and Java 25, and PySpark
supports neither.

```bash
docker compose up -d      # Spark, dbt, DuckDB, Redpanda, Neo4j
make test                 # 26 tests across every stage
```

That works out of the box, because a small slice of the data (about 100,000 events, chosen with
a fixed seed) is committed to the repo. It is what CI runs on every push.

To rebuild from the full corpus you need the data. LANL puts it behind a form at
<https://csr.lanl.gov/data/cyber1/>, which gives you a download token.

```bash
export TARN_LANL_TOKEN='your token'
make fetch                # 7.2 GB
make describe             # count it
make sample               # cut the small slice

make lake                 # a billion rows into Parquet
make rollup
make bench                # the optimization benchmark
make warehouse            # dbt build, tests, the five queries
make stream               # Kafka replay and the streaming job
make graph                # load Neo4j, run the Cypher
make vectors              # embed every person-day, index it, score the search
make site                 # build the demo, then audit it
```

---

## What this is not

- **Not a production system.** It runs on one laptop, plus one run on a free Databricks
  workspace. It has never seen a billion events a day. It has seen a billion events, once, in
  batch.
- **The streaming numbers come from a replay**, from a file, into a Kafka broker on the same
  laptop as the consumer. No network, no cluster, nothing else competing for the machine. They
  measure this pipeline, not a production one.
- **The data is from 2015 and anonymized.** Users and machines are pseudonyms like `U292@DOM1`
  and `C1065`.
- **The dates are made up.** Not the events, the calendar. LANL only gives seconds since the log
  started, so the pipeline picks an arbitrary start date to have something to partition by. Day
  of week would be meaningless, so nothing in the project uses it.
- **Only the SQL on the demo is live.** The Spark benchmark, the streaming lag and the graph
  timings are recorded evidence, replayed. The site says so, on the page.

---

## Layout

```
data/fetch.py        download the data, check it, cut the small committed slice
pipeline/            Spark: parse, sessionize, roll up, benchmark
pipeline/databricks/ the same rollup as a Databricks notebook
warehouse/           dbt star schema, five queries, committed results
streaming/           Kafka replay, the streaming job, the lag measurement
graph/               Neo4j loader, the Cypher, the exports
vector/              the nearest-neighbour index and its honest scoring
bench/               every measured number, with the machine it ran on
site/                the demo
tests/               33 tests
```

---

## Data

A. D. Kent, *Comprehensive, Multi-Source Cyber-Security Events*, Los Alamos National Laboratory
(2015). Public domain (CC0). <https://csr.lanl.gov/data/cyber1/>
