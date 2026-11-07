# 00. Life of a query

> Data in this chapter comes from `psql` against a real three-node cluster on
> one laptop (macOS, Apple M3 Pro), loaded with the four-table dataset of
> chapter 06, with `STOWAGE_PGWIRE_TRACE=1` on node 2. Every other chapter
> zooms into one box of the picture below; this one follows a single query
> through all of them.

```sql
SELECT r.name, c.tier, count(*) AS orders, sum(o.qty * p.price) AS revenue
FROM orders o JOIN customers c ON o.customer_id = c.id
              JOIN products p ON p.id = o.product_id
              JOIN regions r ON r.id = c.region_id
WHERE o.status = 'paid' AND r.name = 'emea'
GROUP BY r.name, c.tier ORDER BY revenue DESC;
```

## The path

```text
 psql ── TCP ──► n2:5482  pgwire          frame 'Q', 300 bytes                       chapter 07
                  │
                  ▼
                 Session: autocommit → begin an SSI transaction (read_ts = HLC now)   chapter 05
                  │
                  ├─ parse    sqlparser → AST
                  ├─ bind     catalog reads: 4 × Get 0x01 "table/…" ──┐              chapter 06
                  ├─ optimize pushdown · join order (DP) · access paths│
                  ├─ execute  4 × Scan 0x02 ‖ table id ‖ 1 … ──────────┤
                  │           hash joins, hash aggregate, sort (Arrow) │
                  └─ commit   read-only: nothing to resolve            │
                                                                       ▼
                 KvClient ── TCP ──► n1:7001 (leader of r1, the only range)            chapter 04
                                      │ TxnGet / TxnScan at read_ts
                                      │ timestamp cache records the read                chapter 05
                                      │ ReadIndex: one heartbeat round confirms n1 still leads   chapter 03
                                      │ wait until applied ≥ the read's barrier
                                      ▼
                                     MVCC scan of the shared B+Tree                     chapters 01–02
                                     newest version ≤ read_ts per key, intents checked
```

## Where the time goes

Eleven runs of the query from `psql` on node 2 (the commit column also includes the other statements in the same log). The SQL gateway is n2, while the
data's range leader is n1:

| Stage | Median | Min | Max | What it does |
|---|--:|--:|--:|---|
| begin | 0.003 ms | 0.002 | 0.007 | a transaction id and an HLC timestamp, no I/O |
| bind | 0.861 ms | 0.724 | 1.379 | 4 catalog `Get`s, each a round trip to n1 and a ReadIndex confirmation there |
| optimize | 0.186 ms | 0.170 | 0.209 | 15 subsets of 4 relations, estimates from `ANALYZE` stats |
| execute | 1.916 ms | 1.717 | 2.310 | 4 scans (1 000 + 200 + 50 + 5 = 1 255 rows read, 603 + 200 + 50 + 1 kept), 3 hash joins, aggregation, sort |
| commit | 0.119 ms | 0.003 | 17.1 | read-only: no record, no intents; the 17 ms outlier is the `ANALYZE` that wrote statistics |

One query as the server saw it (the `pg` trace, node 2):

```text
07:30:07.506214  pg ► Q len 300  SELECT r.name, c.tier, count(*) AS orders, sum(o.qty * p.price) …
07:30:07.509588  pg statement timings: begin 0.003 ms · bind 1.016 ms · optimize 0.183 ms · execute 1.938 ms · commit 0.117 ms
07:30:07.509619  pg ◄ T len 103  name · tier · orders · revenue
07:30:07.509628  pg ◄ D len 50   emea · gold · 64 · 15917.070000000003
07:30:07.509633  pg ◄ D len 52   emea · bronze · 60 · 13781.749999999998
07:30:07.509638  pg ◄ D len 52   emea · silver · 10 · 3470.9900000000007
07:30:07.509642  pg ◄ C len 13   SELECT 3
07:30:07.509658  pg ◄ Z len 5    I
```

The server takes 3.4 ms from `Q` in to `Z` out. Two thirds of that is eight
sequential key-value round trips to the leader on another node: four catalog
reads and four scans. That points at the next optimizations, in order of payoff:

1. **Cache descriptors** (CockroachDB leases them) and save the four catalog
   reads per statement.
2. **Issue independent scans concurrently.** Joins wait for both inputs, but the
   four scans do not depend on each other.
3. **Push filters to the leader** (DistSQL). Today the `status = 'paid'` filter
   runs at the gateway after 1 000 orders crossed the network; at the leader
   only 603 would.

Writes are different. Each one is a Raft round trip with an fsync on two of
three nodes. On macOS's `F_FULLFSYNC` that costs 5–10 ms, so an `INSERT` takes
25–35 ms ([chapter 07 §7.4](07-pgwire-on-the-wire.md#74-pgjdbc-frame-by-frame)),
and Linux with `fdatasync` is several times faster
([benchmarks](../benchmarks.md)).

## Where each box is explained

| Box | Chapter |
|---|---|
| Frames on the wire, prepared statements, SQLSTATEs | [07 pgwire on the wire](07-pgwire-on-the-wire.md) |
| Parse, bind, optimize, execute; keys for rows and indexes | [06 Planning a query](06-planning-a-query.md) |
| Snapshot reads, timestamp cache, intents, commit | [05 A transfer transaction](05-a-transfer-transaction.md) |
| Ranges, routing, leaders | [04 A range splits](04-a-range-splits.md) |
| ReadIndex, replication, elections | [03 One write through Raft](03-one-write-through-raft.md) |
| The B+Tree, its pages and its WAL | [01](01-a-page-on-disk.md), [02](02-crash-and-recover.md) |
| How all of it is tested | [A Deterministic simulation](A-deterministic-simulation.md) |
