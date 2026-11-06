# Architecture

Stowage is a distributed SQL + NoSQL database. A single ordered, transactional,
Raft-replicated key space sits underneath two front ends: a Postgres-protocol SQL
layer and a DynamoDB-compatible document API. See
[ADR 0003](adr/0003-layered-cp-architecture.md) for why it is layered this way and
why it is CP.

```
 psql / JDBC                  AWS SDK (DynamoDB API)            stowctl / Console
     │ pgwire                       │ HTTP JSON                       │ gRPC / REST
     ▼                              ▼                                 ▼
┌──────────────────────── stowd (every node: gateway + replica) ──────────────────────┐
│ SQL layer       sqlparser → binder → logical plan → Cascades-lite CBO → Arrow exec  │
│ Document layer  condition expressions, TransactWriteItems, GSI, streams             │
│ Catalog         online schema change (delete-only → write-only → backfill → public) │
├──────────────────────────────────────────────────────────────────────────────────────┤
│ Transactions    HLC timestamps · MVCC · write intents · txn records · 2PC · SI/SSI   │
├──────────────────────────────────────────────────────────────────────────────────────┤
│ Distributed KV  ranges · Multi-Raft · split/merge · leases · follower reads          │
├──────────────────────────────────────────────────────────────────────────────────────┤
│ Storage engine  B+Tree + buffer pool + WAL (LSM adapter for comparison)             │
└──────────────────────────────────────────────────────────────────────────────────────┘
        ▲ heartbeats, placement                                   │ changefeeds
   stow-pd ×3 (a Raft group: range placement and rebalancing)      ▼  stdout / Kafka / S3
```

## Crates

| Crate | Role | Since |
|---|---|---|
| `stowage-common` | environment traits (`Clock`, `Rng`, `Disk`, `Transport`), HLC, errors, OS implementations | M0 |
| `stowage-sim` | deterministic single-threaded cluster simulator | M0 |
| `stowd` | the node binary | M0 (environment check only) |
| `stowage-storage` | B+Tree, buffer pool, WAL, recovery; fjall LSM adapter (feature `lsm`) for comparison | M1 |
| `stow-bench` | benchmarks (`engine` today; YCSB and cluster workloads later) | M1 |
| `stowage-raft` | Raft core as a pure state machine; log store; snapshots | M2 |
| `stowage-kv` | ranges, Multi-Raft, splits, routing | M3 |
| `stowage-pd`, `stow-pd` | placement driver | M3 |
| `stowage-txn` | MVCC, intents, coordinator, timestamp cache, deadlock detection | M4 |
| `stowage-sql`, `stowage-pgwire` | SQL engine and Postgres protocol | M5 |
| `stowage-docapi`, `stowage-cdc` | DynamoDB API, changefeeds, backup / PITR | M6–M7 |

## Invariants every layer keeps

1. **No ambient non-determinism.** Time, randomness, files and the network come only
   from the environment traits, so the simulator can drive every stateful component
   ([ADR 0001](adr/0001-deterministic-simulation.md)).
2. **Byte order equals logical order.** Keys and timestamps use memcomparable
   encodings, so the storage engine only needs ordered byte-string keys.
3. **Durability follows POSIX.** File contents are durable after `sync()`.
   Namespace changes are durable after `sync_dir()`. The simulator enforces both.

## Storage engine (M1)

- **Pages.** Slotted 8 KiB pages with a CRC32C (verified on every read from disk).
  A meta page (page 0) holds the root pointer. [Chapter 01](illustrated/01-a-page-on-disk.md).
- **Buffer pool.** Fixed frames with clock eviction. Pins (residency) are separate
  from latches (`RwLock` per frame).
- **Concurrency.** Many readers and one writer at a time. Readers crab down with
  shared latches. The writer tries an optimistic descent first (exclusive latch on
  the leaf only), and falls back to pessimistic exclusive crabbing when the leaf
  must split.
- **Logging.** One mini-transaction per `WriteBatch` → one WAL record. Each page in
  it is logged as a full image on its first change after a checkpoint (with the
  hole left out), and as byte deltas after that.
- **Recovery.** Redo-only from the checkpoint's LSN. The WAL tail is cut at the
  first bad CRC, and the end-of-recovery checkpoint follows.
  [Chapter 02](illustrated/02-crash-and-recover.md).
- **Failure.** Fail-stop: an error after pages were modified stops the engine, and
  reopening recovers. A batch must fit in the buffer pool, because all of its pages
  stay pinned until commit.
- **Not yet:** page merges on delete (as in PostgreSQL, deleted space is reused but
  pages are never merged), fuzzy checkpoints (writers wait during a checkpoint),
  overflow pages (key ≤ 1 KiB, key + value ≤ 2 KiB).

See [ADR 0004](adr/0004-btree-redo-logging.md) for the reasoning.

## Status

M0: workspace, environment traits, HLC and the simulator are in place.
M1: storage engine, with crash tests and benchmarks.
Later sections of this document are filled in as each milestone lands. The
[illustrated chapters](illustrated/README.md) show each piece with real data.
