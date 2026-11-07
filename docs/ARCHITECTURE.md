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
        ▲ store heartbeats (gossip)                               │ changefeeds
   scheduler: runs on the leader of range 1 (repair, balance)    ▼  stdout / Kafka / S3
```

## Crates

| Crate | Role | Since |
|---|---|---|
| `stowage-common` | environment traits (`Clock`, `Rng`, `Disk`, `Transport`), HLC, errors, OS implementations | M0 |
| `stowage-sim` | deterministic single-threaded cluster simulator | M0 |
| `stowd` | the node binary | M0 (environment check only) |
| `stowage-storage` | B+Tree, buffer pool, WAL, recovery; fjall LSM adapter (feature `lsm`) for comparison | M1 |
| `stow-bench` | benchmarks (`engine` today; YCSB and cluster workloads later) | M1 |
| `stowage-raft` | Raft core as a pure state machine; `DiskStorage`; joint consensus; ReadIndex/lease reads | M2 |
| `stowage-kv` | ranges, Multi-Raft store, splits, routing, scheduler; MVCC, intents, transaction records, timestamp cache, the transaction coordinator | M3–M4 |
| `stowage-node`, `stowctl` | TCP runtime, async client, CLI | M3 |
| `stowage-sql` | catalog, keys, binder, optimizer, Arrow executor, sessions | M5 |
| `stowage-pgwire` | the Postgres wire protocol, served by every `stowd` | M5 |
| `stowage-docapi` | the DynamoDB-compatible API, GSI worker, Streams | M6 |
| (in `stowage-kv` / `stowage-node`) | changefeeds: `Changes` windows, `Changefeed` | M6 |

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

## Consensus (M2)

- **Core.** `stowage_raft::Raft` is a synchronous state machine: ticks and messages
  go in; messages, committed entries, ready reads and snapshots come out. It
  persists term, vote and entries through `Storage` before emitting anything that
  depends on them. [ADR 0005](adr/0005-raft-as-a-pure-state-machine.md).
- **Elections.** Randomized timeouts, pre-vote, and check-quorum with leader
  stickiness. Leadership transfer uses `TimeoutNow`.
- **Replication.** Probe and replicate states per follower, pipelined `Append`s with
  an in-flight limit, fast backtracking with conflict hints, and `InstallSnapshot`
  for followers behind the compacted log. A leader commits only entries of its
  own term by counting replicas.
- **Reads.** ReadIndex (a heartbeat round confirms leadership) or lease (a quorum
  acknowledged a heartbeat within `election_ticks - margin`). Both wait for the
  leader's first commit in its term.
- **Membership.** Joint consensus: `C_old,new` is appended, it commits under both
  majorities, and then `C_new` is appended automatically. A leader that is not in
  `C_new` steps down once `C_new` commits.
- **Storage.** `DiskStorage`: CRC-framed log file, atomically replaced hard-state
  and snapshot files, generation-based rewrite on compaction.
- **Verification.**
  - The simulator runs a KV store on Raft under partitions, crashes, loss,
    duplication and leader churn. The client history must be linearizable, and an
    oracle asserts at most one leader per term and identical applied entries.
  - Scenario tests cover Figure 8, durable votes and lease safety.
  - Mutation testing measures what the suite catches.
  - [Chapter 03](illustrated/03-one-write-through-raft.md).

## Distributed KV (M3)

- **Ranges.** `[start, end)` spans of the key space, each a Raft group, replicated 3 ways.
  All of a node's replicas share one B+Tree, so a split is a metadata-only Raft
  command. A new range starts at a placeholder snapshot (index 5, term 5).
- **Shared Raft log.** One `LogEngine` file per node, holding every group's
  entries, hard state and snapshot metadata. It is fsynced once per store-loop
  iteration, before anything leaves the node.
- **Snapshots** are generated on demand from current state. Nodes create
  **uninitialized placeholder replicas** for ranges they hear about before
  having them; those can vote and wait for a snapshot.
- **Scheduler** on the leader of range 1. It repairs dead or under-replicated
  ranges first, then balances replicas and leaders, using joint consensus and
  leadership transfer.
- **Routing** by store heartbeats (the higher generation wins). Clients cache
  ranges, follow `NotLeader`, `Locate` after a miss, and treat `Ambiguous` writes
  as unknown.
- **Node runtime.** The store runs on its own OS thread, so group commit happens
  per loop iteration. Peers talk over length-prefixed TCP frames with a two-way
  HELLO; `stowctl` and `stow-bench kv` are the clients.
- [ADR 0006](adr/0006-multi-raft-ranges.md),
  [chapter 04](illustrated/04-a-range-splits.md).

## Transactions (M4)

- **MVCC in the shared engine.** `'d' ‖ esc(key) ‖ !ts` versions (newest first),
  `'i' ‖ esc(key)` write intents, `'x' ‖ esc(anchor) ‖ id` transaction records.
  All sort into the owning range, so splits and snapshots carry them unchanged.
- **Timestamps.** HLC everywhere: on every request and every Raft message. A leader
  puts each write above its **timestamp cache**, which records the highest read
  per key and span when the read *arrives*. A read waits for every write that
  arrived before it, including writes queued for the next proposal batch.
- **Commit.** One Raft command flips the record on the anchor's range to
  `Committed`. Resolving intents afterwards is cleanup that anyone may do.
- **Conflicts.** Readers push a writer's `min_commit_ts`; writers abort lower
  priorities; both wait for higher ones. Abandoned records (no heartbeat for
  2 s) are aborted.
- **Isolation.** SI refreshes written keys over `(read_ts, commit_ts]` before
  committing higher; SSI refreshes read keys too. A failed refresh restarts the
  transaction.
- **Coordinator.** `TxnCoord`, a pure state machine driven by the simulator's
  driver and by `KvClient::transact` over TCP. Every request is idempotent.
- **Verification.**
  - Bank-transfer invariants and Elle-style list-append histories, checked under
    the nemesis on fixed and random seeds.
  - A replica divergence checker in the simulations: every node must produce the
    same engine writes for each (range, index).
  - `stow-bench bank`, which replays the committed transfers of a real cluster
    run.
- [ADR 0007](adr/0007-transactions.md),
  [chapter 05](illustrated/05-a-transfer-transaction.md).

## SQL (M5)

- **Front end.** `sqlparser` (Postgres dialect) → binder → optimizer → executor.
  The binder resolves names against descriptors read in the statement's
  transaction, unifies types and rewrites aggregation. Columns are referenced
  by statement-unique ids.
- **Storage.** Rows at `0x02 ‖ table ‖ 1 ‖ key(pk)`, secondary indexes at
  `0x02 ‖ table ‖ index ‖ key(cols) [‖ key(pk)]`, descriptors at
  `0x01 "table/" ‖ name`. Key encodings are memcomparable.
- **Optimizer.** Predicate pushdown (outer-join aware), hash-join keys,
  dynamic-programming join order on `ANALYZE` statistics, primary-key and
  index range selection, limit pushdown.
- **Executor.** Arrow record batches, one per operator; kernels for
  expressions; row-format keys for hash join and hash aggregation; `lexsort`.
- **Sessions.** Autocommit with internal retry on 40001; `BEGIN … COMMIT` with
  Postgres' failed-transaction semantics; `EXPLAIN [ANALYZE|VERBOSE]`.
- **Protocol.** pgwire v3, simple and extended, text and binary.
- [ADR 0008](adr/0008-sql-layer.md), chapters
  [00](illustrated/00-life-of-a-query.md),
  [06](illustrated/06-planning-a-query.md),
  [07](illustrated/07-pgwire-on-the-wire.md),
  [compatibility](compat.md).

## Changefeeds, documents, schema changes (M6)

- **Changefeeds.** `Changes{span, (from, to]}` is a read at `to`: it closes `to`
  in the timestamp cache, and unresolved intents at or below `to` block it
  until they are pushed above `to` or resolved. A feed is a sequence of
  adjacent windows, which delivers every version exactly once (`stowctl
  changefeed`).
- **DynamoDB API.** Items at `0x03 ‖ table ‖ 0 ‖ key(hash) [‖ key(range)]`. One
  SSI transaction per call, and one for each `TransactWriteItems`. GSIs are
  kept by a single leased worker from the changefeed, with entries, ledger and
  cursor committed together. Streams are windows over a table.
- **Online `CREATE INDEX`.** Write-only, then batched backfill in SSI
  transactions, then public.
- [ADR 0009](adr/0009-cdc-and-documents.md), chapters
  [08](illustrated/08-a-dynamodb-putitem.md) and
  [09](illustrated/09-online-add-index.md).

## Status

M0: workspace, environment traits, HLC and the simulator are in place.
M1: storage engine, with crash tests and benchmarks.
M2: single-group Raft with simulation, scenario and mutation testing.
M3: Multi-Raft key-value layer, TCP cluster, Docker compose.
M4: distributed transactions (SI and SSI) on MVCC, verified in simulation and on a real cluster.
M5: SQL engine and Postgres wire protocol.
M6: changefeeds, the DynamoDB-compatible API, online index builds.
Later sections of this document are filled in as each milestone lands. The
[illustrated chapters](illustrated/README.md) show each piece with real data.
