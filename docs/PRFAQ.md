# PRFAQ: Stowage

*Working backwards: the press release we would want to write on launch day, then
the questions customers and the team would ask. Every number below was measured
in this repository; the source is linked next to it.*

## Press release

**Stowage: one serializable, replicated database behind both a Postgres and a
DynamoDB-compatible API**

*Teams that today run a relational database for orders and a key-value store for
carts, sessions and tracking can point both at one cluster: one thing to
operate, one backup chain, one consistency model.*

Applications grow two databases. The order system wants SQL, joins and
transactions. The shopping cart, the session store and the delivery tracker
want a key-value API that scales and never makes them think about schemas.
Keeping the two in sync is a recurring source of bugs and an extra system to
secure, back up and page someone for.

Stowage is a distributed database in which every row and every item lives in
one ordered, Raft-replicated key space. A Postgres wire protocol front end
serves `psql`, JDBC and Flyway. A DynamoDB-compatible JSON API serves the AWS
SDKs. Both store their data in the same key space (SQL rows under one prefix,
documents under another) and commit through the same serializable transaction
layer. So replication, backup, point-in-time restore, quotas and failure
behaviour are the same for both. A table belongs to one API; Stowage does not
expose SQL tables as DynamoDB tables or the other way round.

- **Correct by construction, checked by machine.**
  - A deterministic simulator runs the whole cluster on one thread under
    partitions, crashes, torn writes and clock skew. Every run is checked by
    a linearizability checker and an Elle-style dependency-cycle checker.
  - Six classic Raft bugs planted on purpose are all caught
    ([chapter 03](illustrated/03-one-write-through-raft.md)).
  - On a real cluster, serializable bank transfers under random process
    kills and freezes conserve every cent, with zero anomalies in a serial
    replay ([chapter 11](illustrated/11-ap-vs-cp.md#116-correctness-under-chaos)).
- **Runs the code teams already have.** The Flyway migrations of two
  production-style services (Conveyor's catalog and inventory, Milkrun's
  sync service) apply unchanged. A Java AWS SDK v2 client replays a
  tracking service's DynamoDB calls, including TTL
  ([compatibility](compat.md)).
- **Operable.**
  - TLS, SCRAM, `GRANT`/`REVOKE` and an audit log.
  - Backups that restore to any second, including the second before a
    `DELETE` without `WHERE` ([chapter 10](illustrated/10-backup-and-pitr.md)).
  - Per-statement statistics, Prometheus alerts with runbooks, an
    operations console, and a Helm chart tested on Kubernetes in CI.

"We ran Postgres for orders and a key-value store for carts and tracking: two
backup schedules, two failure modes, two on-call runbooks," says a hypothetical
early adopter. "Now it is one cluster, and the key-value side is serializable
too."

## External FAQ

**How fast is it?**
It depends on what you ask it to promise. We ran YCSB-A (50 % reads, 50 %
read-modify-write updates, zipfian(0.99), 32 clients) on three nodes on one
laptop ([chapter 11](illustrated/11-ap-vs-cp.md)):
- With every update a serializable transaction and every write fsynced on two
  of three replicas, Stowage does **196 ops/s**.
- With the same transactions and replication alone as the durability
  guarantee, it does **12 221 ops/s**. That is 80 % of a Dynamo-style AP
  store (Cartwheel) on the same workload, which makes no serializability
  promise at all.
- The difference is almost entirely the laptop's `F_FULLFSYNC` (≈11 ms).
  Server NVMe with power-loss protection flushes in tens of microseconds.

**What happens when a node fails?**
No request failed in any of our fault runs.
- When a node is **killed**, clients fail over at once and throughput dips
  by a quarter while the two survivors take over its ranges.
- When a node is **frozen** (a partition), requests already sent to it wait
  for the client's 3 s timeout, then go to the new leaders.
- When it **comes back**, it catches up with one snapshot per range and full
  throughput returns within about a second.

**Is it really serializable?**
Yes, for transactions that ask for it (the default), and snapshot isolation
for those that ask for SI. Under SSI, the Elle-style checker finds no
dependency cycle on any seed in simulation. Under SI it finds only write
skew, which SI permits. The same checks run in CI on every commit with
fresh random seeds.

**Do I have to change my application?**
- For Postgres: if it uses the common dialect (DDL, DML, joins, aggregates,
  subqueries, `CHECK`/`FOREIGN KEY`, `ALTER TABLE`, `pg_catalog` lookups),
  usually no. Unsupported syntax fails with SQLSTATE `0A000` and names
  the construct. Known gaps (for example `INTERVAL` arithmetic, triggers, stored
  procedures) are listed in [compat.md](compat.md).
- For the DynamoDB API: the item, query, scan, transaction, GSI, streams and
  TTL calls are implemented; see the same page.

**How do I avoid hot-key contention?**
Read with `SELECT … FOR UPDATE` (or `GetForUpdate` in the KV client) when you
are about to write what you read. Conflicting writers then queue on the key
instead of restarting. On a simulated counter incremented by 12 clients,
that took restarts from about 500 to 0–20
([ADR 0011](adr/0011-contention-and-group-commit.md)).

## Internal FAQ

**Why build a CP system when Cartwheel already exists?**
They answer different questions.
- Cartwheel accepts every write, even during a partition, and leaves
  conflicting versions to the reader.
- Stowage refuses to acknowledge a write it cannot make durable on a
  majority, and orders every conflict.

Running both through the same faults turns the CAP trade-off from a slogan
into numbers (chapter 11).

**What was the hardest bug?**
A zombie replica. A node applied a range split long after the right-hand
range had removed it, and recreated that range with the membership of long
ago. Another removed node, which had forgotten its votes, voted for it, and
the zombie committed a different entry at an index the real range had
already committed. The simulator's replica-divergence checker caught it on
one seed out of many. The fix took three parts:
- tombstones with the removing generation;
- generations on every Raft message;
- replica GC from gossip.

**What would we build next?**
1. **One-phase commit for single-range transactions.** A locked
   read-modify-write takes four sequential Raft writes today, and that bounds
   a hot key at roughly 12 updates/s on a laptop disk.
2. **Parallel commits.**
3. **Follower reads** for stale-tolerant queries.
4. **An on-disk changefeed sink** (Kafka, S3) beyond the built-in one.

**What are the known risks?**
- One remaining simulation flake: an SSI list-append transaction can hit its
  30-restart limit under the nemesis on about 1 seed in 100. The code before
  ADR 0011 had the same failure mode.
- Snapshot generation still runs on the store thread, so a very large range
  could stall a node; streaming snapshots would remove that.
- The benchmarks are from a laptop: compare ratios, not absolute numbers.
