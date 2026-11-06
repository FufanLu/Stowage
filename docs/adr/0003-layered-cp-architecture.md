# ADR 0003: One transactional KV, with SQL and document layers on top

- Status: accepted
- Date: 2026-11-06

## Context

Stowage must serve two kinds of clients:

- relational clients that speak the Postgres protocol (JDBC, Flyway, Exposed);
- document clients that use the DynamoDB API (AWS SDKs).

Building two storage stacks would double the replication and transaction work and
leave us with two consistency stories.

## Decision

Follow FoundationDB's "layers" design and CockroachDB's range architecture.

```
pgwire  ─┐                     ┌─ DynamoDB JSON API
         ▼                     ▼
   SQL layer               Document layer
   (catalog, CBO, Arrow)   (conditions, GSI, streams)
         └──────────┬──────────┘
          Transaction layer: HLC, MVCC, intents, 2PC, SI / SSI
                    │
          Distributed KV: ranges, Multi-Raft, splits, leases
                    │
          Storage engine: B+Tree (+ LSM adapter for comparison)
```

- **The system is CP.** Each range is a Raft group. A write is acknowledged only
  after a majority has persisted it. During a partition, the minority side refuses
  writes instead of accepting them and diverging.
- **Both front ends map onto the same ordered keyspace** through memcomparable key
  encoding:
  - SQL rows use `/table_id/index_id/pk…`;
  - DynamoDB items use `/doc/table_id/hash/range`;
  - MVCC versions append a descending timestamp to either.
- **One transaction protocol serves both front ends.** A SQL `BEGIN … COMMIT` and a
  DynamoDB `TransactWriteItems` both use the same coordinator.

## Why CP, given that the author also built an AP store

The companion project Cartwheel is a leaderless Dynamo-style store. It is always
writable and resolves conflicts with vector clocks and CRDTs. Stowage takes the
opposite trade on purpose:

| | Cartwheel (AP) | Stowage (CP) |
|---|---|---|
| Replication | sloppy quorum, hinted handoff | Raft, majority commit |
| Conflicts | siblings + CRDT merge | prevented by serializable transactions |
| During a partition | both sides accept writes | only the majority side does |
| Good for | carts, sessions | orders, inventory, money |

Milestone M9 runs the same YCSB workloads against both and measures the cost of each
choice: latency, throughput, and availability during partitions.

## Consequences

- Every write pays a Raft round trip, so write latency is bounded below by the
  replication RTT. Leader leases keep reads local. Follower reads serve stale-but-
  consistent snapshots when the client allows them.
- The SQL and document layers have no storage code of their own. They are stateless
  translators, so any node can serve any client.
