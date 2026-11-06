# ADR 0005: Hand-written Raft as a pure state machine

- Status: accepted
- Date: 2026-11-06

## Context

Every range of the key space (M3) is replicated by its own Raft group. Consensus
bugs lose acknowledged writes or split the brain, and they show up only under rare
interleavings. Mature Rust implementations exist (`raft-rs` from TiKV,
`openraft`). This project exists to demonstrate the hard parts, though, and it
needs a Raft that runs unmodified inside its deterministic simulator
([ADR 0001](0001-deterministic-simulation.md)).

## Decision

1. **The protocol is a synchronous state machine** (`stowage_raft::Raft`), the same
   shape as etcd/raft and raft-rs.
   - **Inputs:** `tick`, `step(Message)`, `propose`, `read`,
     `propose_membership`, `transfer_leadership`, `compact`.
   - **Outputs:** drained with `take_messages`, `take_committed`, `take_reads`,
     `take_snapshot`.
   - It never touches a clock, a socket or a thread. Time is ticks.
2. **Durability inside `step`.** `Storage` methods are durable on return, and the
   core persists term, vote and entries before it emits any message that depends
   on them. The rule "persist before you reply" lives in one place, not in every
   driver.
3. **Production behaviour is on by default:**
   - pre-vote and check-quorum with leader stickiness;
   - ReadIndex reads, with lease reads as an option;
   - pipelined `Append`s with in-flight limits;
   - fast conflict backtracking;
   - snapshots;
   - joint-consensus membership changes;
   - leadership transfer.
4. **Batching is the driver's job.** `propose(batch)` turns many commands into one
   storage append and one fsync. The node loop (M3) drains its request channel
   into one batch per iteration: group commit.
5. **Three layers of verification:**
   - a simulated KV with a Porcupine-style linearizability checker and an oracle
     for Election Safety and State Machine Safety;
   - a leader-churn nemesis that does non-transitive partitions and fast
     restarts;
   - hand-scripted scenarios for the properties random search rarely reaches:
     Figure 8, durable votes, lease safety.

## Consequences

- The same code runs in the simulator and in `stowd`. A failing seed is a failing
  unit test.
- `step` performs I/O synchronously. On real disks, a follower's fsync is on the
  critical path of each `Append` it acknowledges. The thesis (§10.2.1) allows the
  leader to write to its own disk in parallel with replication; that would need a
  split "unstable log" (as in etcd) and is deferred until profiling shows it
  matters.
- The full log is kept in memory between snapshots. The state machine must
  snapshot regularly; the KV test harness does so every 16–64 entries.

## Alternatives considered

- **raft-rs / openraft**: production-proven, but they would turn the most
  interview-relevant part of the system into a dependency. openraft is
  async-first, which also fights the simulator.
- **Single-server membership changes** (thesis §4.1): simpler, but unsafe without
  extra care, as the "single-server change bug" shows. Joint consensus is the
  textbook-safe version and handles arbitrary voter-set changes in two steps.
