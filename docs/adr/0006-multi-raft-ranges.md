# ADR 0006: Ranges, Multi-Raft, and a scheduler without a separate service

- Status: accepted
- Date: 2026-11-06

## Context

One Raft group cannot hold a whole database. It is bounded by one leader's CPU,
disk and network, and every write pays for every key. The key space must be cut
into **ranges**, each its own Raft group, that split as they grow and move between
nodes as nodes come and go. Most of the difficulty of Multi-Raft is not in Raft
itself. It is in the seams:

- splitting while replicas lag;
- replicas that exist on some nodes and not yet on others;
- crashes between two writes that belong together;
- deciding who moves what.

## Decision

### Data layout

1. **One storage engine per node, shared by all its replicas.** User data lives
   under `'d' ‖ key`. Ranges partition that space logically; a range owns no
   files. Consequently a **split moves no data**. `Split{key, new_id, generation}`
   is a Raft command. Applying it rewrites two descriptors, and the new right-hand
   range starts serving the same engine keys.
2. **A new range's log starts at a placeholder snapshot (index 5, term 5)**, as in
   TiKV. Every replica that applies the split creates the same initial state, so
   no snapshot is needed. A replica that is *added* later starts empty, below the
   leader's first index, so it always gets one.

### Shared Raft log

3. **One Raft log per node** ([`LogEngine`](../../crates/raft/src/log_engine.rs)).
   All groups append records to one file, and the store fsyncs it once per batch,
   right before any message or reply leaves the node. With a file per group, a
   node hosting 12 ranges spent most of its time in `F_FULLFSYNC`, its heartbeats
   arrived late, and followers kept starting elections (terms 9–13 within 25 s of
   a benchmark).

### Snapshots and placeholder replicas

4. **Snapshots are generated when they are sent**, from the state at the applied
   index (`Config::app_snapshots`). Snapshots stored at compaction time went stale.
   A placeholder snapshot has no data, and an old snapshot describes the range as
   it was before a later split, so a follower that already holds the split-off
   half must refuse it. Both left followers permanently unable to catch up.
5. **Uninitialized replicas.** A node that gets a Raft message for a range it does
   not have creates an empty replica.
   - It can vote. Its vote is persisted like any other, so a range whose other
     replicas missed the split can still elect a leader.
   - It waits for a snapshot. It applies nothing, serves nothing, and owns no span
     in overlap checks.
   - When the node later applies the split itself, the placeholder is upgraded in
     place, keeping its files and therefore its vote.
6. **Before declaring a follower "needs a snapshot", the leader probes once at its
   compaction boundary.** A follower created by the same split already has that
   entry.

### Placement and routing

7. **Placement runs on the leader of range 1.** Range 1 always exists, because a
   split keeps the left half's id. Its state is soft and rebuilt from store
   heartbeats, so failover is just a new leader of range 1. Each round:
   - plans up to four replica moves and four leader transfers on distinct ranges;
   - repairs first: dead stores, then under-replicated ranges;
   - balances replica and leader counts after that;
   - changes membership by joint consensus.
8. **Routing is gossip.** Every store heartbeats its replicas' descriptors to every
   other store, and the higher generation wins on overlap. Clients cache ranges
   (span + leader), follow `NotLeader` hints, and ask `Locate` after a miss.

### Exactly-once and crash safety

9. **Exactly-once is the client's decision.** A write that was proposed but whose
   leader stepped down before applying it returns `Ambiguous` (as in
   CockroachDB). Retrying it blindly could apply it twice, so the client decides.
10. **Proposal ids are prefixed with an incarnation number** that is persisted and
    bumped at every start. An entry proposed before a crash can commit after it,
    and must not be mistaken for a new request that reused its id.
11. **A split's descriptors are fsynced before the new range's Raft state exists.**
    A crash therefore cannot leave Raft state for a range the engine does not
    know.

### Transport

12. **Inter-node transport is length-prefixed frames over TCP** carrying the same
    binary encodings the simulator exercises. It is not gRPC. The node loop is a
    dedicated OS thread that drains its input queue, then flushes: concurrent
    writes to a range become one Raft entry batch and one fsync (group commit).

## Changes from the original plan

- **No separate `stow-pd` service** (decision 7). A Raft-replicated PD would add
  a service, a client and a failure mode. It would hold state that the heartbeats
  already carry; only the range-id allocator would need it, and decision 10's
  scheme replaces that allocator: `(node << 40) | n` with `n` persisted locally.
- **Custom TCP framing instead of tonic** (decision 12). The encodings already
  exist and are fuzzed by the property tests. gRPC is still a candidate for the
  external API.
- **Merges are not implemented yet.** Ranges only split.

## Consequences

- Bugs live in the seams, and the simulator found eight of them (two in the
  Raft core) before any real network was involved. [Chapter 04](../illustrated/04-a-range-splits.md#47-what-the-simulator-caught)
  lists them.
- Every node gossips every range: O(ranges × nodes) per heartbeat. That is fine
  for tens of nodes and thousands of ranges. Beyond that, a meta range
  (CockroachDB) or a PD service (TiKV) becomes necessary.
- Snapshot generation scans the range synchronously on the store thread. Ranges
  are capped by `split_bytes`, but a large range will stall the node while it is
  scanned. Streaming snapshots are future work.
