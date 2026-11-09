# ADR 0011: Contention, group commit, and the replica lifecycle under faults

- Status: accepted
- Date: 2026-11-09

## Context

The first run of the AP-vs-CP benchmark ([chapter 11](../illustrated/11-ap-vs-cp.md)) put YCSB
workload A on a three-node cluster. Every update was a serializable
read-modify-write of one key, and the keys followed a zipfian(0.99) distribution, as YCSB
specifies. Stowage did 26 ops/s with a p99 of 49 s, and throughput fell
during the run (146 ops/s in the first second, 0–10 in the last ten). Once
that was fixed, a second problem showed. A node killed and restarted
under load set off a storm of snapshots and elections for 7–10 s. Then
the fixes for the storm, run against hundreds of random simulation seeds,
uncovered four latent bugs in how replicas are removed and recreated.

### Contention

1. **The store thread fsynced inline.** The node's single store thread wrote and
   fsynced the shared Raft log, then sent what depended on it. With
   macOS `F_FULLFSYNC` at about 11 ms, the thread spent most of its time
   blocked, accepted no input during an fsync, and so formed small batches.
   A leader also held its `Append` messages until its own fsync finished.
   The leader's and the follower's fsyncs therefore ran one after the
   other, even though the Raft core already supported overlapping them
   (`async_durability`, thesis §10.2.1).
2. **Read-modify-write was optimistic.** `Get k; Put k` reads at the snapshot
   and writes later. On a hot key someone else nearly always commits `k`
   in between, so the transaction restarts.
3. **Waiters polled.** A writer that met an intent pushed its holder at once,
   with a random priority, so it aborted the holder half the time. A loser
   polled every 20 ms, so the next holder was whoever polled first.
4. **Plain reads failed on intents.** `KvClient::get` returned an intent as an
   error instead of waiting for the writer.

### Rejoin

5. **Rejected appends restarted snapshots.** A follower still rejecting the
   `Append`s that were in flight before its snapshot pulled the leader out of
   the snapshot state. The leader then requested a new snapshot, one per
   rejection, and generated each copy (a 12 MB scan) on the store thread,
   stalling every range on that node past the election timeout.
6. **The leader compacted under a catching-up follower.** It compacted to its
   applied index every 1 000 entries. A follower that had just installed a
   snapshot found the entries after it already gone and needed another.

### Replica lifecycle

7. **Zombie replicas from late splits.** A node that applied a split long
   after the right-hand range had removed it created that range afresh, with
   the membership of long ago. One such zombie got the vote of a node that
   had also been removed and had since recreated an empty placeholder. With it,
   the zombie won an election and committed a different entry at an index the
   real range had already committed. The simulator's replica-divergence
   checker caught this (`rebalancing_spreads_replicas_and_leaders`, seed 12).
   Other zombies were never contacted again, and their stale spans blocked
   every snapshot for the ranges that now covered them.
8. **A pending destroy outlived a re-add.** A replica that applied "remove
   me" and then "add me back" in one batch of entries was still destroyed.
9. **A stale match index after recreation.** A leader kept the `matched` index of
   a follower whose replica had been destroyed and recreated empty. It probed
   at `matched` forever and was rejected every time.
10. **An uninitialized replica could lead.** A replica reopened from a Raft log
    whose descriptor a crash had lost won an election on the strength of that
    log. It then led a range it could neither apply nor serve, and its
    heartbeats kept the others from electing anyone.

## Decision

**Pipelined log sync** (`crates/node/src/server.rs`). A syncer thread writes and
fsyncs staged log batches in order. All batches queued during one fsync are
written by the next pass and covered by a single fsync. The store thread
keeps accepting input in the meantime. What it sends before the batch is
durable is decided by message type:
- a leader's `Append` goes out at once;
- client responses go out at once, because they only ever reflect committed
  state, which a quorum has persisted;
- every other peer message (votes, acknowledgements) waits for its batch
  and every batch before it.

**Locking reads** (`TxnOp::GetForUpdate`, Raft command `Lock`). The read takes
the key's write intent, holding the newest committed value unchanged, and
returns that value. Nobody can commit the key before this transaction ends,
so the key is neither refreshed nor checked for a newer version at
commit. This is `SELECT … FOR UPDATE`.

**A lock wait queue on the leaseholder** (`Replica::lock_waits`). A
transactional write or locking read that meets another transaction's intent
parks per key in arrival order, instead of being answered at once. When the
intent's `Resolve` applies, the parked requests are proposed again, oldest
first. A request still parked after 8 ticks (80 ms) is answered `Conflict`,
which must stay under any client's resend timeout. The queue lives only on
the leader and changes no replicated state.

**Wait before wounding** (`WOUND_AFTER = 1`). A writer's first push carries
priority 0, which only learns the holder's state (and aborts it only if its
heartbeats stopped). Only after one lost round, about 100 ms, may its random
priority abort the holder. A queue on a hot key therefore takes turns
instead of aborting whoever holds the lock, while deadlocks are still broken.

**Plain reads wait for writers.** On an intent, `KvClient::get` asks the
writer's record for its state. It resolves the intent if the writer has
finished and otherwise waits. It never skips the intent: a committed
transaction is acknowledged before its intents are resolved.

**Snapshots** (`crates/raft/src/raft.rs`):
- A rejection that arrives while a snapshot is in flight is ignored.
- A snapshot is resent only `snapshot_retry_ticks` (50) after it was
  sent, not after two quiet heartbeat intervals.
- `Raft::compaction_floor` keeps the log for followers that are
  streaming (`Replicate`), or receiving a snapshot (counted from its
  index), within 50 000 entries.

**Replica lifecycle** (`crates/kv/src/store.rs`):
- **Tombstones.** A replica removed from a range leaves
  `tombstone(range) = generation that removed it`.
- **Generations on Raft messages.** `PeerMsg::Raft` carries the sender's
  descriptor generation. A node without an initialized replica of the range
  drops messages older than its tombstone, so a zombie cannot recruit it.
- **Late splits.** A split applied late does not create a right half that a
  tombstone says has removed us. It still writes the same keys (every replica
  applies an entry identically) and deletes them right after; reopening
  skips them too.
- **Replica GC.** If gossip shows a newer generation of a local range that
  excludes us, the replica is destroyed and tombstoned.
- **Re-add clears a pending destroy.** A later configuration that includes us
  cancels the destroy.
- **A probe rejected exactly at `matched`, by a follower whose log ends
  before it**, resets `matched`. Requiring the probe to be at `matched` itself
  ignores a reordered rejection of an older probe.
- **Uninitialized replicas never campaign** (`Raft::set_promotable`). They
  still vote.

## Consequences

The same laptop, the same workload, zipfian(0.99) over 50 000 keys, 32 clients:

| | before | after |
|---|---:|---:|
| load, distinct keys | 460 writes/s | 1 485 writes/s |
| blind update (`Get` then `Put`) | 468 ops/s | 707–723 ops/s |
| serializable read-modify-write | 26 ops/s, p99 49 s | ~200–250 ops/s, p99 2–4 s |
| the same, `--unsafe-skip-fsync` | — | ~13 800 ops/s, p99 31 ms |
| kill -9 and restart a node (no fsync) | 87 snapshots and 21 elections in 5 s, about 7 s degraded | 3 snapshots (one per range), 0 elections, back to full speed in about 1 s |
| simulated hot counter, 12 clients, 120 increments | 445–513 restarts (optimistic) | 0–20 restarts (locking) |

- **The update rate of a hot key is bounded by its critical section.** A
  locked read-modify-write needs four sequential Raft writes: lock, put,
  commit record, resolve. On macOS each of them waits for an `F_FULLFSYNC`,
  so the hottest zipfian key takes about 12 updates a second, and the queue behind it
  is the p99. Without fsync the same protocol does about 14 k ops/s,
  close to Cartwheel. Fsync, not consensus, is the cost here. A
  shorter critical section needs one-phase commit for single-range
  transactions, or write buffering with parallel commits; neither is built.
- `--unsafe-skip-fsync` exists for benchmarks only. It gives the durability a
  Dynamo-style store relies on by default: acknowledged writes survive a
  process crash, not a power loss of a majority.
- **Tests.**
  - New: `bank_conserves_money_with_locking_reads` (SSI and SI under the
    nemesis) and `hot_counter_locking_reads_restart_less`.
  - Fixed: `minority_side_cannot_commit` asserted that a partitioned
    minority commits nothing new. With async durability it may legitimately
    commit an entry a majority already had, once the old leader's own fsync
    finishes; HEAD failed 7 of 20 alternative seeds. It now asserts the real
    invariant: no commit beyond what some majority node held.
- **Random seeds.** On 100 random seeds of every simulation, this change
  fails 1 of 29 tests and the code before it fails 2. The remaining failure
  in both is the known liveness bound: an SSI list-append transaction gives
  up after 30 restarts under the nemesis.

## Alternatives considered

- **Age-based wound-wait** (the older transaction always wins). It was tried
  and reverted. It changed who restarts, not how many, and readers that
  waited for older writers starved the bank and list-append workloads into
  their restart limits. The lock queue was the change that mattered.
- **Distributed deadlock detection** (CockroachDB's `txnwait` queue). It is
  more precise, but far more machinery than a ~100 ms wait before wounding.
- **Readers skip pending intents.** That is cheaper, but it can miss a write
  that was already acknowledged.
- **Replica IDs instead of generations** (CockroachDB gives every replica
  incarnation its own ID). They are cleaner, but they change every
  descriptor and message. The range generation already orders configurations,
  so a tombstone with a generation gives the same protection here.
