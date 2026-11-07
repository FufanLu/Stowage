# 05. A transfer transaction

> Data in this chapter comes from
> `cargo run --release -p stowage-kv --example trace_txn` (the simulator, seed 5)
> and from `cargo nextest run --release -p stowage-kv -E 'binary(txn)'`.
> Code: [`mvcc.rs`](../../crates/kv/src/mvcc.rs),
> [`eval.rs`](../../crates/kv/src/eval.rs),
> [`txn.rs`](../../crates/kv/src/txn.rs),
> [`store.rs`](../../crates/kv/src/store.rs),
> [`elle.rs`](../../crates/sim/src/elle.rs). Design:
> [ADR 0007](../adr/0007-transactions.md).

Move 30 from `alice` to `rob`. The two keys live in different ranges (split at
`"m"`), so different Raft groups, and possibly different leaders. Either both
writes happen or neither does, and no reader may ever see the money in flight.
This chapter follows:

1. what a transaction leaves in the storage engine, byte by byte;
2. one uncontended transfer, request by request;
3. the timestamp cache moving a write above a read;
4. a reader pushing a writer, and the refresh that then fails;
5. how the tests know it is right;
6. what it costs on a real cluster;
7. the nine bugs the tests caught on the way.

## 5.1 Three kinds of keys

Chapter 04 stored `'d' ‖ key → value`. With transactions, every range span holds
three kinds of engine keys:

```text
 'd' ‖ esc(key) ‖ !ts        a committed version             newest first
 'i' ‖ esc(key)              a write intent: uncommitted, owned by one transaction
 'x' ‖ esc(anchor) ‖ txn id  a transaction record, next to the txn's first key
```

`esc` makes keys prefix-free and keeps them in order: `0x00` becomes `00 ff`, and
every key ends in `00 01`. `!ts` is the bitwise NOT of the 12-byte big-endian
timestamp, so a forward scan meets the newest version first. These are the real
keys from the trace:

```text
 version   64 | 61 6c 69 63 65 00 01 | e7 1a b4 5a 35 ec 17 bf  ff ff ff ff
           'd'  "alice"     end        !wall                    !logical
           → alice @ 1793923202.009000000,0 = 100

 intent    69 | 61 6c 69 63 65 00 01
           'i'  "alice"     end
           → value 70, owner txn 2 at write_ts 1793923202.094000000,3, priority 100

 record    78 | 61 6c 69 63 65 00 01 | 00 00 00 00 00 00 00 00 00 00 00 02 7e 54 03 18
           'x'  anchor "alice"         txn id (128 bit: sequence ‖ random)
           → Committed, commit_ts 1793923202.094000000,3
```

`!wall` decodes as `~e71ab45a35ec17bf = 0x18e54ba5ca13e840` =
1 793 923 202 009 000 000 ns. A timestamp is a **hybrid logical clock** value:
wall-clock nanoseconds plus a logical counter, written `wall,logical`. Every
message between nodes and every request carries one, and each node's clock
moves forward to the largest timestamp it has seen. A node never hands out a
timestamp below one it has already observed, even when its wall clock lags.

All three key kinds sort into the span of the range that owns `key` (or
`anchor`). A split therefore carries intents and records along with the data,
and a range snapshot is the same scan as ever.

## 5.2 One transfer, with nobody in the way

The coordinator is a state machine with no I/O (`TxnCoord`). It emits requests
and consumes replies; a driver routes them by key and resends on timeouts. Each
request below is one round trip to a range leader. Writes go through Raft, and
reads are served by the leader. The `n1 engine ±` lines are what node 1's storage
engine gained or lost when the command applied.

```text
    1.0ms  T1 ─► n1  TxnGet alice @1793923202.094000000,2 txn=2
   12.0ms  T1 ◄─ n1  Value(100)
   12.0ms  T1 ─► n1  TxnGet rob @1793923202.094000000,2 txn=2
   20.0ms  T1 ◄─ n1  Value(100)
   20.0ms  T1 ─► n1  TxnPut alice=70 txn=2 write_ts=1793923202.094000000,2 anchor=alice
              n1 engine + intent   alice = 70 (txn 2 write_ts 1793923202.094000000,3)
   31.0ms  T1 ◄─ n1  Written(1793923202.094000000,3)         ← moved up one tick (§5.3)
   31.0ms  T1 ─► n1  TxnPut rob=130 txn=2 write_ts=1793923202.094000000,3 anchor=alice
              n1 engine + intent   rob = 130 (txn 2 write_ts 1793923202.094000000,3)
   42.0ms  T1 ◄─ n1  Written(1793923202.094000000,3)
   42.0ms  T1 ─► n1  Refresh alice (…094000000,2, …094000000,3] txn=2
   42.0ms  T1 ─► n1  Refresh rob   (…094000000,2, …094000000,3] txn=2
   51.0ms  T1 ◄─ n1  Refreshed(true)
   54.0ms  T1 ◄─ n1  Refreshed(true)
   54.0ms  T1 ─► n1  EndTxn txn=2 COMMIT @1793923202.094000000,3
              n1 engine + record   Committed commit_ts 1793923202.094000000,3   ← THE commit point
   64.0ms  T1 ◄─ n1  Txn { Committed, … }
   64.0ms  T1 ─► n1  Resolve alice txn=2 commit @…,3
   64.0ms  T1 ─► n1  Resolve rob   txn=2 commit @…,3
              n1 engine + version  rob@1793923202.094000000,3 = 130
              n1 engine − intent   rob = 130
              n1 engine + version  alice@1793923202.094000000,3 = 70
              n1 engine − intent   alice = 70
   78.0ms  T1 ◄─ n1  Done ×2                                  ← 78 ms, 9 round trips
```

```text
                    TxnGet ×2         TxnPut ×2              EndTxn           Resolve ×2
 read_ts  ───────────●─●──────────────────────────────────────────────────────────────────
 write_ts            ●──────────────●─(bumped)─●─ refresh ok ─● Committed ──► versions
 engine              ·              i:alice    i:rob          x:record        d:alice d:rob
 visible to readers  old values ……………………………………………………… new values from here on
```

The transaction is committed when **one** Raft command, the `EndTxn` that flips
the record to `Committed`, applies. Before that, a reader that meets an intent
looks up the record and finds `Pending`. After that, it finds `Committed` and the
commit timestamp, even if the coordinator dies before resolving anything. The
`Resolve`s are only cleanup: they turn intents into versions so that later
readers need not visit the record. This is how CockroachDB does it, with "2PC"
reduced to one durable write on one range plus asynchronous cleanup.

Reads and writes carry different timestamps. Reads happen at `read_ts`, the
snapshot. Writes happen at `write_ts`, which may only grow. A commit at
`write_ts > read_ts` is fine **if nothing the transaction depends on changed in
between**, and that is what `Refresh` checks. Under snapshot isolation (SI) it
refreshes the keys it wrote: first committer wins. Under SSI it refreshes those
plus every key it read.

## 5.3 The timestamp cache moves a write above a read

Why did T1's write land at `,3` when it asked for `,2`? Every leader keeps a
**timestamp cache**: the highest timestamp at which each key was read. A write
below it would change what that read saw, so the leader moves the write above it.
T1 itself read `alice` at `,2`, and the write moved one logical tick. The cache
does not record who read, so a transaction's own reads push it as well. The only
cost is the refresh in §5.2, which then succeeds trivially. CockroachDB avoids
this by storing the reader's transaction id in the cache; Stowage does not yet.

Part 2 of the trace shows the same mechanism working against another
transaction. An audit (reads `alice` and `rob`, read-only) starts 3 ms after
transfer T2, with a clock 3 ms ahead:

```text
    1.0ms     T2 ─► n1  TxnGet alice @1793923202.192000000,2 txn=3
    4.0ms  audit ─► n1  TxnGet alice @1793923202.195000000,0 txn=4    ← ts cache: alice read at .195
   18.0ms  audit ◄─ n1  Value(70)
   23.0ms  audit ◄─ n1  Value(130)  → audit commits at .195,0 and saw 70 + 130 = 200 ✓
   27.0ms     T2 ─► n1  TxnPut alice=60 txn=3 write_ts=1793923202.192000000,2
              n1 engine + intent   alice = 60 (txn 3 write_ts 1793923202.195000000,1)   ← above the audit
   37.0ms     T2 ◄─ n1  Written(1793923202.195000000,1)
   52.0ms     T2 ─► n1  Refresh alice (…192000000,2, …195000000,1]
   52.0ms     T2 ─► n1  Refresh rob   (…192000000,2, …195000000,1]
   67.0ms     T2 ◄─ n1  Refreshed(true) ×2  → EndTxn COMMIT @…195000000,1
```

T2 wanted to write at `.192`, below the audit's read at `.195`. Had it done so, the
audit's snapshot at `.195` would retroactively contain a write it never saw.
Instead, T2 commits at `.195,1`, after the audit, and the refresh proves that
moving its reads from `.192` up to `.195,1` changes nothing.

The cache must be updated **when the read arrives**, not when it is served (§5.7,
bug 5). A leader serves a read only after every write that arrived before it
has applied, including writes still queued for the next proposal batch (bug 9).
A write evaluated in that gap would otherwise slip below the read.

## 5.4 A reader pushes a writer, and the refresh that fails

Part 3 makes two writers collide. L (priority 7) and H (priority 9000) both move
money from `alice` to `rob`. H starts while L holds intents on both keys:

```text
   30.0ms        L ─► n1  TxnPut rob=145 txn=5 …
   31.0ms        H ─► n1  TxnGet alice @1793923202.338000000,2 txn=6
   42.0ms        H ◄─ n1  Conflict(intent of txn=5 write_ts=1793923202.308000000,3 prio=7)
   42.0ms        H ─► n1  Push txn=5 Timestamp { ts: 1793923202.338000000,2, priority: 9000 }
              n1 engine + record   Pending commit_ts 0 min_commit_ts 1793923202.338000000,3
   54.0ms        H ◄─ n1  Txn { Pending, min_commit_ts=1793923202.338000000,3 }
   54.0ms        H ─► n1  TxnGet alice @1793923202.338000000,2 txn=6 skip=["5"]
   69.0ms        H ◄─ n1  Value(60)                         ← L's intent ignored: L commits above us
```

H's read at `.338,2` found L's intent written at `.308,3`, below the read. H may
not simply ignore it, because L might commit at `.308,3` and H's snapshot would
then be missing L's write. H **pushes** L's record instead. It writes
`min_commit_ts = .338,3`, which means L may no longer commit at or below H's read.
Now H can read past the intent. (A push only works for the higher priority. A
lower-priority reader waits, so a long transaction cannot be pushed forever.)

L finds out at its own commit:

```text
   58.0ms        L ─► n1  EndTxn txn=5 COMMIT @1793923202.308000000,3
   67.0ms        L ◄─ n1  Txn { Pending, min_commit_ts=1793923202.338000000,3 }   ← refused: too low
   67.0ms        L ─► n1  Refresh alice (…308000000,3, …338000000,3]
   67.0ms        L ─► n1  Refresh rob   (…308000000,3, …338000000,3]
   83.0ms        L ◄─ n1  Refreshed(true) ×2
   83.0ms        L ─► n1  EndTxn txn=5 COMMIT @1793923202.338000000,3
              n1 engine + record   Committed commit_ts 1793923202.338000000,3
```

Then H writes. It meets L's intent again, pushes to abort, and learns that L has
already committed. H resolves L's intent for it, writes its own, and refreshes:

```text
   88.0ms        H ◄─ n1  Conflict(intent of txn=5 …)
   88.0ms        H ─► n1  Push txn=5 Abort { priority: 9000 }
  104.0ms        H ◄─ n1  Txn { Committed, commit_ts=1793923202.338000000,3 }
  104.0ms        H ─► n1  Resolve alice txn=5 commit @…338000000,3          ← H cleans up for L
  116.0ms        H ─► n1  TxnPut alice=59 txn=6 write_ts=1793923202.338000000,2
  127.0ms        H ◄─ n1  Written(1793923202.338000000,4)
  138.0ms        H ─► n1  Refresh alice (…338000000,2, …338000000,4]
  150.0ms        H ◄─ n1  Refreshed(false)                  ← L committed alice at .338,3: inside the window
  151.0ms        H ─► n1  EndTxn txn=6 ABORT …  +  Resolve ×2 abort
  165.0ms        H ─► n1  TxnGet alice @1793923202.338000000,5 txn=10000000000000006   ← epoch 1
  177.0ms        H ◄─ n1  Value(55)
   …
  226.0ms        H ─► n1  EndTxn … COMMIT @1793923202.338000000,6           → 223 ms, 1 restart
```

H read `alice = 60`, but L committed `alice = 55` at `.338,3`, which falls between
H's read (`.338,2`) and H's write (`.338,4`). Committing would have been a lost
update: `59` computed from a stale `60`. The refresh catches it, and H restarts
at a fresh timestamp with a higher priority. Its new id is the old one plus an
epoch in the top bits, so the record key changes from `…00 00 00 00 …06` to
`…00 00 00 01 …06`.

The record's states, all observed above:

```text
              Push(ts) by higher priority: min_commit_ts := ts+1
                  ┌──────┐
                  ▼      │
   (implicit) ─► Pending ─┴─ EndTxn(commit, ts ≥ min_commit_ts) ──► Committed (commit_ts)
       │            │
       │            ├─ EndTxn(commit, ts < min_commit_ts) ──► stays Pending; coordinator refreshes
       │            ├─ EndTxn(abort) / Push(abort) by higher priority ──► Aborted
       └────────────┴─ no heartbeat for 2 s, and anyone pushes ──► Aborted
```

A transaction that never wrote a record is treated as `Pending` since its start.
Records only move forward, and every request is idempotent, so a driver may
resend anything after a timeout or an `Ambiguous` reply.

## 5.5 How the tests know

Four simulator workloads run under a nemesis that partitions, crashes and
restarts nodes, with 2% message loss. They run on fixed seeds in CI and on
random seeds in sweeps.

| Test | Checks |
|---|---|
| `bank_conserves_money_under_{ssi,si}` | 5 accounts, transfers plus read-only audits; **every committed audit sums to 500** |
| `list_append_is_serializable_under_ssi` | 4 hot lists, 1–3 read/append ops per txn; the Elle-style checker finds **no dependency cycle** |
| `list_append_under_si_shows_at_most_write_skew` | same under SI: **only G2** (anti-dependency) cycles; no G0, G1c or G-single |
| `write_skew_si_allows_ssi_prevents` | two doctors on call: SI lets both leave, SSI keeps one, by restarting |

List-append is the checker's favourite workload, from Jepsen's Elle. Appends
reveal the version order: a read of `[3,7,9]` says 3, 7 and 9 were installed in
that order. From that order the checker builds write-write (ww), write-read (wr)
and read-write (rw, anti-dependency) edges between transactions, and then looks
for cycles:

```text
  G0       cycle of ww only                  dirty write          — forbidden by both
  G1c      cycle of ww + wr                  circular info flow   — forbidden by both
  G-single cycle with exactly one rw edge    read skew            — forbidden by SI
  G2       cycle with ≥ 2 rw edges           write skew           — allowed by SI, not SSI
```

On the fixed CI seeds (`STOWAGE_TXN_DUMP=1` prints this):

```text
ELLE SSI seed 11: 270 committed, edges {Ww: 260, Wr: 202, Rw: 206}, cycles {}
ELLE SSI seed 12: 309 committed, edges {Ww: 275, Wr: 235, Rw: 239}, cycles {}
ELLE SI  seed 21: 400 committed, edges {Ww: 352, Wr: 326, Rw: 331}, cycles {"G2": 8}
ELLE SI  seed 22: 371 committed, edges {Ww: 290, Wr: 328, Rw: 336}, cycles {"G2": 5}
```

Hundreds of anti-dependency (rw) edges per run show the workload is contended
enough to matter. SI duly shows write skew (G2) and nothing worse; SSI shows no
cycle at all. Beyond the fixed seeds, the five transaction tests and the three
KV simulations pass a sweep of 60 random seeds with the batching node of bug 9
and the replica divergence checker of bug 8 switched on. The transaction tests
alone also passed 120 seeds before the batching change.

## 5.6 On a real cluster

`stow-bench bank` runs the same transfer and audit transactions through
`KvClient::transact`, which drives the coordinator state machine over TCP. It
funds 1 000 accounts with 1 000 each, pre-split into 6 ranges, and runs 32
transactions at a time, 10% of them audits of 10 accounts. At the end, one
snapshot read checks the total. The bench also replays every committed transfer
in commit-timestamp order and counts transfers that read a state their
predecessors did not leave.

```bash
./target/release/stow-bench bank --accounts 1000 --concurrency 32 --duration 20 --presplit 6 --isolation ssi
```

| Environment | Isolation | txn/s | p50 ms | p99 ms | restarts | replay anomalies | total |
|---|---|--:|--:|--:|--:|--:|---|
| Docker Desktop, Linux (fdatasync), fresh cluster | SSI | 77 | 293 | 3 804 | 78 | 0 | 1 000 000 ✓ |
| macOS native (`F_FULLFSYNC`), 3 runs | SSI | 40–50 | 596–737 | 1 855–2 340 | 37–39 | 0 | 1 000 000 ✓ |
| macOS native, n2 `kill -9`ed at 10 s, restarted at 18 s | SSI | 53 | 561 | 1 897 | 89 | 0 | 1 000 000 ✓ |
| macOS native, after that restart, 2 runs | SI | 52 | 589–594 | 1 714–1 784 | 30–33 | 0 | 1 000 000 ✓ |

Every write is a Raft round trip with an fsync on two of three nodes. An
uncontended transfer makes 4 of them in sequence (two intents, the record,
the resolves) plus 5 reads and refreshes. That puts a single transfer near
200 ms on macOS, where each `F_FULLFSYNC` costs about 5–10 ms, and makes
throughput fsync-bound. Pipelining intent writes and committing in parallel
with them (CockroachDB's parallel commits) would remove most of those round
trips; they are not built yet.

One Docker run went badly. After killing and restarting a container, later runs
fell from 77 to 3 txn/s, and the nodes kept holding elections (terms passed
100). Soon after, the Docker daemon itself stopped. The same kill and restart
on native processes recovered within a second, and later runs did not slow
down, so this looks environmental. It is not proven, though, and it is noted
here to check again when Docker runs return in M9.

## 5.7 What broke on the way

Every bug below was found by the tests above, or by the bench in §5.6, in this
order, before anything was committed. Bugs 1–7 surfaced on the fixed seeds. Bug 8 needed a sweep of
random seeds. Bug 9 needed a real cluster.

1. **The timestamp cache pushed writes into the future.** A new leader does not
   know which reads its predecessor served, so it sets the cache's low-water
   mark. Setting it to `now + max_offset` moved every write 250 ms ahead, and
   non-transactional reads then looked stale. Fix: Raft messages now carry the
   sender's HLC (so a new leader's clock is already past every read its
   predecessor served), the low-water mark is `now`, and non-transactional reads
   read the latest version.
2. **Lost updates under SI.** A write checked for newer versions above its
   `write_ts`, but the snapshot was at `read_ts`. A write committed in between
   went unseen. Fix: before committing above `read_ts`, refresh the written keys
   (SI) or the written and read keys (SSI).
3. **Stale replies fed a new attempt.** After a restart, a late reply to the
   previous attempt's read was taken as the new attempt's read. Fix: requests
   carry tags, and replies to tags the current attempt is not waiting for are
   dropped.
4. **A resent write moved its own intent up.** `TxnPut` is resent on timeout. The
   second copy was evaluated against a newer timestamp cache and rewrote the
   intent at a higher timestamp, while the coordinator, which ignored the
   duplicate reply, still committed lower. A reader between the two timestamps
   would skip an intent that then committed below it. Fix: rewriting your own
   intent keeps its timestamp. (Found by reading the code while chasing the next
   bug; the audits kept failing until that one was fixed too.)
5. **The timestamp cache was updated when a read was served, not when it
   arrived.** An audit saw 492 instead of 500. Dumping every version showed a
   transfer that read `acct02 = 72` although `80` had been committed below its
   read timestamp. A write evaluated while the read waited for its write
   barrier landed below it. A `Refresh` also recorded nothing, so a write could
   still land inside a window just validated. Fix: record on arrival, and record
   refreshes as reads of their span at `to`.
6. **Restarted transactions looked abandoned.** A restart kept the original
   start time, and pushers judge liveness by it. After 2 s of retries, every
   push aborted the transaction outright, which caused a livelock. Fix: a
   restart is a fresh record with a fresh start time.
7. **Starvation.** One SSI transaction gave up after 31 restarts. Its priority
   rose with each restart, but readers' timestamp pushes ignored priority: any
   reader could push it, and its refresh then failed. Fix: a timestamp push
   succeeds only for a higher priority; otherwise the reader waits.
8. **A snapshot labelled older than its data.** A 120-seed sweep found an audit
   that saw 489 under SI, in 1 seed out of several hundred. The version dump
   showed `acct01 = 40` committed at 16.0 s, but a read at 18.5 s returned the
   older 29. The three nodes' engines disagreed: n1 had the version and n2 and
   n3 did not, at the same applied index. Raft gives every node the same log, so
   the state machine itself had diverged.

   To find where, the tests now run a **divergence checker**
   (`store::divergence`). Every node fingerprints each applied entry together
   with the engine writes it produced, per (range, index), and a node that
   applies a position differently panics with the recent replica events:

   ```text
     event: n2 reopens r1099511627777 ["acct04", ∞) gen 4 on {1, 2, 3}        ← n2 restarted
     …
     event: n1 installs a snapshot of r1099511627777 ["acct04", ∞) gen 4 on {1, 2, 3} at index 134 (was at 99)
   replica divergence: n1 applied range 1099511627777 index 135 differently from n2:
   term 7 Ok(Resolve { key: "acct04", txn_id: 2376…8024, commit: false, commit_ts: 1793923212.431218240,4 })
     writes [Put { key: "lapplied…", value: 135 }]          ← n1: no intent to remove; n2 had removed one
   ```

   (This run was recaptured later, with the fix reverted, on seed
   6775913158062310169; the first one had shown the same thing at index 328.)
   The snapshot labelled index 134 already contained the effect of entry 135:
   the intent that entry removes was already gone. Its sender, n2, had
   restarted: the store reloads its own applied index
   (say 330) from the engine, but Raft's `applied` restarted at the last
   compaction point. The node became leader and served a snapshot request
   before the two caught up, so Raft labelled the snapshot with its lower index
   while the data came from the engine at 330. The receiver then re-applied
   entries on top of state that already contained them. Here a `Resolve` found
   nothing to resolve; in the first run a `Put` met its own future version
   (`WriteTooOld`) and wrote nothing.

   Fix: on open, the store hands Raft its applied index (`restore_applied`,
   which also raises the commit index, since applied entries were committed),
   and snapshots are only generated when the two agree. Like the bugs in
   chapter 04, this one lives in the seam between Raft and the state machine.
   Raft's linearizability tests could not see it: the Raft layer was right.
9. **A write queued behind a read: only on the real cluster.** Every simulated
   test passed, yet `stow-bench bank` against three real `stowd` processes lost
   money in every run: 20 accounts and 32 concurrent transactions ended at a
   total of 19 924 instead of 20 000. The bench replays every committed
   transfer in commit-timestamp order and reports any whose reads its
   predecessors did not leave:

   ```text
   anomaly: transfer 4->7 of 76 committed at 1794003813.348549000,13 read (1173, 960), replay has (1085, 960)
       last writer of 4: Some("4->18 88 at 1794003813.348549000,11 read (1173, 1027)")
   ```

   This is a lost update. Two transfers both read 1173 from account 4, and both
   committed. A fresh cluster with a single range and a stable leader (term 6,
   one election) still lost money, which ruled out leader changes. The real
   difference was batching. The server handles up to 4 096 queued inputs before
   one `flush()` proposes the writes among them; the simulated node flushed
   after every message. A `TxnPut` gets its timestamp from the timestamp cache
   when it *arrives*, then waits in the batch. A read arriving next records a
   higher timestamp and waits for `raft.last_index()`. That barrier did not
   include the queued write, so the read missed a write that then committed
   below it.

   Fix: a read that finds writes queued on its replica proposes them first, so
   the barrier covers them. The simulated node now batches the way the server
   does: inputs arriving within 0.5 ms share one flush. With that change and the
   fix reverted, three of the five transaction tests fail within seconds, and
   the Elle checker reports the anomaly as a G-single cycle. The lesson: a
   simulator proves only what its model of the runtime includes.
