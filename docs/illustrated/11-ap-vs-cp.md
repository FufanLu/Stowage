# 11. AP vs CP

> Data in this chapter comes from `bench/ap-vs-cp.sh A 30` (YCSB workload A
> against [Cartwheel](https://github.com/FufanLu/Cartwheel) and Stowage, three
> processes each on one laptop) and `scripts/chaos.sh 60 4242`. Raw per-second
> output is in [`bench/out/`](../../bench/out/). Hardware: Apple M3 Pro
> (12 cores), 18 GB, APFS, macOS. Design: [ADR 0011](../adr/0011-contention-and-group-commit.md).

Cartwheel and Stowage were written by the same person to make opposite choices.
Cartwheel is **AP**: leaderless, N = 3, R = W = 2, a sloppy quorum with hinted
handoff, so any node accepts any write and nothing waits for an election.
Stowage is **CP**: every range is a Raft group, and every write is
acknowledged only when a majority has it on disk. On top of that, here each
update is a serializable transaction. This chapter runs the same workload
against both, then breaks one node of each.

## 11.1 The workload

YCSB workload A is 50 % reads and 50 % updates over 50 000 keys. Keys are
`user` + 16 hex digits, chosen with zipfian(0.99), so a few keys are very hot.
There are 32 clients and 100-byte values. Both drivers use the same key
function and the same inverse-CDF zipf table, so the hot keys are the same
keys on both systems:

| | Cartwheel (`wheel-bench`) | Stowage (`stow-bench ycsb`) |
|---|---|---|
| read | quorum read (R = 2) | linearizable read (leaseholder) |
| update | quorum read, then a quorum write (W = 2) carrying the causal context it read | `GetForUpdate k; Put k` in one **serializable** transaction |
| durability of an acknowledged write | in memory on 2 replicas (no fsync by default) | fsynced on 2 of 3 replicas (`F_FULLFSYNC` on macOS) |

That last row matters so much that Stowage also runs a third time, with
`--unsafe-skip-fsync`: the same consensus and the same transactions, but
Cartwheel's durability. This separates the cost of consensus from the cost of
the disk.

Faults (`fault()` in the script):
- **kill**: `SIGKILL` node 2 at t = 10 s, restart it at t = 20 s.
- **pause**: `SIGSTOP` node 2 at t = 10 s, `SIGCONT` it at t = 20 s. To its
  peers a frozen process looks like a partition: its TCP connections stay
  open, but nothing answers.

## 11.2 Results

```text
30 s each · workload A · 32 clients · 50 000 keys zipfian(0.99)

                     healthy                 kill node 2               pause node 2
                     ops/s    p99   p99.9    ops/s    p99   p99.9      ops/s    p99    p99.9   errors
Cartwheel (AP)       15 353   10 ms   21 ms  17 668    9 ms   20 ms   10 293   11 ms  508 ms   11 timeouts
Stowage, no fsync    12 221   35 ms  125 ms   9 896   48 ms  144 ms    9 353   44 ms  145 ms    0
Stowage (CP)            196  3.7 s   12 s       228  3.0 s  9.3 s       218  3.1 s  8.6 s     0
Stowage, blind put      732   69 ms   78 ms
```

Three things stand out.

1. **The disk, not consensus, is the expensive part.** With Cartwheel's
   durability, Stowage does 12 k ops/s against Cartwheel's 15 k, even though
   every update is a serializable transaction through Raft. With an
   `F_FULLFSYNC` on two of three replicas per write, it does 196. On this
   laptop a full flush takes about 11 ms (`stowage_raft_log_sync_seconds`),
   and a locked read-modify-write needs four of them in sequence: lock, put,
   commit record, resolve.
2. **Hot keys are what hurts a CP store.** Blind puts, with no transaction,
   do 732 ops/s with a p99 of 69 ms. Transactions on the same keys do 196 ops/s with a
   p99 of seconds. The hottest zipfian(0.99) key gets 8.3 % of all
   operations, and its updates take turns through the lock queue
   ([11.4](#114-the-first-run-26-opss)). Cartwheel never takes turns: two
   concurrent updates of one key both succeed and are kept as **siblings**
   (dotted version vectors), and the next reader has to reconcile them.
   Stowage's serializable transactions decide which update comes first
   instead.
3. **Stowage reported no errors under either fault; Cartwheel reported 11
   timeouts while a node was frozen.** The CP system was not "less available"
   in the sense of failing requests; it was slower for a few seconds,
   as 11.3 shows.

## 11.3 Second by second

Each bar is one second. Height is ops/s relative to that run's own peak.
The fault starts at second 11 and ends at second 21.

```text
                    kill node 2 at 10 s, restart at 20 s
Cartwheel           ▅▅▆▆▅▆▆▆▆▆██▇▇▇██▇▇▇▇▅▅▅▅▆▆▅▅▆  peak  23172/s, min 13407/s
Stowage, no fsync   ▇██▇██▇███▆▆▆▅▇▇▇▇▇▇▆▄▄▃▄▅▅▆▅▅  peak  12594/s, min  5288/s
Stowage             █▇▇▅▃▄▄▅▄▄▆▆▅▅▅▆▇▇▆▅▆▆▅▆▃▂▃▃▅▄  peak    356/s, min    85/s

                    SIGSTOP node 2 at 10 s, SIGCONT at 20 s
Cartwheel           ▇▇▇▇▇███▇▇          ▆▆▆▆▇▇▇▇▇▇  peak  17883/s, min 72/s
Stowage, no fsync   ▇███████▇▇   ▆▅▆▇▇▇▇▅▅▅▅▅▅▅▆▅▅  peak  12920/s, min  0/s
Stowage             ▆▇▄▃▂▂▃▃▃▃▁  ▅█▇▆▅▅▅▄▃▃▄▅▅▄▃▃▃  peak    451/s, min  0/s
```

**Killing a node costs neither system much.** A killed process's sockets
close, so clients fail over at once. Cartwheel gets *faster*: 20–23 k
ops/s with two nodes, since every write now has exactly the two replicas
it needs. Stowage loses the leaders that were on node 2. The others time
out after 10–20 ticks of 10 ms and elect new ones from the two survivors.
Throughput dips by a quarter, but no second drops near zero.

**A frozen node is the hard case, and it hurts both systems, in different
ways.**
- **Cartwheel**: for the full ten seconds it falls to 72–94 ops/s. The
  frozen node is not "down": connections to it stay open and requests to
  it simply wait. Each of the 32 client threads soon ends up waiting on it.
  The p50 of 505 ms in two of those seconds and the 11 timeouts are those
  waits. Nothing is lost, but the AP store's always-on writes do the clients
  no good while their requests sit on a coordinator that does not answer.
- **Stowage**: about three seconds at zero (seconds 11–13), then back at
  80 % with two nodes for the rest of the pause. The election timeout is
  10–20 ticks of 10 ms, so the survivors can elect new leaders within about
  200 ms. Most of the gap matches the client's request timeout
  (`KvClient::timeout`, 3 s): requests already sent to the frozen leaders
  wait it out before being retried elsewhere.

After the fault, Stowage without fsync ran at 6–9 k ops/s for the last ten
seconds of both runs, below its 12 k before the fault, while node 2 caught up.
A separate run (`kill`, same setup, monitoring `/metrics` every second) was
back above 12 k one second after the restart and at 14 k a few seconds later.
So how fast it recovers varies from run to run, and this chapter does not
claim one figure for it.

## 11.4 The first run: 26 ops/s

The first time this benchmark ran, Stowage did **26 ops/s, with a p99 of 49
seconds**, and throughput fell during the run: 146 ops/s in the first second,
0–10 in the last ten. Four things were wrong
([ADR 0011](../adr/0011-contention-and-group-commit.md)):

```text
                                              before         after
load (32 clients, distinct keys)              460 writes/s   1 485 writes/s
blind update                                  468 ops/s        732 ops/s
serializable read-modify-write                 26 ops/s        196–248 ops/s
```

1. **The store thread fsynced inline.** One `F_FULLFSYNC` (≈11 ms) blocked all
   input, so batches stayed small. A leader also held its `Append`s until its
   own flush finished, so the leader's and the follower's fsyncs ran one
   after the other. Now a syncer thread flushes: every batch queued during
   one flush shares the next one, and a leader's `Append`s leave before its
   own flush, as in Raft thesis §10.2.1.
2. **Read-modify-write was optimistic.** On a hot key, someone else had
   nearly always committed between the read and the write, so the transaction
   restarted. Now `GetForUpdate` takes the write intent at read time.
3. **Waiters polled every 20 ms and aborted holders by random priority.** Now
   the leaseholder parks conflicting writes per key in arrival order and
   wakes them when the intent resolves. A waiter may abort the holder only
   after one lost round.
4. **A plain read failed on an intent** instead of waiting for the writer.

The simulator makes the effect of points 2–3 measurable without a disk. In
`hot_counter_locking_reads_restart_less`, 12 clients increment one counter 120
times. Optimistic reads need 445–513 restarts; locking reads, 0–20. Both lose
no update.

## 11.5 The rejoin storm

Killing a node and restarting it, without fsync, first looked like this:

```text
t   ops/s   node 1: snapshots sent   node 2: elections      (node 2 restarted at t = 20)
20  12550             4                     0
21   8181             9                     9
22   3510            12                    12
24     97            57                    15
25    904            91                    15
29     18            91                    15
```

Node 1 sent 87 snapshots of 3 ranges in five seconds. Each snapshot was the
whole range: 11–13 MB scanned and encoded on the store thread, and identical
copies went out 20–50 ms apart. Two bugs fed each other:

- **Rejections restarted snapshots.** The restarted follower was still
  rejecting the `Append`s sent before its snapshot. Each rejection moved the
  leader from "snapshot in flight" back to probing. The probe found the entries
  compacted, and so requested a new snapshot.
- **Compaction under a catching-up follower.** The leader compacted to its
  applied index every 1 000 entries. By the time a 12 MB snapshot was
  installed, the entries right after it were gone.

Generating snapshots kept the leader's store thread busy past the 100 ms
election timeout, so the other follower started elections as well. Now a
rejection during a snapshot is ignored, a snapshot is resent only after
500 ms, and `compaction_floor` keeps the log that a streaming or
snapshotting follower still needs. The same experiment afterwards:

```text
t   ops/s   node 1: snapshots sent   node 2: snapshots installed   elections (all nodes)
20  11595             6                       0                     unchanged
21   8014             6                       3                     unchanged
22  11917             6                       3                     unchanged
25  14034             6                       3                     unchanged
```

There were three snapshots, one per range, no elections, and full speed one
second after the restart.

### What the fixes exposed

Changing when snapshots and compaction happen also changed the timing of
everything else. Run against hundreds of random simulation seeds, the new
timing exposed four latent bugs in how replicas leave and rejoin ranges. The
simulator's replica-divergence checker caught the worst of them:

```text
replica divergence: n5 applied range 3298534883334 index 6 differently from n3:
term 7 Noop          ← n5
FIRST: term 6 Noop   ← n3, committed long before
```

- **A zombie committed a conflicting entry.** n5 applied a split long after
  the right-hand range had removed it, so it created that range afresh with
  the membership of long ago. n4 had also been removed and had since
  recreated an empty placeholder with no memory of its votes. It voted for
  n5, and the two formed a majority of the old membership. n5 then
  committed a different entry at an index the real range had already
  committed. Fix: a removed replica leaves a **tombstone** with the
  generation that removed it, every Raft message carries the sender's
  generation, and a node drops messages from generations older than its
  tombstone. **Replica GC** destroys a replica once gossip shows a newer
  generation without it.
- **A pending destroy outlived a re-add.** A replica that applied "remove me"
  and then "add me back" in one batch was still destroyed.
- **A stale `matched` after recreation.** A leader probed forever at an index
  the recreated follower no longer had.
- **An orphan led a range.** A replica reopened from a Raft log whose
  descriptor a crash had lost won an election and led a range it could not
  serve. Uninitialized replicas now vote but never campaign.

One existing test turned out to be wrong rather than the code.
`minority_side_cannot_commit` asserted that a partitioned minority commits
nothing new. With asynchronous durability, the old leader may count its own
copy of an entry only after its fsync finishes, which can be after the
partition began, and then legitimately commit an entry two majority nodes
already had. The code before this change failed the test on 7 of 20
alternative seeds. It now checks the real invariant: no commit beyond what
some majority-side node held.

## 11.6 Correctness under chaos

Throughput is half the story; the other half is whether the answers stay
right. `scripts/chaos.sh` runs serializable bank transfers (1 000 accounts, 16
clients, audits mixed in) against three nodes, with fsync on. Meanwhile a
nemesis randomly SIGKILLs and restarts a node or SIGSTOPs it, eight times in
60 s:

```text
seed 4242, 60 s
t=3s SIGKILL node 3, restart after 3s
t=8s SIGSTOP node 2 for 5s
…
t=49s SIGSTOP node 3 for 3s
t=52s nemesis stops
replayed 2922 committed transfers: 0 read a state their predecessors did not leave

ssi · 1000 accounts · 6 ranges · concurrency 16 · 60 s
| txn/s | transfers | audits | restarts | failed | p50 ms | p99 ms | p99.9 ms |
|    54 |      2922 |    322 |       59 |      0 | 187.56 | 3214.93 |  3405.77 |
final audit: total 1000000 (expected 1000000), missing accounts in audits: 0
```

There are two checks.
- **Conservation**: one snapshot read of all 1 000 accounts sums to the
  funded total.
- **Serial replay**: replaying the 2 922 acknowledged transfers in commit
  timestamp order, every one read exactly what its predecessors left.

Both hold, with no failed transaction. Process faults are the only faults
here. Packet loss, delay and asymmetric partitions are exercised by the
deterministic simulator (chapter A), where the Elle-style checker and the
linearizability checker run on every seed.

This chapter does not check Cartwheel's answers. Its design does not claim
serializability: concurrent updates of one key are kept as siblings for the
application to merge, which is the AP trade. No update is lost, but
nothing orders them either.

## 11.7 Reproduce

```bash
CARTWHEEL=~/Cartwheel bench/ap-vs-cp.sh A 30     # ~10 min; writes bench/out/*.txt
scripts/chaos.sh 60 4242                          # bench/out/chaos-bank-seed4242.txt
cargo run --profile dist -p stow-bench -- ycsb --help
```

The script uses ports 7301–7303 (Cartwheel), 7401–7403 (Stowage) and
93xx/949x for metrics, so it can run next to clusters on the default ports.
Numbers vary between runs by tens of percent on a laptop. Compare systems
within one run.
