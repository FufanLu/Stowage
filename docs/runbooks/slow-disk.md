# Slow disk

**Alert:** `StowageSlowFsync`: p99 Raft log sync above 100 ms on one node for
five minutes.

## What it means

A write commits when a quorum has fsynced it. With one slow disk out of three,
the other two still form a quorum, so writes are slower only when the slow
node is the leader (it fsyncs before counting its own vote). With two slow
disks, every write is slow.

For reference, `F_FULLFSYNC` on an Apple SSD takes 4–10 ms; a cloud volume
with provisioned IOPS 1–3 ms; anything over 100 ms at p99 is abnormal.

## Steps

1. Confirm on the host: `iostat -x 5` (Linux) shows `await` and `%util`.
2. Is something else using the disk? Backups (`stowctl backup`) read history
   but write only to the backup directory; put that on a different volume.
3. Move leadership away while you investigate: stop the node
   (`docker compose stop n2`). Its ranges elect leaders elsewhere within a
   second, and the cluster runs on two replicas. Restart it once the disk is
   fixed; it catches up from the log or a snapshot.
4. Cloud volumes: check whether burst credits ran out.

Do **not** turn off fsync to make the alert go away. A write acknowledged
without fsync can be lost by a power failure on two nodes, which Raft cannot
recover from.
