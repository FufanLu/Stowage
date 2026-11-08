# Node down / no leaders

**Alerts:** `StowageNodeDown` (one stowd stopped answering `/metrics` for 30 s),
`StowageNoLeaders` (no range has a leader), `StowageGsiLag` (document writes
arrive but nothing applies).

## What it means

Every range has three replicas, so **one node down is not an outage**: each
range keeps a quorum of two, ranges whose leader was on the lost node elect a
new one within an election timeout (chapter 03), and clients retry through it.
Writes still need two fsyncs, so latency stays the same.

Two nodes down is an outage for every range: no quorum, no reads or writes.
Data is not lost (each committed write is on at least two disks), but the
cluster waits until a quorum returns.

## Steps

1. Which node? The alert's `instance` label, or:
   ```text
   for p in 9481 9482 9483; do curl -fsS -m 2 localhost:$p/health || echo "$p down"; done
   ```
2. Is it the process or the machine? `docker compose ps`, `docker compose logs --tail 100 n2`.
   - Crashed with a panic: keep the log (it names the invariant that failed) and
     open an issue; restart with `docker compose up -d n2`. Restart is safe: the
     node replays its Raft log and its applied index from disk (chapter 02).
   - Disk full: free space or grow the volume, then restart. A node never
     acknowledges a write it has not fsynced, so a full disk stops it rather than
     losing data.
3. After restart, confirm it rejoined: on the restarted node `/status` should
   show every range with a `leader`, and on the others `stowage_kv_snapshots_sent_total`
   may rise if it was far behind (a snapshot instead of log replay).
4. `StowageNoLeaders` with all three nodes up means they cannot talk to each
   other. Check the peer addresses (`--peers`) and the network between nodes
   (port 7001). See [election-storm](election-storm.md).
5. `StowageGsiLag`: the GSI and stream worker holds a 3-second lease; if the
   node that held it died, another takes over after the lease runs out. If
   lag persists with all nodes healthy, check the logs for `gsi worker` errors.

## Replacing a node for good

Start a new node with the same `--node-id` and `--peers` and an empty data
directory. The leaders send it a snapshot of each range. Do not start a new
node with the old node's id while the old one may still be running: two
replicas with the same identity can vote twice.
