# Election storm

**Alert:** `StowageElectionStorm`: more than one Raft election every two
seconds, for two minutes.

## What it means

A healthy cluster elects once per range at startup and after a node fails.
Repeated elections mean leaders are losing contact with followers for longer
than the election timeout: followers stop hearing heartbeats, start an
election, and the range is unavailable for writes until it completes. Pre-vote
(chapter 03) stops a single partitioned node from disrupting the others, so a
storm usually means a problem that affects several nodes.

## Steps

1. Where? `sum by (instance) (rate(stowage_raft_elections_total[1m]))`.
   One node: it is the problem (step 2). All of them: the network or the hosts
   (step 3).
2. A stalled node. The store thread runs each node's ranges; if it stalls on
   an fsync it cannot send heartbeats. Check `stowage_raft_log_sync_seconds`
   for that node ([slow-disk](slow-disk.md)) and the host's CPU steal and
   memory pressure.
3. Network. Packet loss or latency between nodes above the election timeout.
   From one container, `ping n2` and check for loss.
4. Overload. If `stowage_kv_queued_requests` sits near 20000, admission control
   is shedding load (`stowage_kv_requests_shed_total` rising) and the node is
   saturated. Reduce load or add capacity.

## Known case

M4 saw election storms under Docker Desktop on macOS with heavy disk I/O,
while the same cluster run natively elected once. Docker Desktop's file-sharing
fsync can take hundreds of milliseconds. Use named volumes (as the compose file
does), not bind mounts, for data directories.
