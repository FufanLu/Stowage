# Runbooks

Each alert in [`deploy/prometheus/alerts.yml`](../../deploy/prometheus/alerts.yml)
names one of these. They assume the compose layout (nodes `n1`–`n3`, admin HTTP
on 9481–9483, SQL on 5481–5483); for other deployments substitute addresses.

| Runbook | Alerts |
|---|---|
| [node-down](node-down.md) | `StowageNodeDown`, `StowageNoLeaders`, `StowageGsiLag` |
| [election-storm](election-storm.md) | `StowageElectionStorm` |
| [slow-disk](slow-disk.md) | `StowageSlowFsync` |
| [contention](contention.md) | `StowageSerializationFailures` |
| [security](security.md) | `StowageAuthFailures` |

Three tools cover most diagnosis:

```text
curl -s localhost:9481/status | jq '.ranges[] | {id, leader, term, replicas}'   # one node's view of its ranges
curl -s localhost:9481/metrics | grep -v '^#'                                   # raw counters
psql -p 5481 -c "SELECT query, calls, mean_ms, max_ms, errors FROM stowage_stat_statements ORDER BY total_ms DESC LIMIT 10"
```
