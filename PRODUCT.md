# Product

<!-- impeccable:product-schema 1 -->

> Inferred from the repository and the console brief; the owner asked for
> autonomous work without check-ins. Items marked *(inferred)* were not
> confirmed in an interview.

## Platform

web

## Stack

React + TypeScript + Vite, in `web/console`, served by `stowd`'s admin HTTP
listener (the plan in `docs/` fixed the stack before this record existed).

## Users

Database operators and SREs running a Stowage cluster, and engineers
evaluating it. They open the console when something looks wrong (an alert from
`deploy/prometheus/alerts.yml`, a slow query) or to understand how data is laid
out across nodes. They know Raft, ranges and SQL plans; they do not need
concepts explained, they need state shown accurately. *(inferred)*

## Product Purpose

Stowage is a distributed SQL + NoSQL database in Rust: Multi-Raft ranges,
serializable transactions, a Postgres wire protocol and a DynamoDB-compatible
API over one transactional key space. The console makes the cluster's internal
state visible: which ranges exist, which node leads each, how the key space is
split, what a query plan does, which statements cost the most.

## Positioning

One key space, ordered and split into ranges, serving both SQL rows
(`0x02…`) and DynamoDB items (`0x03…`). The console can show that literally:
the range map is the key space, with SQL tables and document tables side by
side. A console for a SQL-only or KV-only store could not show that.

## Operating Context

- Opened from a laptop during an incident or a demo, next to a terminal running
  `psql`, `stowctl` and Grafana.
- Data comes from the node's own admin HTTP: `/status` (ranges with leader,
  term, replicas, generation, bytes), `/metrics` (Prometheus text), `/health`,
  and a SQL endpoint that runs statements as an authenticated user.
- A three-node cluster on one machine is the common case; tens of ranges, not
  thousands.

## Capabilities and Constraints

- Read-only views of cluster state, plus a SQL shell that can run anything the
  user's privileges allow (`EXPLAIN [ANALYZE]` included).
- `EXPLAIN` returns an indented text tree (`Project … → Scan … · span […]`)
  with estimates and, under ANALYZE, actual rows, time and KV requests.
- `stowage_stat_statements` gives calls, total/mean/max ms, rows and errors per
  statement fingerprint.
- No fault injection from the console yet. No multi-cluster view.

## Brand Commitments

- Name: Stowage (a fulfillment-centre term: stowing inventory into bins). Binaries
  `stowd`, `stowctl`, `stow-bench`.
- No Amazon or AWS marks; DynamoDB is named only to describe API compatibility.
- Documentation voice: plain, exact, numbers from real runs.

## Evidence on Hand

Real data only. Chapters in `docs/illustrated/` hold real plans, key hex and
timings; the console must show live cluster data, never invented metrics.

## Product Principles

1. Show the real state, with its units and its age. Never smooth over a missing
   leader or a stale read.
2. The key space is the spine: ranges, tables and items are positions on it.
3. Dense and calm: an incident is not the moment for decoration.
4. Every number links back to where it came from (`/status`, `/metrics`, a SQL
   view), so the console teaches the system's own vocabulary.
