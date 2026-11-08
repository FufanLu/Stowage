# ADR 0010: Enterprise operations: security, backup, quotas, observability

- Status: accepted
- Date: 2026-11-08

## Context

The job descriptions behind this project (Aurora, DynamoDB, Spanner/AlloyDB,
CockroachDB-style teams) ask for more than a correct engine. An operator must be
able to:

- **secure it**: encrypted connections, password authentication that does not
  send passwords, least-privilege grants, an audit trail;
- **recover from mistakes**: backups and point-in-time restore, because
  replication copies a bad `DELETE` as faithfully as a good `INSERT`;
- **protect it from its clients**: one tenant or one runaway job must not take
  the cluster down;
- **see inside it**: metrics, alerts with runbooks, per-statement statistics,
  slow-query logs;
- **upgrade it** without downtime.

## Decision

**Security.**
- TLS for pgwire through rustls (ring provider), negotiated with Postgres'
  `SSLRequest`; `--pg-require-tls` refuses plain connections.
- SCRAM-SHA-256 (RFC 5802/7677) for passwords. The server stores salted keys
  only (`0x01 "user/<name>"`), never the password, and the exchange never
  sends it. Authentication turns on when the first user exists,
  so a fresh cluster is usable by `psql` without setup.
- Postgres-style roles: `CREATE USER`/`ALTER USER`/`DROP USER`, `SUPERUSER`,
  `GRANT`/`REVOKE` of SELECT/INSERT/UPDATE/DELETE per table. The binder checks
  SELECT on every table a query reads (subqueries and joins included); DML
  checks its own privilege; DDL requires a superuser. The user is re-read for
  each statement, so a `REVOKE` applies to open sessions immediately.
- An append-only audit log (JSON lines) of logins, failures, DDL and grants.

**Backup and PITR.** A backup is a changefeed window (ADR 0009): every version
committed in the span in `(from, to]`. Windows are closed, so adjacent
incremental backups miss nothing and repeat nothing, and restoring to any
time `T` inside the chain gives the snapshot a transaction at `T` would read.
Files are a sorted data file plus a manifest with a CRC32C, written
data-then-manifest with a rename, so a partial backup is never mistaken for one.

**Quotas and admission control.**
- Node level: when more than 20 000 client requests wait for the store thread,
  new ones get `Busy`, which clients retry with backoff. Shedding at the door
  keeps queued work bounded instead of letting latency grow without limit.
- User level: `CONNECTION LIMIT n` per user (SQLSTATE 53300 beyond it).
- Table level (DynamoDB API): provisioned `ReadCapacityUnits` /
  `WriteCapacityUnits` become token buckets that refill at the provisioned rate
  and hold 5 seconds of burst, as DynamoDB does; excess requests get
  `ProvisionedThroughputExceededException`, which every SDK retries.

**Observability.**
- One process-wide registry (`stowage_common::metrics`) in the Prometheus text
  format, served by `stowd --http-listen` at `/metrics`, with `/health` and
  `/status` (this node's ranges, leaders and terms). Counters cover Raft
  (elections, log sync latency), KV (proposals, batches, shed requests,
  snapshots, splits), SQL (statements and errors by SQLSTATE) and the DynamoDB
  API (requests, latency, throttles).
- `stowage_stat_statements`, like `pg_stat_statements`: calls, total/mean/max
  time, rows and errors per statement fingerprint (literals replaced by `?`).
- A slow-query log with per-stage timings (begin, bind, optimize, execute, commit).
- Alert rules, each naming a runbook in `docs/runbooks/`, and a Grafana
  dashboard, both in `deploy/`.

**Upgrades.** Nodes restart one at a time (`scripts/rolling-upgrade.sh`),
waiting until the restarted node reports every range with a leader. Three
replicas keep a quorum through each restart.

## Consequences

- Each feature reuses a layer below it rather than adding machinery: backups
  are changefeeds, privileges are catalog keys read in the statement's own
  transaction, the GSI and backup consistency arguments are the same one.
- Token buckets and connection slots are **per node**. A client spreading load
  over three nodes gets up to three times its quota. A cluster-wide quota needs
  a shared counter (a range per tenant, or leases of quota from a coordinator).
- Restores write the past state into an empty cluster with fresh timestamps;
  they do not reproduce history. Restoring in place, and protected timestamps
  for when MVCC GC exists, are future work.
- The audit log and slow log are local files. Shipping them is the platform's
  job (a sidecar, or the tracing target into OpenTelemetry).
- mTLS between nodes is not done: the Raft transport is plain TCP and should run
  on a private network.

## Alternatives considered

- **Snapshot backups (copy each range's storage files).** Faster for large
  data, but consistent across ranges only with a cluster-wide pause or a
  timestamp-consistent snapshot, which is the changefeed again. Point-in-time
  needs the log anyway.
- **MD5 password authentication.** What older Postgres clients default to. It
  is broken and replayable; every current driver speaks SCRAM.
- **Queue without bound and let latency grow.** Simple, but under overload
  every request becomes slow and clients time out after the work is done.
  Shedding early wastes nothing.
- **OpenTelemetry metrics SDK.** Heavier dependency tree for a text format
  that Prometheus, Grafana Agent and the OTel collector all scrape directly.
