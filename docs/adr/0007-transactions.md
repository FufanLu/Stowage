# ADR 0007: Transactions: HLC, MVCC intents, transaction records, SI and SSI

- Status: accepted
- Date: 2026-11-06

## Context

Both the SQL layer (M5) and the DynamoDB layer (M6, `TransactWriteItems`,
condition expressions) need multi-key, multi-range transactions. The candidates:

- **Percolator / TiDB**: a central timestamp oracle (TSO), locks in a column,
  primary-lock commit. Simple, but the TSO is on every transaction's path, and we
  dropped the PD service in ADR 0006.
- **Spanner**: TrueTime, with commit-wait for external consistency. Needs bounded
  clock uncertainty from hardware that we do not have.
- **CockroachDB**: HLC timestamps with no central oracle, write intents, a
  transaction record on one range, read refresh, and SSI by default.
- **Calvin / FaunaDB**: deterministic ordering. Needs every transaction's read and
  write sets up front, which interactive SQL does not have.

## Decision

Follow CockroachDB's model, simplified.

1. **Timestamps are HLC values** (`wall nanos, logical`). Every Raft message
   carries the sender's clock, and nodes update on every message and request. A
   leader assigns each write a timestamp ≥ the transaction's `write_ts` and above
   the timestamp cache.
2. **MVCC in the one engine** (ADR 0006): `'d' ‖ esc(key) ‖ !ts` for versions,
   `'i' ‖ esc(key)` for at most one intent per key, and `'x' ‖ esc(anchor) ‖ id`
   for transaction records. All three sort into the owning range, so splits and
   snapshots need no special cases.
3. **The commit point is one Raft command**: `EndTxn` flips the record on the
   anchor's range to `Committed`. Resolving intents happens afterwards, and
   anyone can do it, the coordinator or any transaction that runs into an
   intent and finds the record decided.
4. **A timestamp cache per leader** records the highest read timestamp per key
   (points) and per span (scans, refreshes). It is updated when a read
   *arrives*, and writes below it move above it. A read then waits until every
   write that arrived before it has applied, including writes still queued for
   the next proposal batch, which it proposes at once. On a leader change the
   cache resets to a low-water mark of the new leader's `now`.
5. **Conflicts are settled by pushing the record**, wound-wait by priority:
   - a reader that meets an intent below its timestamp pushes the owner's
     `min_commit_ts` above its read if it has the higher priority, and waits
     otherwise;
   - a writer that meets an intent aborts the owner if it has the higher
     priority, and backs off otherwise;
   - a record without a heartbeat for 2 s can be aborted by anyone.

   Priorities are random and rise by 2²⁰ on every restart, so no transaction
   starves.
6. **Isolation is per transaction.** Under *SI* a transaction reads at `read_ts`;
   if it commits above that, it first refreshes its written keys (first committer
   wins). Under *SSI* it refreshes every key it read as well. A refresh fails if a
   version was committed in `(read_ts, commit_ts]` or a foreign intent sits at or
   below `commit_ts`. On failure the transaction restarts with a new id.
7. **The coordinator is a pure state machine** (`TxnCoord`), like Raft
   (ADR 0005). The simulator drives it with resends, timeouts, partitions and
   crashes, and the real client drives the same code. Every request is
   idempotent, so a driver may resend anything.
8. **Correctness is checked by an Elle-style list-append checker** (`stowage-sim::elle`)
   that classifies dependency cycles (G0, G1c, G-single, G2), plus a
   bank-transfer invariant and a deterministic write-skew test. CI asserts no
   cycles at all under SSI, and nothing but G2 under SI.

## Changes from the original plan

- **No separate `txn` crate.** MVCC evaluation must run inside Raft apply, so it
  lives in `stowage-kv` (`mvcc.rs`, `eval.rs`, `txn.rs`).
- **No wait queue or deadlock detector.** Wound-wait priorities with bounded
  backoff cannot deadlock: the higher priority always wins, and waiters retry. A
  waits-for graph would cut latency under contention and is future work.
- **Records are written lazily.** A transaction that never meets a conflict has
  no record until `EndTxn`. A push against a missing record treats it as
  `Pending` since the transaction's start.

## Consequences

- No central oracle: a transaction touches only the ranges it uses. The price
  is that wall-clock skew beyond the HLC's reach makes transactions restart. There
  is no read uncertainty interval yet, so a node whose clock is far behind can
  give a transaction a snapshot that misses a write that just committed
  elsewhere. That violates only real-time order (it is not strict
  serializability); the checkers above do not test for it.
- Every write costs a Raft round trip, and so do the commit and the resolves.
  An uncontended two-key transfer takes 9 round trips (chapter 05). Pipelining
  intent writes and parallel commits (CockroachDB's) are the known
  optimizations.
- The timestamp cache does not store transaction ids, so a transaction's own
  reads push its writes and cost a refresh that always succeeds.
