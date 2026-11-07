# ADR 0008: The SQL layer: Postgres dialect, a Selinger optimizer, Arrow execution

- Status: accepted
- Date: 2026-11-07

## Context

The key space (ADRs 0004–0007) is ordered, replicated and transactional. A SQL
front end must:

- run the DDL and DML that ORMs and migration tools generate (Flyway in
  Conveyor, Exposed in Milkrun);
- speak a protocol existing drivers already implement;
- plan joins well enough that a four-table query does not read the database
  four times.

## Decision

1. **Postgres dialect and protocol.** `sqlparser` (`PostgreSqlDialect`) parses.
   The wire protocol is written by hand (`stowage-pgwire`, about 500 lines): it
   is small, the frames are then ours to trace (chapter 07), and it avoids an
   external server framework. Every `stowd` is a SQL gateway; any node accepts
   any query.
2. **Tables are key ranges.** Rows are keyed by `0x02 ‖ table id ‖ index id ‖
   memcomparable(primary key)`, with secondary indexes in the same layout.
   Non-unique entries append the primary key; unique ones map their columns to
   the primary key. A table without a primary key gets a hidden random `rowid`
   (rather than a counter, which would serialize every insert). Descriptors live
   at `0x01 "table/" ‖ name`, so DDL is transactional like everything else.
3. **One interactive transaction per statement or block.** `TxnCoord` gained an
   interactive mode (ADR 0007): operations arrive one at a time, and a conflict
   that needs a restart surfaces as SQLSTATE 40001. Autocommit statements retry
   it internally, up to 20 times. Inside `BEGIN … COMMIT`, the client retries,
   which is what CockroachDB asks of clients too. From its first write, an open
   transaction heartbeats its record in the background, so a session idle
   between statements is not mistaken for an abandoned one.
4. **Plans reference columns by id**, unique per statement, not by position.
   Pushdown and join reordering then never rewrite expressions.
5. **Optimizer: rules plus a cost-based join order.**
   - Predicate pushdown respects outer-join semantics.
   - Equality conditions become hash-join keys.
   - Inner joins are reordered by dynamic programming over subsets (bushy,
     avoiding cross products, up to 10 relations), on cardinalities from
     `ANALYZE`: rows, distinct values, NULLs and histograms.
   - Each scan picks the primary key or a secondary index from equalities on a
     key prefix plus one range.

   This is Selinger's System R search, not a Cascades memo. With few
   transformation rules, a memo would add machinery without changing any plan
   here.
6. **Execution on Arrow, operator-at-a-time.** Every operator produces one record
   batch. Expressions use Arrow compute kernels, hash join and hash aggregation
   key on Arrow's row format, and sorts use `lexsort`. Only scans talk to the
   key-value layer.
7. **Errors are Postgres errors.** Every error has its SQLSTATE (23505, 40001,
   42P01, …), because drivers and ORMs act on the code, not the text.

## Changes from the original plan

- **No DistSQL yet.** Filters and aggregates run at the gateway, after rows
  cross the network. Pushing them to range leaders needs expression evaluation
  inside the KV layer and partial aggregation; chapter 00 measures what it
  would save.
- **No Cascades-lite memo** (decision 5).
- **The SQLite sqllogictest corpus is not run.** Stowage's own `.slt` files use
  its format, and the same files run on an in-memory store and on a real
  three-node cluster in CI.

## Consequences

- Statement latency is dominated by sequential key-value round trips: four
  catalog reads before planning, then one per scan (chapter 00). Descriptor
  caching and concurrent scans are the cheap wins.
- Each `INSERT` row costs a primary-key existence check plus one Raft write per
  index entry. Bulk loads are slow (1 000 rows in about 30 s on macOS with
  `F_FULLFSYNC`) until writes are pipelined (ADR 0007).
- Operators materialize whole batches, so memory scales with the largest
  intermediate result. Streaming batches and spilling are future work. A cross
  product above 50 million rows is refused rather than attempted.
- `SERIAL` is a transactional counter, so concurrent inserts into one table
  conflict on it and retry. Postgres sequences are non-transactional precisely
  to avoid this.
