# Contention: serialization failures

**Alert:** `StowageSerializationFailures`: clients get SQLSTATE `40001` more than
once a second for five minutes.

## What it means

Stowage runs transactions at SERIALIZABLE by default (chapter 05). When two
transactions conflict, one of them is told to retry with `40001`
(`restart transaction`). Drivers and ORMs that follow Postgres conventions
retry automatically; ones that do not surface the error to the user. Some
retries are normal. A sustained rate means many transactions touch the same
rows.

## Steps

1. Which statements?
   ```sql
   SELECT query, calls, errors, mean_ms FROM stowage_stat_statements
   ORDER BY errors DESC LIMIT 10;
   ```
2. Which rows? Look at those statements. The usual patterns:
   - **A counter row** that every transaction increments (`UPDATE stats SET n = n + 1`).
     Shard it into N rows and sum on read, or use a sequence.
   - **Read-then-write without the row lock**: `SELECT` a row, compute, then
     `UPDATE` it. Use `UPDATE … SET x = x - 10 WHERE … ` in one statement so
     the read and write happen at the same timestamp.
   - **Long transactions** that read many rows, then write. Any write to a row
     they read forces a refresh at commit, which fails if the row changed.
     Keep transactions short; move reads that do not need to be consistent
     out of the transaction.
3. The client: confirm it retries `40001`. pgjdbc does not by itself; Spring's
   `@Retryable` or a loop around the transaction does.
4. The slow log (`stowd --slow-log`) records statements slower than the
   session's `slow_query_ms` with their per-stage timings, including time spent
   waiting on another transaction's intent.
