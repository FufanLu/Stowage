# 09. Online ADD INDEX

> Data in this chapter comes from
> `cargo nextest run --release -p stowage-sql -E 'binary(online_index)' --no-capture`:
> a real three-node cluster in one process, a 300-row table, three writers
> changing it throughout, and `CREATE INDEX` with batches of 25 rows. Code:
> [`dml.rs`](../../crates/sql/src/dml.rs) (`create_index_start`,
> `backfill_batch`, `create_index_finish`, `check_index`),
> [`session.rs`](../../crates/sql/src/session.rs) (`create_index_online`).
> Design: [ADR 0009](../adr/0009-cdc-and-documents.md).

An index on a busy table must be built **without stopping the writes**. Two
naive approaches fail:

- **One transaction** reads every row and writes every entry. It conflicts with
  every concurrent writer, so it either aborts them or restarts itself for as
  long as anyone writes. That is what Stowage did inside an explicit
  `BEGIN … COMMIT` block, and still does there.
- **Make the index visible, then fill it.** Readers that use it miss the rows
  not yet backfilled.

Google F1 ("Online, Asynchronous Schema Change", 2013) splits the change into
states that each differ from the next by one small step, so two nodes one state
apart never disagree in a harmful way. CockroachDB adopted that scheme. Stowage
uses the two states an *added* index needs:

```text
            write-only                                           public
  ┌───────────────────────────────┐                  ┌─────────────────────────────┐
  │ writes maintain the index     │   backfill done  │ writes maintain it          │
  │ reads ignore it               │ ───────────────► │ the optimizer may read it   │
  └───────────────────────────────┘                  └─────────────────────────────┘
        ▲ txn 1: descriptor gains the index           txn N: descriptor flips the flag
        │ txns 2 … N−1: backfill, 25 rows each
```

## 9.1 The timeline

```text
    106.7 ms  index 2 on items added write-only
   1625.6 ms  backfill batch 1: 25 rows (25 so far)
   3525.0 ms  backfill batch 2: 25 rows (50 so far)
   4339.4 ms  backfill batch 3: 25 rows (75 so far)
     …
  15989.0 ms  backfill batch 12: 25 rows (300 so far)
  16750.4 ms  backfill batch 13: 25 rows (325 so far)
  17504.8 ms  backfill batch 14: 25 rows (350 so far)
  17665.0 ms  backfill batch 15: 2 rows (352 so far)
  17867.7 ms  index 2 public after 352 rows in 15 batches
299 rows, 299 entries, 325 concurrent writes
```

The table started with 300 rows and ended with 299; 352 rows were backfilled.
The difference is the writers: they inserted rows ahead of the backfill cursor
(ids from 1000 up) and deleted rows behind it. Every one of the 325 concurrent
writes found the index in the descriptor and maintained it, whether or not the
backfill had reached that row yet.

Each batch is slow (0.6–1.9 s): its 25 puts are sequential Raft writes, and
three writers compete for the same rows. A batch that loses a conflict runs
again (chapter 05 explains the refresh that forces it). That is the price of
not blocking anyone. Pipelined writes (ADR 0007) would make batches much
cheaper.

## 9.2 Why it is correct

Four races, and what settles each:

| Race | Outcome |
|---|---|
| A writer changes a row the **current batch** has read | The batch is an SSI transaction. Its refresh over the rows it scanned fails, it restarts, and it re-reads the new value. It never writes the stale entry. |
| A writer changes a row **after** its batch committed | The writer sees the index (write-only) and replaces the entry itself: delete old, put new. |
| A writer **started before** the index existed, commits after | It read the old descriptor. The descriptor key is in its read set, so its refresh fails and it restarts with the new descriptor (SSI again). |
| Two rows with the same value under a **unique** index | A writer checks the entries that exist. The backfill finds an entry pointing at a different row and fails the build, which is rolled back (index removed, entries deleted), as Postgres fails `CREATE UNIQUE INDEX`. |

SSI does the coordination. No schema lease is needed, and no gateway has to wait
for others to catch up with a new version. The catch is in the third row: a
writer running at snapshot isolation does not refresh its reads, so it could
commit against the old descriptor and skip the index. Stowage's SQL sessions
default to SSI, and the gap is documented in ADR 0009.

## 9.3 How the test knows

`check_index` recomputes, from the table, the exact set of entries the index
should hold, and compares it with what is there: missing entries, stray ones,
and unique entries pointing at the wrong row. After the build: 299 rows, 299
entries, 0 problems. Then:

- `EXPLAIN` shows the optimizer reading `index items_color` now that it is
  public; it ignored the index while it was write-only;
- `count(*) WHERE color = 'red'` through the index equals the same count
  through a full scan.

Does the test catch a broken build? With the writers changed to skip
write-only indexes (`for ix in t.indexes.iter().filter(|i| i.public)` in the
three write paths), it fails:

```text
299 rows, 341 entries, 262 concurrent writes
160 problems, e.g. Some("missing entry [02, 00, 00, 00, 01, 00, 00, 00, 02, 01, 63, 31, 35, 00, 01, 01, 80, …]")
test index_builds_online_under_writes ... FAILED
```

`63 31 35` is `c15`: a color one writer set *after* the backfill passed that
row. Without write-only maintenance, that change is lost to the index forever.
