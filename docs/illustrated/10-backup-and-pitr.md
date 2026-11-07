# 10. Backup and point-in-time recovery

> Data in this chapter comes from `stowctl` and `psql` against two real
> three-node clusters on one laptop, and from
> `cargo nextest run --release -p stowage-backup` (CI). Code:
> [`crates/backup/src/lib.rs`](../../crates/backup/src/lib.rs). Design: ADR 0009
> (changefeeds) and the M7 notes in [ADR 0010](../adr/0010-enterprise-operations.md).

Someone runs `DELETE FROM orders` and forgets the `WHERE`. Replication does
not help: Raft faithfully copies the mistake to all three replicas in
milliseconds. What helps is a copy of the past, and the ability to stop at the
exact moment before the mistake.

## 10.1 The accident

```text
$ psql -c "CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT, total FLOAT, status TEXT)" \
       -c "INSERT INTO orders VALUES (1,'acme',99.5,'paid'), …, (5,'umbrella',42,'refunded')"
$ stowctl backup backups --full
full backup to 1794055541261075000,0 (2026-11-07 12:45:41.261 UTC): 7 versions of 7 keys, 560 bytes

$ psql -c "INSERT INTO orders VALUES (6,'acme',15,'open'),(7,'globex',77,'paid')" \
       -c "UPDATE orders SET status = 'paid' WHERE id = 2"
time before the accident: 2026-11-07 12:45:42
$ psql -c "DELETE FROM orders"                        ← meant: WHERE status = 'refunded'
DELETE 7

$ stowctl backup backups
incremental backup to 1794055543607800000,0 (2026-11-07 12:45:43.607 UTC): 10 versions of 7 keys, 475 bytes
$ stowctl backups backups
full         (                   0,0,  1794055541261075000,0]  2026-11-07 12:45:41.261 UTC        7 versions
incremental  ( 1794055541261075000,0,  1794055543607800000,0]  2026-11-07 12:45:43.607 UTC       10 versions
```

The full backup has 7 versions: the 5 rows plus 2 catalog keys (the table
descriptor and the table-id counter). The incremental one has 10: the 2 new
rows, the update, and the **7 deletion tombstones**. A backup holds the
deletions as well as the rows.

## 10.2 Restore to a moment, not to a backup

```text
$ stowctl --peers <new cluster> restore backups --as-of "2026-11-07 12:45:42"
restored 9 keys as of 1794055542000000000,4294967295 from 2 backups (17 versions read)
$ psql -p 5491 -c "SELECT * FROM orders ORDER BY id"
 id | customer | total |  status
----+----------+-------+----------
  1 | acme     |  99.5 | paid
  2 | acme     |    20 | paid          ← the update before the accident is there
  3 | globex   |  5.25 | paid
  4 | initech  |   310 | paid
  5 | umbrella |    42 | refunded
  6 | acme     |    15 | open          ← so are the inserts after the full backup
  7 | globex   |    77 | paid
(7 rows)
```

The target time is between the two backups' end points, yet the restore
contains every write before it and none after. That works because a backup is
**every committed version in a time window**, not a snapshot at the window's
end. The restore reads both backups, keeps for each key the newest version at
or before the target (the 7 tombstones come after it and are ignored), and
writes the survivors into the empty cluster. A wall-clock target becomes the
timestamp `wall,4294967295`: everything committed in that nanosecond or
earlier.

## 10.3 Why adjacent windows are enough

```text
  full: (0, t1]          incremental: (t1, t2]       incremental: (t2, t3]
  ──────────────────────┼───────────────────────────┼──────────────────────────►
```

Each backup is a changefeed window (chapter 08 §8.3):

- **Closed.** Reading the window entered `t2` into every leader's timestamp
  cache, so no transaction can commit at or below `t2` later. The next backup,
  starting at `t2`, cannot miss a commit that was still in flight.
- **Complete.** Unresolved intents at or below `t2` were pushed above it or
  resolved first, so no half-done transaction is cut in two.
- **Consistent across ranges.** One timestamp bounds every range's window, so
  the restored state as of any `T` is the snapshot a transaction reading at `T`
  would have seen. It does not mix ranges at different moments.

`state_as_of` also checks the chain. An incremental backup whose `from` is not
the previous backup's `to` is a gap, and the restore refuses it.

## 10.4 The files

```text
backups/01794055541261075000-0000000000/manifest.json
{ "kind": "full", "from": "0,0", "to": "1794055541261075000,0",
  "versions": 7, "keys": 7, "bytes": 560, "crc32c": 2392725578,
  "to_utc": "2026-11-07 12:45:41.261 UTC" }

backups/01794055541261075000-0000000000/versions.bin
00000000: 0b00 0000 0173 6571 2f74 6162 6c65 7360  .....seq/tables`      klen 11, key 0x01 "seq/tables"
00000010: 573a 6202 c4e5 1801 0000 0001 0800 0000  W:b.............      wall (LE), logical 1, live, vlen 8
00000020: 0100 0000 0000 0000 0d00 0000 0174 6162  .............tab      value 1 (the counter)
00000030: 6c65 2f6f 7264 6572 7360 573a 6202 c4e5  le/orders`W:b...      next: 0x01 "table/orders"
…
000000b0: 0000 0001 0180 0000 0000 0000 0190 149a                         a row key: 02 ‖ table 1 ‖ index 1 ‖ id 1
```

Records are sorted by key, then timestamp. The data file is written and
fsynced before the manifest, and the manifest is renamed into place, so a
directory without a manifest was never a backup. The CRC32C covers the data
file and is checked on every restore.

## 10.5 What the test asserts

`crates/backup/tests/pitr.rs` runs the same story with three clusters in one
process, in CI:

- a table with a secondary index, a full backup, more writes and an update,
  then a `DELETE` that removes every row, then an incremental backup;
- a restore to just before the `DELETE` into cluster B, which returns all four
  rows with the updated balance; a lookup through the restored **index**
  works too;
- a second restore into the now non-empty cluster B, which is refused;
- the latest state in cluster C, which has the table with zero rows.

## 10.6 Limits

- Backups go to a directory. An S3 or MinIO target needs only an
  object-store writer for the same two files.
- Every backup scans the span's history, because there is no MVCC garbage
  collection yet. Once GC exists, each backup schedule must hold a protected
  timestamp, or GC could remove versions the next incremental needs.
- A restore writes keys one by one with fresh timestamps into an empty
  cluster. The restored cluster has the past state, not the past history.
