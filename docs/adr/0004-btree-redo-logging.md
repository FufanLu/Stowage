# ADR 0004: B+Tree storage engine with redo-only mini-transaction logging

- Status: accepted
- Date: 2026-11-06

## Context

The layer above the storage engine is a Raft state machine. It applies committed
entries one at a time, in log order, on a single thread. The SQL layer above that
needs fast point lookups and ordered range scans for primary and secondary indexes.
The companion project Cartwheel already has a hand-written LSM engine, so this
engine should show the other classic design.

## Decision

1. **B+Tree on slotted 8 KiB pages** with a separate meta page, and a CRC32C per
   page checked on every read from disk.
2. **Single writer, concurrent readers.**
   - Readers use latch crabbing.
   - The writer tries an optimistic descent (exclusive latch on the leaf only),
     and restarts with pessimistic exclusive crabbing when the leaf must split.
   - Because there is exactly one writer, the optimistic leaf cannot be split by
     anyone else between the shared descent and the exclusive latch.
3. **Physical redo logging per mini-transaction.**
   - A whole `WriteBatch` and any splits it causes become one WAL record.
   - Each changed page is logged as byte deltas against its pre-mtr image.
   - The first change to a page after a checkpoint logs a full image instead
     (PostgreSQL's `full_page_writes`), leaving out the page's free gap.
4. **No undo.** A batch either reaches the WAL as one record or is never logged. A
   failure after pages were modified in memory but before the record was written
   stops the engine (fail-stop), and reopening recovers. Transaction rollback
   happens above, in the MVCC layer (M4), which never overwrites committed
   versions.
5. **Sharp checkpoints, and the checkpoint pointer in its own file**, replaced
   atomically with write-tmp → fsync → rename → fsync-dir.

## Consequences

- Recovery needs no B+Tree logic: replay only copies bytes. A bug in split code
  cannot also break the code that repairs its effects.
- Log volume is higher than with physiological records. A mid-page insert logs the
  shifted part of the slot array (~250 B average delta in the benchmark). Images
  dominate when checkpoints are frequent. Measured in
  [chapter 02 §2.5](../illustrated/02-crash-and-recover.md#25-what-durability-costs).
- Random inserts into a tree larger than the cache are slower than in an LSM tree
  (one dirty-page eviction per insert). Point reads and scans are faster.
- Writers wait during a checkpoint. A fuzzy checkpoint (tracking a dirty-page
  table and the minimum recovery LSN) is the known next step if this shows up in
  tail latency.

## Alternatives considered

- **ARIES with undo**: needed when uncommitted changes can reach disk ("steal"
  across transactions). Our mtrs are short and pin their pages until commit, and
  MVCC handles rollback, so undo would add complexity without a user.
- **Double-write buffer (InnoDB)** instead of full-page images: it writes every
  page twice, all the time. Full-page images cost only on the first change after a
  checkpoint.
- **Use RocksDB or another existing engine**: kept only as a comparison baseline
  (fjall behind `Engine`). Writing the engine is the point of this milestone.
