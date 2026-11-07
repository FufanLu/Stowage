# ADR 0009: Changefeeds, a DynamoDB-compatible API, and online schema changes

- Status: accepted
- Date: 2026-11-07

## Context

Three requirements share one primitive:

- **CDC.** Downstream systems (search, caches, a data lake) need every committed
  change, once, in order. Conveyor publishes order events today through an
  outbox table.
- **A document API.** Conveyor uses DynamoDB Local for some of its data. A
  DynamoDB-compatible API lets existing SDK code run on Stowage, global
  secondary indexes (GSIs) and Streams included.
- **Schema changes on live tables.** `CREATE INDEX` must not block writers.

The primitive is "every version committed in a span between two timestamps,
and a guarantee that nothing else will ever commit there".

## Decision

1. **Changefeeds are closed windows over MVCC history.** `Changes{span, (from,
   to]}` is evaluated like a read at `to`:
   - it enters the timestamp cache on arrival, so no write can land at or below
     `to` afterwards;
   - an unresolved intent at or below `to` blocks it. The client pushes that
     transaction above `to` at top priority (a pending writer is not aborted,
     only moved) or resolves it if it is finished.

   The answer is then complete, and a feed is a sequence of adjacent windows:
   each version is delivered exactly once, and `to` is a resolved timestamp a
   consumer may checkpoint. No per-range rangefeed state exists. A feed polls,
   and its cost is a scan of the span's versions (no GC yet, so it grows with
   history).
2. **The DynamoDB API is a separate key prefix** (`0x03`), with descriptors at
   `0x01 "ddb/"`. Items are their DynamoDB JSON. Key attributes are encoded
   memcomparably (numbers as doubles), and every call is one SSI transaction.
   `TransactWriteItems` is a single transaction over all its items, with
   DynamoDB's cancellation reasons. Expressions (condition, filter, key
   condition, update, projection) have their own parser.
3. **GSIs are maintained from the changefeed, asynchronously**, as in DynamoDB
   (GSI reads are eventually consistent there too).
   - One worker in the cluster holds a lease key: 3 s, renewed when less than
     2 s remain.
   - It reads windows of base-table changes. In one transaction per window it
     deletes the entries recorded for each changed item in a ledger
     (`0x04 ‖ table ‖ key`), writes the new entries, updates the ledger, and
     advances its cursor.
   - The cursor commits with the entries, so a crash or a lease handover never
     applies a window twice or skips one. A second worker that believes it
     holds the lease finds the cursor already advanced and does nothing.
4. **Streams are changefeed windows over a table.** The shard iterator encodes
   the last delivered timestamp. A record's `OldImage` is a historical read just
   below its commit timestamp, which also tells `INSERT` from `MODIFY`.
5. **Online `CREATE INDEX` follows F1's write-only → public states**
   (chapter 09). The backfill runs in small SSI transactions. SSI's refresh of
   the descriptor key makes writers that read an older descriptor restart, so
   no schema lease or version-wait protocol is needed.

## Changes from the original plan

- **No Kafka or S3 sink yet.** `stowctl changefeed` prints the feed, and the
  `Changefeed` type is the library API for writing one.
- **No `UpdateTable` for adding a GSI to an existing table.** The worker would
  need a backfill like chapter 09's.
- **No SigV4 verification yet** (M7). Any credentials are accepted.

## Consequences

- Index maintenance and Streams cost a scan of the changed span's history per
  poll. MVCC garbage collection, with a protected timestamp for each feed's
  cursor as CockroachDB has, is needed before tables grow large.
- GSI lag is poll interval + one window transaction, about 100–300 ms locally
  ([chapter 08](../illustrated/08-a-dynamodb-putitem.md)). DynamoDB documents
  "usually under a second".
- A writer at snapshot isolation does not refresh its read of the descriptor,
  so during an index build it could commit without maintaining the new index.
  SQL sessions default to SSI. Running DML at `REPEATABLE READ` during an index
  build is unsafe until DML refreshes descriptor reads at every isolation level.
- DynamoDB numbers keep their exact decimal text in the item, but sort by
  their double value in keys, which is exact up to 2⁵³. DynamoDB allows 38
  significant digits.
