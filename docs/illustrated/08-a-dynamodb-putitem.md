# 08. A DynamoDB PutItem

> Data in this chapter comes from
> `cargo run --release -p stowage-docapi --example trace_putitem`, a real
> three-node cluster in one process. The boto3 test
> [`tests/compat/dynamodb/smoke.py`](../../tests/compat/dynamodb/smoke.py)
> covers the same ground through the AWS SDK. Code:
> [`crates/docapi/src/lib.rs`](../../crates/docapi/src/lib.rs),
> [`expr.rs`](../../crates/docapi/src/expr.rs),
> [`model.rs`](../../crates/docapi/src/model.rs); changefeeds in
> [`client.rs`](../../crates/node/src/client.rs) and
> [`eval.rs`](../../crates/kv/src/eval.rs). Design:
> [ADR 0009](../adr/0009-cdc-and-documents.md).

The table:

```text
orders  hash key customer (S), range key order_id (N)
        GSI by_status on status (S), projection ALL; a stream with new and old images
```

## 8.1 The request

DynamoDB's protocol is one HTTP POST per call. The operation is a header, and
the body is JSON in which every value carries its type:

```text
► POST / HTTP/1.1
► X-Amz-Target: DynamoDB_20120810.PutItem
► Content-Type: application/x-amz-json-1.0
►
► {"ConditionExpression":"attribute_not_exists(order_id)",
►  "Item":{"customer":{"S":"acme"},"order_id":{"N":"42"},"status":{"S":"paid"},"total":{"N":"99.5"}},
►  "TableName":"orders"}
◄ (74.7 ms)
◄ HTTP/1.1 200 OK
◄ {}
```

Inside, the call is **one SSI transaction**:

1. read the table descriptor (`0x01 "ddb/orders"`);
2. compute the item's key, then read the current item there (none);
3. evaluate `attribute_not_exists(order_id)` against it (true);
4. write the item and commit.

The 75 ms are the commit's Raft writes with `F_FULLFSYNC` on macOS. The
condition is evaluated in the transaction, so two concurrent PutItems with the
same condition cannot both succeed: the second one's refresh fails, it
retries, and now the condition is false. That is the behaviour DynamoDB promises.

Sent again:

```text
◄ (0.6 ms)
◄ HTTP/1.1 400 Bad Request
◄ {"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}
```

0.6 ms: a read, a false condition and nothing to write, so there is no Raft
round. SDKs map `__type` to their exception class, so boto3 raises
`ConditionalCheckFailedException`.

## 8.2 The item in the key space

```text
key   03 | 00 00 00 01 | 00 00 00 00 | 01 61 63 6d 65 00 01 | 01 c0 45 00 00 00 00 00 00
      ddb   table 1      base table    "acme" (S)             42 (N) as a double:
                         (index 0)                            0x4045000000000000, sign bit flipped
value {"customer":{"S":"acme"},"order_id":{"N":"42"},"status":{"S":"paid"},"total":{"N":"99.5"}}
```

The same memcomparable encoding as SQL rows (chapter 06 §6.1), so a `Query`
on `customer = :c AND order_id BETWEEN :a AND :b` is one contiguous scan.
Numbers become doubles, so `2 < 10` in key order, not `"10" < "2"`. The value is
the item exactly as sent, so `99.5` and `42` keep their decimal text.

## 8.3 The change becomes a feed event

A changefeed asks for every version committed in a span in a window
`(from, to]`:

```text
1794055077.319756000,1  key 03 00 00 00 01 00 00 00 00 01 61 63 6d 65 00 01 01 c0 45 …  90 bytes
```

The request is served like a read at `to`. It enters the leader's timestamp
cache, so nothing can commit at or below `to` in the span afterwards. An
unresolved intent at or below `to` blocks the window until the feed pushes
that transaction above `to` (at top priority: the writer is moved, not aborted)
or resolves it. When the answer arrives, the window is **closed**: no later
commit will land inside it. Polling adjacent windows therefore delivers every
version exactly once.

`crates/node/tests/changefeed.rs` checks this with four writers committing
conflicting two-key transactions for 3 s while a feed polls every 50 ms. All
92 committed writes were delivered, none twice, and nothing else arrived.
Without the timestamp-cache entry, the same test reports commits that landed
in a window after it was read:

```text
2 committed writes never delivered, e.g. Some(([99, 102, 47, 102], "1794037729.621045000,0", [119, 51, 45, 49, 52]))
```

## 8.4 The GSI entry, written from the feed

DynamoDB GSIs are **eventually consistent**, and so are Stowage's. One worker
per cluster holds a 3-second lease key. It polls the feed over every table's
items, and in one transaction per window it:

1. deletes the entries recorded in each changed item's **ledger**;
2. writes the new entries;
3. updates the ledger;
4. advances its cursor.

```text
the GSI query finds it after 209 ms

key   03 00 00 00 01 00 00 00 00 01 61 63 6d 65 00 01 01 c0 45 …                     the item
key   03 00 00 00 01 | 00 00 00 01 | 01 70 61 69 64 00 01 | 01 61 63 6d 65 00 01 01 c0 45 …
                       GSI 1          "paid" (status)       + the item's key, so entries are unique
      value: the item (projection ALL), 90 bytes
key   04 00 00 00 01 | 03 00 00 00 01 00 00 00 00 01 61 63 6d 65 … → ["03 … by_status entry"] (83 bytes)
      ledger           the base key
```

The ledger answers what the feed alone cannot: which entries the *previous*
version of this item had, so an `UpdateItem` that changes `status` from `paid`
to `open` deletes the old entry. The API test asserts exactly that.

The cursor commits in the same transaction as the entries, so a worker that
crashes mid-window, or a lease that changes hands, never applies a window
twice or skips one. A second worker that wrongly believes it holds the lease
finds the cursor already advanced and writes nothing.

Lag is one poll (100 ms) plus one window transaction: 209 ms here.

## 8.5 The stream record

A stream is a changefeed over one table. The shard iterator is the last
delivered timestamp, and `OldImage` comes from a historical read just below the
commit timestamp. Its absence is what makes this record an `INSERT`:

```json
{
  "eventName": "INSERT",
  "eventSource": "aws:dynamodb",
  "dynamodb": {
    "Keys": {"customer": {"S": "acme"}, "order_id": {"N": "42"}},
    "NewImage": {"customer": {"S": "acme"}, "order_id": {"N": "42"}, "status": {"S": "paid"}, "total": {"N": "99.5"}},
    "SequenceNumber": "017940550773197560000000000001",
    "StreamViewType": "NEW_AND_OLD_IMAGES"
  }
}
```

The sequence number is the commit timestamp (20 digits of wall nanoseconds,
then 10 of the logical counter), so sequence numbers sort in commit order, as
DynamoDB guarantees within a shard.

## 8.6 What the SDK test covers

`smoke.py` runs against node 1 of a real cluster:

```text
ok   create / list / describe table
ok   conditional put (and its failure)
ok   get / update with SET, ADD, REMOVE, a condition, ReturnValues=ALL_NEW
ok   query: begins_with, BETWEEN + filter, descending
ok   scan with pagination (Limit=2)
ok   transact write: commit, and cancel with reasons (nothing applied)
ok   batch write / batch get
ok   GSI query by status and total (eventually consistent) → [15, 40, 99]
ok   stream: 11 records, 9 INSERT, 2 MODIFY, with old and new images
ok   delete item; ResourceNotFound and Validation errors
ok   delete table
all passed
```

`TransactWriteItems` is one transaction over all its items. When one condition
fails, the call reports one cancellation reason per item, in order:
`["None", "ConditionalCheckFailed"]`. That is what the SDKs parse.
