# Postgres compatibility

Stowage speaks the Postgres wire protocol (version 3) and a subset of its SQL
dialect. This page lists what works today; anything else returns SQLSTATE
`0A000` (*feature not supported*) with the construct named.

## Clients

| Client | Status | Verified by |
|---|---|---|
| `psql` 16+ | queries, DDL, transactions | manual runs (chapter 07) |
| pgjdbc 42.7 | default, `prepareThreshold=1`, `binaryTransfer=true` | [`tests/compat/jdbc/Smoke.java`](../tests/compat/jdbc/Smoke.java) |
| tokio-postgres 0.7 | simple and extended protocol, binary results | `crates/pgwire/tests/protocol.rs` (CI) |

`psql`'s `\d` meta-commands query `pg_catalog` in ways not yet supported.
`information_schema.tables`, `information_schema.columns` and `pg_tables` work.

## Types

| Postgres | Stowage | Notes |
|---|---|---|
| `smallint`, `int`, `integer`, `bigint`, `serial`, `bigserial` | `bigint` | 64-bit everywhere |
| `real`, `double precision`, `float`, `numeric`, `decimal` | `double precision` | `numeric` is **not** exact decimal yet |
| `text`, `varchar(n)`, `char(n)`, `uuid`, `json`, `jsonb` | `text` | lengths not enforced; JSON stored as text |
| `boolean` | `boolean` | |
| `timestamp`, `timestamptz`, `date` | `timestamp` | microseconds, UTC; `date` has a time of midnight |
| `bytea` | `bytea` | |

## Statements

| | Supported | Not yet |
|---|---|---|
| DDL | `CREATE TABLE [IF NOT EXISTS]` with `PRIMARY KEY`, `NOT NULL`, `DEFAULT` constant, `UNIQUE`, `SERIAL`; `CREATE [UNIQUE] INDEX [IF NOT EXISTS]` (backfilled in the same transaction); `DROP TABLE/INDEX [IF EXISTS]`; `ANALYZE` | `ALTER TABLE`, views, `CREATE TABLE AS`, sequences as objects, schemas |
| Constraints | primary key, unique, not null | `FOREIGN KEY` and `CHECK` are **accepted and not enforced** |
| DML | `INSERT … VALUES / SELECT`, `ON CONFLICT DO NOTHING`, `UPDATE … WHERE`, `DELETE … WHERE` | `RETURNING`, `ON CONFLICT DO UPDATE`, `UPDATE … FROM`, `DELETE … USING`, `COPY` |
| Queries | `SELECT [DISTINCT]`, inner / left / cross joins (`ON`, `USING`), derived tables, `WHERE`, `GROUP BY` (names, positions, expressions), `HAVING`, `ORDER BY` (names, positions, expressions, `NULLS FIRST/LAST`), `LIMIT`/`OFFSET`, uncorrelated `IN (SELECT …)`, `EXISTS`, scalar subqueries | `WITH`, `UNION`/`INTERSECT`, window functions, correlated subqueries, right/full joins, `DISTINCT ON` |
| Expressions | arithmetic, comparisons, `AND/OR/NOT` (three-valued), `IS [NOT] NULL`, `IN (…)`, `BETWEEN`, `LIKE/ILIKE`, `CASE`, `CAST`/`::`, `\|\|` | |
| Functions | `count/sum/avg/min/max` (with `DISTINCT`), `lower`, `upper`, `length`, `abs`, `round`, `coalesce`, `nullif`, `substr`/`substring`, `now()`, `version()`, `current_database()` | everything else |
| Transactions | `BEGIN [ISOLATION LEVEL …]`, `COMMIT`, `ROLLBACK`, `SET TRANSACTION`; SSI by default; `REPEATABLE READ` and `READ COMMITTED` run as SI | savepoints |
| Session | `SET name = value` (stored; `transaction_isolation` honoured), `SHOW name` | |

## Behaviour that differs from Postgres

- **Serialization failures are more common.** Every transaction is SSI by
  default, so conflicting transactions fail with `40001` (*restart transaction*)
  where Postgres' default `READ COMMITTED` would let them through. Autocommit
  statements retry automatically; explicit transactions must be retried by the
  client, as with CockroachDB.
- **After an error in a transaction block**, every statement fails with `25P02`
  until `ROLLBACK`, as in Postgres. `COMMIT` of such a block rolls back.
- `SERIAL` values come from a transactional counter: no gaps after a rollback,
  and concurrent inserts into one table conflict on it.

# DynamoDB compatibility

Every `stowd` serves the DynamoDB JSON protocol (`X-Amz-Target:
DynamoDB_20120810.*` and `DynamoDBStreams_20120810.*`) on `--ddb-listen`
(default `127.0.0.1:8480`). Point an SDK's `endpoint_url` there. Verified by
[`tests/compat/dynamodb/smoke.py`](../tests/compat/dynamodb/smoke.py) (boto3)
and `crates/docapi/tests/api.rs` (CI).

| Operation | Supported | Notes |
|---|---|---|
| `CreateTable`, `DescribeTable`, `ListTables`, `DeleteTable` | ✅ | hash or hash + range keys; GSIs at creation; streams; billing settings accepted and ignored |
| `PutItem`, `GetItem`, `UpdateItem`, `DeleteItem` | ✅ | `ConditionExpression`, `ReturnValues` (`ALL_OLD`, `ALL_NEW`, `UPDATED_*`), `ProjectionExpression` |
| `Query`, `Scan` | ✅ | key conditions `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `begins_with`; `FilterExpression`; `Limit` + `ExclusiveStartKey`; `ScanIndexForward=false`; `Select=COUNT`; `IndexName` |
| `BatchWriteItem`, `BatchGetItem` | ✅ | each request applied separately, as in DynamoDB |
| `TransactWriteItems`, `TransactGetItems` | ✅ | one serializable transaction; `CancellationReasons` |
| Streams: `ListStreams`, `DescribeStream`, `GetShardIterator`, `GetRecords` | ✅ | one shard; `NEW_AND_OLD_IMAGES`; `TRIM_HORIZON`, `LATEST`, `AT/AFTER_SEQUENCE_NUMBER` |
| `UpdateTable`, LSIs, TTL, PartiQL, backups | — | |

Expressions: comparisons, `BETWEEN`, `IN`, `AND/OR/NOT`, `attribute_exists`,
`attribute_not_exists`, `attribute_type`, `begins_with`, `contains`, `size`;
`#name` and `:value` placeholders; nested paths (`a.b[0]`). Updates support
`SET` (with `+`, `-`, `if_not_exists`, `list_append`), `REMOVE`, `ADD`
(numbers and sets) and `DELETE` (sets).

Differences:
- **GSIs are eventually consistent**, as in DynamoDB. They lag by about one
  changefeed poll (≈100–300 ms on one machine).
- **Numbers** keep their exact text in items, but key ordering uses doubles
  (exact to 2⁵³; DynamoDB allows 38 digits).
- No 1 MB page limit, no capacity accounting yet (M7 adds quotas), and
  credentials are not verified (SigV4 arrives in M7).
- `ScanIndexForward=false` reads the whole key range before reversing it.
