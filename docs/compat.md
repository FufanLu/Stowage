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
| Flyway 13.12 (PostgreSQL support) | `migrate`, history table, advisory lock, `info` | [`tests/compat/flyway/run.sh`](../tests/compat/flyway/run.sh) (CI): Conveyor's catalog and inventory migrations and Milkrun's sync schema, unmodified |

`information_schema.tables`, `.columns`, `.schemata` and `pg_tables` work, as
does a `pg_catalog` derived from Stowage's catalog: `pg_namespace`, `pg_class`,
`pg_attribute`, `pg_index`, `pg_type` (built-in types), `pg_roles`,
`pg_database`, `pg_settings`, and empty `pg_depend`, `pg_description`,
`pg_proc`, `pg_constraint` and similar, so the joins tools run against them
succeed. `psql`'s `\d` uses functions such as `pg_get_userbyid` that are not
there yet.

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
| DDL | `CREATE TABLE [IF NOT EXISTS]` with `PRIMARY KEY`, `NOT NULL`, `DEFAULT` (constants and `now()`), `UNIQUE`, `CHECK`, `REFERENCES`, `SERIAL`; `CREATE [UNIQUE] INDEX [IF NOT EXISTS]` (backfilled in the same transaction; `WHERE` builds a full index); `ALTER TABLE` `ADD [CONSTRAINT] PRIMARY KEY` (empty table) / `UNIQUE` / `CHECK` / `FOREIGN KEY` [`NOT VALID`], `DROP CONSTRAINT`, `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, `ALTER COLUMN SET/DROP NOT NULL`, `SET/DROP DEFAULT`; `DROP TABLE/INDEX [IF EXISTS] [CASCADE]`; `ANALYZE` | `ALTER TABLE … RENAME TO`, `ALTER COLUMN TYPE`, views, `CREATE TABLE AS`, sequences as objects, schemas, partial unique indexes |
| Constraints | primary key, unique, not null, **`CHECK`** (NULL passes), **`FOREIGN KEY`** to a primary key or unique index, `ON DELETE NO ACTION / RESTRICT / CASCADE / SET NULL`, `MATCH SIMPLE` | `ON DELETE SET DEFAULT`, `ON UPDATE CASCADE`, deferrable constraints |
| DML | `INSERT … VALUES / SELECT`, `ON CONFLICT DO NOTHING`, `UPDATE … WHERE`, `DELETE … WHERE` | `RETURNING`, `ON CONFLICT DO UPDATE`, `UPDATE … FROM`, `DELETE … USING`, `COPY` |
| Queries | `SELECT [DISTINCT]`, inner / left / cross joins (`ON`, `USING`), derived tables, `WHERE`, `GROUP BY` (names, positions, expressions), `HAVING`, `ORDER BY` (names, positions, expressions, `NULLS FIRST/LAST`), `LIMIT`/`OFFSET`, `UNION [ALL]`, uncorrelated `IN (SELECT …)`, `EXISTS`, scalar subqueries | `WITH`, `INTERSECT`/`EXCEPT`, window functions, correlated subqueries, right/full joins, `DISTINCT ON` |
| Expressions | arithmetic, comparisons, `AND/OR/NOT` (three-valued), `IS [NOT] NULL`, `IN (…)`, `BETWEEN`, `LIKE/ILIKE`, POSIX regular expressions `~ ~* !~ !~*`, `CASE`, `CAST`/`::`, `\|\|` | `INTERVAL` arithmetic |
| Functions | `count/sum/avg/min/max` (with `DISTINCT`), `lower`, `upper`, `length`, `abs`, `round`, `coalesce`, `nullif`, `substr`/`substring`, `now()`, `version()`, `current_database()`, `current_schema`, `current_user`, `pg_try_advisory_xact_lock`, `pg_advisory_xact_lock` | everything else |
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
- **Constraints run in the writing transaction.** A foreign-key check is a read
  of the parent's key, so a concurrent delete of that parent conflicts with the
  insert under SSI instead of slipping past it. Deleting a referenced parent
  searches each referencing table through an index on its foreign-key columns
  when there is one, and by scanning it otherwise.
- **Advisory locks.** `pg_try_advisory_xact_lock(k)` writes a lock key in the
  transaction: a second transaction taking the same lock conflicts (waits or
  gets `40001`) rather than receiving `false`. Session-level advisory locks are
  accepted and always granted.
- **`ALTER TABLE` rewrites in one transaction.** Adding or dropping a column
  rewrites every row inside the statement's transaction; on large tables that
  is one large transaction, unlike the batched online `CREATE INDEX`.
- `SERIAL` values come from a transactional counter: no gaps after a rollback,
  and concurrent inserts into one table conflict on it.

# DynamoDB compatibility

Every `stowd` serves the DynamoDB JSON protocol (`X-Amz-Target:
DynamoDB_20120810.*` and `DynamoDBStreams_20120810.*`) on `--ddb-listen`
(default `127.0.0.1:8480`). Point an SDK's `endpoint_url` there. Verified by
[`tests/compat/dynamodb/smoke.py`](../tests/compat/dynamodb/smoke.py) (boto3),
[`tests/compat/dynamodb-java`](../tests/compat/dynamodb-java/src/main/java/Tracking.java)
(AWS SDK for Java 2.57, async client: every call Conveyor's tracking service
makes, with its expressions copied verbatim, in CI) and
`crates/docapi/tests/api.rs` (CI).

| Operation | Supported | Notes |
|---|---|---|
| `CreateTable`, `DescribeTable`, `ListTables`, `DeleteTable` | ✅ | hash or hash + range keys; GSIs at creation; streams; billing settings accepted and ignored |
| `PutItem`, `GetItem`, `UpdateItem`, `DeleteItem` | ✅ | `ConditionExpression`, `ReturnValues` (`ALL_OLD`, `ALL_NEW`, `UPDATED_*`), `ProjectionExpression` |
| `Query`, `Scan` | ✅ | key conditions `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `begins_with`; `FilterExpression`; `Limit` + `ExclusiveStartKey`; `ScanIndexForward=false`; `Select=COUNT`; `IndexName` |
| `BatchWriteItem`, `BatchGetItem` | ✅ | each request applied separately, as in DynamoDB |
| `TransactWriteItems`, `TransactGetItems` | ✅ | one serializable transaction; `CancellationReasons` |
| Streams: `ListStreams`, `DescribeStream`, `GetShardIterator`, `GetRecords` | ✅ | one shard; `NEW_AND_OLD_IMAGES`; `TRIM_HORIZON`, `LATEST`, `AT/AFTER_SEQUENCE_NUMBER` |
| `UpdateTimeToLive`, `DescribeTimeToLive` | ✅ | expired items (a number attribute, epoch seconds, in the past) are deleted in the background, about once a second, 100 per table per round; deletes show in streams as `REMOVE` and leave the GSIs |
| `UpdateTable`, LSIs, PartiQL, backups | — | |

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
