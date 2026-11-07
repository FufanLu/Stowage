# 07. pgwire on the wire

> Data in this chapter comes from a raw-socket Python client that prints every
> byte (§7.1–7.3), from `STOWAGE_PGWIRE_TRACE=1` frame logs of the pgjdbc smoke
> test [`tests/compat/jdbc/Smoke.java`](../../tests/compat/jdbc/Smoke.java)
> (§7.4), and from a real three-node cluster. Code:
> [`crates/pgwire/src/lib.rs`](../../crates/pgwire/src/lib.rs),
> [`session.rs`](../../crates/sql/src/session.rs).

Every `stowd` speaks the Postgres frontend/backend protocol, version 3, on
`--pg-listen`, so `psql`, JDBC, `tokio-postgres` and every ORM built on them
connect without a Stowage-specific driver. That made Postgres compatibility
the cheapest way to make the database usable. This chapter reads the protocol
byte by byte.

After startup, every frame has the same shape:

```text
 type (1 byte, ASCII) ‖ length (i32 big-endian, includes itself but not the type) ‖ body
```

## 7.1 Startup

```text
► (startup)  len 39
     00 00 00 27 | 00 03 00 00 | 75 73 65 72 00 73 74 6f 77 61 67 65 00 64 61 74 61 62 61 73 65 00 73 74 6f 77 61 67 65 00 00
     length 39     protocol 3.0  "user"     "stowage"            "database"           "stowage"           end
◄ R  len 8     00 00 00 00                          AuthenticationOk (0 = no password: TLS and SCRAM arrive in M7)
◄ S  len 24    server_version.16.0.                 ParameterStatus: what the client should assume
◄ S  len 25    server_encoding.UTF8.
◄ S  …         client_encoding, DateStyle, TimeZone, integer_datetimes, standard_conforming_strings, application_name
◄ K  len 12    00 00 00 05 00 00 00 00              BackendKeyData: process id 5, secret 0 (for cancel requests)
◄ Z  len 5     49                                   ReadyForQuery, status 'I' (idle)
```

The startup message is the only one without a type byte, for historical reasons.
A client may first send an `SSLRequest` (length 8, code 80877103). Stowage
answers `N` (no TLS yet), and the client continues in plain text; pgjdbc did
exactly that in §7.4. `server_version 16.0` matters: drivers switch features on
it.

The last byte of every `ReadyForQuery` is the session's transaction state: `I`
idle, `T` in a transaction block, `E` in a failed one that only `ROLLBACK` ends.
Clients rely on it, and §7.4 shows it change.

## 7.2 A simple query

```text
► Q  len 59   SELECT id, name FROM regions WHERE id <= 2 ORDER BY id.
◄ T  len 50   RowDescription
     00 02                                                        2 columns
     69 64 00 | 00 00 00 00 | 00 00 | 00 00 00 14 | 00 08 | ff ff ff ff | 00 00
     "id"       table oid 0  attnum  type oid 20    len 8   typmod −1     format 0 (text)
              (bigint)
     6e 61 6d 65 00 | 00 00 00 00 | 00 00 | 00 00 00 19 | ff ff | ff ff ff ff | 00 00
     "name"                              type oid 25    len −1 (variable)
                                         (text)
◄ D  len 19   DataRow   00 02 | 00 00 00 01 31 | 00 00 00 04 65 6d 65 61
                        2 cols  len 1 "1"       len 4 "emea"
◄ D  len 23   DataRow   00 02 | 00 00 00 01 32 | 00 00 00 08 61 6d 65 72 69 63 61 73      "2", "americas"
◄ C  len 13   CommandComplete  SELECT 2.
◄ Z  len 5    49 ('I')
```

In the simple protocol every value is text, as `psql` prints it: `1`, `t`,
`2026-11-06 12:30:00`. A NULL is a length of −1 with no bytes. The type OIDs are
Postgres' own (20 `int8`, 25 `text`, 701 `float8`, 1114 `timestamp`, 16
`bool`), and they decide how a client parses each column.

## 7.3 The extended protocol, with a binary parameter

Prepared statements split one query into five messages, which the client
pipelines and the server answers after `Sync`:

```text
► P  len 89   Parse: name "", "SELECT name, price FROM products WHERE price > $1 ORDER BY price DESC LIMIT 2",
              1 parameter type: 00 00 02 bd (701, float8)
► B  len 28   Bind
     00 | 00 | 00 01 00 01 | 00 01 | 00 00 00 08  40 57 c0 00 00 00 00 00 | 00 01 00 00
     portal stmt  1 format: binary  1 param  len 8   95.0 as IEEE big-endian  1 result format: text
► D  len 6    Describe portal ""
► E  len 9    Execute portal "", no row limit
► S  len 4    Sync
◄ 1           ParseComplete
◄ 2           BindComplete
◄ T  len 53   RowDescription: "name" text, "price" type 701 len 8
◄ D  len 21   "prod3", "98"
◄ D  len 25   "prod27", "97.41"
◄ C  len 13   SELECT 2
◄ Z  len 5    'I'
```

`40 57 c0 00 00 00 00 00` is 95.0 as a float8. The client chose **binary**
format for this parameter. pgjdbc does this for doubles (§7.4), and
tokio-postgres asks for binary *results* by default. Stowage decodes binary
parameters of the fixed-width types. Binary results are encoded the way
Postgres' `typsend` would: `int8` and `float8` big-endian, `bool` one byte,
`timestamp` as microseconds since 2000-01-01. Parameters then bind exactly like
string literals: `$1` becomes the text `95`, which the binder converts to the
type of `price` once, at bind time (chapter 06 §6.2).

The protocol is pipelined. If any message before `Sync` fails, the server
reports the error and **skips everything until `Sync`**, so a client never sees
results of statements that depended on a failed one.

## 7.4 pgjdbc, frame by frame

The smoke test connects, creates a table, runs five prepared `INSERT`s, a
transfer in a transaction, two `SELECT`s and a duplicate key. The server-side
trace, with milliseconds since connect:

```text
    0.00ms  ► startup code 80877103 len 8                       SSLRequest → N
    8.02ms  ► startup code 196608 len 103  user·stowage·database·stowage·client_encoding·UTF8·DateStyle·ISO…
    8.11ms  ◄ Z len 5 I
   23.73ms  ► Q  SET application_name = 'PostgreSQL JDBC Driver'
   23.88ms  ◄ C  SET
  114.57ms  ► P  ·INSERT INTO jdbc_accounts VALUES ($1, $2, $3, $4)    unnamed statement
  115.40ms  ► B  ······…owner1····@%·…2026-11-01 0…                  long and double sent binary, text as text
  115.44ms  ► D  P·
  115.44ms  ◄ n                                                       NoData: an INSERT has no rows
  115.45ms  ► E
  140.43ms  ◄ C  INSERT 0 1                                           25 ms: the Raft writes (pk check + row)
   …         (the same for owner2 … owner4)
  250.98ms  ► P  S_1·INSERT INTO jdbc_accounts VALUES ($1, $2, $3, $4)    5th execution: a *named* statement
  251.05ms  ► D  SS_1·                                                 Describe the statement once
  251.15ms  ◄ t  len 22                                                ParameterDescription: 4 parameter types
  251.17ms  ► B  ·S_1·…owner5…                                         from now on only Bind + Execute
  284.34ms  ◄ C  INSERT 0 1
  285.01ms  ► Q  BEGIN
  285.06ms  ◄ Z  T                                                     ← in a transaction block
  285.07ms  ► P  ·UPDATE jdbc_accounts SET balance = balance + $1 WHERE id = $2
  293.26ms  ◄ C  UPDATE 1
  293.27ms  ◄ Z  T
  305.74ms  ► P  S_2·COMMIT
  338.31ms  ◄ C  COMMIT                                                29 ms: EndTxn + resolving intents (chapter 05)
  338.57ms  ◄ Z  I                                                     ← idle again
  342.47ms  ◄ T  len 102  id · owner · balance · opened
  342.99ms  ◄ D  ······2····owner2····26····2026-11-02 09:00:00
  343.01ms  ◄ C  SELECT 4
  350.46ms  ◄ E  len 95  SERROR·VERROR·C23505·Mduplicate key value violates unique constraint "jdbc_accounts_pkey"
  350.48ms  ◄ Z  I                                                     the connection lives on
  351.60ms  ► X                                                        Terminate
```

Three driver behaviours show here:

- **Named statements after a threshold.** pgjdbc prepares statements on the
  server only from the fifth execution (`prepareThreshold=5`). It then Describes
  once and afterwards sends only `Bind` and `Execute`. With
  `prepareThreshold=1&binaryTransfer=true` it also asks for binary results. The
  smoke test passes in all three modes.
- **Transactions are statements.** `setAutoCommit(false)` sends `BEGIN` as a
  simple query, and `commit()` is a prepared `COMMIT`. The `Z` byte follows
  along: `I` → `T` → `I`.
- **Errors carry SQLSTATE.** `C23505` is what makes Spring translate the error
  into `DuplicateKeyException`, and what makes a retry loop recognise `40001`.
  The other fields are `S`/`V` severity and `M` message. Stowage maps every
  error to Postgres' code (`Error::sqlstate`).

The timing gaps are the database, not the protocol. Parsing, binding and
encoding take tens of microseconds. An `INSERT` waits 25–35 ms for two Raft
writes with `F_FULLFSYNC` on macOS, and `COMMIT` waits for the transaction
record and the intent resolution.

## 7.5 What is supported

| Protocol feature | State |
|---|---|
| Startup, `SSLRequest` (declined), `CancelRequest` (ignored) | ✅ |
| Simple query, several statements per message | ✅ |
| Parse / Bind / Describe (statement and portal) / Execute (with row limit, `PortalSuspended`) / Sync / Close / Flush | ✅ |
| Text and binary parameters and results for bool, int, float, text, timestamp, bytea | ✅ |
| `ErrorResponse` with SQLSTATE; skip-until-Sync after an error | ✅ |
| Authentication: trust only | M7 adds TLS and SCRAM-SHA-256 |
| `COPY`, `LISTEN/NOTIFY`, function call | — |

Verified by `crates/pgwire/tests/protocol.rs` (tokio-postgres, both protocols,
binary results, SQLSTATEs, transactions) against a real cluster in CI; by
`psql`; and by pgjdbc 42.7 in its default, named-statement and binary-transfer
modes.
