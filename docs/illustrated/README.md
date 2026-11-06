# Stowage, illustrated

Each chapter follows **one concrete request** through one part of Stowage, in the
spirit of "the life of a query". Every number, hex dump, trace line and plan in these
chapters comes from actually running the code at the commit that added the chapter.
Each chapter names the command that regenerates its data.

| # | Chapter | Follows | Milestone | Status |
|---|---|---|---|---|
| A | [Deterministic simulation](A-deterministic-simulation.md) | one ping through a lossy, partitioned, crashing network; one write through a power loss | M0 | ✅ |
| 00 | Life of a query | `SELECT … JOIN … GROUP BY`, from psql to the result rows | M5 | planned |
| 01 | [A page on disk](01-a-page-on-disk.md) | three keys in a leaf, then the insert that splits it, byte by byte | M1 | ✅ |
| 02 | [Crash and recover](02-crash-and-recover.md) | a power cut that tears 26 pages and the WAL tail, then recovery; what durability costs | M1 | ✅ |
| 03 | One write through Raft | a `PUT`, from proposal to commit index | M2 | planned |
| 04 | A range splits | a hot range that splits and moves | M3 | planned |
| 05 | A transfer transaction | a two-range transfer: intents, txn record, HLC, conflicts | M4 | planned |
| 06 | Planning a query | AST → logical plan → memo → physical plan → Arrow batches | M5 | planned |
| 07 | pgwire on the wire | Parse / Bind / Execute frames from a JDBC client | M5 | planned |
| 08 | A DynamoDB PutItem | a conditional `PutItem`, key encoding, GSI update via CDC | M6 | planned |
| 09 | Online ADD INDEX | an index built while writes continue (F1 schema states) | M6 | planned |
| 10 | Backup and PITR | an accidental `DELETE`, restored to a point in time | M7 | planned |
| 11 | AP vs CP | YCSB against Stowage and Cartwheel, healthy and partitioned | M9 | planned |
