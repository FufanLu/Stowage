# Stowage, illustrated

Each chapter follows **one concrete request** through one part of Stowage, in the
spirit of "the life of a query". Every number, hex dump, trace line and plan in these
chapters comes from actually running the code at the commit that added the chapter.
Each chapter names the command that regenerates its data.

| # | Chapter | Follows | Milestone | Status |
|---|---|---|---|---|
| A | [Deterministic simulation](A-deterministic-simulation.md) | one ping through a lossy, partitioned, crashing network; one write through a power loss | M0 | ✅ |
| 00 | [Life of a query](00-life-of-a-query.md) | a four-table `JOIN … GROUP BY` from psql to the rows, with the time spent in each box | M5 | ✅ |
| 01 | [A page on disk](01-a-page-on-disk.md) | three keys in a leaf, then the insert that splits it, byte by byte | M1 | ✅ |
| 02 | [Crash and recover](02-crash-and-recover.md) | a power cut that tears 26 pages and the WAL tail, then recovery; what durability costs | M1 | ✅ |
| 03 | [One write through Raft](03-one-write-through-raft.md) | the first election, one `PUT` and one `GET` message by message, a minority partition, and the bugs the tests catch | M2 | ✅ |
| 04 | [A range splits](04-a-range-splits.md) | a split with no data moved, a replica moving by joint consensus, routing after both, real-disk costs, eight bugs from the seams | M3 | ✅ |
| 05 | [A transfer transaction](05-a-transfer-transaction.md) | a two-range transfer byte by byte, the timestamp cache, a push and a failed refresh, Elle-style checking, nine bugs (one only a real cluster showed) | M4 | ✅ |
| 06 | [Planning a query](06-planning-a-query.md) | rows and index entries as keys, binding, pushdown, join order from `ANALYZE` stats, access paths, `EXPLAIN ANALYZE` on a real cluster | M5 | ✅ |
| 07 | [pgwire on the wire](07-pgwire-on-the-wire.md) | startup, a simple query and a prepared one byte by byte, then pgjdbc frame by frame | M5 | ✅ |
| 08 | A DynamoDB PutItem | a conditional `PutItem`, key encoding, GSI update via CDC | M6 | planned |
| 09 | Online ADD INDEX | an index built while writes continue (F1 schema states) | M6 | planned |
| 10 | Backup and PITR | an accidental `DELETE`, restored to a point in time | M7 | planned |
| 11 | AP vs CP | YCSB against Stowage and Cartwheel, healthy and partitioned | M9 | planned |
