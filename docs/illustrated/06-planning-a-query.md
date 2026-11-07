# 06. Planning a query

> Data in this chapter comes from
> `cargo run --release -p stowage-sql --example trace_query` (the in-memory
> store, §6.1–6.3) and from `psql` against a real three-node cluster loaded with
> 5 regions, 200 customers, 50 products and 1 000 orders (§6.4–6.6; the script
> is in §6.4). Code: [`keycodec.rs`](../../crates/sql/src/keycodec.rs),
> [`binder.rs`](../../crates/sql/src/binder.rs),
> [`optimizer.rs`](../../crates/sql/src/optimizer.rs),
> [`exec.rs`](../../crates/sql/src/exec.rs). Design:
> [ADR 0008](../adr/0008-sql-layer.md).

Chapters 01–05 built an ordered, replicated, transactional key space. This
chapter puts tables on it and follows one SQL query from text to rows:

```text
 text ─► sqlparser AST ─► binder ─► bound plan ─► optimizer ─► physical plan ─► executor ─► Arrow batch
          (syntax)        (names,    (what to      (pushdown,    (how to          (vectorized
                          types,     compute)      join order,   compute it)       kernels)
                          aggregates)              indexes)
```

## 6.1 A table is a range of keys

```sql
CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, country TEXT);
CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount FLOAT);
CREATE INDEX orders_customer ON orders (customer_id);
```

After two `INSERT`s, this is the whole key space, byte for byte:

```text
 catalog "table/customers"
     key   01 74 61 62 6c 65 2f 63 75 73 74 6f 6d 65 72 73           0x01 ‖ "table/customers"
 customers row [Int(1), Text("acme"), Text("us")]
     key   02 | 00 00 00 01 | 00 00 00 01 | 01 80 00 00 00 00 00 00 01
           tables  table 1      index 1      not-null, 1 with the sign bit flipped
                                (primary)
     value 01 02 01 00 00 00 00 00 00 00 04 04 00 00 00 61 63 6d 65 04 02 00 …   (26 bytes)
           v1 int 1 (LE)                 text len 4  "acme"          text …
 orders row [Int(10), Int(1), Float(99.5)]
     key   02 00 00 00 02 00 00 00 01 01 80 00 00 00 00 00 00 0a
 orders index orders_customer entry
     key   02 | 00 00 00 02 | 00 00 00 02 | 01 80 00 00 00 00 00 00 01 | 01 80 00 00 00 00 00 00 0a
                 table 2       index 2       customer_id = 1               id = 10 (the primary key)
     value (0 bytes)
```

Every key component is **memcomparable**: comparing encoded keys bytewise gives
the same order as comparing the SQL values. Integers flip their sign bit, so
−5 sorts before 1. Floats flip all bits when negative. Text uses the MVCC
layer's `00 → 00 ff` escaping with an `00 01` terminator, so `"a"` < `"a\0"` <
`"ab"` even inside a composite key. Three things follow from that:

- a primary-key lookup is one `Get`;
- a range predicate on the key is one contiguous `Scan`;
- an index entry is the indexed value followed by the primary key, so all
  orders of customer 1 are adjacent, in id order, and the key alone says where
  each row is.

The row lives in the same key space as Raft ranges (chapter 04), so a big table
simply splits into many ranges. MVCC (chapter 05) adds the timestamp suffix
beneath all of this, and the SQL layer never sees it.

## 6.2 Binding: names, types and aggregates

```sql
SELECT c.name, sum(o.amount) AS total
FROM orders o JOIN customers c ON o.customer_id = c.id
WHERE c.country = 'us'
GROUP BY c.name
```

The parser (`sqlparser`) only knows syntax: `c.name` is a
`CompoundIdentifier(["c", "name"])` and `sum(o.amount)` is a `Function` named
`sum` with one argument (an abridged excerpt of the 600-line AST dump):

```text
 Query { body: Select(Select {
     projection: [ UnnamedExpr(CompoundIdentifier([Ident { value: "c" }, Ident { value: "name" }])),
                   ExprWithAlias { expr: Function(Function { name: ObjectName([Identifier(Ident { value: "sum" })]),
                                                             args: List(FunctionArgumentList { args: [Unnamed(Expr(
                                                                CompoundIdentifier([Ident { value: "o" }, …
```

The binder resolves this against the catalog, read in the statement's
transaction:

- `c.name` becomes **column id** #… of the `customers` scan. Every column in a
  plan gets an id unique within the statement. Expressions refer to ids, never
  positions, so the optimizer can reorder joins without rewriting them.
- Types are unified: `'us'` is a string literal compared with a `TEXT` column.
  In `7.0 / 2` the integer widens to float. A string literal compared with a
  `TIMESTAMP` column is parsed as a timestamp at bind time, once.
- Aggregates are collected (`sum(o.amount)` becomes aggregate #0). Every other
  column reference in the select list must then match a `GROUP BY` expression,
  or binding fails with Postgres' error 42803, *column "x" must appear in the
  GROUP BY clause*.

The **bound plan** is a literal translation, joins as written:

```text
Bound plan:
Project c.name, sum  (rows≈10)
  → HashAggregate by c.name · sum(o.amount)  (rows≈10)
    → Filter (c.country = 'us')  (rows≈100)
      → NestedLoopJoin · residual (o.customer_id = c.id)  (rows≈1000)
        → Scan orders o · full scan  (rows≈1000)
        → Scan customers c · full scan  (rows≈1000)
```

## 6.3 Optimizing

```text
Optimized plan:
Project c.name, sum  (rows≈10)
  → HashAggregate by c.name · sum(o.amount)  (rows≈10)
    → HashJoin on o.customer_id = c.id  (rows≈100)
      → Scan orders o · full scan  (rows≈1000)
      → Scan customers c · full scan · filter (c.country = 'us')  (rows≈100)
```

Two rewrites happened:

1. **Predicate pushdown.** `c.country = 'us'` reads only `customers`, so it moved
   into that scan and runs before the join. The `ON` condition joined the
   `WHERE` conjuncts, and `o.customer_id = c.id`, one side per input, became the
   **hash-join key**. Over a `LEFT JOIN` the rules differ: a `WHERE` predicate on
   the null-extended side must stay above the join (it may reject the NULL
   rows), while an `ON` predicate on that side may go below.
2. **Join method.** With an equality key the join is a hash join that builds on
   the right input. The cost model puts the smaller side there (§6.4).

The estimates in parentheses are guesses until `ANALYZE` has run: 1 000 rows per
table, 1/10 for an equality on a non-key column, 1/3 for a range (Postgres uses
similar defaults). After `ANALYZE` on the tiny tables they become 3, 2 and 1.

## 6.4 Join order on real statistics

The cluster now holds the four-table dataset:

```sql
-- generated by a seeded script: 5 regions, 200 customers, 50 products, 1000 orders
SELECT r.name, c.tier, count(*) AS orders, sum(o.qty * p.price) AS revenue
FROM orders o JOIN customers c ON o.customer_id = c.id
              JOIN products p ON p.id = o.product_id
              JOIN regions r ON r.id = c.region_id
WHERE o.status = 'paid' AND r.name = 'emea'
GROUP BY r.name, c.tier ORDER BY revenue DESC;
```

The optimizer flattens a tree of inner joins into a set of relations and the
predicates between them, then searches **every subset** bottom-up (Selinger
style, bushy trees allowed). Each set is joined from two smaller sets connected
by a predicate, with cross products only when nothing else connects them. The
cost of a plan is the sum of its intermediate result sizes. Four relations means
15 subsets and a few dozen splits.

Before `ANALYZE`, every table looks like 1 000 rows:

```text
 HashJoin on p.id = o.product_id  (rows≈10)
   → Scan products p · full scan  (rows≈1000)                         ← "big": probe side
   → HashJoin on c.id = o.customer_id  (rows≈10)
     → HashJoin on c.region_id = r.id  (rows≈100)
       → Scan customers c · full scan  (rows≈1000)
       → Scan regions r · full scan · filter (r.name = 'emea')  (rows≈100)
     → Scan orders o · full scan · filter (o.status = 'paid')  (rows≈100)
```

After `ANALYZE`, which collects row counts, distinct values, NULLs and a
16-bucket equi-depth histogram per column into the table descriptor:

```text
 Sort revenue DESC  (rows≈15, actual 3 rows, 7.428 ms)
   → Project r.name, c.tier, count, sum  (rows≈15, actual 3 rows, 7.090 ms)
     → HashAggregate by r.name, c.tier · count(*), sum((CAST(o.qty AS double precision) * p.price))  (rows≈15, actual 3 rows, 7.087 ms)
       → HashJoin on o.product_id = p.id  (rows≈67, actual 134 rows, 5.104 ms)
         → HashJoin on o.customer_id = c.id  (rows≈67, actual 134 rows, 4.767 ms)
           → Scan orders o · full scan · filter (o.status = 'paid')  (rows≈333, actual 603 rows, 3.061 ms, 1 kv requests)
           → HashJoin on c.region_id = r.id  (rows≈40, actual 45 rows, 1.667 ms)
             → Scan customers c · full scan  (rows≈200, actual 200 rows, 0.357 ms, 1 kv requests)
             → Scan regions r · full scan · filter (r.name = 'emea')  (rows≈1, actual 1 rows, 0.164 ms, 1 kv requests)
         → Scan products p · full scan  (rows≈50, actual 50 rows, 0.303 ms, 1 kv requests)
 Execution: 3 rows in 7.472 ms
```

Now the plan builds hash tables on the small sides. `regions` is filtered to
one row and joined with `customers` first, which yields the 45 EMEA customers.
That result is the build side for the 603 paid orders, and the 50 products are
the build side for the result. This is `EXPLAIN ANALYZE` on the real cluster:
every scan is **one** range scan (`1 kv requests`) at the gateway's
transaction timestamp, and the whole query takes 7.5 ms.

The estimates are not perfect, and they say why:

| Node | Estimate | Actual | Why |
|---|--:|--:|---|
| `o.status = 'paid'` | 333 | 603 | 3 distinct values → 1/3; the data is skewed 3:1:1 toward `paid` |
| `r.name = 'emea'` | 1 | 1 | 5 distinct names → 1/5 of 5 rows |
| customers ⋈ regions | 40 | 45 | 200 × 1 / 5 distinct region ids |
| orders ⋈ customers | 67 | 134 | inherits the 333 |

Equality selectivity is `1 / distinct values`. The histograms are collected but
used only by index selection so far. Postgres' most-common-values list would fix
the `status` estimate, and that is the obvious next step.

The answer, checked against a Python recomputation from the same seeded data:

```text
 name |  tier  | orders |      revenue
------+--------+--------+--------------------
 emea | gold   |     64 | 15917.070000000003
 emea | bronze |     60 | 13781.749999999998
 emea | silver |     10 | 3470.9900000000007
```

## 6.5 Access paths: key ranges and indexes

A scan's filter is matched against the primary key and every secondary index:
equalities on a key prefix, then at most one range on the next column. The
cheapest match wins. A secondary index pays one extra `Get` per row found,
which is what the cost model's factor of 2 approximates:

```text
 EXPLAIN ANALYZE SELECT name FROM customers WHERE id BETWEEN 10 AND 12
 Project customers.name  (rows≈22, actual 3 rows, 0.467 ms)
   → Scan customers · primary key >= 10 <= 12
          · span [02000000020000000101800000000000000a, 02000000020000000101800000000000000d)
          · filter ((customers.id >= 10) AND (customers.id <= 12))  (rows≈22, actual 3 rows, 0.465 ms, 1 kv requests)

 EXPLAIN ANALYZE SELECT o.id, o.qty FROM orders o WHERE o.customer_id = 17
 Project o.id, o.qty  (rows≈5, actual 4 rows, 1.172 ms)
   → Scan orders o · index orders_customer = (17)
          · span [020000000400000002018000000000000011, 020000000400000002018000000000000012)
          · filter (o.customer_id = 17)  (rows≈5, actual 4 rows, 1.170 ms, 5 kv requests)
```

The spans are the keys of §6.1. `…0a` to `…0d` covers ids 10–12; the upper
bound is the inclusive 12 turned into "everything before 13". The index scan is
1 request for the index span, plus 4 `Get`s for the rows it found. The filter
stays on top as a re-check, which costs little and keeps the access path a
pure optimization.

## 6.6 Execution: Arrow batches

Each operator consumes whole Arrow record batches and produces one
(operator-at-a-time, as in MonetDB, rather than Volcano's row-at-a-time
`next()`):

- **Expressions** evaluate a column at a time with Arrow compute kernels:
  `cmp::eq`, `numeric::mul`, kleene `and`/`or` (`NULL AND false = false`),
  `like`, and `zip` for `CASE`. Integer overflow and division by zero surface as
  SQLSTATE 22003 and 22012.
- **Hash join and hash aggregation** key on Arrow's *row format*
  (`RowConverter`), the same memcomparable idea as §6.1, built in memory. A
  multi-column key becomes one byte string to hash. Rows with a NULL key never
  match.
- **Sort** is `lexsort_to_indices` followed by `take`, and **LIMIT** is a slice.
  With no filter, LIMIT goes into the scan, so `SELECT … LIMIT 10` reads 10 keys.

The result of §6.2's query is one batch:

```text
column 0 "name" text → arrow Utf8 · 1 values
column 1 "total" double precision → arrow Float64 · 1 values
[Some("acme"), Some("119.5")]
```

## 6.7 What broke on the way

1. **`SELECT 7.0 / 2` returned 3.** Type unification converted a literal to the
   other side's type, so `7.0` became the integer 7. Numbers now widen
   (int → float) before any literal is converted. Only untyped string literals
   take the other side's type.
2. **A panic in `filter`.** Arrow's `prep_null_mask_filter` unwraps the null
   buffer and panics when a boolean array has none. Every SQL condition now goes
   through `null_as_false`, which returns arrays without NULLs unchanged.
3. **Every query paid ~22 ms per key-value request** when the SQL gateway was
   not the range leader. The client slept 20 ms before following a
   `NotLeader` hint, as if it were a failure. It also cached leaders only for
   ranges it had already `Locate`d, so the cache stayed empty and every request
   went through the redirect again. The four-table query took 89 ms from
   node 1 after a restart and 2–3 ms after the fix (the same plan, the same
   data). The same fix lifted `stow-bench bank` on macOS from 40–50 to 74
   transactions/s ([benchmarks](../benchmarks.md#m5--sql-and-the-routing-fix)).
4. **`substr(…)` was "not supported".** sqlparser parses `SUBSTRING`/`substr` as a
   dedicated `Expr::Substring` node rather than a function call.
5. **An idle transaction was aborted as abandoned.** An interactive transaction
   heartbeated only while the client was driving it. A transaction that wrote,
   then sat idle between statements (a user thinking inside `BEGIN … COMMIT`),
   looked abandoned after 2 s to any reader pushing it, and was aborted. A test
   that interleaves two transactions on one task failed under load, about once
   in five full-workspace runs. Now a background task heartbeats the record from
   the first write until commit or rollback. The regression test
   `idle_transaction_survives_a_waiting_reader` keeps a reader retrying for 4 s
   against an idle writer: it fails with the heartbeat disabled and passes with
   it.
