//! Real data for `docs/illustrated/06-planning-a-query.md`.
//!
//! ```text
//! cargo run --release -p stowage-sql --example trace_query
//! ```
//!
//! On the in-memory store: two small tables, their rows and index entries as
//! raw key-value pairs, the AST of one query, its bound and optimized plans,
//! and the Arrow batch the executor returns.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use stowage_kv::txn::Isolation;
use stowage_sql::keycodec::{self, PRIMARY};
use stowage_sql::kv::KvStore;
use stowage_sql::{MemKv, Output, Session, catalog};

fn hex(b: &[u8]) -> String {
    b.iter()
        .map(|x| format!("{x:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let kv = MemKv::default();
    let store: Arc<dyn KvStore> = Arc::new(kv.clone());
    let mut s = Session::new(store.clone());
    s.execute(
        "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, country TEXT);
         CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount FLOAT);
         CREATE INDEX orders_customer ON orders (customer_id);
         INSERT INTO customers VALUES (1, 'acme', 'us'), (2, 'globex', 'de');
         INSERT INTO orders VALUES (10, 1, 99.5), (11, 1, 20), (12, 2, 5.25);",
    )
    .await
    .unwrap();

    println!("== the key space ==");
    let mut t = store.begin(Isolation::Serializable).await.unwrap();
    for (k, v) in t.scan(&[], &[], 100).await.unwrap() {
        let what = match k[0] {
            keycodec::META => format!("catalog {:?}", String::from_utf8_lossy(&k[1..])),
            _ => {
                let table = u32::from_be_bytes(k[1..5].try_into().unwrap());
                let index = u32::from_be_bytes(k[5..9].try_into().unwrap());
                let tables = catalog::list_tables(t.as_mut()).await.unwrap();
                let tbl = tables.iter().find(|x| x.id == table).unwrap();
                if index == PRIMARY {
                    format!(
                        "{} row {:?}",
                        tbl.name,
                        keycodec::decode_row(&v, tbl.columns.len()).unwrap()
                    )
                } else {
                    let ix = tbl.indexes.iter().find(|i| i.id == index).unwrap();
                    format!("{} index {} entry", tbl.name, ix.name)
                }
            }
        };
        println!(
            "{what}\n    key   {}\n    value {} ({} bytes)",
            hex(&k),
            hex(&v[..v.len().min(24)]),
            v.len()
        );
    }
    t.rollback().await.unwrap();

    let q = "SELECT c.name, sum(o.amount) AS total FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.country = 'us' GROUP BY c.name";
    println!("\n== AST ==");
    let ast = stowage_sql::session::parse(q).unwrap();
    let dbg = format!("{:#?}", ast[0]);
    for l in dbg
        .lines()
        .filter(|l| {
            !l.contains("span")
                && !l.contains("Span")
                && !l.contains("Location")
                && !l.contains("line:")
                && !l.contains("column:")
        })
        .take(60)
    {
        println!("{l}");
    }
    println!("    …");

    for sql in [
        format!("EXPLAIN VERBOSE {q}"),
        "ANALYZE".into(),
        format!("EXPLAIN VERBOSE {q}"),
    ] {
        println!(
            "\n== {} ==",
            sql.split_whitespace().take(2).collect::<Vec<_>>().join(" ")
        );
        for o in s.execute(&sql).await.unwrap() {
            for r in o.rows() {
                println!("{}", r[0].clone().unwrap_or_default());
            }
        }
    }

    println!("\n== result batch ==");
    let out = s.execute(q).await.unwrap().pop().unwrap();
    if let Output::Rows { columns, batch } = &out {
        for (i, (name, ty)) in columns.iter().enumerate() {
            println!(
                "column {i} {name:?} {ty} → arrow {} · {} values",
                batch.column(i).data_type(),
                batch.column(i).len()
            );
        }
        for r in out.rows() {
            println!("{r:?}");
        }
    }
}
