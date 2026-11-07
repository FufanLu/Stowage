//! CREATE INDEX while writers insert, update and delete the same table, on a
//! real three-node cluster: afterwards every row has exactly its entry.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use stowage_kv::txn::Isolation;
use stowage_node::testing::TestCluster;
use stowage_sql::{ClusterKv, KvStore, Session, dml};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn index_builds_online_under_writes() {
    let cluster = TestCluster::start(3).await.unwrap();
    let store: Arc<dyn KvStore> = Arc::new(ClusterKv {
        client: cluster.client(),
    });
    let mut s = Session::new(store.clone());
    s.execute("CREATE TABLE items (id INT PRIMARY KEY, color TEXT, qty INT)")
        .await
        .unwrap();
    let values: Vec<String> = (0..300)
        .map(|i| format!("({i}, '{}', {})", ["red", "green", "blue"][i % 3], i % 7))
        .collect();
    s.execute(&format!("INSERT INTO items VALUES {}", values.join(", ")))
        .await
        .unwrap();

    // Writers keep changing rows, including the indexed column, during the build.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut writers = Vec::new();
    for w in 0..3u64 {
        let (store, stop) = (store.clone(), stop.clone());
        writers.push(tokio::spawn(async move {
            let mut s = Session::new(store);
            let mut n = 0u64;
            let mut done = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                n += 1;
                let id = (n * 37 + w * 101) % 300;
                let sql = match n % 3 {
                    0 => format!("UPDATE items SET color = 'c{n}' WHERE id = {id}"),
                    1 => format!("DELETE FROM items WHERE id = {id}"),
                    _ => format!(
                        "INSERT INTO items VALUES ({}, 'new{w}', 1) ON CONFLICT DO NOTHING",
                        1000 + w * 10_000 + n
                    ),
                };
                if s.execute(&sql).await.is_ok() {
                    done += 1;
                }
            }
            done
        }));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    s.execute("SET index_backfill_batch = 25").await.unwrap();
    s.execute("CREATE INDEX items_color ON items (color)")
        .await
        .unwrap();
    let log = s.schema_log.clone();
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let mut writes = 0;
    for w in writers {
        writes += w.await.unwrap();
    }
    for (t, step) in &log {
        eprintln!("{t:9.1} ms  {step}");
    }
    assert!(log.len() >= 4, "the backfill ran in batches: {log:?}");
    assert!(writes > 10, "writers barely ran: {writes}");

    let mut txn = store.begin(Isolation::Serializable).await.unwrap();
    let (rows, entries, problems) = dml::check_index(txn.as_mut(), "items", "items_color")
        .await
        .unwrap();
    txn.rollback().await.unwrap();
    eprintln!("{rows} rows, {entries} entries, {writes} concurrent writes");
    assert!(
        problems.is_empty(),
        "{} problems, e.g. {:?}",
        problems.len(),
        problems.first()
    );
    assert_eq!(rows, entries);

    // The index is public and used, and agrees with a full scan.
    let plan = s
        .execute("EXPLAIN SELECT id FROM items WHERE color = 'red'")
        .await
        .unwrap();
    assert!(
        plan[0]
            .rows()
            .iter()
            .any(|r| r[0].as_deref().unwrap_or("").contains("index items_color")),
        "{:?}",
        plan[0].rows()
    );
    let by_index = s
        .execute("SELECT count(*) FROM items WHERE color = 'red'")
        .await
        .unwrap()[0]
        .rows();
    let by_scan = s
        .execute("SELECT count(*) FROM items WHERE color || '' = 'red'")
        .await
        .unwrap()[0]
        .rows();
    assert_eq!(by_index, by_scan);
}
