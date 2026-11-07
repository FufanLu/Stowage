//! A real Postgres client (tokio-postgres) against the wire protocol, over a
//! real three-node cluster: simple queries, the extended protocol with binary
//! results, transactions, and SQLSTATEs.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use stowage_node::testing::TestCluster;
use stowage_sql::{ClusterKv, KvStore};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_client_works() {
    let cluster = TestCluster::start(3).await.unwrap();
    let store: Arc<dyn KvStore> = Arc::new(ClusterKv {
        client: cluster.client(),
    });
    let addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    };
    let a = addr.clone();
    tokio::spawn(async move { stowage_pgwire::serve(&a, store).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let (host, port) = addr.split_once(':').unwrap();
    let (client, conn) = tokio_postgres::connect(
        &format!("host={host} port={port} user=stowage dbname=stowage"),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);

    client
        .batch_execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT, score DOUBLE PRECISION, ok BOOLEAN, at TIMESTAMP);
             INSERT INTO t VALUES (1, 'a', 1.5, true, '2026-11-06 10:00:00'), (2, 'b', NULL, false, NULL)",
        )
        .await
        .unwrap();

    // Extended protocol, binary results.
    let rows = client
        .query("SELECT id, name, score, ok FROM t ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, &str>(1), "a");
    assert_eq!(rows[0].get::<_, Option<f64>>(2), Some(1.5));
    assert!(rows[0].get::<_, bool>(3));
    assert_eq!(rows[1].get::<_, Option<f64>>(2), None);

    // Parameters (sent as text, typed by the server).
    let rows = client
        .query("SELECT name FROM t WHERE id = $1", &[&"2"])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, &str>(0), "b");

    // Simple query protocol, text results.
    let msgs = client.simple_query("SELECT count(*) FROM t").await.unwrap();
    let count = msgs.iter().find_map(|m| {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            r.get(0).map(str::to_owned)
        } else {
            None
        }
    });
    assert_eq!(count.as_deref(), Some("2"));

    // Transactions and SQLSTATEs.
    client
        .batch_execute("BEGIN; UPDATE t SET name = 'z' WHERE id = 1; ROLLBACK")
        .await
        .unwrap();
    let rows = client
        .query("SELECT name FROM t WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, &str>(0), "a");
    let err = client
        .batch_execute("INSERT INTO t VALUES (1, 'dup', 0, true, NULL)")
        .await
        .unwrap_err();
    assert_eq!(err.code().map(|c| c.code()), Some("23505"));
    let err = client.query("SELECT nope FROM t", &[]).await.unwrap_err();
    assert_eq!(err.code().map(|c| c.code()), Some("42703"));
    // The connection is still usable after errors.
    let rows = client.query("SELECT 40 + 2", &[]).await.unwrap();
    assert_eq!(rows[0].get::<_, i64>(0), 42);
}
