//! Point-in-time recovery from a real cluster to a second one: a full backup,
//! more writes, an accidental DELETE, an incremental backup; then a restore to
//! just before the DELETE.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use stowage_node::testing::TestCluster;
use stowage_sql::{ClusterKv, KvStore, Session};

async fn session(c: &TestCluster) -> Session {
    let store: Arc<dyn KvStore> = Arc::new(ClusterKv { client: c.client() });
    Session::new(store)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_to_before_an_accidental_delete() {
    let dir = std::env::temp_dir().join(format!("stowage-pitr-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let a = TestCluster::start(3).await.unwrap();
    let mut s = session(&a).await;
    s.execute("CREATE TABLE accounts (id INT PRIMARY KEY, owner TEXT, balance INT); CREATE INDEX accounts_owner ON accounts (owner)").await.unwrap();
    s.execute("INSERT INTO accounts VALUES (1, 'ada', 100), (2, 'grace', 200), (3, 'linus', 300)")
        .await
        .unwrap();
    let full = stowage_backup::backup(&a.client(), &dir, true)
        .await
        .unwrap();
    assert_eq!(full.kind, "full");
    s.execute("INSERT INTO accounts VALUES (4, 'barbara', 400); UPDATE accounts SET balance = 150 WHERE id = 1").await.unwrap();
    let before_delete = a.client().now();
    s.execute("DELETE FROM accounts WHERE balance > 100")
        .await
        .unwrap(); // oops: meant < 100
    let inc = stowage_backup::backup(&a.client(), &dir, false)
        .await
        .unwrap();
    assert_eq!(
        (inc.kind.as_str(), inc.from.as_str()),
        ("incremental", full.to.as_str())
    );

    // Restore to a fresh cluster, as of just before the DELETE.
    let b = TestCluster::start(3).await.unwrap();
    let r = stowage_backup::restore(&b.client(), &dir, before_delete, false)
        .await
        .unwrap();
    assert_eq!(r.backups, 2);
    let mut t = session(&b).await;
    let rows = t
        .execute("SELECT id, owner, balance FROM accounts ORDER BY id")
        .await
        .unwrap()[0]
        .rows();
    let got: Vec<String> = rows
        .iter()
        .map(|r| {
            r.iter()
                .map(|v| v.clone().unwrap_or_default())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    assert_eq!(
        got,
        ["1 ada 150", "2 grace 200", "3 linus 300", "4 barbara 400"]
    );
    // The index came back too, and SERIAL / table ids continue where they were.
    let by_owner = t
        .execute("SELECT id FROM accounts WHERE owner = 'grace'")
        .await
        .unwrap()[0]
        .rows();
    assert_eq!(by_owner, vec![vec![Some("2".to_owned())]]);
    // A restore over a non-empty cluster is refused.
    assert!(
        stowage_backup::restore(&b.client(), &dir, before_delete, false)
            .await
            .is_err()
    );

    // The latest state has the DELETE applied, and the table still exists.
    let (latest, _, _) = stowage_backup::state_as_of(&dir, stowage_common::Timestamp::MAX).unwrap();
    let c = TestCluster::start(3).await.unwrap();
    for (k, v) in &latest {
        c.client().put(k, v).await.unwrap();
    }
    let mut u = session(&c).await;
    // Every balance was above 100: the DELETE emptied the table.
    let rows = u.execute("SELECT count(*) FROM accounts").await.unwrap()[0].rows();
    assert_eq!(rows, vec![vec![Some("0".to_owned())]]);
    let _ = std::fs::remove_dir_all(&dir);
}
