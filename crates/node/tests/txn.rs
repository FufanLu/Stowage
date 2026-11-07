//! Interactive transactions against a real three-node cluster in this process.

#![allow(clippy::unwrap_used)]

use stowage_kv::txn::Isolation;
use stowage_node::TxnError;
use stowage_node::testing::TestCluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interactive_transactions() {
    let cluster = TestCluster::start(3).await.unwrap();
    let c = cluster.client();
    c.split(b"m").await.unwrap();

    // Writes are visible to the transaction itself, invisible to others until commit.
    let mut t = c.begin(Isolation::Serializable);
    t.put(b"alice", b"100").await.unwrap();
    t.put(b"rob", b"5").await.unwrap();
    assert_eq!(t.get(b"alice").await.unwrap().as_deref(), Some(&b"100"[..]));
    let rows = t.scan(b"a", b"", 10).await.unwrap(); // crosses the split at "m"
    assert_eq!(
        rows.iter().map(|(k, _)| k.as_slice()).collect::<Vec<_>>(),
        vec![&b"alice"[..], b"rob"]
    );
    let mut other = c.begin(Isolation::Serializable);
    assert_eq!(
        other.get(b"rob").await.ok().flatten(),
        None,
        "uncommitted write leaked"
    );
    other.rollback().await.unwrap();
    t.commit().await.unwrap();

    // A rolled-back transaction leaves nothing behind.
    let mut t = c.begin(Isolation::Serializable);
    t.put(b"carol", b"1").await.unwrap();
    t.rollback().await.unwrap();
    let mut r = c.begin(Isolation::Serializable);
    assert_eq!(r.get(b"carol").await.unwrap(), None);
    assert_eq!(r.scan(b"", b"", 10).await.unwrap().len(), 3); // \0ready, alice, rob
    r.commit().await.unwrap();

    // Lost update: two transactions read rob, both write it; one must be told to retry.
    let mut a = c.begin(Isolation::Serializable);
    let mut b = c.begin(Isolation::Serializable);
    assert_eq!(a.get(b"rob").await.unwrap().as_deref(), Some(&b"5"[..]));
    assert_eq!(b.get(b"rob").await.unwrap().as_deref(), Some(&b"5"[..]));
    let ra = async {
        a.put(b"rob", b"6").await?;
        a.commit().await
    }
    .await;
    let rb = async {
        b.put(b"rob", b"7").await?;
        b.commit().await
    }
    .await;
    let retried = [&ra, &rb]
        .iter()
        .filter(|r| matches!(r, Err(TxnError::Retry(_))))
        .count();
    assert_eq!(retried, 1, "exactly one must retry: {ra:?} {rb:?}");
}
