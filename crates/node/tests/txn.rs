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

/// A transaction that sits idle between statements (a user thinking inside
/// BEGIN … COMMIT) keeps its record alive: a conflicting reader that waits on it
/// for longer than the abandonment timeout must not abort it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_transaction_survives_a_waiting_reader() {
    let cluster = TestCluster::start(3).await.unwrap();
    let c = cluster.client();
    let mut t = c.begin(Isolation::Serializable);
    t.put(b"k", b"1").await.unwrap();
    let c2 = c.clone();
    // An application retrying its read for 4 s: its pushes keep arriving after
    // the writer's record would look abandoned (2 s) without heartbeats.
    let reader = tokio::spawn(async move {
        let until = tokio::time::Instant::now() + std::time::Duration::from_secs(4);
        while tokio::time::Instant::now() < until {
            let mut r = c2.begin(Isolation::Serializable);
            let v = r.get(b"k").await;
            let _ = r.rollback().await;
            if matches!(v, Ok(Some(_))) {
                return true;
            }
        }
        false
    });
    tokio::time::sleep(std::time::Duration::from_secs(3)).await; // > the 2 s timeout, while the reader pushes
    t.commit()
        .await
        .expect("an idle but heartbeating transaction was aborted");
    let _ = reader.await.unwrap();
    let mut r = c.begin(Isolation::Serializable);
    assert_eq!(r.get(b"k").await.unwrap().as_deref(), Some(&b"1"[..]));
}
