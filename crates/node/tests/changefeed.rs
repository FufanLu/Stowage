//! A changefeed delivers every committed version exactly once, at its commit
//! timestamp, while writers commit and conflict concurrently.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use stowage_kv::txn::Isolation;
use stowage_node::Changefeed;
use stowage_node::testing::TestCluster;

/// (key, commit timestamp, value)
type Write = (Vec<u8>, String, Vec<u8>);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changefeed_is_exactly_once() {
    let cluster = TestCluster::start(3).await.unwrap();
    let c = cluster.client();
    c.split(b"cf/m").await.unwrap(); // the feed spans two ranges
    let start_ts = c.now();
    let committed: Arc<Mutex<BTreeSet<Write>>> = Arc::default();
    let stop = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut writers = Vec::new();
    for w in 0..4u64 {
        let (c, committed) = (c.clone(), committed.clone());
        writers.push(tokio::spawn(async move {
            let mut n = 0u64;
            while tokio::time::Instant::now() < stop {
                n += 1;
                // Two keys per transaction, from a small set: conflicts happen.
                let k1 =
                    format!("cf/{}", ["a", "f", "p", "x"][((w + n) % 4) as usize]).into_bytes();
                let k2 =
                    format!("cf/{}", ["c", "k", "r", "z"][((w * 3 + n) % 4) as usize]).into_bytes();
                let v = format!("w{w}-{n}").into_bytes();
                let mut t = c.begin(Isolation::Serializable);
                let r = async {
                    t.put(&k1, &v).await?;
                    t.put(&k2, &v).await?;
                    t.commit().await
                }
                .await;
                if let Ok(ts) = r {
                    let mut set = committed.lock().unwrap();
                    set.insert((k1, ts.to_string(), v.clone()));
                    set.insert((k2, ts.to_string(), v));
                }
            }
        }));
    }
    let mut feed = Changefeed::new(c.clone(), b"cf/", b"cf0", start_ts);
    let mut seen: Vec<Write> = Vec::new();
    while tokio::time::Instant::now() < stop {
        let (events, _) = feed.poll().await.unwrap();
        seen.extend(
            events
                .into_iter()
                .map(|e| (e.key, e.ts.to_string(), e.value.unwrap_or_default())),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for w in writers {
        w.await.unwrap();
    }
    let (events, _) = feed.poll().await.unwrap();
    seen.extend(
        events
            .into_iter()
            .map(|e| (e.key, e.ts.to_string(), e.value.unwrap_or_default())),
    );
    let committed = committed.lock().unwrap().clone();
    let unique: BTreeSet<_> = seen.iter().cloned().collect();
    assert_eq!(unique.len(), seen.len(), "a change was delivered twice");
    let missing: Vec<_> = committed.difference(&unique).collect();
    let extra: Vec<_> = unique.difference(&committed).collect();
    assert!(
        missing.is_empty(),
        "{} committed writes never delivered, e.g. {:?}",
        missing.len(),
        missing.first()
    );
    assert!(
        extra.is_empty(),
        "{} delivered writes never committed, e.g. {:?}",
        extra.len(),
        extra.first()
    );
    assert!(
        committed.len() > 20,
        "only {} writes committed",
        committed.len()
    );
    eprintln!("{} committed writes, all delivered once", committed.len());
}
