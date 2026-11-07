//! A real cluster inside a test process: `n` nodes on loopback ports, each with
//! its own data directory, running the production server code.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use stowage_common::NodeId;
use stowage_kv::StoreConfig;

use crate::{KvClient, NodeConfig};

#[derive(Debug)]
pub struct TestCluster {
    pub peers: BTreeMap<NodeId, String>,
    dir: PathBuf,
}

impl TestCluster {
    /// Start `n` nodes and wait until the cluster accepts writes.
    pub async fn start(n: u64) -> std::io::Result<Self> {
        let mut peers = BTreeMap::new();
        for id in 1..=n {
            // Ask the OS for a free port, then release it for the node.
            let l = std::net::TcpListener::bind("127.0.0.1:0")?;
            peers.insert(id, l.local_addr()?.to_string());
        }
        let dir = std::env::temp_dir().join(format!(
            "stowage-test-{}-{}",
            std::process::id(),
            peers
                .values()
                .next()
                .map_or(String::new(), |a| a.replace([':', '.'], "-"))
        ));
        for (&id, addr) in &peers {
            let cfg = NodeConfig {
                store: StoreConfig::new(id, peers.keys().copied().collect()),
                listen: addr.parse().map_err(std::io::Error::other)?,
                peers: peers.clone(),
                data_dir: dir.join(format!("n{id}")),
                tick: Duration::from_millis(10),
            };
            std::fs::create_dir_all(&cfg.data_dir)?;
            tokio::spawn(async move {
                if let Err(e) = crate::run(cfg).await {
                    tracing::error!(error = %e, "test node stopped");
                }
            });
        }
        let c = Self { peers, dir };
        let client = c.client();
        for _ in 0..200 {
            if client.put(b"\x00ready", b"1").await.is_ok() {
                return Ok(c);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err(std::io::Error::other("cluster did not become writable"))
    }

    pub fn client(&self) -> Arc<KvClient> {
        Arc::new(KvClient::new(self.peers.clone()))
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
