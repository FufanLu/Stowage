//! `stowd` — a Stowage node.
//!
//! ```text
//! stowd --node-id 1 --listen 0.0.0.0:7001 \
//!       --peers 1=n1:7001,2=n2:7001,3=n3:7001 --data-dir /data
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use clap::Parser;
use stowage_kv::StoreConfig;
use stowage_node::{NodeConfig, parse_peers};
use stowage_storage::BTreeOptions;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(version, about = "Stowage database node")]
struct Args {
    /// Node id within the cluster (non-zero).
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    node_id: u64,

    /// Address to listen on for peers and clients.
    #[arg(long)]
    listen: SocketAddr,

    /// Every node: `1=host:port,2=host:port,...` (including this one).
    #[arg(long)]
    peers: String,

    /// Data directory.
    #[arg(long)]
    data_dir: PathBuf,

    /// Voters of range 1 when bootstrapping a new cluster (default: the first three peers).
    #[arg(long)]
    initial_replicas: Option<String>,

    /// Split a range above this size, MiB.
    #[arg(long, default_value_t = 64)]
    split_mib: u64,

    /// Storage engine cache, MiB.
    #[arg(long, default_value_t = 64)]
    cache_mib: u64,

    /// Raft tick, milliseconds (elections time out after 10–20 ticks).
    #[arg(long, default_value_t = 10)]
    tick_ms: u64,

    /// Log as JSON lines.
    #[arg(long)]
    log_json: bool,

    /// Postgres wire protocol listener (SQL); empty to disable.
    #[arg(long, default_value = "127.0.0.1:5480", env = "STOWAGE_PG_LISTEN")]
    pg_listen: String,

    /// DynamoDB-compatible API listener (HTTP); empty to disable.
    #[arg(long, default_value = "127.0.0.1:8480", env = "STOWAGE_DDB_LISTEN")]
    ddb_listen: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if args.log_json {
        builder.json().init();
    } else {
        builder.init();
    }

    let peers = parse_peers(&args.peers).map_err(anyhow::Error::msg)?;
    anyhow::ensure!(
        peers.contains_key(&args.node_id),
        "--peers must include this node ({})",
        args.node_id
    );
    let mut store = StoreConfig::new(args.node_id, peers.keys().copied().collect());
    if let Some(list) = &args.initial_replicas {
        store.initial_replicas = list
            .split(',')
            .map(|s| s.trim().parse().context("bad --initial-replicas"))
            .collect::<anyhow::Result<_>>()?;
    }
    store.split_bytes = args.split_mib << 20;
    store.engine = BTreeOptions {
        cache_pages: usize::try_from(args.cache_mib << 20 >> 13)?,
        ..BTreeOptions::default()
    };
    tracing::info!(node = args.node_id, ?peers, data_dir = %args.data_dir.display(), "starting stowd");
    if !args.ddb_listen.is_empty() {
        let client = std::sync::Arc::new(stowage_node::KvClient::new(peers.clone()));
        let store: std::sync::Arc<dyn stowage_sql::KvStore> =
            std::sync::Arc::new(stowage_sql::ClusterKv {
                client: client.clone(),
            });
        let addr = args.ddb_listen.clone();
        tokio::spawn(async move {
            if let Err(e) = stowage_docapi::serve(&addr, store, Some(client)).await {
                tracing::error!(error = %e, "DynamoDB listener failed");
            }
        });
    }
    if !args.pg_listen.is_empty() {
        // SQL sessions reach the key space through the same client any
        // application would use: every node is a gateway.
        let client = std::sync::Arc::new(stowage_node::KvClient::new(peers.clone()));
        let store: std::sync::Arc<dyn stowage_sql::KvStore> =
            std::sync::Arc::new(stowage_sql::ClusterKv { client });
        let addr = args.pg_listen.clone();
        tokio::spawn(async move {
            if let Err(e) = stowage_pgwire::serve(&addr, store).await {
                tracing::error!(error = %e, "pgwire listener failed");
            }
        });
    }
    stowage_node::run(NodeConfig {
        store,
        listen: args.listen,
        peers,
        data_dir: args.data_dir,
        tick: Duration::from_millis(args.tick_ms),
    })
    .await
    .context("node stopped")
}
