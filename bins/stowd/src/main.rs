//! `stowd` — a Stowage node.
//!
//! M0 only validates its environment: it opens the data directory through the same
//! `Disk` trait the storage engine will use, and starts the node's hybrid logical
//! clock. Later milestones add the storage engine, Raft, and the SQL / DynamoDB
//! front ends behind this same entry point.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context as _;
use clap::Parser;
use stowage_common::os::{OsDisk, SystemClock};
use stowage_common::{Disk as _, Hlc};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(version, about = "Stowage database node")]
struct Args {
    /// Node id within the cluster (non-zero).
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u64).range(1..))]
    node_id: u64,

    /// Data directory.
    #[arg(long, default_value = "./data/n1")]
    data_dir: PathBuf,

    /// Maximum tolerated clock offset between nodes, in milliseconds.
    #[arg(long, default_value_t = 500)]
    max_offset_ms: u64,

    /// Log as JSON lines instead of human-readable text.
    #[arg(long)]
    log_json: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    init_logging(args.log_json);

    let disk = OsDisk::open(&args.data_dir)
        .with_context(|| format!("opening data dir {}", args.data_dir.display()))?;
    let files = disk.list().context("listing data dir")?;
    let hlc = Hlc::new(Arc::new(SystemClock), args.max_offset_ms * 1_000_000);

    tracing::info!(
        node_id = args.node_id,
        data_dir = %args.data_dir.display(),
        files = files.len(),
        hlc = %hlc.now(),
        version = env!("CARGO_PKG_VERSION"),
        "stowd environment ok (M0: no services yet)"
    );
    Ok(())
}

fn init_logging(json: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
}
