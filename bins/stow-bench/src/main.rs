//! `stow-bench` — Stowage benchmarks.
//!
//! ```text
//! stow-bench engine --engine both --keys 500000 --value-size 100
//! ```

use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use clap::{Parser, Subcommand, ValueEnum};
use hdrhistogram::Histogram;
use stowage_common::Rng;
use stowage_common::os::{OsDisk, Xoshiro256};
use stowage_storage::lsm::LsmEngine;
use stowage_storage::{BTree, BTreeOptions, Engine, WriteBatch};

#[derive(Debug, Parser)]
#[command(version, about = "Stowage benchmarks")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Single-threaded storage-engine workloads on the real filesystem.
    Engine(EngineArgs),
    /// Concurrent key-value workload against a running cluster.
    Kv(KvArgs),
}

#[derive(Debug, clap::Args)]
struct KvArgs {
    #[arg(
        long,
        default_value = "1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003"
    )]
    peers: String,
    #[arg(long, default_value_t = 100_000)]
    keys: u64,
    #[arg(long, default_value_t = 100)]
    value_size: usize,
    /// Concurrent requests in flight.
    #[arg(long, default_value_t = 64)]
    concurrency: usize,
    /// Seconds of measured load.
    #[arg(long, default_value_t = 20)]
    duration: u64,
    /// Fraction of operations that are reads.
    #[arg(long, default_value_t = 0.5)]
    read_ratio: f64,
    /// Split the key space into this many ranges before loading.
    #[arg(long, default_value_t = 12)]
    presplit: u64,
    /// Skip loading the keys (they are already there).
    #[arg(long)]
    no_load: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Which {
    Btree,
    Lsm,
    Both,
}

#[derive(Debug, clap::Args)]
struct EngineArgs {
    #[arg(long, value_enum, default_value_t = Which::Both)]
    engine: Which,
    #[arg(long, default_value_t = 500_000)]
    keys: u64,
    #[arg(long, default_value_t = 100)]
    value_size: usize,
    /// Cache size for either engine, MiB.
    #[arg(long, default_value_t = 32)]
    cache_mib: u64,
    /// B+Tree checkpoint interval, MiB of WAL.
    #[arg(long, default_value_t = 256)]
    checkpoint_mib: u64,
    #[arg(long, default_value_t = 200_000)]
    gets: u64,
    #[arg(long, default_value_t = 20_000)]
    scans: u64,
    #[arg(long, default_value_t = 2_000)]
    sync_writes: u64,
    /// Scratch directory (wiped first).
    #[arg(long)]
    dir: Option<PathBuf>,
}

/// 20-byte keys: `user` + 16 hex digits of a scrambled index, so insertion order
/// is random with respect to key order.
fn key(i: u64) -> Vec<u8> {
    let h = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (i >> 7);
    format!("user{h:016x}").into_bytes()
}

struct Phase {
    name: &'static str,
    ops: u64,
    secs: f64,
    hist: Histogram<u64>,
}

impl Phase {
    fn print(&self, engine: &str) {
        let q = |p| self.hist.value_at_quantile(p) as f64 / 1e3;
        println!(
            "| {engine:<6} | {:<20} | {:>10.0} | {:>8.2} | {:>8.2} | {:>9.2} |",
            self.name,
            self.ops as f64 / self.secs,
            q(0.5),
            q(0.99),
            q(0.999),
        );
    }
}

fn timed(
    name: &'static str,
    ops: u64,
    mut f: impl FnMut(u64) -> anyhow::Result<()>,
) -> anyhow::Result<Phase> {
    let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)?;
    let start = Instant::now();
    for i in 0..ops {
        let t = Instant::now();
        f(i)?;
        hist.record(u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX))?;
    }
    Ok(Phase {
        name,
        ops,
        secs: start.elapsed().as_secs_f64(),
        hist,
    })
}

fn dir_size(dir: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            match e.metadata() {
                Ok(m) if m.is_dir() => total += dir_size(&e.path()),
                Ok(m) => total += m.len(),
                Err(_) => {}
            }
        }
    }
    total
}

fn run_engine(
    name: &str,
    engine: &dyn Engine,
    a: &EngineArgs,
    dir: &Path,
    settle: &dyn Fn() -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let value = vec![0x5Au8; a.value_size];
    let mut rng = Xoshiro256::seeded(7);
    let mut phases = Vec::new();

    phases.push(timed("load (no sync)", a.keys, |i| {
        let mut b = WriteBatch::new();
        b.put(key(i), value.clone());
        Ok(engine.write(&b, false)?)
    })?);
    engine.sync()?;
    phases.push(timed("get (uniform)", a.gets, |_| {
        let k = key(rng.gen_range(0, a.keys));
        anyhow::ensure!(engine.get(&k)?.is_some(), "missing key");
        Ok(())
    })?);
    phases.push(timed("get (missing)", a.gets, |i| {
        let k = format!("zzzz{i:016x}").into_bytes();
        anyhow::ensure!(engine.get(&k)?.is_none(), "unexpected key");
        Ok(())
    })?);
    phases.push(timed("scan 100", a.scans, |_| {
        let start = key(rng.gen_range(0, a.keys));
        let got = engine.scan(Bound::Included(&start), Bound::Unbounded, 100)?;
        anyhow::ensure!(!got.is_empty(), "empty scan");
        Ok(())
    })?);
    phases.push(timed("update (fsync each)", a.sync_writes, |_| {
        let mut b = WriteBatch::new();
        b.put(key(rng.gen_range(0, a.keys)), value.clone());
        Ok(engine.write(&b, true)?)
    })?);
    engine.sync()?;
    settle()?;
    for p in &phases {
        p.print(name);
    }
    let user = a.keys * (20 + a.value_size as u64);
    println!(
        "| {name:<6} | on-disk size         | {:>7.1} MiB for {:.1} MiB of user data ({:.2}x) |",
        dir_size(dir) as f64 / (1 << 20) as f64,
        user as f64 / (1 << 20) as f64,
        dir_size(dir) as f64 / user as f64
    );
    Ok(())
}

fn engine_bench(a: &EngineArgs) -> anyhow::Result<()> {
    let root = a
        .dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("stow-bench"));
    println!(
        "keys {} · value {} B · cache {} MiB · single thread · {}",
        a.keys,
        a.value_size,
        a.cache_mib,
        std::env::consts::ARCH
    );
    println!("\n| engine | phase                |    ops/s   | p50 µs   | p99 µs   | p99.9 µs  |");
    println!("|--------|----------------------|-----------:|---------:|---------:|----------:|");
    if matches!(a.engine, Which::Btree | Which::Both) {
        let dir = root.join("btree");
        let _ = std::fs::remove_dir_all(&dir);
        let opts = BTreeOptions {
            cache_pages: usize::try_from(a.cache_mib << 20 >> 13)?,
            checkpoint_wal_bytes: a.checkpoint_mib << 20,
        };
        let (t, _) = BTree::open(Arc::new(OsDisk::open(&dir)?), opts)?;
        // Measure size after a checkpoint, so pages are in the data file and old WAL
        // segments are gone.
        run_engine("btree", &t, a, &dir, &|| Ok(t.checkpoint().map(drop)?))?;
        let s = t.stats();
        let user = a.keys * (20 + a.value_size as u64);
        let written = s.wal.bytes + s.pool.page_writes * 8192;
        println!(
            "\nbtree: height {} · {} pages · {} leaf / {} internal splits · WAL {:.1} MiB \
             ({} full-page images averaging {:.0} B, {} deltas averaging {:.0} B) · {} page write-backs · \
             {} checkpoints · write amplification ≈ {:.1}x · {:.1}% optimistic writes",
            s.height,
            s.pages,
            s.leaf_splits,
            s.internal_splits,
            s.wal.bytes as f64 / (1 << 20) as f64,
            s.images_logged,
            s.image_bytes as f64 / s.images_logged.max(1) as f64,
            s.deltas_logged,
            s.delta_bytes as f64 / s.deltas_logged.max(1) as f64,
            s.pool.page_writes,
            s.checkpoints,
            written as f64 / user as f64,
            100.0 * s.optimistic_writes as f64
                / (s.optimistic_writes + s.pessimistic_writes).max(1) as f64,
        );
    }
    if matches!(a.engine, Which::Lsm | Which::Both) {
        let dir = root.join("lsm");
        let _ = std::fs::remove_dir_all(&dir);
        let e = LsmEngine::open(&dir, a.cache_mib << 20)?;
        run_engine("lsm", &e, a, &dir, &|| Ok(()))?;
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().cmd {
        Cmd::Engine(a) => engine_bench(&a),
        Cmd::Kv(a) => tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(kv_bench(a)),
    }
}

#[derive(Default)]
struct KvStats {
    reads: Option<Histogram<u64>>,
    writes: Option<Histogram<u64>>,
    ambiguous: u64,
    errors: u64,
}

async fn kv_bench(a: KvArgs) -> anyhow::Result<()> {
    use std::sync::Arc;
    use std::time::Duration;
    let client = Arc::new(stowage_node::KvClient::new(
        stowage_node::parse_peers(&a.peers).map_err(anyhow::Error::msg)?,
    ));
    let key = |i: u64| format!("user{:016x}", i.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    if a.presplit > 1 {
        // Keys are uniform hex: split at evenly spaced prefixes.
        for i in 1..a.presplit {
            let at = format!("user{:016x}", u64::MAX / a.presplit * i);
            client.split(at.as_bytes()).await?;
        }
        println!(
            "pre-split into {} ranges; waiting 5 s for the scheduler to spread leaders",
            a.presplit
        );
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    let value = vec![b'v'; a.value_size];
    if !a.no_load {
        let t = Instant::now();
        let next = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut tasks = Vec::new();
        for _ in 0..a.concurrency {
            let (client, next, value) = (client.clone(), next.clone(), value.clone());
            let keys = a.keys;
            tasks.push(tokio::spawn(async move {
                loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if i >= keys {
                        return;
                    }
                    let _ = client.put(key(i).as_bytes(), &value).await;
                }
            }));
        }
        for t in tasks {
            t.await?;
        }
        println!(
            "loaded {} keys in {:.1} s ({:.0} writes/s)",
            a.keys,
            t.elapsed().as_secs_f64(),
            a.keys as f64 / t.elapsed().as_secs_f64()
        );
    }
    let deadline = Instant::now() + Duration::from_secs(a.duration);
    let mut tasks = Vec::new();
    for w in 0..a.concurrency {
        let (client, value) = (client.clone(), value.clone());
        let (keys, ratio) = (a.keys, a.read_ratio);
        tasks.push(tokio::spawn(async move {
            let mut rng = Xoshiro256::seeded(w as u64 + 1);
            let mut st = KvStats {
                reads: Some(Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("hist")),
                writes: Some(
                    Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("hist"),
                ),
                ..KvStats::default()
            };
            while Instant::now() < deadline {
                let k = key(rng.gen_range(0, keys));
                let t = Instant::now();
                if rng.gen_bool(ratio) {
                    match client.get(k.as_bytes()).await {
                        Ok(_) => {
                            let _ = st
                                .reads
                                .as_mut()
                                .map(|h| h.record(t.elapsed().as_nanos() as u64));
                        }
                        Err(_) => st.errors += 1,
                    }
                } else {
                    match client.put(k.as_bytes(), &value).await {
                        Ok(()) => {
                            let _ = st
                                .writes
                                .as_mut()
                                .map(|h| h.record(t.elapsed().as_nanos() as u64));
                        }
                        Err(stowage_node::ClientError::Ambiguous) => st.ambiguous += 1,
                        Err(_) => st.errors += 1,
                    }
                }
            }
            st
        }));
    }
    let mut reads = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)?;
    let mut writes = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)?;
    let (mut ambiguous, mut errors) = (0, 0);
    for t in tasks {
        let st = t.await?;
        if let Some(h) = st.reads {
            reads.add(h)?;
        }
        if let Some(h) = st.writes {
            writes.add(h)?;
        }
        ambiguous += st.ambiguous;
        errors += st.errors;
    }
    let secs = a.duration as f64;
    let q = |h: &Histogram<u64>, p: f64| h.value_at_quantile(p) as f64 / 1e6;
    println!(
        "\nconcurrency {} · {} keys · value {} B · read ratio {} · {} s",
        a.concurrency, a.keys, a.value_size, a.read_ratio, a.duration
    );
    println!("| op    |   ops/s | p50 ms | p99 ms | p99.9 ms |");
    println!("|-------|--------:|-------:|-------:|---------:|");
    for (name, h) in [("read", &reads), ("write", &writes)] {
        println!(
            "| {name:<5} | {:>7.0} | {:>6.2} | {:>6.2} | {:>8.2} |",
            h.len() as f64 / secs,
            q(h, 0.5),
            q(h, 0.99),
            q(h, 0.999)
        );
    }
    println!(
        "total {:.0} ops/s · ambiguous writes {ambiguous} · errors {errors}",
        (reads.len() + writes.len()) as f64 / secs
    );
    let ranges = client.ranges().await?;
    let mut leaders = std::collections::BTreeMap::new();
    for r in &ranges {
        *leaders.entry(r.leader.unwrap_or(0)).or_insert(0) += 1;
    }
    println!("{} ranges; leaders per node {:?}", ranges.len(), leaders);
    Ok(())
}
