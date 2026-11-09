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
    /// Bank transfers and audits as transactions against a running cluster;
    /// checks that every audit sees the same total.
    Bank(BankArgs),
    /// YCSB core workloads A/B/C, defined exactly as Cartwheel's wheel-bench
    /// defines them, for the AP-vs-CP comparison (chapter 11).
    Ycsb(YcsbArgs),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Update {
    /// GetForUpdate and Put in one serializable transaction: no update is
    /// lost, and writers of a hot key queue on its lock.
    Txn,
    /// Get and Put in one serializable transaction (optimistic: a concurrent
    /// commit to the key restarts it).
    Occ,
    /// A plain Get, then a plain Put (each linearizable, the pair not atomic).
    Blind,
}

#[derive(Debug, clap::Args)]
struct YcsbArgs {
    #[arg(
        long,
        default_value = "1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003"
    )]
    peers: String,
    /// A = 50% read / 50% update, B = 95 / 5, C = read only.
    #[arg(long, default_value = "A")]
    workload: String,
    #[arg(long, default_value_t = 50_000)]
    keys: u64,
    /// Insert this many keys first (0: assume they are there).
    #[arg(long, default_value_t = 0)]
    load: u64,
    #[arg(long, default_value_t = 100)]
    value_size: usize,
    /// Clients in flight (wheel-bench's --threads).
    #[arg(long, default_value_t = 32)]
    threads: usize,
    #[arg(long, default_value_t = 30)]
    duration: u64,
    #[arg(long, value_enum, default_value_t = Update::Txn)]
    update: Update,
    /// Split the key space into this many ranges before loading.
    #[arg(long, default_value_t = 1)]
    presplit: u64,
}

/// Uniform in [0, 1) from 53 random bits.
fn unit(rng: &mut Xoshiro256) -> f64 {
    use stowage_common::env::Rng as _;
    (rng.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
}

/// wheel-bench's key for rank `r`, so both systems store the same keys.
fn ycsb_key(rank: u64) -> Vec<u8> {
    format!(
        "user{:016x}",
        (rank + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15)
    )
    .into_bytes()
}

/// Zipfian over [0, n) by an inverse-CDF table, theta 0.99 (YCSB's default),
/// the same construction as wheel-bench.
struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new(n: u64, theta: f64) -> Self {
        let sum: f64 = (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum();
        let mut acc = 0.0;
        let cdf = (1..=n)
            .map(|i| {
                acc += 1.0 / (i as f64).powf(theta) / sum;
                acc
            })
            .collect();
        Self { cdf }
    }

    fn next(&self, rng: &mut Xoshiro256) -> u64 {
        let u = unit(rng);
        self.cdf.partition_point(|&c| c < u).min(self.cdf.len() - 1) as u64
    }
}

#[derive(Default)]
struct YcsbStats {
    ops: std::sync::atomic::AtomicU64,
    errors: std::sync::atomic::AtomicU64,
    unavailable: std::sync::atomic::AtomicU64,
    restarts: std::sync::atomic::AtomicU64,
    hist: std::sync::Mutex<Option<Histogram<u64>>>,
}

async fn ycsb_bench(a: YcsbArgs) -> anyhow::Result<()> {
    use std::sync::Arc;
    use std::sync::atomic::Ordering::Relaxed;
    use std::time::Duration;
    use stowage_kv::txn::{Isolation, Outcome, Step, TxnOp};
    let client = Arc::new(stowage_node::KvClient::new(
        stowage_node::parse_peers(&a.peers).map_err(anyhow::Error::msg)?,
    ));
    let value = vec![b'x'; a.value_size];
    if a.presplit > 1 {
        for i in 1..a.presplit {
            client
                .split(format!("user{:016x}", u64::MAX / a.presplit * i).as_bytes())
                .await?;
        }
        println!(
            "pre-split into {} ranges; waiting 5 s for leaders to spread",
            a.presplit
        );
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    if a.load > 0 {
        let t = Instant::now();
        let next = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let failed = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut tasks = Vec::new();
        for _ in 0..a.threads {
            let (client, next, failed, value, load) = (
                client.clone(),
                next.clone(),
                failed.clone(),
                value.clone(),
                a.load,
            );
            tasks.push(tokio::spawn(async move {
                loop {
                    let i = next.fetch_add(1, Relaxed);
                    if i >= load {
                        return;
                    }
                    if client.put(&ycsb_key(i), &value).await.is_err() {
                        failed.fetch_add(1, Relaxed);
                    }
                }
            }));
        }
        for t in tasks {
            t.await?;
        }
        let secs = t.elapsed().as_secs_f64();
        println!(
            "load: {} keys in {secs:.1}s = {:.0} writes/s ({} failed)",
            a.load,
            a.load as f64 / secs,
            failed.load(Relaxed)
        );
        if a.duration == 0 {
            return Ok(());
        }
    }
    let read_ratio = match a.workload.as_str() {
        "C" => 1.0,
        "B" => 0.95,
        _ => 0.5,
    };
    let zipf = Arc::new(Zipf::new(a.keys, 0.99));
    let stats: Vec<Arc<YcsbStats>> = (0..a.threads)
        .map(|_| {
            let s = YcsbStats::default();
            *s.hist.lock().expect("hist") =
                Some(Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("hist"));
            Arc::new(s)
        })
        .collect();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut tasks = Vec::new();
    for (t, st) in stats.iter().enumerate() {
        let (client, zipf, st, stop, value, update) = (
            client.clone(),
            zipf.clone(),
            st.clone(),
            stop.clone(),
            value.clone(),
            a.update,
        );
        tasks.push(tokio::spawn(async move {
            let mut rng = Xoshiro256::seeded(1000 + t as u64);
            while !stop.load(Relaxed) {
                let key = ycsb_key(zipf.next(&mut rng));
                let started = Instant::now();
                let write = unit(&mut rng) >= read_ratio;
                let ok = if !write {
                    client.get(&key).await.map(drop)
                } else if update == Update::Blind {
                    match client.get(&key).await {
                        Ok(_) => client.put(&key, &value).await,
                        Err(e) => Err(e),
                    }
                } else {
                    let (k, v) = (key.clone(), value.clone());
                    let program: stowage_kv::txn::Program = Box::new(move |_, done| match done {
                        0 if update == Update::Txn => Step::Op(TxnOp::GetForUpdate(k.clone())),
                        0 => Step::Op(TxnOp::Get(k.clone())),
                        1 => Step::Op(TxnOp::Put(k.clone(), v.clone())),
                        _ => Step::Commit,
                    });
                    match client.transact(Isolation::Serializable, program).await {
                        Ok(Outcome::Committed { restarts, .. }) => {
                            st.restarts.fetch_add(u64::from(restarts), Relaxed);
                            Ok(())
                        }
                        Ok(other) => {
                            if std::env::var("YCSB_DEBUG").is_ok() {
                                eprintln!("outcome: {other:?}");
                            }
                            Err(stowage_node::ClientError::Timeout)
                        }
                        Err(e) => Err(e),
                    }
                };
                if let Some(h) = st.hist.lock().expect("hist").as_mut() {
                    let _ = h.record(started.elapsed().as_nanos() as u64);
                }
                st.ops.fetch_add(1, Relaxed);
                if let Err(e) = ok {
                    if st.errors.load(Relaxed) < 2 && std::env::var("YCSB_DEBUG").is_ok() {
                        eprintln!("error: {e:?} after {:?}", started.elapsed());
                    }
                    st.errors.fetch_add(1, Relaxed);
                    if matches!(
                        e,
                        stowage_node::ClientError::Timeout
                            | stowage_node::ClientError::Exhausted(..)
                    ) {
                        st.unavailable.fetch_add(1, Relaxed);
                    }
                }
            }
        }));
    }
    println!(
        "workload {} ({:.0}% reads, updates {:?}), {} clients, {} keys zipfian(0.99)",
        a.workload,
        read_ratio * 100.0,
        a.update,
        a.threads,
        a.keys
    );
    println!(
        "{:>4} {:>9} {:>7} {:>9} {:>9} {:>9}",
        "t(s)", "ops/s", "errors", "p50", "p99", "p99.9"
    );
    let snapshot = |stats: &[Arc<YcsbStats>]| -> anyhow::Result<(u64, u64, Histogram<u64>)> {
        let mut h = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)?;
        let (mut ops, mut errors) = (0, 0);
        for s in stats {
            ops += s.ops.load(Relaxed);
            errors += s.errors.load(Relaxed);
            if let Some(x) = s.hist.lock().expect("hist").as_ref() {
                h.add(x)?;
            }
        }
        Ok((ops, errors, h))
    };
    let ms = |ns: u64| {
        let m = ns as f64 / 1e6;
        if m < 10.0 {
            format!("{m:.2}ms")
        } else {
            format!("{m:.0}ms")
        }
    };
    let (mut prev_ops, mut prev_err) = (0, 0);
    let mut prev_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)?;
    for sec in 1..=a.duration {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (ops, errors, h) = snapshot(&stats)?;
        let mut interval = h.clone();
        interval.subtract(&prev_hist)?;
        println!(
            "{sec:>4} {:>9} {:>7} {:>9} {:>9} {:>9}",
            ops - prev_ops,
            errors - prev_err,
            ms(interval.value_at_quantile(0.5)),
            ms(interval.value_at_quantile(0.99)),
            ms(interval.value_at_quantile(0.999))
        );
        (prev_ops, prev_err, prev_hist) = (ops, errors, h);
    }
    stop.store(true, Relaxed);
    for t in tasks {
        t.await?;
    }
    let (ops, errors, h) = snapshot(&stats)?;
    let unavailable: u64 = stats.iter().map(|s| s.unavailable.load(Relaxed)).sum();
    let restarts: u64 = stats.iter().map(|s| s.restarts.load(Relaxed)).sum();
    println!(
        "total: {ops} ops in {}s = {:.0} ops/s, {errors} errors ({unavailable} unavailable/timeout), {restarts} transaction restarts, p50 {} p99 {} p99.9 {}",
        a.duration,
        ops as f64 / a.duration as f64,
        ms(h.value_at_quantile(0.5)),
        ms(h.value_at_quantile(0.99)),
        ms(h.value_at_quantile(0.999))
    );
    Ok(())
}

#[derive(Debug, clap::Args)]
struct BankArgs {
    #[arg(
        long,
        default_value = "1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003"
    )]
    peers: String,
    #[arg(long, default_value_t = 1000)]
    accounts: u64,
    /// Transactions in flight.
    #[arg(long, default_value_t = 32)]
    concurrency: usize,
    #[arg(long, default_value_t = 20)]
    duration: u64,
    /// Fraction of transactions that are audits reading `audit_size` accounts.
    #[arg(long, default_value_t = 0.1)]
    audit_ratio: f64,
    #[arg(long, default_value_t = 10)]
    audit_size: u64,
    /// `ssi` or `si`.
    #[arg(long, default_value = "ssi")]
    isolation: String,
    /// Split the accounts into this many ranges first.
    #[arg(long, default_value_t = 6)]
    presplit: u64,
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
        Cmd::Bank(a) => tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(bank_bench(a)),
        Cmd::Ycsb(a) => tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(ycsb_bench(a)),
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

const BALANCE: u64 = 1000;

/// A committed transfer: (commit timestamp, from, to, amount, reads).
type Committed = (
    stowage_common::Timestamp,
    u64,
    u64,
    u64,
    Vec<Option<Vec<u8>>>,
);

fn acct(i: u64) -> Vec<u8> {
    format!("acct{i:06}").into_bytes()
}

fn balance(v: &Option<Vec<u8>>) -> u64 {
    v.as_ref()
        .and_then(|b| b.get(..8))
        .map_or(0, |b| u64::from_le_bytes(b.try_into().unwrap_or([0; 8])))
}

async fn bank_bench(a: BankArgs) -> anyhow::Result<()> {
    use std::sync::Arc;
    use std::time::Duration;
    use stowage_kv::txn::{Isolation, Outcome, Step, TxnOp};
    let iso = match a.isolation.as_str() {
        "si" => Isolation::Snapshot,
        _ => Isolation::Serializable,
    };
    let client = Arc::new(stowage_node::KvClient::new(
        stowage_node::parse_peers(&a.peers).map_err(anyhow::Error::msg)?,
    ));
    for i in 1..a.presplit {
        client.split(&acct(a.accounts / a.presplit * i)).await?;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    // Fund every account, 50 per transaction.
    let t = Instant::now();
    for chunk in (0..a.accounts).collect::<Vec<_>>().chunks(50) {
        let keys: Vec<u64> = chunk.to_vec();
        let out = client
            .transact(
                iso,
                Box::new(move |_, done| match keys.get(done) {
                    Some(&k) => Step::Op(TxnOp::Put(acct(k), BALANCE.to_le_bytes().to_vec())),
                    None => Step::Commit,
                }),
            )
            .await?;
        anyhow::ensure!(
            matches!(out, Outcome::Committed { .. }),
            "funding failed: {out:?}"
        );
    }
    println!(
        "funded {} accounts in {:.1} s",
        a.accounts,
        t.elapsed().as_secs_f64()
    );
    let deadline = Instant::now() + Duration::from_secs(a.duration);
    let mut tasks = Vec::new();
    for w in 0..a.concurrency {
        let client = client.clone();
        let (accounts, audit_ratio, audit_size) = (a.accounts, a.audit_ratio, a.audit_size);
        tasks.push(tokio::spawn(async move {
            let mut rng = Xoshiro256::seeded(w as u64 + 7);
            let mut h = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("hist");
            let (mut transfers, mut audits, mut restarts, mut failed, mut bad) =
                (0u64, 0u64, 0u64, 0u64, 0u64);
            let mut history: Vec<Committed> = Vec::new();
            while Instant::now() < deadline {
                let t = Instant::now();
                let audit = rng.gen_bool(audit_ratio);
                let mut xfer = (0, 0, 0);
                let program: stowage_kv::txn::Program = if audit {
                    // A read-only snapshot of `audit_size` consecutive accounts.
                    let start = rng.gen_range(0, accounts.saturating_sub(audit_size).max(1));
                    Box::new(move |_, done| {
                        if (done as u64) < audit_size {
                            Step::Op(TxnOp::Get(acct(start + done as u64)))
                        } else {
                            Step::Commit
                        }
                    })
                } else {
                    let from = rng.gen_range(0, accounts);
                    let to = (from + rng.gen_range(1, accounts)) % accounts;
                    let amt = rng.gen_range(1, 100);
                    xfer = (from, to, amt);
                    Box::new(move |reads, done| match done {
                        0 => Step::Op(TxnOp::Get(acct(from))),
                        1 => Step::Op(TxnOp::Get(acct(to))),
                        2 if balance(&reads[0]) < amt => Step::Commit,
                        2 => Step::Op(TxnOp::Put(
                            acct(from),
                            (balance(&reads[0]) - amt).to_le_bytes().to_vec(),
                        )),
                        3 => Step::Op(TxnOp::Put(
                            acct(to),
                            (balance(&reads[1]) + amt).to_le_bytes().to_vec(),
                        )),
                        _ => Step::Commit,
                    })
                };
                match client.transact(iso, program).await {
                    Ok(Outcome::Committed {
                        restarts: r,
                        reads,
                        commit_ts,
                        ..
                    }) => {
                        if !audit {
                            history.push((commit_ts, xfer.0, xfer.1, xfer.2, reads.clone()));
                        }
                        let _ = h.record(t.elapsed().as_nanos() as u64);
                        restarts += u64::from(r);
                        if audit {
                            audits += 1;
                            if reads.iter().any(Option::is_none) {
                                bad += 1;
                            }
                        } else {
                            transfers += 1;
                        }
                    }
                    _ => failed += 1,
                }
            }
            (h, transfers, audits, restarts, failed, bad, history)
        }));
    }
    let mut lat = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)?;
    let (mut transfers, mut audits, mut restarts, mut failed, mut bad) = (0, 0, 0, 0, 0);
    let mut history = Vec::new();
    for t in tasks {
        let (h, tr, au, re, fa, ba, hi) = t.await?;
        history.extend(hi);
        lat.add(h)?;
        transfers += tr;
        audits += au;
        restarts += re;
        failed += fa;
        bad += ba;
    }
    // The invariant: one snapshot read of every account sums to the start total.
    let n = a.accounts;
    let out = client
        .transact(
            iso,
            Box::new(move |_, done| {
                if (done as u64) < n {
                    Step::Op(TxnOp::Get(acct(done as u64)))
                } else {
                    Step::Commit
                }
            }),
        )
        .await?;
    let Outcome::Committed { reads, .. } = out else {
        anyhow::bail!("final audit did not commit: {out:?}");
    };
    let total: u64 = reads.iter().map(balance).sum();
    // Replay the committed transfers in commit-timestamp order: under SSI each one
    // must have read exactly the state its predecessors left.
    history.sort_by_key(|h| h.0);
    let mut state = vec![BALANCE; usize::try_from(n).unwrap_or(0)];
    let mut last: Vec<Option<String>> = vec![None; state.len()];
    let mut anomalies = 0;
    for (ts, from, to, amt, r) in &history {
        let (f, t) = (
            usize::try_from(*from).unwrap_or(0),
            usize::try_from(*to).unwrap_or(0),
        );
        let (rf, rt) = (balance(&r[0]), balance(&r[1]));
        if (rf, rt) != (state[f], state[t]) {
            anomalies += 1;
            if anomalies <= 5 {
                println!(
                    "anomaly: transfer {from}->{to} of {amt} committed at {ts} read ({rf}, {rt}), replay has ({}, {})",
                    state[f], state[t]
                );
                println!(
                    "    last writer of {from}: {:?}\n    last writer of {to}: {:?}",
                    last[f], last[t]
                );
            }
        }
        if rf >= *amt {
            last[f] = Some(format!("{from}->{to} {amt} at {ts} read ({rf}, {rt})"));
            last[t] = last[f].clone();
        }
        if rf >= *amt {
            state[f] = rf - amt;
            state[t] = rt + amt;
        }
    }
    println!(
        "replayed {} committed transfers: {anomalies} read a state their predecessors did not leave",
        history.len()
    );
    for (i, r) in reads.iter().enumerate() {
        if balance(r) != state[i] {
            println!(
                "account {i}: store has {}, replay has {}",
                balance(r),
                state[i]
            );
            for (ts, from, to, amt, rr) in &history {
                if *from == i as u64 || *to == i as u64 {
                    println!(
                        "    {ts} {from}->{to} {amt} read ({}, {})",
                        balance(&rr[0]),
                        balance(&rr[1])
                    );
                }
            }
        }
    }
    let secs = a.duration as f64;
    let q = |p: f64| lat.value_at_quantile(p) as f64 / 1e6;
    println!(
        "\n{} · {} accounts · {} ranges · concurrency {} · {} s",
        a.isolation, a.accounts, a.presplit, a.concurrency, a.duration
    );
    println!("| txn/s | transfers | audits | restarts | failed | p50 ms | p99 ms | p99.9 ms |");
    println!("|------:|----------:|-------:|---------:|-------:|-------:|-------:|---------:|");
    println!(
        "| {:>5.0} | {transfers:>9} | {audits:>6} | {restarts:>8} | {failed:>6} | {:>6.2} | {:>6.2} | {:>8.2} |",
        (transfers + audits) as f64 / secs,
        q(0.5),
        q(0.99),
        q(0.999)
    );
    println!(
        "final audit: total {total} (expected {}), missing accounts in audits: {bad}",
        n * BALANCE
    );
    anyhow::ensure!(total == n * BALANCE, "money was created or destroyed");
    Ok(())
}
