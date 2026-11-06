//! `kill -9` crash loop against the real filesystem.
//!
//! ```text
//! cargo run --release -p stowage-storage --example crashtest -- loop --iterations 200
//! ```
//!
//! The parent repeatedly spawns `crashtest write <dir>`. The child opens the tree
//! (running recovery), then writes synced batches forever and prints `ack <n>` after
//! each one returns. After a random 20–300 ms the parent sends SIGKILL, reopens the
//! tree itself, and checks:
//!
//! - the recovered batch number `n` is at least the last acknowledged one;
//! - the whole tree equals the deterministic model after exactly `n` batches.
//!
//! This covers *process* crashes: the OS page cache survives, so unsynced writes may
//! still reach the disk. Power loss with torn writes is covered by the simulator
//! (`tests/simulation.rs`).

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use stowage_common::Rng;
use stowage_common::os::{OsDisk, Xoshiro256};
use stowage_storage::{BTree, BTreeOptions, WriteBatch};

const MARKER: &[u8] = b"\x00batch";
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn opts() -> BTreeOptions {
    BTreeOptions {
        cache_pages: 64,
        checkpoint_wal_bytes: 512 << 10,
    }
}

/// Batch `n` is a pure function of `n`, so any prefix can be recomputed.
fn batch(n: u64) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let mut rng = Xoshiro256::seeded(n.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let mut ops = Vec::new();
    for _ in 0..rng.gen_range(1, 8) {
        let k = format!("key{:05}", rng.gen_range(0, 3000)).into_bytes();
        if rng.gen_bool(0.2) {
            ops.push((k, None));
        } else {
            let len = usize::try_from(rng.gen_range(0, 600)).unwrap_or(0);
            ops.push((k, Some(vec![(n % 251) as u8; len])));
        }
    }
    ops.push((MARKER.to_vec(), Some(n.to_le_bytes().to_vec())));
    ops
}

fn model_after(n: u64) -> Model {
    let mut m = Model::new();
    for i in 1..=n {
        for (k, v) in batch(i) {
            match v {
                Some(v) => m.insert(k, v),
                None => m.remove(&k),
            };
        }
    }
    m
}

fn open(dir: &Path) -> anyhow::Result<BTree> {
    let disk = Arc::new(OsDisk::open(dir)?);
    Ok(BTree::open(disk, opts())?.0)
}

fn recovered_batch(t: &BTree) -> anyhow::Result<u64> {
    Ok(match t.get(MARKER)? {
        Some(v) => u64::from_le_bytes(v.as_slice().try_into()?),
        None => 0,
    })
}

fn writer(dir: &Path) -> anyhow::Result<()> {
    let t = open(dir)?;
    let mut n = recovered_batch(&t)?;
    let stdout = std::io::stdout();
    loop {
        n += 1;
        let mut b = WriteBatch::new();
        for (k, v) in batch(n) {
            match v {
                Some(v) => b.put(k, v),
                None => b.delete(k),
            };
        }
        t.write(&b, true)?;
        let mut out = stdout.lock();
        writeln!(out, "ack {n}")?;
        out.flush()?;
    }
}

fn run_loop(dir: &Path, iterations: u32) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir)?;
    let exe = std::env::current_exe()?;
    let mut rng = Xoshiro256::from_entropy();
    let mut acked = 0u64;
    let started = Instant::now();
    let (mut lost_total, mut max_recovery) = (0u64, Duration::ZERO);
    for it in 1..=iterations {
        let mut child = Command::new(&exe)
            .arg("write")
            .arg(dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("no stdout"))?;
        let (tx, rx) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Some(n) = line
                    .strip_prefix("ack ")
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    let _ = tx.send(n);
                }
            }
        });
        std::thread::sleep(Duration::from_millis(rng.gen_range(20, 300)));
        child.kill()?; // SIGKILL
        child.wait()?;
        reader
            .join()
            .map_err(|_| anyhow::anyhow!("reader panicked"))?;
        while let Ok(n) = rx.try_recv() {
            acked = acked.max(n);
        }

        let t0 = Instant::now();
        let t = open(dir)?;
        max_recovery = max_recovery.max(t0.elapsed());
        let n = recovered_batch(&t)?;
        anyhow::ensure!(
            n >= acked,
            "iteration {it}: recovered batch {n} < acked {acked}"
        );
        let got: Model = t
            .iter(Bound::Unbounded, Bound::Unbounded)
            .collect::<Result<_, _>>()?;
        anyhow::ensure!(
            got == model_after(n),
            "iteration {it}: state != model after batch {n}"
        );
        t.check()?;
        lost_total += n - acked;
        acked = n;
        if it % 20 == 0 || it == iterations {
            println!(
                "iteration {it:>4}: batch {n:>6} ok, {} keys, height {}, {:.1}s elapsed",
                got.len(),
                t.height()?,
                started.elapsed().as_secs_f64()
            );
        }
    }
    println!(
        "\n{iterations} kill -9 crashes, 0 violations; last batch {acked}; \
         batches durable but unacknowledged at kill time: {lost_total}; slowest recovery {:.1} ms",
        max_recovery.as_secs_f64() * 1e3
    );
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("write") => writer(Path::new(&args[2])),
        Some("loop") => {
            let iterations = args
                .iter()
                .position(|a| a == "--iterations")
                .and_then(|i| args.get(i + 1))
                .and_then(|s| s.parse().ok())
                .unwrap_or(50);
            let dir = args
                .iter()
                .position(|a| a == "--dir")
                .and_then(|i| args.get(i + 1))
                .map_or_else(
                    || std::env::temp_dir().join("stowage-crashtest"),
                    PathBuf::from,
                );
            let _ = std::fs::remove_dir_all(&dir);
            run_loop(&dir, iterations)
        }
        _ => {
            anyhow::bail!("usage: crashtest loop [--iterations N] [--dir D] | crashtest write DIR")
        }
    }
}
