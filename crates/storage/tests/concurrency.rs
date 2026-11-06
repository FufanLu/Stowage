//! Readers racing one writer, with a buffer pool small enough that readers evict
//! pages the writer just dirtied.
//!
//! Invariants checked by every reader:
//! - a key inserted before a scan started is in that scan, even though leaves split
//!   under the scan (no skipped keys);
//! - scans return strictly increasing keys (no duplicates after a split moves keys
//!   right);
//! - a reader never sees a key's version go backwards.

#![allow(clippy::unwrap_used)] // test code

use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;

use stowage_common::os::Xoshiro256;
use stowage_common::{Disk, Rng};
use stowage_sim::{CrashMode, SimDisk};
use stowage_storage::{BTree, BTreeOptions, WriteBatch};

const KEYS: u64 = 6000;

fn key(i: u64) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

fn version(v: &[u8]) -> u64 {
    u64::from_le_bytes(v[..8].try_into().unwrap())
}

#[test]
fn readers_never_miss_keys_during_splits() {
    let disk = Arc::new(SimDisk::new(CrashMode::DropUnsynced));
    let opts = BTreeOptions {
        cache_pages: 64,
        checkpoint_wal_bytes: 1 << 20,
    };
    let tree = Arc::new(BTree::open(disk as Arc<dyn Disk>, opts).unwrap().0);
    let inserted = Arc::new(AtomicU64::new(0)); // keys [0, inserted) are committed
    let stop = Arc::new(AtomicBool::new(false));

    let writer = {
        let (tree, inserted, stop) = (tree.clone(), inserted.clone(), stop.clone());
        thread::spawn(move || {
            let mut rng = Xoshiro256::seeded(1);
            let mut seq = 1u64;
            let mut next = 0u64;
            while next < KEYS {
                let mut b = WriteBatch::new();
                // Grow the key space (causing splits) and update old keys.
                for _ in 0..4 {
                    if next < KEYS {
                        b.put(
                            key(next),
                            [seq.to_le_bytes().as_slice(), &[0u8; 120]].concat(),
                        );
                        next += 1;
                    }
                }
                for _ in 0..2 {
                    if next > 0 {
                        let k = rng.gen_range(0, next);
                        b.put(key(k), [seq.to_le_bytes().as_slice(), &[1u8; 200]].concat());
                    }
                }
                tree.write(&b, false).unwrap();
                inserted.store(next, Ordering::Release);
                seq += 1;
            }
            stop.store(true, Ordering::Release);
            seq
        })
    };

    let readers: Vec<_> = (0..4u64)
        .map(|r| {
            let (tree, inserted, stop) = (tree.clone(), inserted.clone(), stop.clone());
            thread::spawn(move || {
                let mut rng = Xoshiro256::seeded(100 + r);
                let mut seen: HashMap<u64, u64> = HashMap::new();
                let (mut scans, mut gets) = (0u64, 0u64);
                while !stop.load(Ordering::Acquire) {
                    let committed = inserted.load(Ordering::Acquire);
                    if r % 2 == 0 {
                        let all: Vec<_> = tree
                            .iter(Bound::Unbounded, Bound::Unbounded)
                            .map(Result::unwrap)
                            .collect();
                        assert!(
                            all.windows(2).all(|w| w[0].0 < w[1].0),
                            "scan not strictly sorted"
                        );
                        assert!(
                            all.len() as u64 >= committed,
                            "scan saw {} keys, {committed} were committed before it started",
                            all.len()
                        );
                        scans += 1;
                    } else if committed > 0 {
                        let k = rng.gen_range(0, committed);
                        let v = tree.get(&key(k)).unwrap().expect("committed key missing");
                        let ver = version(&v);
                        let prev = seen.insert(k, ver).unwrap_or(0);
                        assert!(ver >= prev, "key {k} went back from v{prev} to v{ver}");
                        gets += 1;
                    }
                }
                (scans, gets)
            })
        })
        .collect();

    let writes = writer.join().unwrap();
    let mut scans = 0;
    let mut gets = 0;
    for r in readers {
        let (s, g) = r.join().unwrap();
        scans += s;
        gets += g;
    }
    assert_eq!(tree.check().unwrap(), KEYS);
    let stats = tree.stats();
    assert!(stats.pool.evictions > 1000, "{stats:?}");
    assert!(
        scans > 0 && gets > 1000,
        "scans {scans}, gets {gets}, writes {writes}"
    );
}
