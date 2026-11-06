//! Functional tests of the B+Tree against a `BTreeMap` model.

#![allow(clippy::unwrap_used)] // test code

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use proptest::prelude::*;
use stowage_common::Disk;
use stowage_sim::{CrashMode, SimDisk};
use stowage_storage::{BTree, BTreeOptions, Engine, WriteBatch};

fn open(disk: &Arc<SimDisk>, opts: BTreeOptions) -> BTree {
    BTree::open(disk.clone() as Arc<dyn Disk>, opts).unwrap().0
}

fn small_cache() -> BTreeOptions {
    BTreeOptions {
        cache_pages: 16, // force eviction and write-back constantly
        checkpoint_wal_bytes: 256 << 10,
    }
}

fn key(i: u32) -> Vec<u8> {
    format!("user{i:08}").into_bytes()
}

/// ~200-byte values: ~36 entries per leaf, so 20k keys need a 3-level tree.
fn value(i: u32) -> Vec<u8> {
    format!("v{i:0>199}").into_bytes()
}

/// A batch pins every page it touches until commit, so big batches need a
/// bigger pool than [`small_cache`].
fn batch_cache() -> BTreeOptions {
    BTreeOptions {
        cache_pages: 256,
        ..small_cache()
    }
}

#[test]
fn empty_tree() {
    let disk = Arc::new(SimDisk::new(CrashMode::DropUnsynced));
    let (t, report) = BTree::open(disk.clone(), BTreeOptions::default()).unwrap();
    assert!(report.created);
    assert_eq!(t.get(b"nope").unwrap(), None);
    assert_eq!(t.height().unwrap(), 1);
    assert_eq!(t.check().unwrap(), 0);
    assert!(t.iter(Bound::Unbounded, Bound::Unbounded).next().is_none());
}

#[test]
fn many_inserts_split_and_stay_ordered() {
    let disk = Arc::new(SimDisk::new(CrashMode::DropUnsynced));
    let t = open(&disk, small_cache());
    // Insert in a scrambled order so splits happen everywhere, not only at the right edge.
    let n = 20_000u32;
    for i in 0..n {
        // Multiplying by a constant coprime to n permutes 0..n (in u64: no wrap-around).
        let k = u32::try_from(u64::from(i) * 2_654_435_761 % u64::from(n)).unwrap();
        let mut b = WriteBatch::new();
        b.put(key(k), value(k));
        t.write(&b, false).unwrap();
    }
    assert_eq!(t.check().unwrap(), u64::from(n));
    let s = t.stats();
    assert!(s.height >= 3, "height {}", s.height);
    assert!(
        s.leaf_splits > 100 && s.internal_splits > 0 && s.root_splits >= 2,
        "{s:?}"
    );
    assert!(s.pool.evictions > 0, "a 16-page cache must evict");
    for k in [0, 1, 4_999, n - 1] {
        assert_eq!(t.get(&key(k)).unwrap().unwrap(), value(k));
    }
    let all: Vec<_> = t
        .iter(Bound::Unbounded, Bound::Unbounded)
        .map(Result::unwrap)
        .collect();
    assert_eq!(all.len(), n as usize);
    assert!(all.windows(2).all(|w| w[0].0 < w[1].0));
}

#[test]
fn overwrite_with_larger_values_and_delete() {
    let disk = Arc::new(SimDisk::new(CrashMode::DropUnsynced));
    let t = open(&disk, batch_cache());
    for round in 0..4usize {
        let mut b = WriteBatch::new();
        for i in 0..500 {
            b.put(key(i), vec![round as u8; 10 + round * 300]);
        }
        t.write(&b, false).unwrap();
    }
    assert_eq!(t.get(&key(7)).unwrap().unwrap().len(), 10 + 3 * 300);
    let mut b = WriteBatch::new();
    for i in (0..500).step_by(2) {
        b.delete(key(i));
    }
    b.delete(b"never-existed".to_vec());
    t.write(&b, true).unwrap();
    assert_eq!(t.check().unwrap(), 250);
    assert_eq!(t.get(&key(2)).unwrap(), None);
    assert!(t.get(&key(3)).unwrap().is_some());
}

#[test]
fn scan_bounds() {
    let disk = Arc::new(SimDisk::new(CrashMode::DropUnsynced));
    let t = open(&disk, batch_cache());
    let mut b = WriteBatch::new();
    for i in 0..3000 {
        b.put(key(i), b"x".to_vec());
    }
    t.write(&b, false).unwrap();
    let got = t
        .scan(
            Bound::Included(&key(100)),
            Bound::Excluded(&key(2500)),
            usize::MAX,
        )
        .unwrap();
    assert_eq!(got.len(), 2400);
    assert_eq!(got[0].0, key(100));
    assert_eq!(got.last().unwrap().0, key(2499));
    let got = t
        .scan(Bound::Excluded(&key(100)), Bound::Included(&key(102)), 10)
        .unwrap();
    assert_eq!(
        got.iter().map(|p| p.0.clone()).collect::<Vec<_>>(),
        vec![key(101), key(102)]
    );
    assert_eq!(
        t.scan(Bound::Unbounded, Bound::Unbounded, 5).unwrap().len(),
        5
    );
}

#[test]
fn batch_larger_than_the_pool_fails_stop_and_recovers() {
    let disk = Arc::new(SimDisk::new(CrashMode::DropUnsynced));
    let t = open(&disk, small_cache());
    let mut ok = WriteBatch::new();
    ok.put(key(0), b"before".to_vec());
    t.write(&ok, true).unwrap();
    let mut huge = WriteBatch::new();
    for i in 1..2000 {
        huge.put(key(i), vec![7u8; 1000]);
    }
    let err = t.write(&huge, true).unwrap_err();
    assert!(err.to_string().contains("buffer pool exhausted"), "{err}");
    assert!(
        t.get(&key(0)).is_err(),
        "engine must refuse service after a failed write"
    );
    drop(t);
    // Nothing of the failed batch was logged; the earlier write is intact.
    let t = open(&disk, small_cache());
    assert_eq!(t.get(&key(0)).unwrap().unwrap(), b"before");
    assert_eq!(t.get(&key(1)).unwrap(), None);
    assert_eq!(t.check().unwrap(), 1);
}

#[test]
fn rejects_bad_entries_without_failing_the_engine() {
    let disk = Arc::new(SimDisk::new(CrashMode::DropUnsynced));
    let t = open(&disk, BTreeOptions::default());
    let mut b = WriteBatch::new();
    b.put(vec![1u8; 2000], vec![0u8; 100]);
    assert!(t.write(&b, false).is_err());
    let mut b = WriteBatch::new();
    b.put(Vec::new(), b"v".to_vec());
    assert!(t.write(&b, false).is_err());
    let mut b = WriteBatch::new();
    b.put(b"ok".to_vec(), b"v".to_vec());
    t.write(&b, false).unwrap();
    assert_eq!(t.get(b"ok").unwrap().unwrap(), b"v");
}

#[test]
fn reopen_after_clean_close_and_after_checkpoints() {
    let disk = Arc::new(SimDisk::new(CrashMode::DropUnsynced));
    {
        let t = open(&disk, small_cache());
        for i in 0..5000 {
            let mut b = WriteBatch::new();
            b.put(key(i), i.to_le_bytes().to_vec());
            t.write(&b, false).unwrap();
        }
        t.sync().unwrap();
        assert!(
            t.stats().checkpoints > 0,
            "256 KiB threshold must trigger checkpoints"
        );
    }
    let (t, report) = BTree::open(disk.clone(), small_cache()).unwrap();
    assert!(!report.created);
    assert!(report.checkpoint.is_some());
    assert_eq!(t.check().unwrap(), 5000);
    assert_eq!(t.get(&key(4321)).unwrap().unwrap(), 4321u32.to_le_bytes());
    // Old WAL segments were deleted by checkpoints: only a couple remain.
    let segments = disk
        .list()
        .unwrap()
        .iter()
        .filter(|n| n.starts_with("wal-"))
        .count();
    assert!(segments <= 2, "{segments} wal segments left");
}

#[derive(Debug, Clone)]
enum Action {
    Put(u16, u16),
    Delete(u16),
    Checkpoint,
    Reopen,
}

fn action() -> impl Strategy<Value = Action> {
    prop_oneof![
        8 => (0u16..400, 0u16..1500).prop_map(|(k, v)| Action::Put(k, v)),
        3 => (0u16..400).prop_map(Action::Delete),
        1 => Just(Action::Checkpoint),
        1 => Just(Action::Reopen),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn behaves_like_a_btreemap(actions in proptest::collection::vec(action(), 1..300)) {
        let disk = Arc::new(SimDisk::new(CrashMode::DropUnsynced));
        let mut t = open(&disk, small_cache());
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for a in actions {
            match a {
                Action::Put(k, vlen) => {
                    let (k, v) = (format!("k{k:05}").into_bytes(), vec![(vlen % 251) as u8; usize::from(vlen)]);
                    let mut b = WriteBatch::new();
                    b.put(k.clone(), v.clone());
                    t.write(&b, false).unwrap();
                    model.insert(k, v);
                }
                Action::Delete(k) => {
                    let k = format!("k{k:05}").into_bytes();
                    let mut b = WriteBatch::new();
                    b.delete(k.clone());
                    t.write(&b, false).unwrap();
                    model.remove(&k);
                }
                Action::Checkpoint => { t.checkpoint().unwrap(); }
                Action::Reopen => {
                    t.sync().unwrap();
                    drop(t);
                    t = open(&disk, small_cache());
                }
            }
        }
        prop_assert_eq!(t.check().unwrap(), model.len() as u64);
        let got: Vec<_> = t.iter(Bound::Unbounded, Bound::Unbounded).map(Result::unwrap).collect();
        let want: Vec<_> = model.into_iter().collect();
        prop_assert_eq!(got, want);
    }
}
