//! Crash-recovery under simulated power loss with torn writes.
//!
//! Each round opens the tree (running recovery), checks the recovered state, issues a
//! few hundred random batches, then cuts the power. Unsynced WAL tails and data-file
//! page writes survive whole, torn at a sector boundary, or not at all.
//!
//! The invariant: after recovery the tree equals the model state after **exactly
//! some batch `n`**, with `acked <= n <= issued`.
//! - Every batch acknowledged by a synced write is present (durability).
//! - No batch is half-applied (atomicity).
//! - Batches never appear out of order (prefix property).
//!
//! Each batch also writes a marker key with its sequence number, which tells us
//! which `n` recovery landed on.

#![allow(clippy::unwrap_used)] // test code

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use stowage_common::os::Xoshiro256;
use stowage_common::{Disk, Rng};
use stowage_sim::{CrashMode, SimDisk, for_each_seed};
use stowage_storage::{BTree, BTreeOptions, WriteBatch};

const MARKER: &[u8] = b"\x00batch";
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn opts() -> BTreeOptions {
    BTreeOptions {
        cache_pages: 48,
        checkpoint_wal_bytes: 96 << 10,
    }
}

fn state(t: &BTree) -> Model {
    t.iter(Bound::Unbounded, Bound::Unbounded)
        .map(Result::unwrap)
        .collect()
}

#[derive(Debug, Default)]
struct Totals {
    crashes: u64,
    batches: u64,
    torn_tails: u64,
    images_replayed: u64,
    deltas_replayed: u64,
    lost_unsynced: u64,
}

fn run(seed: u64, rounds: u32) -> Totals {
    let disk = Arc::new(SimDisk::new(CrashMode::Torn));
    let mut rng = Xoshiro256::seeded(seed);
    // history[i] = model after batch `base + i`; history[0] is the last acked state.
    let mut base: u64 = 0;
    let mut history: Vec<Model> = vec![Model::new()];
    let mut totals = Totals::default();

    for round in 0..rounds {
        let (t, report) = BTree::open(disk.clone() as Arc<dyn Disk>, opts()).unwrap();
        totals.torn_tails += u64::from(report.torn_tail_bytes > 0);
        totals.images_replayed += report.images;
        totals.deltas_replayed += report.deltas_applied;
        t.check()
            .unwrap_or_else(|e| panic!("seed {seed} round {round}: {e}"));

        let got = state(&t);
        let n = got
            .get(MARKER)
            .map_or(0, |v| u64::from_le_bytes(v.as_slice().try_into().unwrap()));
        let issued = base + history.len() as u64 - 1;
        assert!(
            (base..=issued).contains(&n),
            "seed {seed} round {round}: recovered batch {n}, acked {base}, issued {issued}"
        );
        let expected = &history[usize::try_from(n - base).unwrap()];
        assert_eq!(
            &got, expected,
            "seed {seed} round {round}: state differs from batch {n}"
        );
        totals.lost_unsynced += issued - n;
        // The recovered state is now the durable baseline.
        let mut model = expected.clone();
        base = n;
        history = vec![model.clone()];

        let batches = rng.gen_range(50, 300);
        for _ in 0..batches {
            let mut b = WriteBatch::new();
            for _ in 0..rng.gen_range(1, 6) {
                let k = format!("key{:04}", rng.gen_range(0, 600)).into_bytes();
                if rng.gen_bool(0.25) {
                    b.delete(k.clone());
                    model.remove(&k);
                } else {
                    let len = usize::try_from(rng.gen_range(0, 900)).unwrap();
                    let v = vec![u8::try_from(rng.gen_range(0, 256)).unwrap(); len];
                    b.put(k.clone(), v.clone());
                    model.insert(k, v);
                }
            }
            let seq = base + history.len() as u64;
            b.put(MARKER.to_vec(), seq.to_le_bytes().to_vec());
            model.insert(MARKER.to_vec(), seq.to_le_bytes().to_vec());

            let sync = rng.gen_bool(0.2);
            t.write(&b, sync).unwrap();
            totals.batches += 1;
            history.push(model.clone());
            if sync {
                // Acknowledged: from now on recovery must reach at least `seq`.
                base = seq;
                history = vec![model.clone()];
            }
            if rng.gen_bool(0.01) {
                t.checkpoint().unwrap();
            }
        }
        // Power loss. Dropping the tree runs no shutdown logic: there is none.
        drop(t);
        disk.crash(&mut rng);
        totals.crashes += 1;
    }
    totals
}

#[test]
fn recovers_a_prefix_after_every_torn_crash() {
    for_each_seed(&[1, 2, 3, 4, 5, 6, 7, 8], |seed| {
        let totals = run(seed, 25);
        assert!(totals.crashes == 25 && totals.batches > 1000, "{totals:?}");
        assert!(
            totals.images_replayed > 0,
            "full-page images never exercised: {totals:?}"
        );
    });
}

/// Over the fixed seeds, the interesting paths must actually be hit: torn WAL tails
/// and unsynced batches being lost.
#[test]
fn crash_test_exercises_torn_tails_and_lost_tails() {
    let mut all = Totals::default();
    for seed in [11, 12, 13, 14] {
        let t = run(seed, 20);
        all.torn_tails += t.torn_tails;
        all.lost_unsynced += t.lost_unsynced;
        all.crashes += t.crashes;
    }
    assert!(all.torn_tails > 0, "{all:?}");
    assert!(all.lost_unsynced > 0, "{all:?}");
}
