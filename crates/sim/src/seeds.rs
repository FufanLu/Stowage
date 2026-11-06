//! Seed selection for simulation tests.
//!
//! - `STOWAGE_SIM_SEED=<n>` runs exactly that seed (to reproduce a failure).
//! - `STOWAGE_SIM_SEEDS=<k>` adds `k` fresh random seeds on top of the fixed ones
//!   (CI uses this to keep exploring new schedules on every run).

use std::panic::{self, AssertUnwindSafe};

use stowage_common::Rng;
use stowage_common::os::Xoshiro256;

/// The seeds a simulation test should run.
pub fn seeds(fixed: &[u64]) -> Vec<u64> {
    if let Some(seed) = env_u64("STOWAGE_SIM_SEED") {
        return vec![seed];
    }
    let mut out = fixed.to_vec();
    let extra = env_u64("STOWAGE_SIM_SEEDS").unwrap_or(0);
    let mut rng = Xoshiro256::from_entropy();
    out.extend((0..extra).map(|_| rng.next_u64()));
    out
}

/// Run `f` once per seed from [`seeds`]. On panic, print the reproducing command
/// before propagating the failure.
pub fn for_each_seed(fixed: &[u64], mut f: impl FnMut(u64)) {
    for seed in seeds(fixed) {
        if let Err(panic) = panic::catch_unwind(AssertUnwindSafe(|| f(seed))) {
            eprintln!(
                "\n*** simulation failed with seed {seed}; reproduce with STOWAGE_SIM_SEED={seed}\n"
            );
            panic::resume_unwind(panic);
        }
    }
}

fn env_u64(name: &str) -> Option<u64> {
    let raw = std::env::var(name).ok()?;
    match raw.trim().parse() {
        Ok(v) => Some(v),
        Err(_) => panic!("{name}={raw:?} is not a u64"),
    }
}
