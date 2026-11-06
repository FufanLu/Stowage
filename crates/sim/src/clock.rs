use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use stowage_common::Clock;

/// Wall-clock origin of every simulation: 2026-11-06T00:00:00Z. A fixed origin keeps
/// HLC timestamps in traces identical across runs of the same seed.
pub const WALL_ORIGIN_NANOS: u64 = 1_793_923_200 * 1_000_000_000;

/// The simulation's virtual time, shared by every node's clock.
#[derive(Debug, Clone, Default)]
pub struct SimTime(Arc<AtomicU64>);

impl SimTime {
    pub fn now(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    pub(crate) fn advance_to(&self, t: u64) {
        debug_assert!(t >= self.now(), "virtual time must not go backwards");
        self.0.store(t, Ordering::Relaxed);
    }
}

/// One node's view of time: shared virtual monotonic time plus a per-node wall-clock
/// skew, so tests can model NTP disagreement between nodes.
#[derive(Debug)]
pub struct SimClock {
    time: SimTime,
    skew_nanos: AtomicI64,
}

impl SimClock {
    pub fn new(time: SimTime) -> Self {
        Self {
            time,
            skew_nanos: AtomicI64::new(0),
        }
    }

    pub fn set_skew(&self, skew_nanos: i64) {
        self.skew_nanos.store(skew_nanos, Ordering::Relaxed);
    }

    pub fn skew(&self) -> i64 {
        self.skew_nanos.load(Ordering::Relaxed)
    }
}

impl Clock for SimClock {
    fn monotonic_nanos(&self) -> u64 {
        self.time.now()
    }

    fn wall_nanos(&self) -> u64 {
        (WALL_ORIGIN_NANOS + self.time.now()).saturating_add_signed(self.skew())
    }
}
