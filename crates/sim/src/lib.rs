//! Deterministic simulation for Stowage, in the style of FoundationDB's simulator.
//!
//! A whole cluster runs on one thread, driven by a priority queue of events ordered
//! by virtual time. Every source of non-determinism — message latency, loss,
//! duplication, partitions, timer firing, clock skew, crashes and which unsynced
//! writes survive a crash — comes from one seeded RNG. A failing run is therefore
//! replayed exactly by re-running its seed:
//!
//! ```text
//! STOWAGE_SIM_SEED=1234 cargo nextest run -p stowage-raft
//! ```
//!
//! Components under test implement [`Actor`]: a message/timer handler that talks to
//! the world only through its [`Ctx`] and the [`stowage_common::Disk`] /
//! [`stowage_common::Clock`] it was built with.

mod clock;
mod disk;
mod seeds;
mod world;

pub use clock::{SimClock, SimTime};
pub use disk::{CrashMode, SimDisk};
pub use seeds::{for_each_seed, seeds};
pub use world::{Actor, CLIENT, Ctx, NetConfig, NodeEnv, SimConfig, SimStats, Simulation, TimerId};

/// Nanoseconds per millisecond, for readable configs.
pub const MS: u64 = 1_000_000;
/// Nanoseconds per second.
pub const SEC: u64 = 1_000_000_000;
