//! Shared building blocks for every Stowage crate.
//!
//! The most important thing in here is [`env`]: the four traits (`Clock`, `Rng`,
//! `Disk`, `Transport`) that every stateful component is written against. Production
//! wires them to the OS and to gRPC; tests wire them to the deterministic simulator in
//! `stowage-sim`. See `docs/adr/0001-deterministic-simulation.md`.

pub mod env;
pub mod error;
pub mod hlc;
pub mod os;

pub use env::{Clock, Disk, DiskFile, NodeId, Rng, Transport};
pub use error::{Error, Result};
pub use hlc::{Hlc, Timestamp};
