//! Crate-spanning error type. Individual crates add their own variants by wrapping.

use std::io;

/// Result alias used across Stowage.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors shared by every layer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] io::Error),

    /// Data on disk or on the wire failed validation (bad checksum, bad magic, ...).
    #[error("corruption: {0}")]
    Corruption(String),

    /// A remote clock is further ahead of ours than the configured maximum offset.
    /// Accepting its timestamp would break the HLC uncertainty bound, so we refuse.
    #[error("clock offset {offset_nanos}ns exceeds max offset {max_offset_nanos}ns")]
    ClockOffset {
        offset_nanos: u64,
        max_offset_nanos: u64,
    },

    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}
