//! Stowage's distributed key-value layer.
//!
//! The key space is cut into **ranges**; each range is a Raft group replicated on
//! (by default) three nodes. A node's [`store::Store`] hosts many replicas over one
//! shared storage engine, so splitting a range is a metadata operation: no data
//! moves. Routing is gossiped; placement is decided by a scheduler that runs on
//! whichever node leads range 1.

pub mod codec;
pub mod eval;
pub mod msg;
pub mod mvcc;
pub mod prefix_disk;
pub mod routing;
pub mod store;
pub mod txn;
pub mod types;

pub use msg::{KvError, PeerMsg, Request, Response};
pub use store::{Staged, Store, StoreConfig, StoreStats, Token};
pub use types::{RangeDescriptor, RangeId, RangeInfo};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("codec: {0}")]
    Codec(String),
    #[error("storage: {0}")]
    Storage(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
