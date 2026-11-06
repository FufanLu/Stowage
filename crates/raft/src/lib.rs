//! Raft consensus (Ongaro & Ousterhout 2014; Ongaro's thesis 2014).
//!
//! - [`raft::Raft`]: the protocol as a deterministic state machine (no I/O except
//!   through [`storage::Storage`]).
//! - [`storage::DiskStorage`]: durable log + hard state + snapshot on a
//!   [`stowage_common::Disk`], so it runs unchanged on real files and in the
//!   simulator.
//! - [`types`]: entries, messages, membership (joint consensus).

pub mod codec;
pub mod raft;
pub mod storage;
pub mod types;

pub use raft::{Config, Raft, RaftStats, ReadMode, Role};
pub use storage::{DiskStorage, MemStorage, Storage};
pub use types::{
    Body, Entry, EntryKind, HardState, Index, Membership, Message, NodeId, Snapshot, Term,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("storage: {0}")]
    Storage(String),
    #[error("not the leader (leader hint: {leader:?})")]
    NotLeader { leader: Option<NodeId> },
    #[error("a membership change is already in progress")]
    ConfigChangeInProgress,
    #[error("a leadership transfer is in progress")]
    TransferInProgress,
    #[error("invalid membership: {0}")]
    InvalidMembership(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
