//! Stowage's storage engine: an ordered key-value store on a B+Tree.
//!
//! ```text
//!  write(batch) ─► mini-transaction ─► WAL record (full images / byte deltas) ─► fsync
//!                       │
//!                       ▼ pages latched + pinned
//!                 buffer pool (clock) ◄──► data file "btree.pages" (8 KiB pages, CRC32C)
//!                       ▲
//!  get / scan ──────────┘ latch crabbing from the meta page down
//! ```
//!
//! Durability and recovery: see [`wal`], [`mtr`] and [`checkpoint`]. The tree
//! itself: [`btree`]. Page layout: [`page`].

pub mod btree;
pub mod buffer;
pub mod checkpoint;
pub mod mtr;
pub mod page;
pub mod wal;

#[cfg(feature = "lsm")]
pub mod lsm;

use std::io;
use std::ops::Bound;

pub use btree::{BTree, BTreeOptions, BTreeStats, RecoveryReport};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("corruption: {0}")]
    Corruption(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("buffer pool exhausted: all {frames} frames are pinned")]
    BufferPoolFull { frames: usize },
    /// A write failed after it had started changing pages. The in-memory state can no
    /// longer be trusted, so the engine stops (fail-stop). Reopening recovers from
    /// the WAL.
    #[error("engine failed earlier and must be reopened: {0}")]
    Failed(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Operations applied atomically: after a crash, either all of them or none are
/// visible.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteBatch {
    ops: Vec<Op>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> &mut Self {
        self.ops.push(Op::Put {
            key: key.into(),
            value: value.into(),
        });
        self
    }
    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> &mut Self {
        self.ops.push(Op::Delete { key: key.into() });
        self
    }
    pub fn ops(&self) -> &[Op] {
        &self.ops
    }
    pub fn len(&self) -> usize {
        self.ops.len()
    }
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

pub type KvPair = (Vec<u8>, Vec<u8>);

/// The interface the layers above (Raft state machine, MVCC) program against, so the
/// B+Tree and the LSM comparison engine are interchangeable in benchmarks.
pub trait Engine: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    /// Apply `batch` atomically. With `sync`, the batch is durable on return.
    fn write(&self, batch: &WriteBatch, sync: bool) -> Result<()>;
    /// Up to `limit` pairs with keys in `(start, end)`, in key order.
    fn scan(&self, start: Bound<&[u8]>, end: Bound<&[u8]>, limit: usize) -> Result<Vec<KvPair>>;
    /// Make all completed writes durable.
    fn sync(&self) -> Result<()>;
}
