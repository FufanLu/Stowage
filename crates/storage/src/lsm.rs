//! An LSM engine ([fjall](https://crates.io/crates/fjall)) behind the same [`Engine`]
//! trait, so benchmarks can compare the two classic designs on identical workloads:
//!
//! | | B+Tree (ours) | LSM (fjall) |
//! |---|---|---|
//! | write | update the page in place (logged) | append to memtable + journal; compact later |
//! | point read | one root-to-leaf path | memtable, then each level (bloom filters skip most) |
//! | write amplification | full-page images after checkpoints + page write-back | rewriting data at each compaction level |
//!
//! Benchmark-only: fjall manages its own files, so this engine runs on the real
//! filesystem, not on [`stowage_common::Disk`], and therefore not in the simulator.

use std::io;
use std::ops::Bound;
use std::path::Path;

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};

use crate::{Engine, Error, KvPair, Op, Result, WriteBatch};

pub struct LsmEngine {
    db: Database,
    ks: Keyspace,
}

impl std::fmt::Debug for LsmEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LsmEngine").finish_non_exhaustive()
    }
}

fn err(e: fjall::Error) -> Error {
    Error::Io(io::Error::other(e.to_string()))
}

impl LsmEngine {
    pub fn open(path: &Path, cache_bytes: u64) -> Result<Self> {
        let db = Database::builder(path)
            .cache_size(cache_bytes)
            .open()
            .map_err(err)?;
        let ks = db
            .keyspace("default", KeyspaceCreateOptions::default)
            .map_err(err)?;
        Ok(Self { db, ks })
    }
}

impl Engine for LsmEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.ks.get(key).map_err(err)?.map(|v| v.to_vec()))
    }

    fn write(&self, batch: &WriteBatch, sync: bool) -> Result<()> {
        let mut b = self.db.batch();
        if sync {
            b = b.durability(Some(PersistMode::SyncData));
        }
        for op in batch.ops() {
            match op {
                Op::Put { key, value } => b.insert(&self.ks, key.as_slice(), value.as_slice()),
                Op::Delete { key } => b.remove(&self.ks, key.as_slice()),
            }
        }
        b.commit().map_err(err)
    }

    fn scan(&self, start: Bound<&[u8]>, end: Bound<&[u8]>, limit: usize) -> Result<Vec<KvPair>> {
        let range = (start.map(<[u8]>::to_vec), end.map(<[u8]>::to_vec));
        self.ks
            .range::<Vec<u8>, _>(range)
            .take(limit)
            .map(|g| {
                let (k, v) = g.into_inner().map_err(err)?;
                Ok((k.to_vec(), v.to_vec()))
            })
            .collect()
    }

    fn sync(&self) -> Result<()> {
        self.db.persist(PersistMode::SyncData).map_err(err)
    }
}
