//! Buffer pool: a fixed set of in-memory frames caching pages of the data file.
//!
//! - **Pinning.** [`BufferPool::fetch`] returns a [`Pinned`] handle. A pinned frame is
//!   never evicted. Dropping the handle unpins it.
//! - **Latching.** Each frame's page sits behind its own `RwLock`. Holding the pin
//!   keeps the frame resident; holding the latch protects the bytes. The two are
//!   separate on purpose: the B+Tree keeps pages pinned across a mini-transaction
//!   but holds latches only while crabbing.
//! - **Eviction** uses the clock algorithm. Each frame has a reference bit; the
//!   hand clears bits until it finds an unpinned frame whose bit is already clear.
//! - **The WAL rule.** Before a dirty page is written back, the WAL is flushed up to
//!   that page's LSN. A page on disk therefore never reflects a change whose log
//!   record could be lost.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use parking_lot::lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard};
use parking_lot::{Mutex, RawRwLock, RwLock};
use stowage_common::DiskFile;

use crate::page::{PAGE_SIZE, PageBuf, PageId};
use crate::wal::Wal;
use crate::{Error, Result};

pub type ReadGuard = ArcRwLockReadGuard<RawRwLock, PageBuf>;
pub type WriteGuard = ArcRwLockWriteGuard<RawRwLock, PageBuf>;

#[derive(Debug)]
struct Frame {
    page: Arc<RwLock<PageBuf>>,
    pins: AtomicU32,
    dirty: AtomicBool,
    referenced: AtomicBool,
}

#[derive(Debug)]
struct Table {
    map: HashMap<PageId, usize>,
    owner: Vec<Option<PageId>>,
    hand: usize,
}

/// Counters for benchmarks and docs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PoolStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub page_reads: u64,
    pub page_writes: u64,
}

#[derive(Debug, Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    page_reads: AtomicU64,
    page_writes: AtomicU64,
}

#[derive(Debug)]
pub struct BufferPool {
    frames: Box<[Frame]>,
    table: Mutex<Table>,
    file: Arc<dyn DiskFile>,
    wal: Arc<Wal>,
    counters: Counters,
}

/// A pinned page. Clone-free: one handle, one pin.
#[derive(Debug)]
pub struct Pinned {
    pool: Arc<BufferPool>,
    frame: usize,
    id: PageId,
}

impl Pinned {
    pub fn id(&self) -> PageId {
        self.id
    }
    pub fn read(&self) -> ReadGuard {
        self.pool.frames[self.frame].page.read_arc()
    }
    pub fn write(&self) -> WriteGuard {
        self.pool.frames[self.frame].page.write_arc()
    }
    pub fn mark_dirty(&self) {
        self.pool.frames[self.frame]
            .dirty
            .store(true, Ordering::Release);
    }
}

impl Drop for Pinned {
    fn drop(&mut self) {
        self.pool.frames[self.frame]
            .pins
            .fetch_sub(1, Ordering::AcqRel);
    }
}

/// How to fill a frame on a miss.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Load {
    /// Read the page from the data file and verify its checksum.
    Read,
    /// The page is new (or about to be overwritten by a WAL image): start zeroed.
    Fresh,
}

impl BufferPool {
    pub fn new(capacity: usize, file: Arc<dyn DiskFile>, wal: Arc<Wal>) -> Arc<Self> {
        assert!(capacity >= 8, "buffer pool needs at least 8 frames");
        let frames = (0..capacity)
            .map(|_| Frame {
                page: Arc::new(RwLock::new(PageBuf::zeroed())),
                pins: AtomicU32::new(0),
                dirty: AtomicBool::new(false),
                referenced: AtomicBool::new(false),
            })
            .collect();
        Arc::new(Self {
            frames,
            table: Mutex::new(Table {
                map: HashMap::new(),
                owner: vec![None; capacity],
                hand: 0,
            }),
            file,
            wal,
            counters: Counters::default(),
        })
    }

    pub fn capacity(&self) -> usize {
        self.frames.len()
    }

    /// Pin page `id`, reading it from disk on a miss.
    pub fn fetch(self: &Arc<Self>, id: PageId) -> Result<Pinned> {
        self.pin(id, Load::Read)
    }

    /// Pin page `id` without reading it: for newly allocated pages and for recovery
    /// installing a full image. The caller must initialise its contents.
    pub fn fetch_fresh(self: &Arc<Self>, id: PageId) -> Result<Pinned> {
        self.pin(id, Load::Fresh)
    }

    fn pin(self: &Arc<Self>, id: PageId, load: Load) -> Result<Pinned> {
        let mut t = self.table.lock();
        if let Some(&f) = t.map.get(&id) {
            let frame = &self.frames[f];
            frame.pins.fetch_add(1, Ordering::AcqRel);
            frame.referenced.store(true, Ordering::Release);
            self.counters.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Pinned {
                pool: self.clone(),
                frame: f,
                id,
            });
        }
        self.counters.misses.fetch_add(1, Ordering::Relaxed);
        let f = self.victim(&mut t)?;
        let frame = &self.frames[f];
        if let Some(old) = t.owner[f] {
            // Write back before unmapping, so a failed write leaves the frame intact.
            if frame.dirty.load(Ordering::Acquire) {
                let page = frame.page.read();
                self.write_back(old, &page)?;
                frame.dirty.store(false, Ordering::Release);
            }
            t.map.remove(&old);
            t.owner[f] = None;
            self.counters.evictions.fetch_add(1, Ordering::Relaxed);
        }
        {
            let mut page = frame.page.write();
            match load {
                Load::Fresh => *page = PageBuf::zeroed(),
                Load::Read => *page = self.read_page(id)?,
            }
        }
        frame.pins.store(1, Ordering::Release);
        frame.referenced.store(true, Ordering::Release);
        t.map.insert(id, f);
        t.owner[f] = Some(id);
        Ok(Pinned {
            pool: self.clone(),
            frame: f,
            id,
        })
    }

    /// Clock sweep. Called with the table lock held; pins only increase under that
    /// lock, so an unpinned frame stays unpinned until we return.
    fn victim(&self, t: &mut Table) -> Result<usize> {
        let n = self.frames.len();
        for _ in 0..2 * n + 1 {
            let f = t.hand;
            t.hand = (t.hand + 1) % n;
            let frame = &self.frames[f];
            if frame.pins.load(Ordering::Acquire) > 0 {
                continue;
            }
            if t.owner[f].is_none() || !frame.referenced.swap(false, Ordering::AcqRel) {
                return Ok(f);
            }
        }
        Err(Error::BufferPoolFull { frames: n })
    }

    fn read_page(&self, id: PageId) -> Result<PageBuf> {
        let mut page = PageBuf::zeroed();
        let off = u64::from(id) * PAGE_SIZE as u64;
        self.file
            .read_exact_at(page.bytes_mut(), off)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    Error::Corruption(format!("page {id} is beyond the end of the data file"))
                } else {
                    e.into()
                }
            })?;
        self.counters.page_reads.fetch_add(1, Ordering::Relaxed);
        if !page.verify() {
            return Err(Error::Corruption(format!(
                "page {id} checksum mismatch (stored {:08x}, computed {:08x})",
                page.checksum(),
                page.compute_checksum()
            )));
        }
        Ok(page)
    }

    fn write_back(&self, id: PageId, page: &PageBuf) -> Result<()> {
        self.wal.flush_to(page.lsn())?;
        let mut out = page.clone();
        out.seal();
        self.file
            .write_at(out.bytes(), u64::from(id) * PAGE_SIZE as u64)?;
        self.counters.page_writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Write every dirty page and fsync the data file. Callers must make sure no
    /// mini-transaction is in flight (the engine holds its writer lock).
    pub fn flush_all(&self) -> Result<usize> {
        let t = self.table.lock();
        let mut written = 0;
        for (f, owner) in t.owner.iter().enumerate() {
            let Some(id) = owner else { continue };
            let frame = &self.frames[f];
            if frame.dirty.load(Ordering::Acquire) {
                let page = frame.page.read();
                self.write_back(*id, &page)?;
                frame.dirty.store(false, Ordering::Release);
                written += 1;
            }
        }
        self.file.sync()?;
        Ok(written)
    }

    pub fn dirty_pages(&self) -> usize {
        self.frames
            .iter()
            .filter(|f| f.dirty.load(Ordering::Acquire))
            .count()
    }

    pub fn stats(&self) -> PoolStats {
        let c = &self.counters;
        PoolStats {
            hits: c.hits.load(Ordering::Relaxed),
            misses: c.misses.load(Ordering::Relaxed),
            evictions: c.evictions.load(Ordering::Relaxed),
            page_reads: c.page_reads.load(Ordering::Relaxed),
            page_writes: c.page_writes.load(Ordering::Relaxed),
        }
    }
}
