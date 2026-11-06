//! The B+Tree engine.
//!
//! # Concurrency
//!
//! Many readers, one writer at a time (the `writer` mutex). This matches how the
//! engine is used: above it sits a Raft state machine, which applies committed
//! entries in log order on a single thread.
//!
//! - **Readers** crab down from the meta page with shared latches: latch the child,
//!   then release the parent. A scan moves between leaves by latch-coupling along
//!   right-sibling links.
//! - **The writer** first tries an *optimistic* descent. It finds the leaf with
//!   shared latches, then latches only that leaf exclusively. Nobody else changes the
//!   tree's shape, so the leaf it found is still the right one. If the change fits
//!   in the leaf, it is done.
//! - Otherwise the writer restarts *pessimistically*: it takes exclusive latches from
//!   the meta page down and releases all ancestors whenever it reaches a node that is
//!   safe (cannot split). The pages that will change in the split are then latched
//!   exclusively, so readers wait above them and never observe a half-done split.
//!
//! # Durability
//!
//! Every [`WriteBatch`] runs as one mini-transaction ([`crate::mtr`]): one WAL record
//! that covers every page it changed. Deleting keys never merges pages. An emptied
//! leaf stays in the tree (PostgreSQL's nbtree also only reclaims pages in VACUUM).
//! The MVCC layer above deletes by writing tombstones and garbage-collects in bulk,
//! so merges would rarely pay for their complexity.

use std::collections::{HashMap, VecDeque};
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::Mutex;
use stowage_common::Disk;

use crate::buffer::{BufferPool, Pinned, PoolStats, ReadGuard, WriteGuard};
use crate::checkpoint::{self, Checkpoint};
use crate::mtr::{MtrRecord, PageChange, apply_delta, delta_len, diff, image_len};
use crate::page::{
    Lsn, MAX_ENTRY, MAX_KEY, META_PAGE, PageBuf, PageId, PageKind, SLOT, cell_key, internal_cell,
    leaf_cell, split_point,
};
use crate::wal::{FIRST_LSN, Wal, WalStats};
use crate::{Engine, Error, KvPair, Op, Result, WriteBatch};

pub const DATA_FILE: &str = "btree.pages";

#[derive(Debug, Clone)]
pub struct BTreeOptions {
    /// Buffer pool size in 8 KiB pages.
    pub cache_pages: usize,
    /// Take a checkpoint once this many WAL bytes accumulate after the last one.
    /// This also bounds recovery time.
    pub checkpoint_wal_bytes: u64,
}

impl Default for BTreeOptions {
    fn default() -> Self {
        Self {
            cache_pages: 4096, // 32 MiB
            // Every checkpoint makes the next change to each page log a full image, so
            // short intervals multiply WAL volume (see docs/illustrated/02).
            checkpoint_wal_bytes: 256 << 20,
        }
    }
}

/// What [`BTree::open`] did to get the tree back.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryReport {
    pub checkpoint: Option<Lsn>,
    pub records: u64,
    pub wal_bytes: u64,
    pub images: u64,
    pub deltas_applied: u64,
    pub deltas_skipped: u64,
    pub torn_tail_bytes: u64,
    pub end_lsn: Lsn,
    pub next_page: PageId,
    pub created: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BTreeStats {
    pub wal: WalStats,
    pub pool: PoolStats,
    pub pages: PageId,
    pub height: u32,
    pub leaf_splits: u64,
    pub internal_splits: u64,
    pub root_splits: u64,
    pub images_logged: u64,
    pub image_bytes: u64,
    pub deltas_logged: u64,
    pub delta_bytes: u64,
    pub checkpoints: u64,
    pub optimistic_writes: u64,
    pub pessimistic_writes: u64,
}

#[derive(Debug, Default)]
struct Counters {
    leaf_splits: AtomicU64,
    internal_splits: AtomicU64,
    root_splits: AtomicU64,
    images_logged: AtomicU64,
    image_bytes: AtomicU64,
    deltas_logged: AtomicU64,
    delta_bytes: AtomicU64,
    checkpoints: AtomicU64,
    optimistic: AtomicU64,
    pessimistic: AtomicU64,
}

#[derive(Debug)]
struct WriterState {
    next_page: PageId,
}

/// A pinned, read-latched page. Field order matters: the latch is released before
/// the pin, so an unpinned frame is never still latched.
struct PageRead {
    guard: ReadGuard,
    _pin: Pinned,
}

#[derive(Debug)]
pub struct BTree {
    disk: Arc<dyn Disk>,
    pool: Arc<BufferPool>,
    wal: Arc<Wal>,
    writer: Mutex<WriterState>,
    redo_lsn: AtomicU64,
    failed: AtomicBool,
    opts: BTreeOptions,
    counters: Counters,
}

impl BTree {
    /// Open (or create) the tree stored on `disk`, running crash recovery first.
    pub fn open(disk: Arc<dyn Disk>, opts: BTreeOptions) -> Result<(Self, RecoveryReport)> {
        let ckpt = checkpoint::read(&*disk)?;
        let redo = ckpt.map_or(FIRST_LSN, |c| c.redo_lsn);
        let mut next_page = ckpt.map_or(0, |c| c.next_page);

        let existed = disk.exists(DATA_FILE)?;
        let file = disk.open(DATA_FILE)?;
        if !existed {
            disk.sync_dir()?;
        }

        // Collect the redo window first. Its size is bounded by
        // `checkpoint_wal_bytes`, and the pool needs the opened WAL.
        let mut records = Vec::new();
        let (wal, replay) = Wal::open(disk.clone(), redo, |lsn, payload| {
            records.push((lsn, MtrRecord::decode(payload)?));
            Ok(())
        })?;
        let wal = Arc::new(wal);
        let pool = BufferPool::new(opts.cache_pages, file, wal.clone());

        let mut report = RecoveryReport {
            checkpoint: ckpt.map(|c| c.redo_lsn),
            records: replay.records,
            wal_bytes: replay.bytes,
            torn_tail_bytes: replay.torn_tail_bytes,
            end_lsn: replay.end_lsn,
            ..RecoveryReport::default()
        };
        for (lsn, rec) in records {
            next_page = next_page.max(rec.next_page);
            for (id, change) in rec.pages {
                match change {
                    PageChange::Image(img) => {
                        let pin = pool.fetch_fresh(id)?;
                        let mut g = pin.write();
                        *g = img;
                        g.set_lsn(lsn);
                        pin.mark_dirty();
                        report.images += 1;
                    }
                    PageChange::Delta(ranges) => {
                        let pin = pool.fetch(id)?;
                        let mut g = pin.write();
                        if g.lsn() < lsn {
                            apply_delta(&mut g, &ranges);
                            g.set_lsn(lsn);
                            pin.mark_dirty();
                            report.deltas_applied += 1;
                        } else {
                            report.deltas_skipped += 1;
                        }
                    }
                }
            }
        }

        let tree = Self {
            disk,
            pool,
            wal,
            writer: Mutex::new(WriterState { next_page }),
            redo_lsn: AtomicU64::new(redo),
            failed: AtomicBool::new(false),
            opts,
            counters: Counters::default(),
        };

        if next_page == 0 {
            tree.bootstrap()?;
            report.created = true;
        } else {
            let meta = tree.pool.fetch(META_PAGE)?;
            let g = meta.read();
            if g.kind() != Some(PageKind::Meta) || !g.meta_magic_ok() {
                return Err(Error::Corruption("page 0 is not a meta page".to_owned()));
            }
        }
        if report.records > 0 {
            // End-of-recovery checkpoint: the next crash does not replay this again.
            tree.checkpoint()?;
        }
        report.next_page = tree.writer.lock().next_page;
        tracing::info!(?report, "btree opened");
        Ok((tree, report))
    }

    /// Create the meta page and an empty root leaf, as one logged mtr.
    fn bootstrap(&self) -> Result<()> {
        let mut w = self.writer.lock();
        let mut mtr = Mtr::new(&self.pool, &mut w.next_page);
        {
            let (meta_id, mut meta) = mtr.alloc(PageKind::Meta)?;
            let (root_id, _root) = mtr.alloc(PageKind::Leaf)?;
            debug_assert_eq!(meta_id, META_PAGE);
            meta.set_meta_root(root_id);
        }
        let lsn = self.commit(mtr)?;
        if let Some(lsn) = lsn {
            self.wal.flush_to(lsn)?;
        }
        Ok(())
    }

    // ---- reads ------------------------------------------------------------------

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_failed()?;
        let leaf = self.find_leaf(key)?;
        Ok(match leaf.guard.search(key) {
            Ok(i) => Some(leaf.guard.leaf_value(i).to_vec()),
            Err(_) => None,
        })
    }

    /// Stream pairs with keys in `(start, end)` in order. Holds no latch between
    /// calls to `next`: each leaf is copied out, and the next leaf is found again
    /// from the root, so concurrent splits can never make the scan skip keys.
    pub fn iter(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Iter<'_> {
        Iter {
            tree: self,
            from: start.map(<[u8]>::to_vec),
            end: end.map(<[u8]>::to_vec),
            buf: VecDeque::new(),
            done: false,
        }
    }

    fn read_page(&self, id: PageId) -> Result<PageRead> {
        let pin = self.pool.fetch(id)?;
        Ok(PageRead {
            guard: pin.read(),
            _pin: pin,
        })
    }

    /// Shared-latch crabbing from the meta page to the leaf that covers `key`.
    fn find_leaf(&self, key: &[u8]) -> Result<PageRead> {
        let meta = self.read_page(META_PAGE)?;
        let mut cur = self.read_page(meta.guard.meta_root())?;
        drop(meta);
        while !cur.guard.is_leaf() {
            let child = self.read_page(cur.guard.child_for(key))?;
            cur = child; // the old page (parent) is released after the child is latched
        }
        Ok(cur)
    }

    // ---- writes -----------------------------------------------------------------

    pub fn write(&self, batch: &WriteBatch, sync: bool) -> Result<()> {
        self.check_failed()?;
        for op in batch.ops() {
            validate(op)?;
        }
        let mut w = self.writer.lock();
        let result = (|| {
            let mut mtr = Mtr::new(&self.pool, &mut w.next_page);
            for op in batch.ops() {
                match op {
                    Op::Put { key, value } => self.put(&mut mtr, key, value)?,
                    Op::Delete { key } => self.delete(&mut mtr, key)?,
                }
            }
            self.commit(mtr)
        })();
        let lsn = match result {
            Ok(lsn) => lsn,
            Err(e) => {
                // Pages may already be modified in memory without a WAL record.
                self.failed.store(true, Ordering::Release);
                return Err(Error::Failed(e.to_string()));
            }
        };
        if let (true, Some(lsn)) = (sync, lsn) {
            self.wal.flush_to(lsn)?;
        }
        let since = self.wal.end() - self.redo_lsn.load(Ordering::Acquire);
        if since >= self.opts.checkpoint_wal_bytes {
            drop(w);
            self.checkpoint()?;
        }
        Ok(())
    }

    fn put(&self, mtr: &mut Mtr<'_>, key: &[u8], value: &[u8]) -> Result<()> {
        let cell = leaf_cell(key, value);

        // Optimistic: shared crabbing to the leaf, exclusive latch on the leaf only.
        let leaf_id = self.find_leaf(key)?._pin.id();
        {
            let mut leaf = mtr.write(leaf_id)?;
            let pos = leaf.search(key);
            let reclaim = match pos {
                Ok(i) => leaf.cell(i).len() + SLOT,
                Err(_) => 0,
            };
            if leaf.total_free() + reclaim >= cell.len() + SLOT {
                let i = match pos {
                    Ok(i) => {
                        leaf.remove_cell(i);
                        i
                    }
                    Err(i) => i,
                };
                let ok = leaf.insert_cell(i, &cell);
                debug_assert!(ok, "space was checked");
                self.counters.optimistic.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        }

        // Pessimistic: exclusive crabbing from the meta page; keep unsafe ancestors.
        self.counters.pessimistic.fetch_add(1, Ordering::Relaxed);
        let mut path: Vec<(PageId, WriteGuard)> = Vec::new();
        let meta = mtr.write(META_PAGE)?;
        let mut id = meta.meta_root();
        path.push((META_PAGE, meta));
        let mut leaf = loop {
            let g = mtr.write(id)?;
            if g.is_safe() {
                path.clear();
            }
            if g.is_leaf() {
                break g;
            }
            let child = g.child_for(key);
            path.push((id, g));
            id = child;
        };
        let leaf_id = id;

        let mut cells = leaf.cells();
        match leaf.search(key) {
            Ok(i) => cells[i] = cell,
            Err(i) => cells.insert(i, cell),
        }
        let at = split_point(&cells);
        let (right_id, mut right) = mtr.alloc(PageKind::Leaf)?;
        right.rebuild(&cells[at..]);
        right.set_link(leaf.link());
        leaf.rebuild(&cells[..at]);
        leaf.set_link(right_id);
        let sep = cell_key(&cells[at], true).to_vec();
        drop((leaf, right));
        self.counters.leaf_splits.fetch_add(1, Ordering::Relaxed);
        self.insert_into_parent(mtr, path, leaf_id, sep, right_id)
    }

    /// After `left` split into `left | right`, insert `(sep, right)` into its parent,
    /// splitting upwards as needed. `path` holds the still-latched unsafe ancestors,
    /// deepest last.
    fn insert_into_parent(
        &self,
        mtr: &mut Mtr<'_>,
        mut path: Vec<(PageId, WriteGuard)>,
        mut left: PageId,
        mut sep: Vec<u8>,
        mut right: PageId,
    ) -> Result<()> {
        loop {
            let Some((pid, mut g)) = path.pop() else {
                return Err(Error::Corruption(format!(
                    "split of page {left} reached a released ancestor"
                )));
            };
            if pid == META_PAGE {
                // The root split: grow the tree by one level.
                let (root_id, mut root) = mtr.alloc(PageKind::Internal)?;
                root.set_link(left);
                let ok = root.insert_cell(0, &internal_cell(&sep, right));
                debug_assert!(ok);
                g.set_meta_root(root_id);
                self.counters.root_splits.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            let i = match g.search(&sep) {
                Err(i) => i,
                Ok(_) => {
                    return Err(Error::Corruption(format!(
                        "separator already present in page {pid}"
                    )));
                }
            };
            let cell = internal_cell(&sep, right);
            if g.insert_cell(i, &cell) {
                return Ok(());
            }
            // Split the internal node; the middle separator moves up.
            let mut cells = g.cells();
            cells.insert(i, cell);
            let m = split_point(&cells).clamp(1, cells.len() - 2);
            let up_key = cell_key(&cells[m], false).to_vec();
            let up_child = cell_child(&cells[m]);
            let (new_id, mut new) = mtr.alloc(PageKind::Internal)?;
            new.set_link(up_child);
            new.rebuild(&cells[m + 1..]);
            g.rebuild(&cells[..m]);
            drop((g, new));
            self.counters
                .internal_splits
                .fetch_add(1, Ordering::Relaxed);
            left = pid;
            sep = up_key;
            right = new_id;
        }
    }

    fn delete(&self, mtr: &mut Mtr<'_>, key: &[u8]) -> Result<()> {
        let leaf_id = self.find_leaf(key)?._pin.id();
        let mut leaf = mtr.write(leaf_id)?;
        if let Ok(i) = leaf.search(key) {
            leaf.remove_cell(i);
        }
        Ok(())
    }

    /// Turn the mtr's page changes into one WAL record and stamp its LSN on every
    /// changed page.
    ///
    /// Two passes, each holding **one latch at a time**. Holding several at once in
    /// `touched` order (leaf first, then ancestors) would invert the readers' top-down
    /// order and deadlock against a reader that holds a parent and waits for the
    /// leaf. The pages cannot change in between: this thread is the only writer, and
    /// every touched page stays pinned until the mtr is dropped.
    fn commit(&self, mtr: Mtr<'_>) -> Result<Option<Lsn>> {
        let redo = self.redo_lsn.load(Ordering::Acquire);
        let mut changed = Vec::new();
        let mut pages = Vec::new();
        for t in &mtr.touched {
            let g = t.pin.read();
            let change = match &t.before {
                None => Some(PageChange::Image((*g).clone())),
                Some(before) if before == &*g => None,
                // First change since the checkpoint: log the whole page, so a torn
                // write of this page can be repaired.
                Some(before) if before.lsn() < redo => Some(PageChange::Image((*g).clone())),
                Some(before) => Some(PageChange::Delta(diff(before, &g))),
            };
            drop(g);
            let Some(change) = change else { continue };
            match &change {
                PageChange::Image(img) => {
                    self.counters.images_logged.fetch_add(1, Ordering::Relaxed);
                    self.counters
                        .image_bytes
                        .fetch_add(image_len(img) as u64, Ordering::Relaxed);
                }
                PageChange::Delta(r) => {
                    let bytes = delta_len(r);
                    self.counters.deltas_logged.fetch_add(1, Ordering::Relaxed);
                    self.counters
                        .delta_bytes
                        .fetch_add(bytes as u64, Ordering::Relaxed);
                }
            }
            pages.push((t.pin.id(), change));
            changed.push(&t.pin);
        }
        if pages.is_empty() {
            return Ok(None);
        }
        let rec = MtrRecord {
            next_page: *mtr.next_page,
            pages,
        };
        let lsn = self.wal.append(&rec.encode())?;
        for pin in changed {
            pin.write().set_lsn(lsn);
            pin.mark_dirty();
        }
        Ok(Some(lsn))
    }

    // ---- checkpoints ----------------------------------------------------------------

    /// Flush every dirty page, then move the recovery start point to the current end
    /// of the WAL and delete the segments before it. This is a *sharp* checkpoint:
    /// writes wait while it runs.
    pub fn checkpoint(&self) -> Result<Lsn> {
        self.check_failed()?;
        let w = self.writer.lock();
        self.wal.sync()?;
        let pages = self.pool.flush_all()?;
        let redo = self.wal.rotate()?;
        checkpoint::write(
            &*self.disk,
            Checkpoint {
                redo_lsn: redo,
                next_page: w.next_page,
            },
        )?;
        self.redo_lsn.store(redo, Ordering::Release);
        let removed = self.wal.remove_before(redo)?;
        self.counters.checkpoints.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(redo, pages, removed, "checkpoint");
        Ok(redo)
    }

    pub fn sync(&self) -> Result<()> {
        self.check_failed()?;
        self.wal.sync()
    }

    // ---- introspection ------------------------------------------------------------

    pub fn stats(&self) -> BTreeStats {
        let c = &self.counters;
        BTreeStats {
            wal: self.wal.stats(),
            pool: self.pool.stats(),
            pages: self.writer.lock().next_page,
            height: self.height().unwrap_or(0),
            leaf_splits: c.leaf_splits.load(Ordering::Relaxed),
            internal_splits: c.internal_splits.load(Ordering::Relaxed),
            root_splits: c.root_splits.load(Ordering::Relaxed),
            images_logged: c.images_logged.load(Ordering::Relaxed),
            image_bytes: c.image_bytes.load(Ordering::Relaxed),
            deltas_logged: c.deltas_logged.load(Ordering::Relaxed),
            delta_bytes: c.delta_bytes.load(Ordering::Relaxed),
            checkpoints: c.checkpoints.load(Ordering::Relaxed),
            optimistic_writes: c.optimistic.load(Ordering::Relaxed),
            pessimistic_writes: c.pessimistic.load(Ordering::Relaxed),
        }
    }

    /// Levels from root to leaf (a lone root leaf has height 1).
    pub fn height(&self) -> Result<u32> {
        let meta = self.read_page(META_PAGE)?;
        let mut cur = self.read_page(meta.guard.meta_root())?;
        drop(meta);
        let mut h = 1;
        while !cur.guard.is_leaf() {
            cur = self.read_page(cur.guard.link())?;
            h += 1;
        }
        Ok(h)
    }

    pub fn root(&self) -> Result<PageId> {
        Ok(self.read_page(META_PAGE)?.guard.meta_root())
    }

    /// A copy of page `id` as it is in memory (for docs and `stowctl`).
    pub fn page(&self, id: PageId) -> Result<PageBuf> {
        Ok((*self.read_page(id)?.guard).clone())
    }

    /// Check structural invariants: key order inside and across pages, separator
    /// bounds, uniform leaf depth, and that the sibling chain visits every leaf.
    pub fn check(&self) -> Result<u64> {
        let root = self.root()?;
        let mut leaves = Vec::new();
        let mut depth = None;
        self.check_node(root, None, None, 1, &mut depth, &mut leaves)?;
        // Walk the sibling chain from the leftmost leaf.
        let mut chain = Vec::new();
        let mut id = leaves.first().copied().unwrap_or(root);
        loop {
            chain.push(id);
            let next = self.page(id)?.link();
            if next == 0 {
                break;
            }
            id = next;
        }
        if chain != leaves {
            return Err(Error::Corruption(format!(
                "sibling chain {chain:?} differs from tree order {leaves:?}"
            )));
        }
        let mut n = 0;
        for id in leaves {
            n += self.page(id)?.nslots() as u64;
        }
        Ok(n)
    }

    fn check_node(
        &self,
        id: PageId,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        level: u32,
        depth: &mut Option<u32>,
        leaves: &mut Vec<PageId>,
    ) -> Result<()> {
        let p = self.page(id)?;
        let bad = |msg: String| Err(Error::Corruption(format!("page {id}: {msg}")));
        for i in 0..p.nslots() {
            let k = p.key(i);
            if i > 0 && p.key(i - 1) >= k {
                return bad(format!("keys out of order at slot {i}"));
            }
            if lo.is_some_and(|lo| k < lo) || hi.is_some_and(|hi| k >= hi) {
                return bad(format!("key {k:?} outside separator bounds"));
            }
        }
        if p.is_leaf() {
            match *depth {
                None => *depth = Some(level),
                Some(d) if d != level => {
                    return bad(format!("leaf at depth {level}, expected {d}"));
                }
                Some(_) => {}
            }
            leaves.push(id);
            return Ok(());
        }
        let n = p.nslots();
        let first_hi = if n > 0 { Some(p.key(0)) } else { hi };
        self.check_node(p.link(), lo, first_hi, level + 1, depth, leaves)?;
        for i in 0..n {
            let next_hi = if i + 1 < n { Some(p.key(i + 1)) } else { hi };
            self.check_node(
                p.child(i),
                Some(p.key(i)),
                next_hi,
                level + 1,
                depth,
                leaves,
            )?;
        }
        Ok(())
    }

    fn check_failed(&self) -> Result<()> {
        if self.failed.load(Ordering::Acquire) {
            Err(Error::Failed("a previous write failed".to_owned()))
        } else {
            Ok(())
        }
    }
}

fn cell_child(cell: &[u8]) -> PageId {
    u32::from_le_bytes([cell[2], cell[3], cell[4], cell[5]])
}

fn validate(op: &Op) -> Result<()> {
    let (key, vlen) = match op {
        Op::Put { key, value } => (key, value.len()),
        Op::Delete { key } => (key, 0),
    };
    if key.is_empty() || key.len() > MAX_KEY {
        return Err(Error::InvalidArgument(format!(
            "key length {} not in 1..={MAX_KEY}",
            key.len()
        )));
    }
    if key.len() + vlen > MAX_ENTRY {
        return Err(Error::InvalidArgument(format!(
            "entry of {} bytes exceeds {MAX_ENTRY}",
            key.len() + vlen
        )));
    }
    Ok(())
}

/// Pages a mini-transaction has touched: pinned until commit, with the image each
/// had when first latched (`None` for pages allocated by this mtr).
struct Mtr<'a> {
    pool: &'a Arc<BufferPool>,
    next_page: &'a mut PageId,
    touched: Vec<Touched>,
    index: HashMap<PageId, usize>,
}

struct Touched {
    pin: Pinned,
    before: Option<PageBuf>,
}

impl<'a> Mtr<'a> {
    fn new(pool: &'a Arc<BufferPool>, next_page: &'a mut PageId) -> Self {
        Self {
            pool,
            next_page,
            touched: Vec::new(),
            index: HashMap::new(),
        }
    }

    /// Exclusive latch on `id`, remembering its pre-mtr image the first time.
    /// Never call this for a page whose guard is still held.
    fn write(&mut self, id: PageId) -> Result<WriteGuard> {
        if let Some(&i) = self.index.get(&id) {
            return Ok(self.touched[i].pin.write());
        }
        let pin = self.pool.fetch(id)?;
        let g = pin.write();
        self.index.insert(id, self.touched.len());
        self.touched.push(Touched {
            before: Some((*g).clone()),
            pin,
        });
        Ok(g)
    }

    fn alloc(&mut self, kind: PageKind) -> Result<(PageId, WriteGuard)> {
        let id = *self.next_page;
        *self.next_page = id
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("page id space exhausted".to_owned()))?;
        let pin = self.pool.fetch_fresh(id)?;
        let mut g = pin.write();
        g.init(id, kind);
        self.index.insert(id, self.touched.len());
        self.touched.push(Touched { pin, before: None });
        Ok((id, g))
    }
}

/// Iterator returned by [`BTree::iter`].
pub struct Iter<'a> {
    tree: &'a BTree,
    from: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
    buf: VecDeque<KvPair>,
    done: bool,
}

impl std::fmt::Debug for Iter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iter")
            .field("buffered", &self.buf.len())
            .finish()
    }
}

impl Iter<'_> {
    fn fill(&mut self) -> Result<()> {
        let seek: &[u8] = match &self.from {
            Bound::Included(k) | Bound::Excluded(k) => k,
            Bound::Unbounded => &[],
        };
        let mut cur = self.tree.find_leaf(seek)?;
        loop {
            let p = &cur.guard;
            let first = match &self.from {
                Bound::Unbounded => 0,
                Bound::Included(k) => p.search(k).unwrap_or_else(|i| i),
                Bound::Excluded(k) => p.search(k).map_or_else(|i| i, |i| i + 1),
            };
            for i in first..p.nslots() {
                let k = p.key(i);
                let past_end = match &self.end {
                    Bound::Included(e) => k > e.as_slice(),
                    Bound::Excluded(e) => k >= e.as_slice(),
                    Bound::Unbounded => false,
                };
                if past_end {
                    self.done = true;
                    break;
                }
                self.buf.push_back((k.to_vec(), p.leaf_value(i).to_vec()));
            }
            if self.done || !self.buf.is_empty() {
                break;
            }
            match p.link() {
                0 => {
                    self.done = true;
                    break;
                }
                right => {
                    let next = self.tree.read_page(right)?; // latch-couple to the right
                    cur = next;
                }
            }
        }
        if let Some((k, _)) = self.buf.back() {
            self.from = Bound::Excluded(k.clone());
        }
        Ok(())
    }
}

impl Iterator for Iter<'_> {
    type Item = Result<KvPair>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty()
            && !self.done
            && let Err(e) = self.fill()
        {
            self.done = true;
            return Some(Err(e));
        }
        self.buf.pop_front().map(Ok)
    }
}

impl Engine for BTree {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        BTree::get(self, key)
    }
    fn write(&self, batch: &WriteBatch, sync: bool) -> Result<()> {
        BTree::write(self, batch, sync)
    }
    fn scan(&self, start: Bound<&[u8]>, end: Bound<&[u8]>, limit: usize) -> Result<Vec<KvPair>> {
        self.iter(start, end).take(limit).collect()
    }
    fn sync(&self) -> Result<()> {
        BTree::sync(self)
    }
}
