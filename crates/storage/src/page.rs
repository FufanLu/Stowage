//! Slotted pages.
//!
//! Every page is `PAGE_SIZE` bytes, little-endian, laid out as:
//!
//! ```text
//!  0        8          12    13   14       16          18     20           24        28   32
//!  ┌────────┬──────────┬─────┬────┬────────┬───────────┬──────┬────────────┬─────────┬────┐
//!  │page_lsn│ checksum │kind │ -- │ nslots │cell_start │ frag │ link       │ page_id │ -- │
//!  └────────┴──────────┴─────┴────┴────────┴───────────┴──────┴────────────┴─────────┴────┘
//!  32: slot array (4 bytes per slot: cell offset u16, cell length u16), sorted by key ──►
//!        … free space …
//!  ◄── cells, packed from the end of the page towards the slot array
//! ```
//!
//! - `link` is the right sibling for a leaf and the leftmost child for an internal node.
//! - A leaf cell is `klen u16 ‖ vlen u16 ‖ key ‖ value`.
//! - An internal cell is `klen u16 ‖ child u32 ‖ key`. Child `i` holds keys
//!   `>= key_i` (and `< key_{i+1}`); `link` holds keys `< key_0`.
//! - `frag` counts bytes of dead cells inside the cell area; [`PageBuf::compact`]
//!   reclaims them.
//! - `checksum` is CRC32C over the whole page with the checksum field zeroed. It is
//!   computed when the page is written to disk and verified when it is read back.

use std::cmp::Ordering;
use std::fmt;

pub const PAGE_SIZE: usize = 8192;
pub const HEADER: usize = 32;
pub const SLOT: usize = 4;

/// Largest key accepted by the engine.
pub const MAX_KEY: usize = 1024;
/// Largest `key + value` accepted by the engine. Guarantees at least three entries
/// fit in a page, so a split always produces two non-empty halves that both fit.
pub const MAX_ENTRY: usize = 2048;
/// Free bytes (cell + slot) a page must have to be "safe": an insert into it can never
/// split it.
pub const SAFE_FREE: usize = MAX_ENTRY + 4 + SLOT;

pub type PageId = u32;
pub type Lsn = u64;

/// Page 0 holds the tree's metadata (root page id).
pub const META_PAGE: PageId = 0;

/// Bytes `[0, DIFF_START)` (LSN and checksum) are stamped, not diffed, by the WAL.
pub const DIFF_START: usize = 12;

const OFF_LSN: usize = 0;
const OFF_CHECKSUM: usize = 8;
const OFF_KIND: usize = 12;
const OFF_NSLOTS: usize = 14;
const OFF_CELL_START: usize = 16;
const OFF_FRAG: usize = 18;
const OFF_LINK: usize = 20;
const OFF_PAGE_ID: usize = 24;
/// Meta page body: root page id, followed by a magic number.
const OFF_META_ROOT: usize = HEADER;
const OFF_META_MAGIC: usize = HEADER + 4;
const META_MAGIC: &[u8; 8] = b"STOWBT01";
/// End of the meta page's body; everything after it is unused (and zero).
pub const META_END: usize = OFF_META_MAGIC + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageKind {
    Free = 0,
    Leaf = 1,
    Internal = 2,
    Meta = 3,
}

impl PageKind {
    fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Self::Free,
            1 => Self::Leaf,
            2 => Self::Internal,
            3 => Self::Meta,
            _ => return None,
        })
    }
}

/// An 8 KiB page image.
#[derive(Clone, PartialEq, Eq)]
pub struct PageBuf(Box<[u8; PAGE_SIZE]>);

impl Default for PageBuf {
    fn default() -> Self {
        Self::zeroed()
    }
}

impl fmt::Debug for PageBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PageBuf")
            .field("id", &self.page_id())
            .field("kind", &self.kind())
            .field("lsn", &self.lsn())
            .field("nslots", &self.nslots())
            .finish()
    }
}

impl PageBuf {
    pub fn zeroed() -> Self {
        Self(Box::new([0u8; PAGE_SIZE]))
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let arr: [u8; PAGE_SIZE] = bytes.try_into().ok()?;
        Some(Self(Box::new(arr)))
    }

    pub fn bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.0
    }

    pub fn bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.0
    }

    /// Reset to an empty page of `kind`. Keeps the LSN: the WAL needs it to decide
    /// whether the next change must log a full image.
    pub fn init(&mut self, id: PageId, kind: PageKind) {
        let lsn = self.lsn();
        self.0.fill(0);
        self.set_lsn(lsn);
        self.0[OFF_KIND] = kind as u8;
        self.set_u16(OFF_CELL_START, PAGE_SIZE as u16);
        self.set_u32(OFF_PAGE_ID, id);
        if kind == PageKind::Meta {
            self.0[OFF_META_MAGIC..OFF_META_MAGIC + 8].copy_from_slice(META_MAGIC);
        }
    }

    // ---- header -------------------------------------------------------------------

    pub fn lsn(&self) -> Lsn {
        u64::from_le_bytes(self.array(OFF_LSN))
    }
    pub fn set_lsn(&mut self, lsn: Lsn) {
        self.0[OFF_LSN..OFF_LSN + 8].copy_from_slice(&lsn.to_le_bytes());
    }
    pub fn kind(&self) -> Option<PageKind> {
        PageKind::from_u8(self.0[OFF_KIND])
    }
    pub fn is_leaf(&self) -> bool {
        self.kind() == Some(PageKind::Leaf)
    }
    pub fn nslots(&self) -> usize {
        usize::from(self.u16(OFF_NSLOTS))
    }
    fn set_nslots(&mut self, n: usize) {
        self.set_u16(OFF_NSLOTS, n as u16);
    }
    pub fn cell_start(&self) -> usize {
        usize::from(self.u16(OFF_CELL_START))
    }
    fn set_cell_start(&mut self, v: usize) {
        // PAGE_SIZE (8192) fits in u16.
        self.set_u16(OFF_CELL_START, v as u16);
    }
    pub fn frag(&self) -> usize {
        usize::from(self.u16(OFF_FRAG))
    }
    fn set_frag(&mut self, v: usize) {
        self.set_u16(OFF_FRAG, v as u16);
    }
    /// Right sibling (leaf) or leftmost child (internal). 0 means none: page 0 is the
    /// meta page and is never a tree node.
    pub fn link(&self) -> PageId {
        self.u32(OFF_LINK)
    }
    pub fn set_link(&mut self, v: PageId) {
        self.set_u32(OFF_LINK, v);
    }
    pub fn page_id(&self) -> PageId {
        self.u32(OFF_PAGE_ID)
    }

    // ---- meta page ----------------------------------------------------------------

    pub fn meta_root(&self) -> PageId {
        self.u32(OFF_META_ROOT)
    }
    pub fn set_meta_root(&mut self, root: PageId) {
        self.set_u32(OFF_META_ROOT, root);
    }
    pub fn meta_magic_ok(&self) -> bool {
        &self.0[OFF_META_MAGIC..OFF_META_MAGIC + 8] == META_MAGIC
    }

    // ---- checksum -----------------------------------------------------------------

    pub fn compute_checksum(&self) -> u32 {
        let crc = crc32c::crc32c(&self.0[..OFF_CHECKSUM]);
        let crc = crc32c::crc32c_append(crc, &[0u8; 4]);
        crc32c::crc32c_append(crc, &self.0[OFF_CHECKSUM + 4..])
    }
    pub fn checksum(&self) -> u32 {
        self.u32(OFF_CHECKSUM)
    }
    /// Stamp the checksum just before writing the page out.
    pub fn seal(&mut self) {
        let c = self.compute_checksum();
        self.set_u32(OFF_CHECKSUM, c);
    }
    pub fn verify(&self) -> bool {
        self.checksum() == self.compute_checksum()
    }

    // ---- slots and cells ----------------------------------------------------------

    fn slot(&self, i: usize) -> (usize, usize) {
        let at = HEADER + i * SLOT;
        (usize::from(self.u16(at)), usize::from(self.u16(at + 2)))
    }
    fn set_slot(&mut self, i: usize, off: usize, len: usize) {
        let at = HEADER + i * SLOT;
        self.set_u16(at, off as u16);
        self.set_u16(at + 2, len as u16);
    }

    pub fn cell(&self, i: usize) -> &[u8] {
        let (off, len) = self.slot(i);
        &self.0[off..off + len]
    }

    pub fn key(&self, i: usize) -> &[u8] {
        let cell = self.cell(i);
        let klen = usize::from(u16::from_le_bytes([cell[0], cell[1]]));
        let start = if self.is_leaf() { 4 } else { 6 };
        &cell[start..start + klen]
    }

    pub fn leaf_value(&self, i: usize) -> &[u8] {
        let cell = self.cell(i);
        let klen = usize::from(u16::from_le_bytes([cell[0], cell[1]]));
        &cell[4 + klen..]
    }

    pub fn child(&self, i: usize) -> PageId {
        let cell = self.cell(i);
        u32::from_le_bytes([cell[2], cell[3], cell[4], cell[5]])
    }

    /// Binary search over keys: `Ok(i)` if `key` is at slot `i`, else `Err(insert_at)`.
    pub fn search(&self, key: &[u8]) -> Result<usize, usize> {
        let (mut lo, mut hi) = (0, self.nslots());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.key(mid).cmp(key) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }

    /// For an internal node: the child page whose range contains `key`.
    pub fn child_for(&self, key: &[u8]) -> PageId {
        match self.search(key) {
            Ok(i) => self.child(i),
            Err(0) => self.link(),
            Err(i) => self.child(i - 1),
        }
    }

    /// Contiguous free bytes between the slot array and the cell area.
    pub fn contiguous_free(&self) -> usize {
        self.cell_start() - (HEADER + self.nslots() * SLOT)
    }

    /// Free bytes after compaction.
    pub fn total_free(&self) -> usize {
        self.contiguous_free() + self.frag()
    }

    pub fn is_safe(&self) -> bool {
        self.total_free() >= SAFE_FREE
    }

    /// Bytes of live cells plus their slots.
    pub fn used(&self) -> usize {
        PAGE_SIZE - HEADER - self.total_free()
    }

    /// Insert `cell` at slot position `i`. Returns `false` (page unchanged) if it does
    /// not fit even after compaction.
    pub fn insert_cell(&mut self, i: usize, cell: &[u8]) -> bool {
        let need = cell.len() + SLOT;
        if self.total_free() < need {
            return false;
        }
        if self.contiguous_free() < need {
            self.compact();
        }
        let n = self.nslots();
        debug_assert!(i <= n);
        let off = self.cell_start() - cell.len();
        self.0[off..off + cell.len()].copy_from_slice(cell);
        // Shift slots [i, n) right by one.
        let from = HEADER + i * SLOT;
        let to = HEADER + n * SLOT;
        self.0.copy_within(from..to, from + SLOT);
        self.set_slot(i, off, cell.len());
        self.set_nslots(n + 1);
        self.set_cell_start(off);
        true
    }

    /// Remove slot `i`. Bytes that fall into the hole are zeroed: the WAL omits the
    /// hole from full-page images, so the hole must be all zeros in memory too, or a
    /// later byte delta could skip a byte that "did not change" in memory but is
    /// zero in the recovered page.
    pub fn remove_cell(&mut self, i: usize) {
        let n = self.nslots();
        let (off, len) = self.slot(i);
        let from = HEADER + (i + 1) * SLOT;
        let to = HEADER + n * SLOT;
        self.0.copy_within(from..to, from - SLOT);
        self.0[to - SLOT..to].fill(0);
        self.set_nslots(n - 1);
        if off == self.cell_start() {
            self.0[off..off + len].fill(0);
            self.set_cell_start(off + len);
        } else {
            self.set_frag(self.frag() + len);
        }
    }

    /// The free gap between the slot array and the cell area is all zeros.
    pub fn hole_is_zero(&self) -> bool {
        match self.kind() {
            Some(PageKind::Leaf | PageKind::Internal) => {}
            Some(PageKind::Meta) => return self.0[META_END..].iter().all(|&b| b == 0),
            _ => return true,
        }
        let lo = HEADER + self.nslots() * SLOT;
        let hi = self.cell_start();
        lo > hi || self.0[lo..hi].iter().all(|&b| b == 0)
    }

    /// Rewrite the cell area without holes.
    pub fn compact(&mut self) {
        let cells: Vec<Vec<u8>> = (0..self.nslots()).map(|i| self.cell(i).to_vec()).collect();
        self.rebuild(&cells);
    }

    /// Replace all cells with `cells` (already sorted), keeping header fields.
    pub fn rebuild(&mut self, cells: &[Vec<u8>]) {
        let mut off = PAGE_SIZE;
        // Clear the old body so stale bytes do not linger in images and dumps.
        self.0[HEADER..].fill(0);
        for (i, c) in cells.iter().enumerate() {
            off -= c.len();
            self.0[off..off + c.len()].copy_from_slice(c);
            self.set_slot(i, off, c.len());
        }
        self.set_nslots(cells.len());
        self.set_cell_start(off);
        self.set_frag(0);
    }

    pub fn cells(&self) -> Vec<Vec<u8>> {
        (0..self.nslots()).map(|i| self.cell(i).to_vec()).collect()
    }

    // ---- little-endian helpers ----------------------------------------------------

    fn array<const N: usize>(&self, at: usize) -> [u8; N] {
        let mut out = [0u8; N];
        out.copy_from_slice(&self.0[at..at + N]);
        out
    }
    fn u16(&self, at: usize) -> u16 {
        u16::from_le_bytes(self.array(at))
    }
    fn set_u16(&mut self, at: usize, v: u16) {
        self.0[at..at + 2].copy_from_slice(&v.to_le_bytes());
    }
    fn u32(&self, at: usize) -> u32 {
        u32::from_le_bytes(self.array(at))
    }
    fn set_u32(&mut self, at: usize, v: u32) {
        self.0[at..at + 4].copy_from_slice(&v.to_le_bytes());
    }
}

/// `klen ‖ vlen ‖ key ‖ value`
pub fn leaf_cell(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut c = Vec::with_capacity(4 + key.len() + value.len());
    c.extend_from_slice(&(key.len() as u16).to_le_bytes());
    c.extend_from_slice(&(value.len() as u16).to_le_bytes());
    c.extend_from_slice(key);
    c.extend_from_slice(value);
    c
}

/// `klen ‖ child ‖ key`
pub fn internal_cell(key: &[u8], child: PageId) -> Vec<u8> {
    let mut c = Vec::with_capacity(6 + key.len());
    c.extend_from_slice(&(key.len() as u16).to_le_bytes());
    c.extend_from_slice(&child.to_le_bytes());
    c.extend_from_slice(key);
    c
}

/// Key of an encoded cell (leaf or internal).
pub fn cell_key(cell: &[u8], leaf: bool) -> &[u8] {
    let klen = usize::from(u16::from_le_bytes([cell[0], cell[1]]));
    let start = if leaf { 4 } else { 6 };
    &cell[start..start + klen]
}

/// Index at which to split `cells` so both halves hold about half the bytes.
/// Always leaves at least one cell on each side.
pub fn split_point(cells: &[Vec<u8>]) -> usize {
    let total: usize = cells.iter().map(|c| c.len() + SLOT).sum();
    let mut acc = 0;
    for (i, c) in cells.iter().enumerate() {
        let prev = acc;
        acc += c.len() + SLOT;
        if acc * 2 >= total {
            // Cut before or after cell `i`, whichever is closer to half.
            let at = if total - 2 * prev <= 2 * acc - total {
                i
            } else {
                i + 1
            };
            return at.clamp(1, cells.len() - 1);
        }
    }
    cells.len() / 2
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf_with(keys: &[&str]) -> PageBuf {
        let mut p = PageBuf::zeroed();
        p.init(7, PageKind::Leaf);
        for k in keys {
            let i = p.search(k.as_bytes()).unwrap_err();
            assert!(p.insert_cell(i, &leaf_cell(k.as_bytes(), b"v")));
        }
        p
    }

    #[test]
    fn inserts_keep_slots_sorted() {
        let p = leaf_with(&["m", "c", "x", "a"]);
        let keys: Vec<&[u8]> = (0..p.nslots()).map(|i| p.key(i)).collect();
        assert_eq!(keys, vec![&b"a"[..], b"c", b"m", b"x"]);
        assert_eq!(p.search(b"m"), Ok(2));
        assert_eq!(p.search(b"n"), Err(3));
        assert_eq!(p.leaf_value(0), b"v");
    }

    #[test]
    fn remove_and_compact_reclaim_space() {
        let mut p = leaf_with(&["a", "b", "c"]);
        let free = p.total_free();
        p.remove_cell(1); // "b" is in the middle of the cell area -> fragment
        assert_eq!(p.frag(), 6);
        assert!(p.hole_is_zero());
        p.remove_cell(1); // "c" sits at cell_start -> its bytes return to the hole
        assert!(p.hole_is_zero());
        let i = p.search(b"c").unwrap_err();
        assert!(p.insert_cell(i, &leaf_cell(b"c", b"v")));
        assert_eq!(p.total_free(), free + 6 + SLOT);
        p.compact();
        assert_eq!(p.frag(), 0);
        assert_eq!(p.key(0), b"a");
        assert_eq!(p.key(1), b"c");
    }

    #[test]
    fn fills_until_full_then_refuses() {
        let mut p = PageBuf::zeroed();
        p.init(1, PageKind::Leaf);
        let mut n = 0u32;
        while p.insert_cell(p.nslots(), &leaf_cell(&n.to_be_bytes(), &[0; 100])) {
            n += 1;
        }
        // (8192 - 32) / (4 + 4 + 100 + 4) = 72.8
        assert_eq!(n, 72);
        assert!(!p.is_safe());
    }

    #[test]
    fn internal_child_routing() {
        let mut p = PageBuf::zeroed();
        p.init(2, PageKind::Internal);
        p.set_link(10);
        assert!(p.insert_cell(0, &internal_cell(b"g", 11)));
        assert!(p.insert_cell(1, &internal_cell(b"p", 12)));
        assert_eq!(p.child_for(b"a"), 10);
        assert_eq!(p.child_for(b"g"), 11);
        assert_eq!(p.child_for(b"h"), 11);
        assert_eq!(p.child_for(b"p"), 12);
        assert_eq!(p.child_for(b"z"), 12);
    }

    #[test]
    fn checksum_detects_a_flipped_bit_and_ignores_itself() {
        let mut p = leaf_with(&["k"]);
        p.seal();
        assert!(p.verify());
        p.bytes_mut()[5000] ^= 1;
        assert!(!p.verify());
    }

    #[test]
    fn split_point_balances_bytes() {
        let cells: Vec<Vec<u8>> = (0..10)
            .map(|i| vec![0; if i < 5 { 10 } else { 100 }])
            .collect();
        let at = split_point(&cells);
        assert!((1..10).contains(&at));
        let left: usize = cells[..at].iter().map(Vec::len).sum();
        let right: usize = cells[at..].iter().map(Vec::len).sum();
        assert!(left.abs_diff(right) <= 100, "{left} vs {right}");
    }
}
