//! Mini-transaction (mtr) WAL records.
//!
//! One engine write (a whole [`crate::WriteBatch`]) touches a handful of pages: a leaf,
//! maybe a new right sibling, maybe parents up to a new root. All of those changes go
//! into **one** WAL record, so recovery applies a split entirely or not at all. This
//! is InnoDB's "mini-transaction" idea.
//!
//! For each page, the record carries one of two things:
//!
//! - a **full image**, if this is the page's first change since the last checkpoint
//!   (its LSN is below the checkpoint's redo point) or the page is new. This is
//!   PostgreSQL's `full_page_writes`. A page torn by a crash mid-write is restored
//!   wholesale from this image before any delta is applied to it.
//! - otherwise, a **delta**: the byte ranges that changed, computed by diffing the
//!   page against the copy taken when the mtr first latched it.
//!
//! ```text
//! record  := kind u8 (=1) ‖ next_page u32 ‖ npages u16 ‖ entry*
//! entry   := page_id u32 ‖ tag u8 ‖ body
//! tag 0   := hole_start u16 ‖ hole_end u16 ‖ page[12..hole_start] ‖ page[hole_end..]
//! tag 1   := nranges u16 ‖ (offset u16 ‖ len u16 ‖ bytes[len])*
//!            a len with the top bit set (0x8000 | n) means n zero bytes, sent
//!            without payload: a split zeroes the half of the page that moved out
//! ```
//!
//! An image leaves out the page's *hole*: the free gap between the slot array and
//! the cell area, which carries no data. PostgreSQL does the same with the gap
//! between `pd_lower` and `pd_upper`. A freshly split page is about half hole, so
//! this roughly halves image bytes. Decoding fills the hole with zeros.
//!
//! Redo is idempotent: a delta is applied only if the page's LSN is below the
//! record's LSN.

use crate::page::{DIFF_START, HEADER, META_END, PAGE_SIZE, PageBuf, PageId, PageKind, SLOT};
use crate::{Error, Result};

const KIND_MTR: u8 = 1;
const TAG_IMAGE: u8 = 0;
const TAG_DELTA: u8 = 1;
/// Unchanged runs shorter than this are folded into the surrounding range: a range
/// header costs 4 bytes, so splitting around tiny gaps does not pay.
const MERGE_GAP: usize = 8;
/// Top bit of a range length: "this many zeros, no payload".
const ZERO_RUN: u16 = 0x8000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageChange {
    Image(PageBuf),
    Delta(Vec<(u16, Vec<u8>)>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MtrRecord {
    /// Page allocator high-water mark after this mtr.
    pub next_page: PageId,
    pub pages: Vec<(PageId, PageChange)>,
}

impl MtrRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        out.push(KIND_MTR);
        out.extend_from_slice(&self.next_page.to_le_bytes());
        out.extend_from_slice(&(self.pages.len() as u16).to_le_bytes());
        for (id, change) in &self.pages {
            out.extend_from_slice(&id.to_le_bytes());
            match change {
                PageChange::Image(img) => {
                    out.push(TAG_IMAGE);
                    let (hs, he) = hole(img);
                    debug_assert!(img.hole_is_zero(), "page {id}: non-zero bytes in the hole");
                    out.extend_from_slice(&(hs as u16).to_le_bytes());
                    out.extend_from_slice(&(he as u16).to_le_bytes());
                    out.extend_from_slice(&img.bytes()[DIFF_START..hs]);
                    out.extend_from_slice(&img.bytes()[he..]);
                }
                PageChange::Delta(ranges) => {
                    out.push(TAG_DELTA);
                    out.extend_from_slice(&(ranges.len() as u16).to_le_bytes());
                    for (off, bytes) in ranges {
                        out.extend_from_slice(&off.to_le_bytes());
                        let len = bytes.len() as u16;
                        if bytes.len() > 4 && bytes.iter().all(|&b| b == 0) {
                            out.extend_from_slice(&(len | ZERO_RUN).to_le_bytes());
                        } else {
                            out.extend_from_slice(&len.to_le_bytes());
                            out.extend_from_slice(bytes);
                        }
                    }
                }
            }
        }
        out
    }

    /// Decode a record. Never panics on arbitrary input.
    pub fn decode(mut buf: &[u8]) -> Result<Self> {
        let kind = take_u8(&mut buf)?;
        if kind != KIND_MTR {
            return Err(Error::Corruption(format!("unknown wal record kind {kind}")));
        }
        let next_page = take_u32(&mut buf)?;
        let n = take_u16(&mut buf)?;
        let mut pages = Vec::with_capacity(usize::from(n).min(64));
        for _ in 0..n {
            let id = take_u32(&mut buf)?;
            let change = match take_u8(&mut buf)? {
                TAG_IMAGE => {
                    let hs = usize::from(take_u16(&mut buf)?);
                    let he = usize::from(take_u16(&mut buf)?);
                    if !(DIFF_START <= hs && hs <= he && he <= PAGE_SIZE) {
                        return Err(Error::Corruption(format!("bad image hole {hs}..{he}")));
                    }
                    let mut page = PageBuf::zeroed();
                    page.bytes_mut()[DIFF_START..hs]
                        .copy_from_slice(take(&mut buf, hs - DIFF_START)?);
                    page.bytes_mut()[he..].copy_from_slice(take(&mut buf, PAGE_SIZE - he)?);
                    PageChange::Image(page)
                }
                TAG_DELTA => {
                    let nr = take_u16(&mut buf)?;
                    let mut ranges = Vec::with_capacity(usize::from(nr).min(256));
                    for _ in 0..nr {
                        let off = take_u16(&mut buf)?;
                        let raw = take_u16(&mut buf)?;
                        let len = raw & !ZERO_RUN;
                        if usize::from(off) < DIFF_START
                            || usize::from(off) + usize::from(len) > PAGE_SIZE
                        {
                            return Err(Error::Corruption(format!(
                                "delta range {off}+{len} outside page body"
                            )));
                        }
                        let bytes = if raw & ZERO_RUN != 0 {
                            vec![0u8; usize::from(len)]
                        } else {
                            take(&mut buf, usize::from(len))?.to_vec()
                        };
                        ranges.push((off, bytes));
                    }
                    PageChange::Delta(ranges)
                }
                t => return Err(Error::Corruption(format!("unknown page change tag {t}"))),
            };
            pages.push((id, change));
        }
        if !buf.is_empty() {
            return Err(Error::Corruption(format!(
                "{} trailing bytes in mtr record",
                buf.len()
            )));
        }
        Ok(Self { next_page, pages })
    }
}

/// The free gap `[end of slot array, start of cells)` of a tree page, or the unused
/// tail of the meta page. Other page kinds have no hole (`[PAGE_SIZE, PAGE_SIZE)`).
pub fn hole(page: &PageBuf) -> (usize, usize) {
    match page.kind() {
        Some(PageKind::Meta) => (META_END, PAGE_SIZE),
        Some(PageKind::Leaf | PageKind::Internal) => {
            let lo = HEADER + page.nslots() * SLOT;
            let hi = page.cell_start();
            if lo <= hi && hi <= PAGE_SIZE {
                (lo, hi)
            } else {
                (PAGE_SIZE, PAGE_SIZE)
            }
        }
        _ => (PAGE_SIZE, PAGE_SIZE),
    }
}

/// Bytes an image of `page` occupies in a WAL record.
pub fn image_len(page: &PageBuf) -> usize {
    let (hs, he) = hole(page);
    4 + (hs - DIFF_START) + (PAGE_SIZE - he)
}

/// Byte ranges in `[DIFF_START, PAGE_SIZE)` where `after` differs from `before`.
pub fn diff(before: &PageBuf, after: &PageBuf) -> Vec<(u16, Vec<u8>)> {
    let (b, a) = (before.bytes(), after.bytes());
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    let mut i = DIFF_START;
    while i < PAGE_SIZE {
        if b[i] == a[i] {
            i += 1;
            continue;
        }
        let start = i;
        while i < PAGE_SIZE && b[i] != a[i] {
            i += 1;
        }
        match ranges.last_mut() {
            Some((_, end)) if start - *end < MERGE_GAP => *end = i,
            _ => ranges.push((start, i)),
        }
    }
    ranges
        .into_iter()
        .map(|(s, e)| (s as u16, a[s..e].to_vec()))
        .collect()
}

/// Bytes a delta occupies in a WAL record (zero runs cost only their header).
pub fn delta_len(ranges: &[(u16, Vec<u8>)]) -> usize {
    2 + ranges
        .iter()
        .map(|(_, b)| {
            4 + if b.len() > 4 && b.iter().all(|&x| x == 0) {
                0
            } else {
                b.len()
            }
        })
        .sum::<usize>()
}

/// Apply a delta in place.
pub fn apply_delta(page: &mut PageBuf, ranges: &[(u16, Vec<u8>)]) {
    for (off, bytes) in ranges {
        let off = usize::from(*off);
        page.bytes_mut()[off..off + bytes.len()].copy_from_slice(bytes);
    }
}

fn short() -> Error {
    Error::Corruption("truncated mtr record".to_owned())
}
fn take<'a>(buf: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if buf.len() < n {
        return Err(short());
    }
    let (head, rest) = buf.split_at(n);
    *buf = rest;
    Ok(head)
}
fn take_u8(buf: &mut &[u8]) -> Result<u8> {
    Ok(take(buf, 1)?[0])
}
fn take_u16(buf: &mut &[u8]) -> Result<u16> {
    let b = take(buf, 2)?;
    Ok(u16::from_le_bytes([b[0], b[1]]))
}
fn take_u32(buf: &mut &[u8]) -> Result<u32> {
    let b = take(buf, 4)?;
    Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{PageKind, leaf_cell};
    use proptest::prelude::*;

    #[test]
    fn diff_captures_only_changed_bytes() {
        let mut before = PageBuf::zeroed();
        before.init(3, PageKind::Leaf);
        let mut after = before.clone();
        assert!(after.insert_cell(0, &leaf_cell(b"key", b"value")));
        after.set_lsn(999); // LSN bytes are never diffed
        let d = diff(&before, &after);
        // Changed: nslots + cell_start (header, adjacent -> merged), slot 0, the cell.
        assert_eq!(d.len(), 3, "{d:?}");
        let mut replay = before.clone();
        apply_delta(&mut replay, &d);
        replay.set_lsn(999);
        assert_eq!(replay, after);
    }

    #[test]
    fn record_roundtrip() {
        let mut img = PageBuf::zeroed();
        img.init(5, PageKind::Leaf);
        let rec = MtrRecord {
            next_page: 6,
            pages: vec![
                (5, PageChange::Image(img)),
                (
                    2,
                    PageChange::Delta(vec![(100, vec![1, 2, 3]), (8000, vec![9])]),
                ),
            ],
        };
        let enc = rec.encode();
        assert_eq!(MtrRecord::decode(&enc).unwrap(), rec);
        assert!(
            enc.len() < 100,
            "zero run must not be sent byte by byte: {}",
            enc.len()
        );
    }

    #[test]
    fn image_omits_the_hole() {
        let mut p = PageBuf::zeroed();
        p.init(4, PageKind::Leaf);
        assert!(p.insert_cell(0, &leaf_cell(b"k", &[1; 100])));
        let (hs, he) = hole(&p);
        assert_eq!((hs, he), (HEADER + SLOT, PAGE_SIZE - 105));
        let rec = MtrRecord {
            next_page: 5,
            pages: vec![(4, PageChange::Image(p.clone()))],
        };
        let enc = rec.encode();
        assert_eq!(enc.len(), 7 + 5 + image_len(&p));
        assert!(image_len(&p) < 200, "{}", image_len(&p));
        let PageChange::Image(back) = &MtrRecord::decode(&enc).unwrap().pages[0].1 else {
            panic!("not an image")
        };
        let mut want = p.clone();
        want.bytes_mut()[..DIFF_START].fill(0);
        assert_eq!(back, &want);
    }

    proptest! {
        #[test]
        fn decode_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
            let _ = MtrRecord::decode(&bytes);
        }

        #[test]
        fn decode_rejects_truncation(cut in 0usize..30) {
            let rec = MtrRecord {
                next_page: 9,
                pages: vec![(1, PageChange::Delta(vec![(40, vec![7; 20])]))],
            };
            let enc = rec.encode();
            let cut = cut.min(enc.len() - 1);
            prop_assert!(MtrRecord::decode(&enc[..cut]).is_err());
        }

        #[test]
        fn diff_then_apply_reproduces_page(
            edits in proptest::collection::vec((DIFF_START..PAGE_SIZE, any::<u8>()), 0..200)
        ) {
            let before = PageBuf::zeroed();
            let mut after = before.clone();
            for (at, v) in edits {
                after.bytes_mut()[at] = v;
            }
            let mut replay = before.clone();
            apply_delta(&mut replay, &diff(&before, &after));
            prop_assert_eq!(replay, after);
        }
    }
}
