//! Write-ahead log.
//!
//! The log is one logical byte stream. A record's LSN is its byte offset in that
//! stream. The stream is stored in segment files named `wal-<start LSN as 16 hex
//! digits>.log`; a new segment starts at every checkpoint, so old segments can be
//! deleted whole.
//!
//! Record framing:
//!
//! ```text
//! ┌─────────┬───────────┬──────────────┐
//! │ len u32 │ crc32c u32│ payload[len] │      crc = crc32c(lsn ‖ len ‖ payload)
//! └─────────┴───────────┴──────────────┘
//! ```
//!
//! Folding the LSN into the CRC means a record copied to the wrong place never
//! validates. Recovery reads records in order and stops at the first frame that is
//! short or fails its CRC: that is the torn tail of the last write before the crash.
//! The tail is truncated so new records start at a clean boundary.

use std::sync::Arc;

use parking_lot::Mutex;
use stowage_common::{Disk, DiskFile};

use crate::page::Lsn;
use crate::{Error, Result};

/// First LSN ever issued. LSN 0 means "page never logged".
pub const FIRST_LSN: Lsn = 1;
const FRAME_HEADER: usize = 8;
/// Upper bound on a record, to reject absurd lengths from torn headers quickly.
pub const MAX_RECORD: usize = 64 << 20;
/// When the in-memory tail grows past this, it is written to the file without an
/// fsync (like PostgreSQL writing out full WAL buffers). Those bytes are on disk
/// but not durable, so a crash can tear them: recovery must cope.
pub const WRITE_BEHIND: usize = 64 << 10;

fn segment_name(start: Lsn) -> String {
    format!("wal-{start:016x}.log")
}

fn parse_segment(name: &str) -> Option<Lsn> {
    let hex = name.strip_prefix("wal-")?.strip_suffix(".log")?;
    (hex.len() == 16).then(|| u64::from_str_radix(hex, 16).ok())?
}

fn frame_crc(lsn: Lsn, payload: &[u8]) -> u32 {
    let crc = crc32c::crc32c(&lsn.to_le_bytes());
    let crc = crc32c::crc32c_append(crc, &(payload.len() as u32).to_le_bytes());
    crc32c::crc32c_append(crc, payload)
}

/// What recovery found at the end of the log.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReplayStats {
    pub records: u64,
    pub bytes: u64,
    /// Bytes after the last valid record that were discarded (torn tail).
    pub torn_tail_bytes: u64,
    pub end_lsn: Lsn,
}

#[derive(Debug)]
struct Inner {
    file: Arc<dyn DiskFile>,
    seg_start: Lsn,
    /// Next LSN to assign.
    end: Lsn,
    /// Bytes appended but not yet written to the file, starting at `buf_start`.
    buf: Vec<u8>,
    buf_start: Lsn,
    /// Everything below this LSN is durable.
    durable: Lsn,
    appended_records: u64,
    appended_bytes: u64,
    syncs: u64,
}

/// The log. All methods are internally synchronized.
#[derive(Debug)]
pub struct Wal {
    disk: Arc<dyn Disk>,
    inner: Mutex<Inner>,
}

impl Wal {
    /// Open the log, replay every record at or after `from`, and position the writer
    /// after the last valid record. `apply` receives `(lsn, payload)` in order.
    pub fn open(
        disk: Arc<dyn Disk>,
        from: Lsn,
        mut apply: impl FnMut(Lsn, &[u8]) -> Result<()>,
    ) -> Result<(Self, ReplayStats)> {
        let mut segments: Vec<Lsn> = disk
            .list()?
            .iter()
            .filter_map(|n| parse_segment(n))
            .collect();
        segments.sort_unstable();
        // Segments entirely before `from` are garbage left by an interrupted cleanup.
        let first = segments.iter().rposition(|&s| s <= from).unwrap_or(0);
        let live = segments.get(first..).unwrap_or(&[]).to_vec();

        let mut stats = ReplayStats {
            end_lsn: from,
            ..ReplayStats::default()
        };
        let mut tail: Option<(Lsn, Arc<dyn DiskFile>)> = None;
        for (idx, &start) in live.iter().enumerate() {
            let file = disk.open(&segment_name(start))?;
            let len = file.len()?;
            let mut data =
                vec![0u8; usize::try_from(len).map_err(|_| corrupt("segment too large"))?];
            file.read_exact_at(&mut data, 0)?;
            let mut pos = 0usize;
            let last = idx + 1 == live.len();
            loop {
                let lsn = start + pos as u64;
                match decode_frame(&data[pos..], lsn) {
                    Some(payload) => {
                        let next = pos + FRAME_HEADER + payload.len();
                        if lsn >= from {
                            apply(lsn, payload)?;
                            stats.records += 1;
                            stats.bytes += (next - pos) as u64;
                        }
                        pos = next;
                    }
                    None => break,
                }
            }
            let valid_end = start + pos as u64;
            if pos < data.len() {
                if !last {
                    return Err(corrupt(&format!(
                        "invalid record at lsn {valid_end} in non-final segment {}",
                        segment_name(start)
                    )));
                }
                stats.torn_tail_bytes = (data.len() - pos) as u64;
                file.set_len(pos as u64)?;
                file.sync()?;
            }
            stats.end_lsn = stats.end_lsn.max(valid_end);
            tail = Some((start, file));
        }

        let (seg_start, file) = match tail {
            Some(t) => t,
            None => {
                let start = from.max(FIRST_LSN);
                let file = disk.open(&segment_name(start))?;
                disk.sync_dir()?;
                stats.end_lsn = start;
                (start, file)
            }
        };
        let end = stats.end_lsn;
        Ok((
            Self {
                disk,
                inner: Mutex::new(Inner {
                    file,
                    seg_start,
                    end,
                    buf: Vec::new(),
                    buf_start: end,
                    durable: end,
                    appended_records: 0,
                    appended_bytes: 0,
                    syncs: 0,
                }),
            },
            stats,
        ))
    }

    /// Append a record to the in-memory tail; returns its LSN. Not durable until
    /// [`Wal::flush_to`].
    pub fn append(&self, payload: &[u8]) -> Result<Lsn> {
        if payload.len() > MAX_RECORD {
            return Err(Error::InvalidArgument(format!(
                "wal record of {} bytes",
                payload.len()
            )));
        }
        let mut w = self.inner.lock();
        let lsn = w.end;
        w.buf
            .extend_from_slice(&(payload.len() as u32).to_le_bytes());
        w.buf
            .extend_from_slice(&frame_crc(lsn, payload).to_le_bytes());
        w.buf.extend_from_slice(payload);
        let framed = (FRAME_HEADER + payload.len()) as u64;
        w.end += framed;
        w.appended_records += 1;
        w.appended_bytes += framed;
        if w.buf.len() >= WRITE_BEHIND {
            w.file.write_at(&w.buf, w.buf_start - w.seg_start)?;
            w.buf_start = w.end;
            w.buf.clear();
        }
        Ok(lsn)
    }

    /// Make every record with LSN `<= lsn` durable (write + fdatasync).
    pub fn flush_to(&self, lsn: Lsn) -> Result<()> {
        let mut w = self.inner.lock();
        if lsn < w.durable {
            return Ok(());
        }
        Self::flush_locked(&mut w)
    }

    /// Make everything appended so far durable.
    pub fn sync(&self) -> Result<()> {
        let mut w = self.inner.lock();
        Self::flush_locked(&mut w)
    }

    fn flush_locked(w: &mut Inner) -> Result<()> {
        if !w.buf.is_empty() {
            w.file.write_at(&w.buf, w.buf_start - w.seg_start)?;
            w.buf_start = w.end;
            w.buf.clear();
        }
        if w.durable < w.end {
            w.file.sync()?;
            w.syncs += 1;
            w.durable = w.end;
        }
        Ok(())
    }

    /// Next LSN to be assigned.
    pub fn end(&self) -> Lsn {
        self.inner.lock().end
    }

    pub fn durable(&self) -> Lsn {
        self.inner.lock().durable
    }

    /// Flush, then start a new segment at the current end. Called by checkpoints.
    pub fn rotate(&self) -> Result<Lsn> {
        let mut w = self.inner.lock();
        Self::flush_locked(&mut w)?;
        if w.end == w.seg_start {
            return Ok(w.end);
        }
        let start = w.end;
        let file = self.disk.open(&segment_name(start))?;
        self.disk.sync_dir()?;
        w.file = file;
        w.seg_start = start;
        w.buf_start = start;
        Ok(start)
    }

    /// Delete segments that end before `lsn`.
    pub fn remove_before(&self, lsn: Lsn) -> Result<usize> {
        let current = self.inner.lock().seg_start;
        let mut segments: Vec<Lsn> = self
            .disk
            .list()?
            .iter()
            .filter_map(|n| parse_segment(n))
            .collect();
        segments.sort_unstable();
        let mut removed = 0;
        for w in segments.windows(2) {
            // Segment w[0] covers [w[0], w[1]); it is garbage if w[1] <= lsn.
            if w[1] <= lsn && w[0] != current {
                self.disk.remove(&segment_name(w[0]))?;
                removed += 1;
            }
        }
        if removed > 0 {
            self.disk.sync_dir()?;
        }
        Ok(removed)
    }

    pub fn stats(&self) -> WalStats {
        let w = self.inner.lock();
        WalStats {
            records: w.appended_records,
            bytes: w.appended_bytes,
            syncs: w.syncs,
            end: w.end,
            durable: w.durable,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WalStats {
    pub records: u64,
    pub bytes: u64,
    pub syncs: u64,
    pub end: Lsn,
    pub durable: Lsn,
}

/// Decode one frame at the start of `data`, expected to sit at `lsn`.
pub fn decode_frame(data: &[u8], lsn: Lsn) -> Option<&[u8]> {
    let header = data.get(..FRAME_HEADER)?;
    let len = usize::try_from(u32::from_le_bytes([
        header[0], header[1], header[2], header[3],
    ]))
    .ok()?;
    let crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    if len > MAX_RECORD {
        return None;
    }
    let payload = data.get(FRAME_HEADER..FRAME_HEADER + len)?;
    (frame_crc(lsn, payload) == crc).then_some(payload)
}

fn corrupt(msg: &str) -> Error {
    Error::Corruption(msg.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use stowage_common::os::Xoshiro256;
    use stowage_sim::{CrashMode, SimDisk};

    fn replay(disk: &Arc<SimDisk>, from: Lsn) -> (Wal, Vec<(Lsn, Vec<u8>)>, ReplayStats) {
        let mut seen = Vec::new();
        let (wal, stats) = Wal::open(disk.clone(), from, |lsn, p| {
            seen.push((lsn, p.to_vec()));
            Ok(())
        })
        .unwrap();
        (wal, seen, stats)
    }

    #[test]
    fn append_flush_replay() {
        let disk = Arc::new(SimDisk::new(CrashMode::DropUnsynced));
        let (wal, seen, _) = replay(&disk, FIRST_LSN);
        assert!(seen.is_empty());
        let a = wal.append(b"alpha").unwrap();
        let b = wal.append(b"beta").unwrap();
        assert_eq!(a, FIRST_LSN);
        assert_eq!(b, a + 8 + 5);
        wal.flush_to(b).unwrap();
        drop(wal);
        let (wal, seen, stats) = replay(&disk, FIRST_LSN);
        assert_eq!(seen, vec![(a, b"alpha".to_vec()), (b, b"beta".to_vec())]);
        assert_eq!(stats.end_lsn, wal.end());
        // Replay from the middle skips earlier records.
        drop(wal);
        let (_, seen, _) = replay(&disk, b);
        assert_eq!(seen, vec![(b, b"beta".to_vec())]);
    }

    #[test]
    fn unflushed_records_vanish_on_crash() {
        let disk = Arc::new(SimDisk::new(CrashMode::DropUnsynced));
        let (wal, _, _) = replay(&disk, FIRST_LSN);
        let a = wal.append(b"kept").unwrap();
        wal.flush_to(a).unwrap();
        wal.append(b"lost").unwrap();
        disk.crash(&mut Xoshiro256::seeded(1));
        let (_, seen, _) = replay(&disk, FIRST_LSN);
        assert_eq!(seen, vec![(a, b"kept".to_vec())]);
    }

    #[test]
    fn torn_tail_is_truncated_and_writing_resumes() {
        // Write records, crash in Torn mode, recover: the replayed records must be a
        // prefix of what was appended, and appends after recovery must replay too.
        for seed in 0..100 {
            let disk = Arc::new(SimDisk::new(CrashMode::Torn));
            let (wal, _, _) = replay(&disk, FIRST_LSN);
            let first = wal.append(&[1; 100]).unwrap();
            wal.flush_to(first).unwrap();
            let payloads: Vec<Vec<u8>> = (2..10u8).map(|i| vec![i; 700]).collect();
            for p in &payloads {
                wal.append(p).unwrap();
            }
            wal.sync().unwrap(); // one write of all 8 records...
            let before_crash = wal.end();
            wal.append(&[0xEE; 3000]).unwrap(); // ...and an unsynced one after it
            wal.flush_to(0).unwrap(); // no-op: already durable below `durable`
            {
                // Force the unsynced record to the file without fsync.
                let w = wal.inner.lock();
                w.file.write_at(&w.buf, w.buf_start - w.seg_start).unwrap();
            }
            drop(wal);
            disk.crash(&mut Xoshiro256::seeded(seed));
            let (wal, seen, stats) = replay(&disk, FIRST_LSN);
            // The 9 synced records always survive; the unsynced one survives only if
            // the crash happened to keep all of its bytes.
            assert!(
                seen.len() == 9 || (seen.len() == 10 && seen[9].1 == vec![0xEE; 3000]),
                "seed {seed}: {} records",
                seen.len()
            );
            assert!(stats.end_lsn >= before_crash);
            let next = wal.append(b"after").unwrap();
            wal.sync().unwrap();
            drop(wal);
            let (_, seen, _) = replay(&disk, FIRST_LSN);
            assert_eq!(seen.last().unwrap(), &(next, b"after".to_vec()));
        }
    }

    #[test]
    fn rotate_and_remove_old_segments() {
        let disk = Arc::new(SimDisk::new(CrashMode::DropUnsynced));
        let (wal, _, _) = replay(&disk, FIRST_LSN);
        wal.append(b"one").unwrap();
        let start2 = wal.rotate().unwrap();
        let two = wal.append(b"two").unwrap();
        wal.sync().unwrap();
        assert_eq!(start2, two);
        assert_eq!(disk.list().unwrap().len(), 2);
        assert_eq!(wal.remove_before(start2).unwrap(), 1);
        assert_eq!(disk.list().unwrap(), vec![segment_name(start2)]);
        drop(wal);
        let (_, seen, _) = replay(&disk, start2);
        assert_eq!(seen, vec![(two, b"two".to_vec())]);
    }

    #[test]
    fn segment_names_roundtrip() {
        assert_eq!(segment_name(0x1f), "wal-000000000000001f.log");
        assert_eq!(parse_segment("wal-000000000000001f.log"), Some(0x1f));
        assert_eq!(parse_segment("wal-1f.log"), None);
        assert_eq!(parse_segment("btree.pages"), None);
    }
}
