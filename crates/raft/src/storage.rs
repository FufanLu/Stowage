//! Durable Raft state: hard state (term, vote), the log, and the latest snapshot.
//!
//! Every mutating method is **durable on return**. The core relies on that: it
//! answers a vote request or acknowledges entries only after the corresponding
//! write returned.

use std::sync::Arc;

use stowage_common::{Disk, DiskFile};

use crate::codec;
use crate::types::{Entry, HardState, Index, Membership, Snapshot, SnapshotMeta, Term};
use crate::{Error, Result};

pub trait Storage {
    fn hard_state(&self) -> HardState;
    fn set_hard_state(&mut self, hs: HardState) -> Result<()>;
    /// Metadata of the latest snapshot (index 0 if none).
    fn snapshot_meta(&self) -> &SnapshotMeta;
    fn snapshot(&self) -> Result<Snapshot>;
    /// First index still in the log (snapshot index + 1).
    fn first_index(&self) -> Index {
        self.snapshot_meta().index + 1
    }
    fn last_index(&self) -> Index;
    /// Term of entry `i`; the snapshot's term for `i == snapshot index`; `None` if
    /// compacted away or beyond the end.
    fn term(&self, i: Index) -> Option<Term>;
    /// Entries in `[lo, hi)`, stopping early after `max_bytes` (at least one entry).
    fn entries(&self, lo: Index, hi: Index, max_bytes: usize) -> Vec<Entry>;
    /// Append `entries` (contiguous), first discarding any existing entries at or
    /// after `entries[0].index`.
    fn append(&mut self, entries: &[Entry]) -> Result<()>;
    /// Install a snapshot received from the leader, discarding the whole log.
    fn install_snapshot(&mut self, snap: Snapshot) -> Result<()>;
    /// Record a locally taken snapshot and drop log entries up to its index.
    fn compact(&mut self, snap: Snapshot) -> Result<()>;
}

/// Log entries in memory, shared by both implementations.
#[derive(Debug, Default, Clone)]
struct Log {
    snap: SnapshotMeta,
    entries: Vec<Entry>, // entries[0].index == snap.index + 1
}

impl Log {
    fn last_index(&self) -> Index {
        self.entries.last().map_or(self.snap.index, |e| e.index)
    }
    fn offset(&self, i: Index) -> Option<usize> {
        let first = self.snap.index + 1;
        (i >= first && i <= self.last_index())
            .then(|| usize::try_from(i - first).unwrap_or(usize::MAX))
    }
    fn term(&self, i: Index) -> Option<Term> {
        if i == self.snap.index {
            return Some(self.snap.term);
        }
        self.offset(i).map(|o| self.entries[o].term)
    }
    fn slice(&self, lo: Index, hi: Index, max_bytes: usize) -> Vec<Entry> {
        let mut out = Vec::new();
        let mut bytes = 0;
        for i in lo..hi {
            let Some(o) = self.offset(i) else { break };
            let e = &self.entries[o];
            bytes += e.size();
            if !out.is_empty() && bytes > max_bytes {
                break;
            }
            out.push(e.clone());
        }
        out
    }
    /// Truncate from `entries[0].index` and append; returns the truncation offset.
    fn splice(&mut self, entries: &[Entry]) -> Result<usize> {
        let Some(first) = entries.first() else {
            return Ok(self.entries.len());
        };
        if first.index <= self.snap.index {
            return Err(Error::Storage(format!(
                "append at {} overlaps snapshot at {}",
                first.index, self.snap.index
            )));
        }
        if first.index > self.last_index() + 1 {
            return Err(Error::Storage(format!(
                "append at {} leaves a gap after {}",
                first.index,
                self.last_index()
            )));
        }
        let keep = usize::try_from(first.index - self.snap.index - 1).unwrap_or(usize::MAX);
        self.entries.truncate(keep);
        self.entries.extend_from_slice(entries);
        Ok(keep)
    }
    fn compact_to(&mut self, meta: SnapshotMeta) {
        let drop = self.offset(meta.index).map_or(
            if meta.index > self.last_index() {
                self.entries.len()
            } else {
                0
            },
            |o| o + 1,
        );
        self.entries.drain(..drop);
        self.snap = meta;
    }
}

/// In-memory storage, for unit tests.
#[derive(Debug, Default, Clone)]
pub struct MemStorage {
    hs: HardState,
    log: Log,
    snap_data: Vec<u8>,
}

impl MemStorage {
    pub fn new(membership: Membership) -> Self {
        Self {
            log: Log {
                snap: SnapshotMeta {
                    membership,
                    ..SnapshotMeta::default()
                },
                entries: Vec::new(),
            },
            ..Self::default()
        }
    }
}

impl Storage for MemStorage {
    fn hard_state(&self) -> HardState {
        self.hs
    }
    fn set_hard_state(&mut self, hs: HardState) -> Result<()> {
        self.hs = hs;
        Ok(())
    }
    fn snapshot_meta(&self) -> &SnapshotMeta {
        &self.log.snap
    }
    fn snapshot(&self) -> Result<Snapshot> {
        Ok(Snapshot {
            meta: self.log.snap.clone(),
            data: self.snap_data.clone(),
        })
    }
    fn last_index(&self) -> Index {
        self.log.last_index()
    }
    fn term(&self, i: Index) -> Option<Term> {
        self.log.term(i)
    }
    fn entries(&self, lo: Index, hi: Index, max_bytes: usize) -> Vec<Entry> {
        self.log.slice(lo, hi, max_bytes)
    }
    fn append(&mut self, entries: &[Entry]) -> Result<()> {
        self.log.splice(entries).map(drop)
    }
    fn install_snapshot(&mut self, snap: Snapshot) -> Result<()> {
        self.log.entries.clear();
        self.log.snap = snap.meta;
        self.snap_data = snap.data;
        Ok(())
    }
    fn compact(&mut self, snap: Snapshot) -> Result<()> {
        self.log.compact_to(snap.meta);
        self.snap_data = snap.data;
        Ok(())
    }
}

const HS_FILE: &str = "raft.hardstate";
const SNAP_FILE: &str = "raft.snapshot";

fn log_name(generation: u64) -> String {
    format!("raft-{generation:08}.log")
}

/// Storage on a [`Disk`]:
///
/// - `raft.hardstate` and `raft.snapshot` are small files replaced atomically
///   (write tmp → fsync → rename → fsync dir);
/// - `raft-<generation>.log` holds CRC-framed entries, appended and fsynced. A
///   conflicting suffix is cut with `set_len`; compaction rewrites the remaining
///   entries into the next generation's file.
///
/// The whole log is also kept in memory; snapshots keep it short.
#[derive(Debug)]
pub struct DiskStorage {
    disk: Arc<dyn Disk>,
    hs: HardState,
    log: Log,
    snap_data: Vec<u8>,
    generation: u64,
    file: Arc<dyn DiskFile>,
    /// Byte offset of each in-memory entry in `file`, plus the end offset.
    offsets: Vec<u64>,
    pub fsyncs: u64,
}

impl DiskStorage {
    /// Open existing state, or initialise a new node with `membership`.
    pub fn open(disk: Arc<dyn Disk>, membership: Membership) -> Result<Self> {
        let hs = match read_file(&*disk, HS_FILE)? {
            Some(b) => codec::decode_hard_state(&b)?,
            None => HardState::default(),
        };
        let (snap_meta, snap_data) = match read_file(&*disk, SNAP_FILE)? {
            Some(b) => {
                let s = codec::decode_snapshot(&b)?;
                (s.meta, s.data)
            }
            None => (
                SnapshotMeta {
                    membership,
                    ..SnapshotMeta::default()
                },
                Vec::new(),
            ),
        };
        let mut gens: Vec<u64> = disk
            .list()?
            .iter()
            .filter_map(|n| n.strip_prefix("raft-")?.strip_suffix(".log")?.parse().ok())
            .collect();
        gens.sort_unstable();
        let generation = gens.last().copied().unwrap_or(0);
        let file = disk.open(&log_name(generation))?;
        if gens.is_empty() {
            disk.sync_dir()?;
        }
        // Older generations are leftovers of an interrupted compaction.
        for g in gens.iter().filter(|&&g| g != generation) {
            disk.remove(&log_name(*g))?;
        }

        let mut log = Log {
            snap: snap_meta,
            entries: Vec::new(),
        };
        let mut offsets = vec![0u64];
        let len = file.len()?;
        let mut data =
            vec![0u8; usize::try_from(len).map_err(|_| Error::Storage("log too big".into()))?];
        file.read_exact_at(&mut data, 0)?;
        let mut pos = 0usize;
        while let Some((entry, used)) = codec::decode_frame(&data[pos..]) {
            let entry = codec::decode_entry(entry)?;
            // Entries at or below the snapshot are already covered by it.
            if entry.index > log.snap.index {
                if entry.index != log.last_index() + 1 {
                    return Err(Error::Storage(format!(
                        "log entry {} does not follow {}",
                        entry.index,
                        log.last_index()
                    )));
                }
                log.entries.push(entry);
                offsets.push((pos + used) as u64);
            } else {
                *offsets.last_mut().unwrap_or(&mut 0) = (pos + used) as u64;
            }
            pos += used;
        }
        if (pos as u64) < len {
            // Torn tail from a crash mid-append: those entries were never acked.
            file.set_len(pos as u64)?;
            file.sync()?;
        }
        Ok(Self {
            disk,
            hs,
            log,
            snap_data,
            generation,
            file,
            offsets,
            fsyncs: 0,
        })
    }

    fn write_atomic(&mut self, name: &str, bytes: &[u8]) -> Result<()> {
        let tmp = format!("{name}.tmp");
        let f = self.disk.open(&tmp)?;
        f.set_len(0)?;
        f.write_at(bytes, 0)?;
        f.sync()?;
        self.disk.rename(&tmp, name)?;
        self.disk.sync_dir()?;
        self.fsyncs += 2;
        Ok(())
    }

    /// Rewrite the in-memory log into a fresh generation file.
    fn rewrite_log(&mut self) -> Result<()> {
        let next = self.generation + 1;
        let file = self.disk.open(&log_name(next))?;
        file.set_len(0)?;
        let mut buf = Vec::new();
        let mut offsets = vec![0u64];
        for e in &self.log.entries {
            codec::encode_frame(&mut buf, &codec::encode_entry(e));
            offsets.push(buf.len() as u64);
        }
        file.write_at(&buf, 0)?;
        file.sync()?;
        self.disk.sync_dir()?;
        self.disk.remove(&log_name(self.generation))?;
        self.disk.sync_dir()?;
        self.fsyncs += 3;
        self.generation = next;
        self.file = file;
        self.offsets = offsets;
        Ok(())
    }
}

impl Storage for DiskStorage {
    fn hard_state(&self) -> HardState {
        self.hs
    }
    fn set_hard_state(&mut self, hs: HardState) -> Result<()> {
        if hs != self.hs {
            self.write_atomic(HS_FILE, &codec::encode_hard_state(&hs))?;
            self.hs = hs;
        }
        Ok(())
    }
    fn snapshot_meta(&self) -> &SnapshotMeta {
        &self.log.snap
    }
    fn snapshot(&self) -> Result<Snapshot> {
        Ok(Snapshot {
            meta: self.log.snap.clone(),
            data: self.snap_data.clone(),
        })
    }
    fn last_index(&self) -> Index {
        self.log.last_index()
    }
    fn term(&self, i: Index) -> Option<Term> {
        self.log.term(i)
    }
    fn entries(&self, lo: Index, hi: Index, max_bytes: usize) -> Vec<Entry> {
        self.log.slice(lo, hi, max_bytes)
    }
    fn append(&mut self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let keep = self.log.splice(entries)?;
        self.offsets.truncate(keep + 1);
        let start = *self.offsets.last().unwrap_or(&0);
        let mut buf = Vec::new();
        for e in entries {
            codec::encode_frame(&mut buf, &codec::encode_entry(e));
            self.offsets.push(start + buf.len() as u64);
        }
        if self.file.len()? > start {
            self.file.set_len(start)?;
        }
        self.file.write_at(&buf, start)?;
        self.file.sync()?;
        self.fsyncs += 1;
        Ok(())
    }
    fn install_snapshot(&mut self, snap: Snapshot) -> Result<()> {
        self.write_atomic(SNAP_FILE, &codec::encode_snapshot(&snap))?;
        self.log.entries.clear();
        self.log.snap = snap.meta;
        self.snap_data = snap.data;
        self.rewrite_log()
    }
    fn compact(&mut self, snap: Snapshot) -> Result<()> {
        if snap.meta.index <= self.log.snap.index {
            return Ok(());
        }
        self.write_atomic(SNAP_FILE, &codec::encode_snapshot(&snap))?;
        self.log.compact_to(snap.meta);
        self.snap_data = snap.data;
        self.rewrite_log()
    }
}

fn read_file(disk: &dyn Disk, name: &str) -> Result<Option<Vec<u8>>> {
    if !disk.exists(name)? {
        return Ok(None);
    }
    let f = disk.open(name)?;
    let mut buf = vec![0u8; usize::try_from(f.len()?).unwrap_or(0)];
    f.read_exact_at(&mut buf, 0)?;
    Ok(Some(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EntryKind;
    use stowage_common::os::Xoshiro256;
    use stowage_sim::{CrashMode, SimDisk};

    fn e(term: Term, index: Index) -> Entry {
        Entry {
            term,
            index,
            kind: EntryKind::Normal(format!("cmd{index}").into_bytes()),
        }
    }

    #[test]
    fn append_truncate_reopen() {
        let disk = Arc::new(SimDisk::new(CrashMode::DropUnsynced));
        let m = Membership::new([1, 2, 3]);
        let mut s = DiskStorage::open(disk.clone(), m.clone()).unwrap();
        s.set_hard_state(HardState {
            term: 2,
            vote: Some(1),
        })
        .unwrap();
        s.append(&[e(1, 1), e(1, 2), e(2, 3)]).unwrap();
        s.append(&[e(3, 3), e(3, 4)]).unwrap(); // conflict at 3: replace suffix
        assert_eq!(s.term(3), Some(3));
        assert_eq!(s.last_index(), 4);
        drop(s);
        disk.crash(&mut Xoshiro256::seeded(1));
        let s = DiskStorage::open(disk, m).unwrap();
        assert_eq!(
            s.hard_state(),
            HardState {
                term: 2,
                vote: Some(1)
            }
        );
        assert_eq!(
            s.entries(1, 5, usize::MAX),
            vec![e(1, 1), e(1, 2), e(3, 3), e(3, 4)]
        );
    }

    #[test]
    fn compaction_survives_crash() {
        let disk = Arc::new(SimDisk::new(CrashMode::Torn));
        let m = Membership::new([1]);
        let mut s = DiskStorage::open(disk.clone(), m.clone()).unwrap();
        s.append(&(1..=10).map(|i| e(1, i)).collect::<Vec<_>>())
            .unwrap();
        s.compact(Snapshot {
            meta: SnapshotMeta {
                index: 6,
                term: 1,
                membership: m.clone(),
            },
            data: b"state@6".to_vec(),
        })
        .unwrap();
        s.append(&[e(2, 11)]).unwrap();
        drop(s);
        disk.crash(&mut Xoshiro256::seeded(9));
        let s = DiskStorage::open(disk, m).unwrap();
        assert_eq!(s.first_index(), 7);
        assert_eq!(s.term(6), Some(1));
        assert_eq!(s.term(5), None);
        assert_eq!(s.last_index(), 11);
        assert_eq!(s.snapshot().unwrap().data, b"state@6");
    }

    #[test]
    fn torn_append_is_discarded() {
        // Unsynced appends never happen through the API (append always fsyncs), so
        // emulate a crash in the middle of `write_at` by writing a partial frame.
        let disk = Arc::new(SimDisk::new(CrashMode::DropUnsynced));
        let m = Membership::new([1]);
        let mut s = DiskStorage::open(disk.clone(), m.clone()).unwrap();
        s.append(&[e(1, 1)]).unwrap();
        let end = s.file.len().unwrap();
        s.file.write_at(&[0xAA; 7], end).unwrap();
        s.file.sync().unwrap();
        drop(s);
        let s = DiskStorage::open(disk, m).unwrap();
        assert_eq!(s.last_index(), 1);
    }
}
