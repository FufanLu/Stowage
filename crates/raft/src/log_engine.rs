//! One write-ahead log shared by every Raft group on a node.
//!
//! With one log file per group, a node hosting a hundred ranges pays a hundred
//! fsyncs per round of replication, and on a laptop SSD (~5 ms per `F_FULLFSYNC`)
//! that alone exceeds the election timeout. TiKV's raft-engine and CockroachDB
//! solve this the same way: every group appends to one file, and the node fsyncs
//! it **once** per batch of work.
//!
//! ```text
//! raftlog-<generation>.log :=  record*
//! record := len u32 ‖ crc32c u32 ‖ kind u8 ‖ group u64 ‖ body
//!   1 Entries   body = n u32 ‖ (entry bytes)*     (replaces the suffix from entries[0].index)
//!   2 HardState body = term u64 ‖ has_vote u8 ‖ vote u64
//!   3 Snapshot  body = snapshot encoding           (installed or compacted; discards older entries)
//!   4 Remove    body = —                           (group destroyed)
//! ```
//!
//! Writes are buffered. [`LogEngine::sync`] writes and fsyncs them, and the owner
//! must call it before sending any message or reply that depends on them. The
//! [`crate::Raft`] core never sends anything itself; its driver takes the outbox
//! only after `sync`, so "persist before you reply" holds per batch.
//!
//! When the file grows well past the live data, it is rewritten into the next
//! generation holding only the current state of every group (like a Bitcask
//! merge): snapshot metadata, hard state, and the live entries.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use stowage_common::{Disk, DiskFile};

use crate::codec::{self, Reader};
use crate::storage::{Log, Storage};
use crate::types::{Entry, HardState, Index, Membership, Snapshot, SnapshotMeta, Term};
use crate::{Error, Result};

const ENTRIES: u8 = 1;
const HARD_STATE: u8 = 2;
const SNAPSHOT: u8 = 3;
const REMOVE: u8 = 4;

fn file_name(generation: u64) -> String {
    format!("raftlog-{generation:08}.log")
}

/// Durable state of one group, as rebuilt from the log.
#[derive(Debug, Clone, Default)]
struct GroupState {
    hs: HardState,
    log: Log,
    snap_data: Vec<u8>,
}

#[derive(Debug)]
struct Inner {
    disk: Arc<dyn Disk>,
    file: Arc<dyn DiskFile>,
    generation: u64,
    len: u64,
    buf: Vec<u8>,
    /// Mirror of every group's durable state (for rewrites and reopening groups).
    groups: BTreeMap<u64, GroupState>,
    fsyncs: u64,
    rewrite_at: u64,
}

/// Shared handle; cheap to clone. All groups of a node use one engine.
#[derive(Debug, Clone)]
pub struct LogEngine {
    inner: Arc<Mutex<Inner>>,
}

impl LogEngine {
    /// Open the engine, replaying the newest generation (a torn tail is cut off).
    pub fn open(disk: Arc<dyn Disk>) -> Result<Self> {
        let mut gens: Vec<u64> = disk
            .list()?
            .iter()
            .filter_map(|n| {
                n.strip_prefix("raftlog-")?
                    .strip_suffix(".log")?
                    .parse()
                    .ok()
            })
            .collect();
        gens.sort_unstable();
        let generation = gens.last().copied().unwrap_or(0);
        for g in gens.iter().filter(|&&g| g != generation) {
            disk.remove(&file_name(*g))?; // an older generation left by an interrupted rewrite
        }
        let file = disk.open(&file_name(generation))?;
        if gens.is_empty() {
            disk.sync_dir()?;
        }
        let len = file.len()?;
        let mut data =
            vec![0u8; usize::try_from(len).map_err(|_| Error::Storage("log too big".into()))?];
        file.read_exact_at(&mut data, 0)?;
        let mut groups: BTreeMap<u64, GroupState> = BTreeMap::new();
        let mut pos = 0usize;
        while let Some((payload, used)) = codec::decode_frame(&data[pos..]) {
            apply_record(&mut groups, payload)?;
            pos += used;
        }
        if (pos as u64) < len {
            file.set_len(pos as u64)?;
            file.sync()?;
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                disk,
                file,
                generation,
                len: pos as u64,
                buf: Vec::new(),
                groups,
                fsyncs: 0,
                rewrite_at: 64 << 20,
            })),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Groups that have durable state.
    pub fn groups(&self) -> Vec<u64> {
        self.lock().groups.keys().copied().collect()
    }

    /// Storage for `group`. A group without durable state starts with `membership`
    /// as the configuration of its (empty) snapshot.
    pub fn storage(&self, group: u64, membership: Membership) -> SharedStorage {
        let st = self
            .lock()
            .groups
            .get(&group)
            .cloned()
            .unwrap_or_else(|| GroupState {
                log: Log {
                    snap: SnapshotMeta {
                        membership,
                        ..SnapshotMeta::default()
                    },
                    entries: Vec::new(),
                },
                ..GroupState::default()
            });
        SharedStorage {
            engine: self.clone(),
            group,
            st,
        }
    }

    /// Forget `group` entirely.
    pub fn remove(&self, group: u64) {
        let mut g = self.lock();
        g.groups.remove(&group);
        push_record(&mut g.buf, REMOVE, group, &[]);
    }

    /// Write and fsync everything buffered. Returns whether anything was synced.
    pub fn sync(&self) -> Result<bool> {
        let mut g = self.lock();
        if g.buf.is_empty() {
            return Ok(false);
        }
        let buf = std::mem::take(&mut g.buf);
        let at = g.len;
        g.file.write_at(&buf, at)?;
        g.file.sync()?;
        g.len += buf.len() as u64;
        g.fsyncs += 1;
        if g.len > g.rewrite_at {
            rewrite(&mut g)?;
        }
        Ok(true)
    }

    /// Detach everything buffered for a background writer: the file to write to,
    /// the offset reserved for it, and the bytes. Records appended afterwards go
    /// into the next batch.
    pub fn take_batch(&self) -> Option<LogBatch> {
        let mut g = self.lock();
        if g.buf.is_empty() {
            return None;
        }
        let bytes = std::mem::take(&mut g.buf);
        let offset = g.len;
        g.len += bytes.len() as u64;
        g.fsyncs += 1;
        Some(LogBatch {
            file: g.file.clone(),
            offset,
            bytes,
        })
    }

    /// Whether a rewrite is due. The caller must run [`LogEngine::rewrite`] only
    /// while no detached batch is still being written.
    pub fn needs_rewrite(&self) -> bool {
        let g = self.lock();
        g.len > g.rewrite_at
    }

    pub fn rewrite(&self) -> Result<()> {
        let mut g = self.lock();
        if !g.buf.is_empty() {
            let buf = std::mem::take(&mut g.buf);
            let at = g.len;
            g.file.write_at(&buf, at)?;
            g.file.sync()?;
            g.len += buf.len() as u64;
        }
        rewrite(&mut g)
    }

    pub fn fsyncs(&self) -> u64 {
        self.lock().fsyncs
    }

    pub fn bytes(&self) -> u64 {
        self.lock().len
    }

    /// Lower the rewrite threshold (tests).
    pub fn set_rewrite_at(&self, bytes: u64) {
        self.lock().rewrite_at = bytes;
    }

    fn append_record(&self, kind: u8, group: u64, body: &[u8]) -> Result<()> {
        let mut g = self.lock();
        push_record(&mut g.buf, kind, group, body);
        // Keep the mirror in step, exactly as replay would.
        let mut payload = vec![kind];
        payload.extend_from_slice(&group.to_le_bytes());
        payload.extend_from_slice(body);
        apply_record(&mut g.groups, &payload)
    }
}

/// Buffered log bytes detached for writing off the caller's thread.
#[derive(Debug)]
pub struct LogBatch {
    file: Arc<dyn DiskFile>,
    offset: u64,
    bytes: Vec<u8>,
}

impl LogBatch {
    /// Write and fsync. Safe to call from another thread, concurrently with new
    /// appends (which go into the engine's next buffer).
    pub fn write(&self) -> Result<()> {
        self.file.write_at(&self.bytes, self.offset)?;
        self.file.sync()?;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

fn push_record(buf: &mut Vec<u8>, kind: u8, group: u64, body: &[u8]) {
    let mut payload = Vec::with_capacity(9 + body.len());
    payload.push(kind);
    payload.extend_from_slice(&group.to_le_bytes());
    payload.extend_from_slice(body);
    codec::encode_frame(buf, &payload);
}

fn entries_body(entries: &[Entry]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in entries {
        let enc = codec::encode_entry(e);
        body.extend_from_slice(&(enc.len() as u32).to_le_bytes());
        body.extend_from_slice(&enc);
    }
    body
}

fn hard_state_body(hs: &HardState) -> Vec<u8> {
    let mut b = Vec::with_capacity(17);
    b.extend_from_slice(&hs.term.to_le_bytes());
    b.push(u8::from(hs.vote.is_some()));
    b.extend_from_slice(&hs.vote.unwrap_or(0).to_le_bytes());
    b
}

fn apply_record(groups: &mut BTreeMap<u64, GroupState>, payload: &[u8]) -> Result<()> {
    let mut r = Reader::new(payload);
    let kind = r.u8()?;
    let group = r.u64()?;
    match kind {
        ENTRIES => {
            let n = r.u32()?;
            let mut entries = Vec::with_capacity(usize::try_from(n.min(4096)).unwrap_or(0));
            for _ in 0..n {
                entries.push(codec::decode_entry(&r.bytes()?)?);
            }
            r.done()?;
            groups.entry(group).or_default().log.splice(&entries)?;
        }
        HARD_STATE => {
            let term = r.u64()?;
            let has = r.u8()? == 1;
            let vote = r.u64()?;
            r.done()?;
            groups.entry(group).or_default().hs = HardState {
                term,
                vote: has.then_some(vote),
            };
        }
        SNAPSHOT => {
            let snap = codec::decode_snapshot(&r.bytes()?)?;
            r.done()?;
            let st = groups.entry(group).or_default();
            if snap.meta.index >= st.log.snap.index {
                st.log.compact_to(snap.meta);
                st.snap_data = snap.data;
            }
        }
        REMOVE => {
            groups.remove(&group);
        }
        k => return Err(Error::Storage(format!("log record kind {k}"))),
    }
    Ok(())
}

/// Write the live state of every group into the next generation.
fn rewrite(g: &mut Inner) -> Result<()> {
    let next = g.generation + 1;
    let file = g.disk.open(&file_name(next))?;
    file.set_len(0)?;
    let mut buf = Vec::new();
    for (&id, st) in &g.groups {
        let snap = Snapshot {
            meta: st.log.snap.clone(),
            data: st.snap_data.clone(),
        };
        let mut body = Vec::new();
        let enc = codec::encode_snapshot(&snap);
        body.extend_from_slice(&(enc.len() as u32).to_le_bytes());
        body.extend_from_slice(&enc);
        push_record(&mut buf, SNAPSHOT, id, &body);
        push_record(&mut buf, HARD_STATE, id, &hard_state_body(&st.hs));
        if !st.log.entries.is_empty() {
            push_record(&mut buf, ENTRIES, id, &entries_body(&st.log.entries));
        }
    }
    file.write_at(&buf, 0)?;
    file.sync()?;
    g.disk.sync_dir()?;
    g.disk.remove(&file_name(g.generation))?;
    g.disk.sync_dir()?;
    g.file = file;
    g.generation = next;
    g.len = buf.len() as u64;
    g.fsyncs += 3;
    // Rewrite again only after the file doubles past the live size.
    g.rewrite_at = g.rewrite_at.max(2 * g.len);
    Ok(())
}

/// One group's view of a [`LogEngine`]. Reads are served from an in-memory copy;
/// writes are recorded in the shared log and become durable at the next
/// [`LogEngine::sync`].
#[derive(Debug)]
pub struct SharedStorage {
    engine: LogEngine,
    group: u64,
    st: GroupState,
}

impl SharedStorage {
    pub fn engine(&self) -> &LogEngine {
        &self.engine
    }
}

impl Storage for SharedStorage {
    fn hard_state(&self) -> HardState {
        self.st.hs
    }
    fn set_hard_state(&mut self, hs: HardState) -> Result<()> {
        if hs != self.st.hs {
            self.engine
                .append_record(HARD_STATE, self.group, &hard_state_body(&hs))?;
            self.st.hs = hs;
        }
        Ok(())
    }
    fn snapshot_meta(&self) -> &SnapshotMeta {
        &self.st.log.snap
    }
    fn snapshot(&self) -> Result<Snapshot> {
        Ok(Snapshot {
            meta: self.st.log.snap.clone(),
            data: self.st.snap_data.clone(),
        })
    }
    fn last_index(&self) -> Index {
        self.st.log.last_index()
    }
    fn term(&self, i: Index) -> Option<Term> {
        self.st.log.term(i)
    }
    fn entries(&self, lo: Index, hi: Index, max_bytes: usize) -> Vec<Entry> {
        self.st.log.slice(lo, hi, max_bytes)
    }
    fn append(&mut self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.st.log.splice(entries)?;
        self.engine
            .append_record(ENTRIES, self.group, &entries_body(entries))
    }
    fn install_snapshot(&mut self, snap: Snapshot) -> Result<()> {
        let enc = codec::encode_snapshot(&snap);
        let mut body = Vec::with_capacity(4 + enc.len());
        body.extend_from_slice(&(enc.len() as u32).to_le_bytes());
        body.extend_from_slice(&enc);
        self.engine.append_record(SNAPSHOT, self.group, &body)?;
        self.st.log.entries.clear();
        self.st.log.snap = snap.meta;
        self.st.snap_data = snap.data;
        Ok(())
    }
    fn compact(&mut self, snap: Snapshot) -> Result<()> {
        if snap.meta.index <= self.st.log.snap.index {
            return Ok(());
        }
        let enc = codec::encode_snapshot(&snap);
        let mut body = Vec::with_capacity(4 + enc.len());
        body.extend_from_slice(&(enc.len() as u32).to_le_bytes());
        body.extend_from_slice(&enc);
        self.engine.append_record(SNAPSHOT, self.group, &body)?;
        self.st.log.compact_to(snap.meta);
        self.st.snap_data = snap.data;
        Ok(())
    }
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
            kind: EntryKind::Normal(format!("c{index}").into_bytes()),
        }
    }

    #[test]
    fn many_groups_one_fsync() {
        let disk = Arc::new(SimDisk::new(CrashMode::DropUnsynced));
        let eng = LogEngine::open(disk.clone()).unwrap();
        let mut stores: Vec<SharedStorage> = (1..=50)
            .map(|g| eng.storage(g, Membership::new([1, 2, 3])))
            .collect();
        for s in &mut stores {
            s.set_hard_state(HardState {
                term: 2,
                vote: Some(1),
            })
            .unwrap();
            s.append(&[e(2, 1), e(2, 2)]).unwrap();
        }
        assert!(eng.sync().unwrap());
        assert_eq!(eng.fsyncs(), 1, "50 groups, one fsync");
        drop(stores);
        disk.crash(&mut Xoshiro256::seeded(1));
        let eng = LogEngine::open(disk).unwrap();
        assert_eq!(eng.groups().len(), 50);
        let s = eng.storage(17, Membership::default());
        assert_eq!(
            s.hard_state(),
            HardState {
                term: 2,
                vote: Some(1)
            }
        );
        assert_eq!(s.last_index(), 2);
    }

    #[test]
    fn unsynced_records_vanish_and_suffix_replacement_replays() {
        let disk = Arc::new(SimDisk::new(CrashMode::Torn));
        let eng = LogEngine::open(disk.clone()).unwrap();
        let mut s = eng.storage(1, Membership::new([1]));
        s.append(&[e(1, 1), e(1, 2), e(1, 3)]).unwrap();
        s.append(&[e(2, 2)]).unwrap(); // conflict: replaces 2..=3
        eng.sync().unwrap();
        s.append(&[e(2, 3)]).unwrap(); // never synced
        drop(s);
        disk.crash(&mut Xoshiro256::seeded(4));
        let eng = LogEngine::open(disk).unwrap();
        let s = eng.storage(1, Membership::default());
        assert_eq!(s.entries(1, 10, usize::MAX), vec![e(1, 1), e(2, 2)]);
    }

    #[test]
    fn rewrite_keeps_only_live_state() {
        let disk = Arc::new(SimDisk::new(CrashMode::DropUnsynced));
        let eng = LogEngine::open(disk.clone()).unwrap();
        eng.set_rewrite_at(8 << 10);
        let mut a = eng.storage(1, Membership::new([1]));
        let mut b = eng.storage(2, Membership::new([1]));
        for i in 1..=300 {
            a.append(&[e(1, i)]).unwrap();
            b.append(&[e(1, i)]).unwrap();
            if i % 50 == 0 {
                a.compact(Snapshot {
                    meta: SnapshotMeta {
                        index: i - 5,
                        term: 1,
                        membership: Membership::new([1]),
                    },
                    data: vec![],
                })
                .unwrap();
            }
            eng.sync().unwrap();
        }
        eng.remove(2);
        eng.sync().unwrap();
        assert!(
            disk.list()
                .unwrap()
                .iter()
                .any(|n| n != "raftlog-00000000.log"),
            "a rewrite happened"
        );
        drop((a, b));
        let eng = LogEngine::open(disk).unwrap();
        assert_eq!(eng.groups(), vec![1]);
        let a = eng.storage(1, Membership::default());
        assert_eq!(a.first_index(), 296);
        assert_eq!(a.last_index(), 300);
    }
}
