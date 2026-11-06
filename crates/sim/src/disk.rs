//! An in-memory [`Disk`] that models what survives a power loss.
//!
//! Each file keeps two images: the *volatile* one every read sees, and the *durable*
//! one that survives a crash. `sync()` folds the file's pending writes into its
//! durable image. The namespace works the same way: creates, removes and renames are
//! visible at once but only survive a crash after `sync_dir()`.
//!
//! On [`SimDisk::crash`], what happens to writes that were never synced depends on
//! the [`CrashMode`]:
//! - `DropUnsynced` loses all of them (the friendliest real disk);
//! - `Torn` keeps a random subset, in order, and may tear a multi-sector write at a
//!   512-byte boundary — the behaviour that makes WAL checksums necessary.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard};

use stowage_common::{Disk, DiskFile, Rng};

/// Sector size used when tearing writes.
pub const SECTOR: usize = 512;

/// What a crash does to unsynced writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CrashMode {
    /// Every unsynced write is lost.
    #[default]
    DropUnsynced,
    /// Each unsynced write independently survives, is torn at a sector boundary, or
    /// is lost.
    Torn,
}

#[derive(Debug)]
enum PendingOp {
    Write { offset: u64, data: Vec<u8> },
    SetLen(u64),
}

#[derive(Debug, Default)]
struct Inode {
    volatile: Vec<u8>,
    durable: Vec<u8>,
    pending: Vec<PendingOp>,
}

impl Inode {
    fn apply(image: &mut Vec<u8>, op: &PendingOp) {
        match op {
            PendingOp::Write { offset, data } => write_into(image, *offset, data),
            PendingOp::SetLen(len) => image.resize(to_usize(*len), 0),
        }
    }
}

type InodeRef = Arc<Mutex<Inode>>;

#[derive(Debug, Default)]
struct State {
    /// Incremented on every crash; handles opened in an older epoch are dead.
    epoch: u64,
    live: BTreeMap<String, InodeRef>,
    durable: BTreeMap<String, InodeRef>,
    stats: DiskStats,
}

/// I/O counters, used by benchmarks and illustrated docs (write amplification, fsyncs).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DiskStats {
    pub bytes_written: u64,
    pub writes: u64,
    pub syncs: u64,
    pub dir_syncs: u64,
    pub crashes: u64,
}

/// A simulated data directory. Clone the `Arc` to share it between a node's
/// incarnations: the disk outlives crashes, the process does not.
#[derive(Debug)]
pub struct SimDisk {
    state: Arc<Mutex<State>>,
    mode: CrashMode,
}

impl SimDisk {
    pub fn new(mode: CrashMode) -> Self {
        Self {
            state: Arc::default(),
            mode,
        }
    }

    pub fn stats(&self) -> DiskStats {
        lock(&self.state).stats
    }

    /// Simulate power loss: decide the fate of unsynced writes, roll the namespace
    /// back to its last `sync_dir`, and invalidate every open handle.
    pub fn crash(&self, rng: &mut dyn Rng) {
        let mut st = lock(&self.state);
        st.epoch += 1;
        st.stats.crashes += 1;
        let mut seen: Vec<*const Mutex<Inode>> = Vec::new();
        for inode in st.durable.values() {
            let ptr = Arc::as_ptr(inode);
            if seen.contains(&ptr) {
                continue;
            }
            seen.push(ptr);
            let mut ino = lock(inode);
            let pending = std::mem::take(&mut ino.pending);
            if self.mode == CrashMode::Torn {
                for op in &pending {
                    match surviving_part(op, rng) {
                        Some(op) => Inode::apply(&mut ino.durable, &op),
                        None => continue,
                    }
                }
            }
            ino.volatile = ino.durable.clone();
        }
        st.live = st.durable.clone();
    }
}

/// Decide what part of an unsynced op reaches the platter in `Torn` mode.
fn surviving_part(op: &PendingOp, rng: &mut dyn Rng) -> Option<PendingOp> {
    match op {
        PendingOp::SetLen(len) => rng.gen_bool(0.5).then_some(PendingOp::SetLen(*len)),
        PendingOp::Write { offset, data } => {
            let roll = rng.gen_range(0, 4);
            match roll {
                0 | 1 => Some(PendingOp::Write {
                    offset: *offset,
                    data: data.clone(),
                }),
                2 if data.len() > SECTOR => {
                    let sectors = data.len().div_ceil(SECTOR) as u64;
                    let keep = to_usize(rng.gen_range(1, sectors)) * SECTOR;
                    Some(PendingOp::Write {
                        offset: *offset,
                        data: data[..keep].to_vec(),
                    })
                }
                _ => None,
            }
        }
    }
}

impl Disk for SimDisk {
    fn open(&self, name: &str) -> io::Result<Arc<dyn DiskFile>> {
        validate(name)?;
        let mut st = lock(&self.state);
        let inode = st.live.entry(name.to_owned()).or_default().clone();
        Ok(Arc::new(SimFile {
            inode,
            disk: self.state.clone(),
            epoch: st.epoch,
        }))
    }

    fn exists(&self, name: &str) -> io::Result<bool> {
        Ok(lock(&self.state).live.contains_key(name))
    }

    fn remove(&self, name: &str) -> io::Result<()> {
        lock(&self.state)
            .live
            .remove(name)
            .map(drop)
            .ok_or_else(|| not_found(name))
    }

    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        validate(to)?;
        let mut st = lock(&self.state);
        let inode = st.live.remove(from).ok_or_else(|| not_found(from))?;
        st.live.insert(to.to_owned(), inode);
        Ok(())
    }

    fn list(&self) -> io::Result<Vec<String>> {
        Ok(lock(&self.state).live.keys().cloned().collect())
    }

    fn sync_dir(&self) -> io::Result<()> {
        let mut st = lock(&self.state);
        st.durable = st.live.clone();
        st.stats.dir_syncs += 1;
        Ok(())
    }
}

#[derive(Debug)]
struct SimFile {
    inode: InodeRef,
    disk: Arc<Mutex<State>>,
    epoch: u64,
}

impl SimFile {
    fn live(&self) -> io::Result<MutexGuard<'_, Inode>> {
        if lock(&self.disk).epoch != self.epoch {
            return Err(io::Error::other("stale file handle: the node crashed"));
        }
        Ok(lock(&self.inode))
    }
}

impl DiskFile for SimFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let ino = self.live()?;
        let start = to_usize(offset);
        if start >= ino.volatile.len() {
            return Ok(0);
        }
        let n = buf.len().min(ino.volatile.len() - start);
        buf[..n].copy_from_slice(&ino.volatile[start..start + n]);
        Ok(n)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        {
            let mut ino = self.live()?;
            write_into(&mut ino.volatile, offset, buf);
            ino.pending.push(PendingOp::Write {
                offset,
                data: buf.to_vec(),
            });
        }
        let mut st = lock(&self.disk);
        st.stats.writes += 1;
        st.stats.bytes_written += buf.len() as u64;
        Ok(())
    }

    fn sync(&self) -> io::Result<()> {
        {
            let mut guard = self.live()?;
            let ino = &mut *guard;
            for op in ino.pending.drain(..) {
                Inode::apply(&mut ino.durable, &op);
            }
        }
        lock(&self.disk).stats.syncs += 1;
        Ok(())
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.live()?.volatile.len() as u64)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        let mut ino = self.live()?;
        ino.volatile.resize(to_usize(len), 0);
        ino.pending.push(PendingOp::SetLen(len));
        Ok(())
    }
}

fn write_into(image: &mut Vec<u8>, offset: u64, data: &[u8]) {
    let start = to_usize(offset);
    let end = start + data.len();
    if image.len() < end {
        image.resize(end, 0);
    }
    image[start..end].copy_from_slice(data);
}

fn to_usize(v: u64) -> usize {
    usize::try_from(v).expect("simulated file larger than address space")
}

fn validate(name: &str) -> io::Result<()> {
    if name.is_empty() || name.contains('/') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid file name {name:?}"),
        ));
    }
    Ok(())
}

fn not_found(name: &str) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, format!("{name} not found"))
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use stowage_common::os::Xoshiro256;

    fn read_all(f: &dyn DiskFile) -> Vec<u8> {
        let mut buf = vec![0u8; usize::try_from(f.len().unwrap()).unwrap()];
        f.read_exact_at(&mut buf, 0).unwrap();
        buf
    }

    #[test]
    fn unsynced_writes_are_lost_on_crash() {
        let disk = SimDisk::new(CrashMode::DropUnsynced);
        let mut rng = Xoshiro256::seeded(1);
        let f = disk.open("wal").unwrap();
        disk.sync_dir().unwrap();
        f.write_at(b"durable", 0).unwrap();
        f.sync().unwrap();
        f.write_at(b"-volatile", 7).unwrap();
        assert_eq!(read_all(&*f), b"durable-volatile");

        disk.crash(&mut rng);
        assert!(f.write_at(b"x", 0).is_err(), "old handle must be dead");
        let f = disk.open("wal").unwrap();
        assert_eq!(read_all(&*f), b"durable");
    }

    #[test]
    fn namespace_changes_need_sync_dir() {
        let disk = SimDisk::new(CrashMode::DropUnsynced);
        let mut rng = Xoshiro256::seeded(1);
        let f = disk.open("MANIFEST.tmp").unwrap();
        f.write_at(b"v2", 0).unwrap();
        f.sync().unwrap();
        disk.rename("MANIFEST.tmp", "MANIFEST").unwrap();
        disk.crash(&mut rng);
        assert!(disk.list().unwrap().is_empty(), "rename not dir-synced");

        let f = disk.open("MANIFEST.tmp").unwrap();
        f.write_at(b"v2", 0).unwrap();
        f.sync().unwrap();
        disk.rename("MANIFEST.tmp", "MANIFEST").unwrap();
        disk.sync_dir().unwrap();
        disk.crash(&mut rng);
        assert_eq!(disk.list().unwrap(), vec!["MANIFEST".to_owned()]);
        assert_eq!(read_all(&*disk.open("MANIFEST").unwrap()), b"v2");
    }

    #[test]
    fn torn_mode_keeps_an_ordered_subset() {
        // Across many seeds, Torn mode must sometimes keep, sometimes drop and
        // sometimes tear an unsynced multi-sector write, and never invent bytes.
        let (mut kept, mut dropped, mut torn) = (0, 0, 0);
        for seed in 0..200 {
            let disk = SimDisk::new(CrashMode::Torn);
            let mut rng = Xoshiro256::seeded(seed);
            let f = disk.open("wal").unwrap();
            disk.sync_dir().unwrap();
            let payload = vec![0xAB; 4 * SECTOR];
            f.write_at(&payload, 0).unwrap();
            disk.crash(&mut rng);
            let got = read_all(&*disk.open("wal").unwrap());
            assert!(got.iter().all(|&b| b == 0xAB));
            match got.len() {
                0 => dropped += 1,
                n if n == payload.len() => kept += 1,
                n => {
                    assert_eq!(n % SECTOR, 0);
                    torn += 1;
                }
            }
        }
        assert!(
            kept > 0 && dropped > 0 && torn > 0,
            "{kept}/{dropped}/{torn}"
        );
    }

    #[test]
    fn stats_count_io() {
        let disk = SimDisk::new(CrashMode::DropUnsynced);
        let f = disk.open("a").unwrap();
        f.write_at(&[1; 100], 0).unwrap();
        f.write_at(&[2; 28], 100).unwrap();
        f.sync().unwrap();
        let s = disk.stats();
        assert_eq!((s.writes, s.bytes_written, s.syncs), (2, 128, 1));
    }
}
