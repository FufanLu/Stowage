//! Production implementations of the [`crate::env`] traits.

use std::fs::{self, File, OpenOptions};
use std::hash::{BuildHasher, Hasher, RandomState};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::env::{Clock, Disk, DiskFile, Rng};

/// The OS clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn monotonic_nanos(&self) -> u64 {
        static ORIGIN: OnceLock<Instant> = OnceLock::new();
        let origin = *ORIGIN.get_or_init(Instant::now);
        u64::try_from(origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    fn wall_nanos(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }
}

/// xoshiro256** — small, fast, and good enough for jitter, sampling and
/// simulation. Not for cryptography.
#[derive(Debug, Clone)]
pub struct Xoshiro256 {
    s: [u64; 4],
}

impl Xoshiro256 {
    /// Deterministic generator; equal seeds give equal streams.
    pub fn seeded(seed: u64) -> Self {
        // Expand the seed with SplitMix64, as the xoshiro authors recommend.
        let mut x = seed;
        let mut next = || {
            x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = x;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        };
        Self {
            s: [next(), next(), next(), next()],
        }
    }

    /// Seeded from process-level entropy (std's per-process hash keys).
    pub fn from_entropy() -> Self {
        let mut h = RandomState::new().build_hasher();
        h.write_u64(SystemClock.wall_nanos());
        Self::seeded(h.finish())
    }
}

impl Rng for Xoshiro256 {
    fn next_u64(&mut self) -> u64 {
        let s = &mut self.s;
        let result = s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = s[1] << 17;
        s[2] ^= s[0];
        s[3] ^= s[1];
        s[1] ^= s[2];
        s[0] ^= s[3];
        s[2] ^= t;
        s[3] = s[3].rotate_left(45);
        result
    }
}

/// A data directory on the local filesystem.
#[derive(Debug, Clone)]
pub struct OsDisk {
    root: PathBuf,
    skip_fsync: bool,
}

impl OsDisk {
    pub fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            skip_fsync: false,
        })
    }

    /// **Unsafe, for benchmarks only**: every fsync becomes a no-op, so an
    /// acknowledged write survives a process crash (the OS has it) but not a
    /// power loss of a majority. This is the durability that replication alone
    /// gives, which is what Dynamo-style stores such as Cartwheel run with by
    /// default; it lets the two be compared like for like.
    pub fn skip_fsync(mut self, skip: bool) -> Self {
        self.skip_fsync = skip;
        self
    }

    fn path(&self, name: &str) -> io::Result<PathBuf> {
        if name.is_empty() || name.contains('/') || name == "." || name == ".." {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid file name {name:?}"),
            ));
        }
        Ok(self.root.join(name))
    }
}

#[derive(Debug)]
struct OsFile(File, bool);

impl DiskFile for OsFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.0.read_at(buf, offset)
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.0.write_all_at(buf, offset)
    }
    fn sync(&self) -> io::Result<()> {
        if self.1 {
            return Ok(());
        }
        self.0.sync_data()
    }
    fn len(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }
}

impl Disk for OsDisk {
    fn open(&self, name: &str) -> io::Result<Arc<dyn DiskFile>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.path(name)?)?;
        Ok(Arc::new(OsFile(file, self.skip_fsync)))
    }
    fn exists(&self, name: &str) -> io::Result<bool> {
        self.path(name)?.try_exists()
    }
    fn remove(&self, name: &str) -> io::Result<()> {
        fs::remove_file(self.path(name)?)
    }
    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        fs::rename(self.path(from)?, self.path(to)?)
    }
    fn list(&self) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && let Some(name) = entry.file_name().to_str()
            {
                names.push(name.to_owned());
            }
        }
        names.sort();
        Ok(names)
    }
    fn sync_dir(&self) -> io::Result<()> {
        if self.skip_fsync {
            return Ok(());
        }
        File::open(&self.root)?.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xoshiro_is_deterministic_and_seed_sensitive() {
        let mut a = Xoshiro256::seeded(42);
        let mut b = Xoshiro256::seeded(42);
        let mut c = Xoshiro256::seeded(43);
        let xs: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
        let ys: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
        let zs: Vec<u64> = (0..8).map(|_| c.next_u64()).collect();
        assert_eq!(xs, ys);
        assert_ne!(xs, zs);
    }

    #[test]
    fn gen_range_stays_in_bounds() {
        let mut r = Xoshiro256::seeded(7);
        for _ in 0..10_000 {
            let v = r.gen_range(10, 13);
            assert!((10..13).contains(&v));
        }
        assert!(!r.gen_bool(0.0));
        assert!(r.gen_bool(1.0));
    }

    #[test]
    fn os_disk_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let disk = OsDisk::open(dir.path()).unwrap();
        let f = disk.open("a.log").unwrap();
        f.write_at(b"hello", 0).unwrap();
        f.write_at(b"!", 5).unwrap();
        f.sync().unwrap();
        let mut buf = [0u8; 6];
        f.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello!");
        disk.rename("a.log", "b.log").unwrap();
        disk.sync_dir().unwrap();
        assert_eq!(disk.list().unwrap(), vec!["b.log".to_owned()]);
        assert!(disk.open("../escape").is_err());
    }
}
