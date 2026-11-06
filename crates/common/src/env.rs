//! The environment a Stowage node runs in.
//!
//! Every source of non-determinism goes through one of these traits: time ([`Clock`]),
//! randomness ([`Rng`]), persistence ([`Disk`]) and the network ([`Transport`]).
//! Because nothing reaches for `std::time`, `std::fs` or a socket directly, the
//! whole cluster can run inside a single-threaded, seeded simulation where a failing
//! run is replayed exactly from its seed.

use std::fmt::Debug;
use std::io;
use std::sync::Arc;

/// Identifier of a node (a `stowd` process) within a cluster.
pub type NodeId = u64;

/// Time source.
pub trait Clock: Send + Sync + Debug {
    /// Monotonic nanoseconds since an arbitrary origin. Used for timeouts and
    /// elections; never goes backwards.
    fn monotonic_nanos(&self) -> u64;

    /// Wall-clock nanoseconds since the UNIX epoch. May drift between nodes; feeds the
    /// physical half of the [`crate::Hlc`].
    fn wall_nanos(&self) -> u64;
}

/// Random number source. Seeded in simulation, OS-backed in production.
pub trait Rng: Send + Debug {
    fn next_u64(&mut self) -> u64;

    /// Uniform value in `lo..hi`. Panics if the range is empty.
    fn gen_range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(lo < hi, "empty range {lo}..{hi}");
        let span = hi - lo;
        // Lemire's multiply-shift; the bias is < 2^-64 * span and irrelevant here.
        lo + ((u128::from(self.next_u64()) * u128::from(span)) >> 64) as u64
    }

    /// `true` with probability `p` (clamped to `0.0..=1.0`).
    fn gen_bool(&mut self, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        // 53 random bits -> uniform f64 in [0, 1).
        let x = (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64);
        x < p
    }
}

/// An open file. Positional I/O only: callers (WAL, page store) always know their
/// offsets, and positional I/O keeps the sim implementation simple.
pub trait DiskFile: Send + Sync + Debug {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;
    /// Make every write issued so far durable (`fdatasync`).
    fn sync(&self) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
    fn set_len(&self, len: u64) -> io::Result<()>;

    fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Fill `buf` completely or fail with `UnexpectedEof`.
    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            match self.read_at(buf, offset)? {
                0 => return Err(io::ErrorKind::UnexpectedEof.into()),
                n => {
                    buf = &mut buf[n..];
                    offset += n as u64;
                }
            }
        }
        Ok(())
    }
}

/// A flat directory of files, rooted somewhere (a data dir in production, memory in
/// simulation).
///
/// Durability rules mirror POSIX: file *contents* become durable on
/// [`DiskFile::sync`]; file *creation, removal and renames* become durable on
/// [`Disk::sync_dir`]. The simulator enforces exactly these rules when it crashes a node.
pub trait Disk: Send + Sync + Debug {
    /// Open `name`, creating an empty file if it does not exist.
    fn open(&self, name: &str) -> io::Result<Arc<dyn DiskFile>>;
    fn exists(&self, name: &str) -> io::Result<bool>;
    fn remove(&self, name: &str) -> io::Result<()>;
    /// Atomically replace `to` with `from`.
    fn rename(&self, from: &str, to: &str) -> io::Result<()>;
    /// Sorted file names.
    fn list(&self) -> io::Result<Vec<String>>;
    /// Make namespace changes (create / remove / rename) durable.
    fn sync_dir(&self) -> io::Result<()>;
}

/// Fire-and-forget message delivery between nodes. Like a real network, delivery is
/// not guaranteed: protocols built on it (Raft, gossip) must tolerate loss,
/// duplication and reordering.
pub trait Transport<M>: Send + Sync + Debug {
    fn send(&self, to: NodeId, msg: M);
}
