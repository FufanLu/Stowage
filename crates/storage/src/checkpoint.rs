//! The `CHECKPOINT` file: where recovery starts.
//!
//! It is tiny and replaced atomically: write `CHECKPOINT.tmp`, fsync, rename over
//! `CHECKPOINT`, fsync the directory. It never lives inside the data file, because a
//! torn write of page 0 must not be able to lose the recovery starting point.
//!
//! ```text
//! magic "STOWCKP1" ‖ redo_lsn u64 ‖ next_page u32 ‖ crc32c u32      (24 bytes)
//! ```

use stowage_common::Disk;

use crate::page::{Lsn, PageId};
use crate::{Error, Result};

pub const FILE: &str = "CHECKPOINT";
const TMP: &str = "CHECKPOINT.tmp";
const MAGIC: &[u8; 8] = b"STOWCKP1";
const LEN: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checkpoint {
    /// Recovery replays the WAL from here. Every page change below this LSN is
    /// already in the data file.
    pub redo_lsn: Lsn,
    pub next_page: PageId,
}

impl Checkpoint {
    fn encode(&self) -> [u8; LEN] {
        let mut out = [0u8; LEN];
        out[..8].copy_from_slice(MAGIC);
        out[8..16].copy_from_slice(&self.redo_lsn.to_le_bytes());
        out[16..20].copy_from_slice(&self.next_page.to_le_bytes());
        let crc = crc32c::crc32c(&out[..20]);
        out[20..].copy_from_slice(&crc.to_le_bytes());
        out
    }

    fn decode(b: &[u8]) -> Result<Self> {
        if b.len() != LEN || &b[..8] != MAGIC {
            return Err(Error::Corruption("bad CHECKPOINT file".to_owned()));
        }
        let crc = u32::from_le_bytes([b[20], b[21], b[22], b[23]]);
        if crc != crc32c::crc32c(&b[..20]) {
            return Err(Error::Corruption("CHECKPOINT checksum mismatch".to_owned()));
        }
        let mut lsn = [0u8; 8];
        lsn.copy_from_slice(&b[8..16]);
        Ok(Self {
            redo_lsn: u64::from_le_bytes(lsn),
            next_page: u32::from_le_bytes([b[16], b[17], b[18], b[19]]),
        })
    }
}

pub fn read(disk: &dyn Disk) -> Result<Option<Checkpoint>> {
    if !disk.exists(FILE)? {
        return Ok(None);
    }
    let f = disk.open(FILE)?;
    let mut buf = vec![0u8; usize::try_from(f.len()?).unwrap_or(0)];
    f.read_exact_at(&mut buf, 0)?;
    Checkpoint::decode(&buf).map(Some)
}

pub fn write(disk: &dyn Disk, ckpt: Checkpoint) -> Result<()> {
    let f = disk.open(TMP)?;
    f.set_len(0)?;
    f.write_at(&ckpt.encode(), 0)?;
    f.sync()?;
    disk.rename(TMP, FILE)?;
    disk.sync_dir()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use stowage_common::os::Xoshiro256;
    use stowage_sim::{CrashMode, SimDisk};

    #[test]
    fn roundtrip_and_atomic_replace() {
        let disk = SimDisk::new(CrashMode::Torn);
        assert_eq!(read(&disk).unwrap(), None);
        let a = Checkpoint {
            redo_lsn: 100,
            next_page: 7,
        };
        write(&disk, a).unwrap();
        assert_eq!(read(&disk).unwrap(), Some(a));
        // A crash right after a successful write keeps it.
        disk.crash(&mut Xoshiro256::seeded(3));
        assert_eq!(read(&disk).unwrap(), Some(a));
    }

    #[test]
    fn detects_corruption() {
        let mut enc = Checkpoint {
            redo_lsn: 1,
            next_page: 2,
        }
        .encode();
        enc[9] ^= 0xFF;
        assert!(Checkpoint::decode(&enc).is_err());
    }
}
