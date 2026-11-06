//! Real numbers for the disk half of `docs/illustrated/A-deterministic-simulation.md`.
//!
//! ```text
//! cargo run -p stowage-sim --example disk_crash
//! ```

use stowage_common::os::Xoshiro256;
use stowage_common::{Disk, DiskFile};
use stowage_sim::{CrashMode, SimDisk};

fn contents(f: &dyn DiskFile) -> String {
    let len = usize::try_from(f.len().unwrap_or(0)).unwrap_or(0);
    let mut buf = vec![0u8; len];
    let _ = f.read_exact_at(&mut buf, 0);
    String::from_utf8_lossy(&buf).into_owned()
}

fn main() -> std::io::Result<()> {
    let mut rng = Xoshiro256::seeded(42);

    println!("== 1. contents: only synced bytes survive");
    let disk = SimDisk::new(CrashMode::DropUnsynced);
    let wal = disk.open("000001.wal")?;
    disk.sync_dir()?;
    wal.write_at(b"put a=1;", 0)?;
    wal.sync()?;
    wal.write_at(b"put b=2;", 8)?;
    println!("before crash: {:?}", contents(&*wal));
    disk.crash(&mut rng);
    println!(
        "old handle:   {:?}",
        wal.write_at(b"x", 0).map_err(|e| e.to_string())
    );
    println!("after crash:  {:?}", contents(&*disk.open("000001.wal")?));

    println!("\n== 2. namespace: rename survives only after sync_dir");
    let disk = SimDisk::new(CrashMode::DropUnsynced);
    let tmp = disk.open("MANIFEST.tmp")?;
    tmp.write_at(b"v2", 0)?;
    tmp.sync()?;
    disk.rename("MANIFEST.tmp", "MANIFEST")?;
    println!("before crash: {:?}", disk.list()?);
    disk.crash(&mut rng);
    println!("after crash (no sync_dir): {:?}", disk.list()?);

    println!("\n== 3. torn writes: one unsynced 2048-byte write, 1000 crashes");
    let (mut whole, mut lost) = (0, 0);
    let mut torn = [0u32; 4];
    for seed in 0..1000 {
        let disk = SimDisk::new(CrashMode::Torn);
        let mut rng = Xoshiro256::seeded(seed);
        let f = disk.open("wal")?;
        disk.sync_dir()?;
        f.write_at(&[0xAB; 2048], 0)?;
        disk.crash(&mut rng);
        let len = disk.open("wal")?.len()?;
        match len {
            0 => lost += 1,
            2048 => whole += 1,
            n => torn[usize::try_from(n / 512).unwrap_or(0)] += 1,
        }
    }
    println!("survived whole: {whole}");
    println!("lost entirely:  {lost}");
    for (sectors, n) in torn.iter().enumerate().skip(1) {
        println!(
            "torn after {sectors} sector(s) ({} bytes): {n}",
            sectors * 512
        );
    }
    Ok(())
}
