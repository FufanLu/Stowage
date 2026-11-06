//! Produces the real data in `docs/illustrated/01-a-page-on-disk.md` and
//! `02-crash-and-recover.md`.
//!
//! ```text
//! cargo run -p stowage-storage --example illustrate -- page
//! cargo run -p stowage-storage --example illustrate -- crash
//! ```

use std::fmt::Write as _;
use std::sync::Arc;

use stowage_common::Disk;
use stowage_common::os::Xoshiro256;
use stowage_sim::{CrashMode, SimDisk};
use stowage_storage::mtr::{MtrRecord, PageChange, hole, image_len};
use stowage_storage::page::{HEADER, PAGE_SIZE, PageBuf, PageKind, SLOT};
use stowage_storage::wal::decode_frame;
use stowage_storage::{BTree, BTreeOptions, WriteBatch};

fn hexdump(bytes: &[u8], base: usize) -> String {
    let mut out = String::new();
    for (i, chunk) in bytes.chunks(16).enumerate() {
        let hex: Vec<String> = chunk.iter().map(|b| format!("{b:02x}")).collect();
        let ascii: String = chunk
            .iter()
            .map(|&b| if b.is_ascii_graphic() { b as char } else { '.' })
            .collect();
        let _ = writeln!(
            out,
            "  {:04x}  {:<47}  {ascii}",
            base + i * 16,
            hex.join(" ")
        );
    }
    out
}

fn header(p: &PageBuf) -> String {
    format!(
        "page_id={} kind={:?} lsn={} nslots={} cell_start={} frag={} link={} free={}",
        p.page_id(),
        p.kind().unwrap_or(PageKind::Free),
        p.lsn(),
        p.nslots(),
        p.cell_start(),
        p.frag(),
        p.link(),
        p.total_free()
    )
}

/// All WAL records on `disk`, in order: (lsn, record, framed size).
fn wal_records(disk: &SimDisk) -> Vec<(u64, MtrRecord, usize)> {
    let mut names: Vec<String> = disk
        .list()
        .unwrap_or_default()
        .into_iter()
        .filter(|n| n.starts_with("wal-"))
        .collect();
    names.sort();
    let mut out = Vec::new();
    for name in names {
        let start = u64::from_str_radix(&name[4..20], 16).unwrap_or(0);
        let Ok(f) = disk.open(&name) else { continue };
        let mut data = vec![0u8; usize::try_from(f.len().unwrap_or(0)).unwrap_or(0)];
        let _ = f.read_exact_at(&mut data, 0);
        let mut pos = 0;
        while let Some(payload) = decode_frame(&data[pos..], start + pos as u64) {
            if let Ok(rec) = MtrRecord::decode(payload) {
                out.push((start + pos as u64, rec, 8 + payload.len()));
            }
            pos += 8 + payload.len();
        }
    }
    out
}

fn describe(rec: &MtrRecord) -> String {
    rec.pages
        .iter()
        .map(|(id, c)| match c {
            PageChange::Image(img) => {
                let (hs, he) = hole(img);
                format!(
                    "page {id}: full image, {} B logged (hole [{hs}..{he}) of {} B omitted)",
                    image_len(img),
                    he - hs
                )
            }
            PageChange::Delta(r) => {
                let ranges: Vec<String> = r
                    .iter()
                    .map(|(off, b)| {
                        let zeros = b.len() > 4 && b.iter().all(|&x| x == 0);
                        let what = if zeros {
                            "zeros (sent as a length)"
                        } else {
                            "B"
                        };
                        format!(
                            "[{off}..{}) {} {what}",
                            usize::from(*off) + b.len(),
                            b.len()
                        )
                    })
                    .collect();
                format!("page {id}: delta {}", ranges.join(", "))
            }
        })
        .collect::<Vec<_>>()
        .join("\n      ")
}

fn put(t: &BTree, k: &[u8], v: &[u8]) {
    let mut b = WriteBatch::new();
    b.put(k.to_vec(), v.to_vec());
    t.write(&b, true).expect("write");
}

fn page_chapter() {
    let disk = Arc::new(SimDisk::new(CrashMode::DropUnsynced));
    let opts = BTreeOptions::default();
    let (t, report) = BTree::open(disk.clone(), opts).expect("open");
    println!(
        "== open: created={} next_page={}",
        report.created, report.next_page
    );
    for (lsn, rec, size) in wal_records(&disk) {
        println!(
            "  wal lsn {lsn} ({size} B): next_page={}\n      {}",
            rec.next_page,
            describe(&rec)
        );
    }

    println!("\n== three inserts into the root leaf (page 1)");
    put(&t, b"cherry", b"red");
    put(&t, b"apple", b"green");
    put(&t, b"banana", b"yellow");
    let p = t.page(1).expect("page");
    println!("{}", header(&p));
    println!("header + slot array:");
    print!("{}", hexdump(&p.bytes()[..HEADER + p.nslots() * SLOT], 0));
    println!("cell area (end of the page):");
    print!("{}", hexdump(&p.bytes()[p.cell_start()..], p.cell_start()));
    for i in 0..p.nslots() {
        println!(
            "  slot {i}: key={:?} value={:?}",
            String::from_utf8_lossy(p.key(i)),
            String::from_utf8_lossy(p.leaf_value(i))
        );
    }
    println!("\nWAL records for the three inserts:");
    for (lsn, rec, size) in wal_records(&disk).into_iter().skip(1) {
        println!(
            "  lsn {lsn} ({size} B on the log)\n      {}",
            describe(&rec)
        );
    }
    if let Some((_, rec, _)) = wal_records(&disk).last()
        && let Some((_, PageChange::Delta(r))) = rec.pages.first()
    {
        println!("  bytes of the last delta (banana goes between apple and cherry):");
        for (off, b) in r {
            print!("{}", hexdump(b, usize::from(*off)));
        }
    }

    println!("\n== fill the leaf until it splits");
    let before = wal_records(&disk).len();
    let mut i = 0u32;
    while t.stats().leaf_splits == 0 {
        put(&t, format!("fruit-{i:04}").as_bytes(), &[b'.'; 200]);
        i += 1;
    }
    println!("inserted {i} more keys; the {i}th caused the first split");
    let root = t.root().expect("root");
    let r = t.page(root).expect("root page");
    println!("meta page: root = page {root}");
    println!("root  {}", header(&r));
    for s in 0..r.nslots() {
        println!(
            "      separator {:?} -> page {} (keys >= it); link -> page {} (keys < it)",
            String::from_utf8_lossy(r.key(s)),
            r.child(s),
            r.link()
        );
    }
    for id in [r.link(), r.child(0)] {
        let p = t.page(id).expect("leaf");
        println!(
            "leaf  {}\n      first key {:?}, last key {:?}",
            header(&p),
            String::from_utf8_lossy(p.key(0)),
            String::from_utf8_lossy(p.key(p.nslots() - 1))
        );
    }
    let recs = wal_records(&disk);
    let (lsn, rec, size) = &recs[recs.len() - 1];
    println!(
        "\nthe split's mini-transaction: lsn {lsn}, {size} B, {} pages, next_page={}\n      {}",
        rec.pages.len(),
        rec.next_page,
        describe(rec)
    );
    let normal = &recs[before];
    println!(
        "for comparison, an ordinary insert: lsn {}, {} B\n      {}",
        normal.0,
        normal.2,
        describe(&normal.1)
    );
    let s = t.stats();
    println!(
        "\nstats: optimistic writes {}, pessimistic {}, leaf splits {}, root splits {}",
        s.optimistic_writes, s.pessimistic_writes, s.leaf_splits, s.root_splits
    );
}

#[derive(Debug, Default)]
struct PageCensus {
    valid: u32,
    zero: u32,
    torn: Vec<u32>,
}

fn census(disk: &SimDisk) -> PageCensus {
    let mut c = PageCensus::default();
    let Ok(f) = disk.open("btree.pages") else {
        return c;
    };
    let pages = f.len().unwrap_or(0) / PAGE_SIZE as u64;
    for id in 0..pages {
        let mut p = PageBuf::zeroed();
        let _ = f.read_exact_at(p.bytes_mut(), id * PAGE_SIZE as u64);
        if p.verify() {
            c.valid += 1;
        } else if p.bytes().iter().all(|&b| b == 0) {
            c.zero += 1;
        } else {
            c.torn.push(u32::try_from(id).unwrap_or(u32::MAX));
        }
    }
    c
}

fn crash_scenario(seed: u64, verbose: bool) -> Option<()> {
    let disk = Arc::new(SimDisk::new(CrashMode::Torn));
    let opts = BTreeOptions {
        cache_pages: 64,
        checkpoint_wal_bytes: u64::MAX,
    };
    let key = |i: u32| format!("order-{i:06}").into_bytes();
    let (t, _) = BTree::open(disk.clone(), opts.clone()).ok()?;
    for i in 0..10_000 {
        let mut b = WriteBatch::new();
        b.put(key(i), vec![b'a'; 100]);
        t.write(&b, false).ok()?;
    }
    let redo = t.checkpoint().ok()?;
    // 300 acknowledged (synced) updates spread over the tree (evicting and writing
    // back pages), then 600 unsynced updates of 20 hot keys. The hot pages stay
    // cached, so no eviction forces the WAL to disk, and the log's 64 KiB
    // write-behind puts unsynced records into the file without an fsync.
    for i in 0..300 {
        let mut b = WriteBatch::new();
        b.put(key(i * 31), vec![b'b'; 100]);
        t.write(&b, true).ok()?;
    }
    let acked_end = t.stats().wal.durable;
    for i in 0..600 {
        let mut b = WriteBatch::new();
        b.put(key(20_000 + i % 20), vec![b'c'; 1500]);
        t.write(&b, false).ok()?;
    }
    let s = t.stats();
    drop(t);
    disk.crash(&mut Xoshiro256::seeded(seed));

    let c = census(&disk);
    let wal_len: u64 = disk
        .list()
        .ok()?
        .iter()
        .filter(|n| n.starts_with("wal-"))
        .filter_map(|n| disk.open(n).ok()?.len().ok())
        .sum();
    let (t, report) = BTree::open(disk.clone(), opts).ok()?;
    if std::env::var_os("ILLUSTRATE_DEBUG").is_some() {
        eprintln!(
            "seed {seed}: torn tail {} B, torn pages {:?}, wal end {} durable {}",
            report.torn_tail_bytes, c.torn, s.wal.end, s.wal.durable
        );
    }
    if report.torn_tail_bytes == 0 || c.torn.is_empty() {
        return None; // not an illustrative seed; try the next one
    }
    if !verbose {
        return Some(());
    }
    println!("crash seed {seed}");
    println!("\n== before the crash");
    println!("checkpoint redo LSN: {redo}");
    println!(
        "WAL end {} · durable (fsynced) up to {} · acked writes end at {acked_end}",
        s.wal.end, s.wal.durable
    );
    println!(
        "pages written back by eviction after the checkpoint (no fsync): {}",
        s.pool.page_writes
    );
    println!("\n== on disk right after the power cut");
    println!(
        "data file: {} pages valid, {} all-zero, {} torn (checksum mismatch): {:?}",
        c.valid,
        c.zero,
        c.torn.len(),
        c.torn
    );
    println!("WAL bytes present: {wal_len} (from the checkpoint segment on)");
    println!("\n== recovery report\n{report:#?}");
    let mut acked_ok = 0;
    for i in 0..300 {
        if t.get(&key(i * 31)).ok()?.as_deref() == Some(&[b'b'; 100][..]) {
            acked_ok += 1;
        }
    }
    let mut unsynced = 0;
    for i in 0..20 {
        if t.get(&key(20_000 + i)).ok()?.as_deref() == Some(&[b'c'; 1500][..]) {
            unsynced += 1;
        }
    }
    println!("\n== after recovery");
    println!("acknowledged updates present: {acked_ok}/300");
    println!("hot keys whose unsynced 1500-byte update survived: {unsynced}/20");
    println!(
        "structural check: {} keys, height {}",
        t.check().ok()?,
        t.height().ok()?
    );
    let after = census(&disk);
    println!(
        "data file after the end-of-recovery checkpoint: {} valid, {} torn",
        after.valid,
        after.torn.len()
    );
    Some(())
}

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("page") => page_chapter(),
        Some("crash") => {
            let seed = (0..1000).find(|&s| crash_scenario(s, false).is_some());
            match seed {
                Some(s) => {
                    crash_scenario(s, true);
                }
                None => println!("no seed in 0..1000 tore both a page and the WAL tail"),
            }
        }
        _ => eprintln!("usage: illustrate page | crash"),
    }
}
