//! Backups are changefeed windows written to files; restores replay them up to
//! any point in time.
//!
//! ```text
//!  backup 1 (full)         backup 2 (incremental)    backup 3 (incremental)
//!  every version in        every version in          every version in
//!  (0, t1]                 (t1, t2]                  (t2, t3]
//!  ───────────────────────┼─────────────────────────┼─────────────────────────► time
//!                                     ▲ restore --as-of T: for each key, the newest version ≤ T
//! ```
//!
//! A window comes from `KvClient::changes`, which closes its upper bound: no
//! commit will ever land inside a window after it was read, so adjacent
//! backups cover history without gaps or overlaps. Because every version is
//! kept, a restore can stop at any timestamp inside any backup, not only at a
//! backup's end: point-in-time recovery.
//!
//! On disk, one directory per backup:
//!
//! ```text
//!  <dest>/<to-timestamp>/manifest.json   kind, window, counts, file checksum
//!  <dest>/<to-timestamp>/versions.bin    records: u32 klen ‖ key ‖ u64 wall ‖ u32 logical ‖ u8 live ‖ [u32 vlen ‖ value]
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use stowage_common::Timestamp;
use stowage_node::KvClient;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("cluster: {0}")]
    Cluster(String),
    #[error("corrupt backup {0}: {1}")]
    Corrupt(PathBuf, String),
    #[error("{0}")]
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Everything below the cluster's internal keys: catalogs, SQL rows and
/// indexes, documents and their ledgers.
pub const SPAN: (&[u8], &[u8]) = (&[0x01], &[0x05]);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Manifest {
    pub kind: String,
    /// The window `(from, to]`, as `wall,logical`.
    pub from: String,
    pub to: String,
    pub versions: u64,
    pub keys: u64,
    pub bytes: u64,
    pub crc32c: u32,
    /// Wall-clock time of `to`, for people.
    pub to_utc: String,
}

pub fn ts_string(t: Timestamp) -> String {
    format!("{},{}", t.wall, t.logical)
}

pub fn parse_ts(s: &str) -> Option<Timestamp> {
    let (w, l) = s.split_once(',')?;
    Some(Timestamp::new(w.parse().ok()?, l.parse().ok()?))
}

/// A UTC wall-clock time `YYYY-MM-DD HH:MM:SS[.fff]`, or `wall,logical`.
pub fn parse_as_of(s: &str) -> Result<Timestamp> {
    if let Some(t) = parse_ts(s) {
        return Ok(t);
    }
    let micros = parse_utc(s).ok_or_else(|| {
        Error::Invalid(format!(
            "not a timestamp: {s:?} (want `YYYY-MM-DD HH:MM:SS` UTC or `wall,logical`)"
        ))
    })?;
    Ok(Timestamp::new(
        u64::try_from(micros).unwrap_or(0) * 1000,
        u32::MAX,
    ))
}

fn parse_utc(s: &str) -> Option<i64> {
    let s = s.trim().trim_end_matches('Z');
    let (d, t) = s.split_once([' ', 'T']).unwrap_or((s, "00:00:00"));
    let mut dp = d.split('-').map(|x| x.parse::<i64>().ok());
    let (y, m, day) = (dp.next()??, dp.next()??, dp.next()??);
    let mut tp = t.split(':');
    let h: i64 = tp.next()?.parse().ok()?;
    let mi: i64 = tp.next().unwrap_or("0").parse().ok()?;
    let sec: f64 = tp.next().unwrap_or("0").parse().ok()?;
    let yy = if m <= 2 { y - 1 } else { y };
    let era = yy.div_euclid(400);
    let yoe = yy - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day - 1;
    let days = era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468;
    Some((days * 86_400 + h * 3600 + mi * 60) * 1_000_000 + (sec * 1e6) as i64)
}

fn utc(t: Timestamp) -> String {
    let secs = (t.wall / 1_000_000_000) as i64;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.{:03} UTC",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        (t.wall / 1_000_000) % 1000
    )
}

/// Backups in `dest`, oldest first.
pub fn list(dest: &Path) -> Result<Vec<(PathBuf, Manifest)>> {
    let mut out = Vec::new();
    if !dest.exists() {
        return Ok(out);
    }
    for e in std::fs::read_dir(dest)? {
        let p = e?.path();
        let m = p.join("manifest.json");
        if m.exists() {
            let man: Manifest = serde_json::from_slice(&std::fs::read(&m)?)
                .map_err(|e| Error::Corrupt(m.clone(), e.to_string()))?;
            out.push((p, man));
        }
    }
    out.sort_by_key(|(_, m)| parse_ts(&m.to).unwrap_or(Timestamp::MIN));
    Ok(out)
}

/// Take a backup into `dest`: incremental after the newest one there, or full.
pub async fn backup(client: &Arc<KvClient>, dest: &Path, full: bool) -> Result<Manifest> {
    let existing = list(dest)?;
    let from = match existing.last() {
        Some((_, m)) if !full => {
            parse_ts(&m.to).ok_or_else(|| Error::Invalid("bad manifest".into()))?
        }
        _ => Timestamp::MIN,
    };
    let to = client.now();
    let mut changes = client
        .changes(SPAN.0, SPAN.1, from, to)
        .await
        .map_err(|e| Error::Cluster(e.to_string()))?;
    changes.sort_by(|a, b| a.key.cmp(&b.key).then(a.ts.cmp(&b.ts)));
    let mut buf = Vec::new();
    let mut keys = 0u64;
    let mut last: Option<&[u8]> = None;
    for c in &changes {
        if last != Some(c.key.as_slice()) {
            keys += 1;
            last = Some(&c.key);
        }
        buf.extend_from_slice(&(c.key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&c.key);
        buf.extend_from_slice(&c.ts.wall.to_le_bytes());
        buf.extend_from_slice(&c.ts.logical.to_le_bytes());
        match &c.value {
            Some(v) => {
                buf.push(1);
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v);
            }
            None => buf.push(0),
        }
    }
    let man = Manifest {
        kind: if from == Timestamp::MIN {
            "full".into()
        } else {
            "incremental".into()
        },
        from: ts_string(from),
        to: ts_string(to),
        versions: changes.len() as u64,
        keys,
        bytes: buf.len() as u64,
        crc32c: crc32c::crc32c(&buf),
        to_utc: utc(to),
    };
    let dir = dest.join(format!("{:020}-{:010}", to.wall, to.logical));
    std::fs::create_dir_all(&dir)?;
    // Data first, manifest last: a directory without a manifest is not a backup.
    std::fs::write(dir.join("versions.bin"), &buf)?;
    std::fs::File::open(dir.join("versions.bin"))?.sync_all()?;
    let tmp = dir.join("manifest.json.tmp");
    std::fs::write(
        &tmp,
        serde_json::to_vec_pretty(&man).map_err(|e| Error::Invalid(e.to_string()))?,
    )?;
    std::fs::rename(&tmp, dir.join("manifest.json"))?;
    Ok(man)
}

/// One version as read back from a backup.
#[derive(Debug, Clone, PartialEq)]
pub struct Version {
    pub key: Vec<u8>,
    pub ts: Timestamp,
    pub value: Option<Vec<u8>>,
}

pub fn read(dir: &Path, man: &Manifest) -> Result<Vec<Version>> {
    let path = dir.join("versions.bin");
    let buf = std::fs::read(&path)?;
    if crc32c::crc32c(&buf) != man.crc32c {
        return Err(Error::Corrupt(path, "checksum mismatch".into()));
    }
    let bad = || Error::Corrupt(path.clone(), "truncated record".into());
    let mut out = Vec::with_capacity(man.versions as usize);
    let mut i = 0;
    let mut take = |n: usize| -> Result<&[u8]> {
        let s = buf.get(i..i + n).ok_or_else(bad)?;
        i += n;
        Ok(s)
    };
    let u32le = |b: &[u8]| u32::from_le_bytes(b.try_into().unwrap_or([0; 4]));
    for _ in 0..man.versions {
        let kl = u32le(take(4)?) as usize;
        let key = take(kl)?.to_vec();
        let wall = u64::from_le_bytes(take(8)?.try_into().unwrap_or([0; 8]));
        let logical = u32le(take(4)?);
        let value = if take(1)?[0] == 1 {
            let vl = u32le(take(4)?) as usize;
            Some(take(vl)?.to_vec())
        } else {
            None
        };
        out.push(Version {
            key,
            ts: Timestamp::new(wall, logical),
            value,
        });
    }
    Ok(out)
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Restored {
    pub backups: usize,
    pub versions_read: u64,
    pub keys_written: u64,
    pub as_of: String,
}

/// Key → value: the live keys at one moment.
pub type State = BTreeMap<Vec<u8>, Vec<u8>>;

/// The state as of `as_of` (inclusive) according to the backups in `src`.
pub fn state_as_of(src: &Path, as_of: Timestamp) -> Result<(State, usize, u64)> {
    let backups = list(src)?;
    let first_full = backups
        .iter()
        .position(|(_, m)| m.kind == "full")
        .ok_or_else(|| Error::Invalid("no full backup".into()))?;
    let mut state: BTreeMap<Vec<u8>, (Timestamp, Option<Vec<u8>>)> = BTreeMap::new();
    let (mut used, mut read_n) = (0, 0u64);
    let mut expect_from = Timestamp::MIN;
    for (dir, man) in &backups[first_full..] {
        let from = parse_ts(&man.from).unwrap_or(Timestamp::MIN);
        if from > as_of {
            break;
        }
        if man.kind == "full" {
            state.clear();
        } else if from != expect_from {
            return Err(Error::Invalid(format!(
                "backup chain broken before {}: a gap from {} to {}",
                dir.display(),
                ts_string(expect_from),
                man.from
            )));
        }
        expect_from = parse_ts(&man.to).unwrap_or(Timestamp::MIN);
        used += 1;
        for v in read(dir, man)? {
            read_n += 1;
            if v.ts <= as_of && state.get(&v.key).is_none_or(|(t, _)| *t < v.ts) {
                state.insert(v.key, (v.ts, v.value));
            }
        }
    }
    if used == 0 {
        return Err(Error::Invalid("no backup covers that time".into()));
    }
    Ok((
        state
            .into_iter()
            .filter_map(|(k, (_, v))| v.map(|v| (k, v)))
            .collect(),
        used,
        read_n,
    ))
}

/// Restore into an empty cluster the state as of `as_of`.
pub async fn restore(
    client: &Arc<KvClient>,
    src: &Path,
    as_of: Timestamp,
    force: bool,
) -> Result<Restored> {
    let (state, used, read_n) = state_as_of(src, as_of)?;
    let existing = client
        .scan(SPAN.0, SPAN.1, 1)
        .await
        .map_err(|e| Error::Cluster(e.to_string()))?;
    if !existing.is_empty() && !force {
        return Err(Error::Invalid(
            "the cluster is not empty (use --force to restore over it)".into(),
        ));
    }
    let mut n = 0u64;
    for (k, v) in &state {
        client
            .put(k, v)
            .await
            .map_err(|e| Error::Cluster(e.to_string()))?;
        n += 1;
    }
    Ok(Restored {
        backups: used,
        versions_read: read_n,
        keys_written: n,
        as_of: ts_string(as_of),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_roundtrip() {
        let t = parse_as_of("2026-11-07 10:20:30.5").unwrap();
        assert_eq!(utc(t), "2026-11-07 10:20:30.500 UTC");
        assert_eq!(parse_as_of("123,4").unwrap(), Timestamp::new(123, 4));
    }
}
