//! Multi-version data in the storage engine.
//!
//! ```text
//!  'd' ‖ esc(key) ‖ !ts        a committed version (value or tombstone); newest first
//!  'i' ‖ esc(key)              a write intent: an uncommitted version owned by a transaction
//!  'x' ‖ esc(anchor) ‖ txn id  a transaction record, on the range holding its anchor key
//! ```
//!
//! `esc` is the memcomparable escaping used by FoundationDB's tuple layer and
//! CockroachDB: `0x00` becomes `0x00 0xFF` and the key ends with `0x00 0x01`. It
//! keeps byte order equal to key order and makes the key prefix-free, so every
//! version of one key is contiguous and no key's versions interleave with
//! another's. `!ts` is the bitwise NOT of the 12-byte big-endian HLC timestamp:
//! iterating forward yields the newest version first.

use stowage_common::Timestamp;

use crate::codec::{R, W};
use crate::{Error, Result};

pub const VERSION: u8 = b'd';
pub const INTENT: u8 = b'i';
pub const TXN: u8 = b'x';
/// Every prefix whose keys belong to the range covering their user key.
pub const RANGE_PREFIXES: [u8; 3] = [VERSION, INTENT, TXN];

pub fn escape(key: &[u8], out: &mut Vec<u8>) {
    for &b in key {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0x00, 0x01]);
}

/// Inverse of [`escape`]: the user key and the bytes after it.
pub fn unescape(buf: &[u8]) -> Result<(Vec<u8>, &[u8])> {
    let mut out = Vec::with_capacity(buf.len());
    let mut i = 0;
    while i < buf.len() {
        let b = buf[i];
        if b == 0 {
            match buf.get(i + 1) {
                Some(0xFF) => {
                    out.push(0);
                    i += 2;
                }
                Some(0x01) => return Ok((out, &buf[i + 2..])),
                _ => return Err(Error::Codec("bad escaped key".into())),
            }
        } else {
            out.push(b);
            i += 1;
        }
    }
    Err(Error::Codec("unterminated escaped key".into()))
}

fn prefixed(prefix: u8, key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(key.len() + 16);
    k.push(prefix);
    escape(key, &mut k);
    k
}

pub fn version_key(key: &[u8], ts: Timestamp) -> Vec<u8> {
    let mut k = prefixed(VERSION, key);
    k.extend(ts.encode().iter().map(|b| !b));
    k
}

/// `[first, last]` engine keys holding the versions of `key` with timestamp <= `ts`.
pub fn versions_at_or_below(key: &[u8], ts: Timestamp) -> (Vec<u8>, Vec<u8>) {
    let lo = version_key(key, ts);
    let mut hi = prefixed(VERSION, key);
    hi.extend_from_slice(&[0xFF; 12]);
    (lo, hi)
}

pub fn decode_version_key(k: &[u8]) -> Result<(Vec<u8>, Timestamp)> {
    if k.first() != Some(&VERSION) {
        return Err(Error::Codec("not a version key".into()));
    }
    let (user, rest) = unescape(&k[1..])?;
    if rest.len() != Timestamp::ENCODED_LEN {
        return Err(Error::Codec("bad version timestamp".into()));
    }
    let inv: Vec<u8> = rest.iter().map(|b| !b).collect();
    let ts = Timestamp::decode(&inv).map_err(|e| Error::Codec(e.to_string()))?;
    Ok((user, ts))
}

pub fn intent_key(key: &[u8]) -> Vec<u8> {
    prefixed(INTENT, key)
}

pub fn decode_intent_key(k: &[u8]) -> Result<Vec<u8>> {
    if k.first() != Some(&INTENT) {
        return Err(Error::Codec("not an intent key".into()));
    }
    Ok(unescape(&k[1..])?.0)
}

pub fn txn_key(anchor: &[u8], id: u128) -> Vec<u8> {
    let mut k = prefixed(TXN, anchor);
    k.extend_from_slice(&id.to_be_bytes());
    k
}

/// Engine-key bounds `[lo, hi)` of user span `[start, end)` under `prefix`.
pub fn span(prefix: u8, start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let lo = prefixed(prefix, start);
    let hi = if end.is_empty() {
        vec![prefix + 1]
    } else {
        prefixed(prefix, end)
    };
    (lo, hi)
}

/// A committed version's payload: a value or a deletion.
pub fn encode_value(v: Option<&[u8]>) -> Vec<u8> {
    match v {
        Some(v) => {
            let mut out = Vec::with_capacity(v.len() + 1);
            out.push(1);
            out.extend_from_slice(v);
            out
        }
        None => vec![0],
    }
}

pub fn decode_value(b: &[u8]) -> Option<Vec<u8>> {
    match b.first() {
        Some(1) => Some(b[1..].to_vec()),
        _ => None,
    }
}

/// Who owns an intent, and what a pusher needs to find its transaction record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnMeta {
    pub id: u128,
    pub anchor: Vec<u8>,
    pub epoch: u32,
    /// Provisional commit timestamp at the time of the write.
    pub write_ts: Timestamp,
    /// Wall time of the transaction's start (HLC wall nanos): a transaction whose
    /// record never appeared is presumed abandoned after a timeout.
    pub start_wall: u64,
    /// Random priority; the higher one wins a write-write conflict.
    pub priority: u32,
}

pub fn put_ts(w: &mut W, ts: Timestamp) {
    w.u64(ts.wall).u32(ts.logical);
}

pub fn get_ts(r: &mut R<'_>) -> Result<Timestamp> {
    Ok(Timestamp::new(r.u64()?, r.u32()?))
}

impl TxnMeta {
    pub fn encode(&self, w: &mut W) {
        w.u64((self.id >> 64) as u64)
            .u64(self.id as u64)
            .bytes(&self.anchor)
            .u32(self.epoch);
        put_ts(w, self.write_ts);
        w.u64(self.start_wall).u32(self.priority);
    }
    pub fn decode(r: &mut R<'_>) -> Result<Self> {
        let hi = r.u64()?;
        let lo = r.u64()?;
        Ok(Self {
            id: (u128::from(hi) << 64) | u128::from(lo),
            anchor: r.bytes()?,
            epoch: r.u32()?,
            write_ts: get_ts(r)?,
            start_wall: r.u64()?,
            priority: r.u32()?,
        })
    }
}

/// An uncommitted write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Intent {
    pub txn: TxnMeta,
    pub value: Option<Vec<u8>>,
}

impl Intent {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = W::new();
        self.txn.encode(&mut w);
        w.opt_bytes(self.value.as_deref());
        w.finish()
    }
    pub fn decode(b: &[u8]) -> Result<Self> {
        let mut r = R(b);
        let txn = TxnMeta::decode(&mut r)?;
        let value = r.opt_bytes()?;
        r.done()?;
        Ok(Self { txn, value })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    Pending,
    Committed,
    Aborted,
}

/// The authoritative state of a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnRecord {
    pub status: TxnStatus,
    /// Commit timestamp once committed.
    pub commit_ts: Timestamp,
    /// Readers that found this transaction's intents pushed it: it may not commit
    /// below this timestamp.
    pub min_commit_ts: Timestamp,
    /// Wall nanos of the coordinator's last heartbeat.
    pub heartbeat: u64,
}

impl TxnRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = W::new();
        w.u8(match self.status {
            TxnStatus::Pending => 0,
            TxnStatus::Committed => 1,
            TxnStatus::Aborted => 2,
        });
        put_ts(&mut w, self.commit_ts);
        put_ts(&mut w, self.min_commit_ts);
        w.u64(self.heartbeat);
        w.finish()
    }
    pub fn decode(b: &[u8]) -> Result<Self> {
        let mut r = R(b);
        let status = match r.u8()? {
            0 => TxnStatus::Pending,
            1 => TxnStatus::Committed,
            _ => TxnStatus::Aborted,
        };
        let rec = Self {
            status,
            commit_ts: get_ts(&mut r)?,
            min_commit_ts: get_ts(&mut r)?,
            heartbeat: r.u64()?,
        };
        r.done()?;
        Ok(rec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn versions_sort_newest_first_and_stay_with_their_key() {
        let a1 = version_key(b"a", Timestamp::new(10, 0));
        let a2 = version_key(b"a", Timestamp::new(20, 0));
        let ab = version_key(b"a\x00b", Timestamp::new(5, 0));
        let b1 = version_key(b"b", Timestamp::new(1, 0));
        assert!(a2 < a1, "newer version first");
        assert!(a1 < ab && ab < b1, "versions do not interleave across keys");
        assert_eq!(
            decode_version_key(&a2).unwrap(),
            (b"a".to_vec(), Timestamp::new(20, 0))
        );
        let (lo, hi) = versions_at_or_below(b"a", Timestamp::new(15, 0));
        assert!(a2 < lo && lo <= a1 && a1 <= hi && hi < ab);
    }

    #[test]
    fn spans_cover_exactly_their_keys() {
        let (lo, hi) = span(VERSION, b"b", b"d");
        for (k, inside) in [
            (&b"a"[..], false),
            (b"b", true),
            (b"c\x00", true),
            (b"cz", true),
            (b"d", false),
        ] {
            let vk = version_key(k, Timestamp::new(7, 1));
            assert_eq!(vk >= lo && vk < hi, inside, "{k:?}");
        }
        let (lo, hi) = span(INTENT, b"", b"");
        assert!(intent_key(b"\xff\xff") >= lo && intent_key(b"\xff\xff") < hi);
        assert!(version_key(b"x", Timestamp::MIN) < lo || version_key(b"x", Timestamp::MIN) >= hi);
    }

    proptest! {
        #[test]
        fn escape_preserves_order(a in proptest::collection::vec(any::<u8>(), 0..8), b in proptest::collection::vec(any::<u8>(), 0..8)) {
            let (mut ea, mut eb) = (Vec::new(), Vec::new());
            escape(&a, &mut ea);
            escape(&b, &mut eb);
            prop_assert_eq!(a.cmp(&b), ea.cmp(&eb));
            prop_assert_eq!(unescape(&ea).unwrap().0, a);
        }
    }
}
