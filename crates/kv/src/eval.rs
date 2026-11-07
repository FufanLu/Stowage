//! Evaluating MVCC reads and applying MVCC commands against the storage engine.
//!
//! Everything here is a pure function of (engine state, request/command): replicas
//! that apply the same log produce the same writes and the same results.

use std::collections::BTreeMap;
use std::ops::Bound;

use stowage_common::Timestamp;
use stowage_storage::{BTree, Engine as _, WriteBatch};

use crate::msg::Response;
use crate::mvcc::{
    self, INTENT, Intent, RANGE_PREFIXES, TxnMeta, TxnRecord, TxnStatus, VERSION, decode_value,
    encode_value, intent_key, txn_key, version_key, versions_at_or_below,
};
use crate::types::{PushKind, RangeDescriptor};
use crate::{Error, Result};

/// A transaction whose coordinator has not heartbeated for this long (wall
/// nanoseconds) may be aborted by anyone it blocks.
pub const TXN_TIMEOUT_NANOS: u64 = 2_000_000_000;

fn err(e: impl std::fmt::Display) -> Error {
    Error::Storage(e.to_string())
}

/// Every engine pair belonging to `desc` (all MVCC prefixes).
pub fn range_pairs(engine: &BTree, desc: &RangeDescriptor) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    for p in RANGE_PREFIXES {
        let (lo, hi) = mvcc::span(p, &desc.start, &desc.end);
        for kv in engine.iter(Bound::Included(&lo), Bound::Excluded(&hi)) {
            out.push(kv.map_err(err)?);
        }
    }
    Ok(out)
}

pub fn range_bytes(engine: &BTree, desc: &RangeDescriptor) -> Result<u64> {
    Ok(range_pairs(engine, desc)?
        .iter()
        .map(|(k, v)| (k.len() + v.len()) as u64)
        .sum())
}

/// Distinct user keys holding versions in `desc` (for picking a split point).
pub fn user_keys(engine: &BTree, desc: &RangeDescriptor) -> Result<Vec<Vec<u8>>> {
    let (lo, hi) = mvcc::span(VERSION, &desc.start, &desc.end);
    let mut out: Vec<Vec<u8>> = Vec::new();
    for kv in engine.iter(Bound::Included(&lo), Bound::Excluded(&hi)) {
        let (k, _) = kv.map_err(err)?;
        let (user, _) = mvcc::decode_version_key(&k)?;
        if out.last() != Some(&user) {
            out.push(user);
        }
    }
    Ok(out)
}

fn intent(engine: &BTree, key: &[u8]) -> Result<Option<Intent>> {
    engine
        .get(&intent_key(key))
        .map_err(err)?
        .map(|b| Intent::decode(&b))
        .transpose()
}

/// Newest committed version of `key` at or below `ts`: `(ts, value)`.
fn latest_version(
    engine: &BTree,
    key: &[u8],
    ts: Timestamp,
) -> Result<Option<(Timestamp, Option<Vec<u8>>)>> {
    let (lo, hi) = versions_at_or_below(key, ts);
    match engine
        .scan(Bound::Included(&lo), Bound::Included(&hi), 1)
        .map_err(err)?
        .pop()
    {
        Some((k, v)) => Ok(Some((mvcc::decode_version_key(&k)?.1, decode_value(&v)))),
        None => Ok(None),
    }
}

/// Snapshot read of one key at `ts`.
pub fn get(
    engine: &BTree,
    key: &[u8],
    ts: Timestamp,
    txn: Option<u128>,
    skip: &[u128],
) -> Result<Response> {
    if let Some(i) = intent(engine, key)? {
        if Some(i.txn.id) == txn {
            return Ok(Response::Value(i.value)); // read your own write
        }
        if i.txn.write_ts <= ts && !skip.contains(&i.txn.id) {
            return Ok(Response::Conflict(i.txn));
        }
    }
    Ok(Response::Value(
        latest_version(engine, key, ts)?.and_then(|(_, v)| v),
    ))
}

/// Snapshot read of `[start, end)` at `ts`, limited to `desc`, up to `limit` live keys.
#[allow(clippy::too_many_arguments)]
pub fn scan(
    engine: &BTree,
    desc: &RangeDescriptor,
    start: &[u8],
    end: &[u8],
    limit: usize,
    ts: Timestamp,
    txn: Option<u128>,
    skip: &[u128],
) -> Result<Response> {
    let hi_user: &[u8] = if desc.end.is_empty() || (!end.is_empty() && end < desc.end.as_slice()) {
        end
    } else {
        &desc.end
    };
    // Intents first: another transaction's intent at or below `ts` blocks the scan;
    // our own intents override committed versions.
    let mut own: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
    let (ilo, ihi) = mvcc::span(INTENT, start, hi_user);
    for kv in engine.iter(Bound::Included(&ilo), Bound::Excluded(&ihi)) {
        let (k, v) = kv.map_err(err)?;
        let i = Intent::decode(&v)?;
        if Some(i.txn.id) == txn {
            own.insert(mvcc::decode_intent_key(&k)?, i.value);
        } else if i.txn.write_ts <= ts && !skip.contains(&i.txn.id) {
            return Ok(Response::ConflictAt {
                txn: i.txn,
                key: mvcc::decode_intent_key(&k)?,
            });
        }
    }
    let mut rows: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
    let (lo, hi) = mvcc::span(VERSION, start, hi_user);
    let mut current: Option<Vec<u8>> = None;
    let mut live = 0usize;
    for kv in engine.iter(Bound::Included(&lo), Bound::Excluded(&hi)) {
        let (k, v) = kv.map_err(err)?;
        let (user, vts) = mvcc::decode_version_key(&k)?;
        if current.as_ref() == Some(&user) || vts > ts {
            continue; // an older version of a key we have, or a version from the future
        }
        current = Some(user.clone());
        let val = decode_value(&v);
        if val.is_some() {
            live += 1;
        }
        rows.insert(user, val);
        if live >= limit + own.len() {
            break;
        }
    }
    for (k, v) in own {
        rows.insert(k, v);
    }
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = rows
        .into_iter()
        .filter_map(|(k, v)| v.map(|v| (k, v)))
        .take(limit)
        .collect();
    Ok(Response::Rows {
        pairs,
        range_end: desc.end.clone(),
    })
}

/// SSI read refresh: may reads made at `from` be moved to `to`? Not if a version
/// was committed in `(from, to]`, or another transaction holds an intent at or
/// below `to`.
pub fn refresh(
    engine: &BTree,
    start: &[u8],
    end: &[u8],
    from: Timestamp,
    to: Timestamp,
    txn: u128,
) -> Result<bool> {
    let (lo, hi) = mvcc::span(VERSION, start, end);
    for kv in engine.iter(Bound::Included(&lo), Bound::Excluded(&hi)) {
        let (k, _) = kv.map_err(err)?;
        let (_, ts) = mvcc::decode_version_key(&k)?;
        if ts > from && ts <= to {
            return Ok(false);
        }
    }
    let (ilo, ihi) = mvcc::span(INTENT, start, end);
    for kv in engine.iter(Bound::Included(&ilo), Bound::Excluded(&ihi)) {
        let (_, v) = kv.map_err(err)?;
        let i = Intent::decode(&v)?;
        if i.txn.id != txn && i.txn.write_ts <= to {
            return Ok(false);
        }
    }
    Ok(true)
}

/// A changefeed window: every version committed in `(from, to]` in
/// `[start, end)`, up to about `limit` (always whole keys). An intent at or below
/// `to` of a transaction not in `skip` blocks it: the caller must push or resolve
/// that transaction first, or the window could miss a commit at or below `to`.
pub fn changes(
    engine: &BTree,
    start: &[u8],
    end: &[u8],
    from: Timestamp,
    to: Timestamp,
    limit: usize,
    skip: &[u128],
) -> Result<Response> {
    let (ilo, ihi) = mvcc::span(INTENT, start, end);
    for kv in engine.iter(Bound::Included(&ilo), Bound::Excluded(&ihi)) {
        let (k, v) = kv.map_err(err)?;
        let i = Intent::decode(&v)?;
        if i.txn.write_ts <= to && !skip.contains(&i.txn.id) {
            return Ok(Response::ConflictAt {
                txn: i.txn,
                key: mvcc::decode_intent_key(&k)?,
            });
        }
    }
    let (lo, hi) = mvcc::span(VERSION, start, end);
    let mut events = Vec::new();
    let mut last: Option<Vec<u8>> = None;
    for kv in engine.iter(Bound::Included(&lo), Bound::Excluded(&hi)) {
        let (k, v) = kv.map_err(err)?;
        let (user, ts) = mvcc::decode_version_key(&k)?;
        if last.as_ref() != Some(&user) {
            if events.len() >= limit {
                // Stop between keys, so a key's versions never straddle two pages.
                return Ok(Response::Changes {
                    events,
                    resume: user,
                });
            }
            last = Some(user.clone());
        }
        if ts > from && ts <= to {
            events.push(crate::msg::Change {
                key: user,
                ts,
                value: decode_value(&v),
            });
        }
    }
    Ok(Response::Changes {
        events,
        resume: Vec::new(),
    })
}

fn record(engine: &BTree, txn: &TxnMeta) -> Result<Option<TxnRecord>> {
    engine
        .get(&txn_key(&txn.anchor, txn.id))
        .map_err(err)?
        .map(|b| TxnRecord::decode(&b))
        .transpose()
}

fn txn_response(r: &TxnRecord) -> Response {
    Response::Txn {
        status: r.status,
        commit_ts: r.commit_ts,
        min_commit_ts: r.min_commit_ts,
    }
}

/// The record of a transaction that never wrote one: pending since its start.
fn implicit_record(txn: &TxnMeta) -> TxnRecord {
    TxnRecord {
        status: TxnStatus::Pending,
        commit_ts: Timestamp::MIN,
        min_commit_ts: Timestamp::MIN,
        heartbeat: txn.start_wall,
    }
}

/// `Put`: returns the engine writes and the reply.
pub fn put(
    engine: &BTree,
    key: &[u8],
    value: Option<&[u8]>,
    ts: Timestamp,
    txn: Option<&TxnMeta>,
    b: &mut WriteBatch,
) -> Result<Response> {
    let own = match intent(engine, key)? {
        Some(i) if Some(i.txn.id) != txn.map(|t| t.id) => return Ok(Response::Conflict(i.txn)),
        other => other,
    };
    let newest = latest_version(engine, key, Timestamp::MAX)?.map(|(t, _)| t);
    match txn {
        None => {
            // Non-transactional: commit at once, above any existing version.
            let at = match newest {
                Some(n) if n >= ts => n.next(),
                _ => ts,
            };
            b.put(version_key(key, at), encode_value(value));
            Ok(Response::Written(at))
        }
        Some(t) => {
            if let Some(i) = own {
                // Rewriting our own intent (a later write, or a resent duplicate):
                // keep its timestamp. Readers at or above it already push us; moving
                // it up would hide it from a reader between the two timestamps
                // while the transaction may still commit below that reader.
                let mut meta = t.clone();
                meta.write_ts = i.txn.write_ts;
                b.put(
                    intent_key(key),
                    Intent {
                        txn: meta,
                        value: value.map(<[u8]>::to_vec),
                    }
                    .encode(),
                );
                return Ok(Response::Written(i.txn.write_ts.max(t.write_ts)));
            }
            if let Some(n) = newest
                && n >= ts
            {
                return Ok(Response::WriteTooOld(n));
            }
            let mut meta = t.clone();
            meta.write_ts = ts;
            b.put(
                intent_key(key),
                Intent {
                    txn: meta,
                    value: value.map(<[u8]>::to_vec),
                }
                .encode(),
            );
            Ok(Response::Written(ts))
        }
    }
}

pub fn end_txn(
    engine: &BTree,
    txn: &TxnMeta,
    commit: bool,
    commit_ts: Timestamp,
    now_wall: u64,
    b: &mut WriteBatch,
) -> Result<Response> {
    let mut rec = record(engine, txn)?.unwrap_or_else(|| implicit_record(txn));
    match rec.status {
        TxnStatus::Committed | TxnStatus::Aborted => return Ok(txn_response(&rec)),
        TxnStatus::Pending => {}
    }
    if commit {
        if commit_ts < rec.min_commit_ts {
            // A reader pushed us: refresh reads to min_commit_ts and try again.
            return Ok(txn_response(&rec));
        }
        rec.status = TxnStatus::Committed;
        rec.commit_ts = commit_ts;
    } else {
        rec.status = TxnStatus::Aborted;
    }
    rec.heartbeat = now_wall;
    b.put(txn_key(&txn.anchor, txn.id), rec.encode());
    Ok(txn_response(&rec))
}

pub fn heartbeat(
    engine: &BTree,
    txn: &TxnMeta,
    now_wall: u64,
    b: &mut WriteBatch,
) -> Result<Response> {
    let mut rec = record(engine, txn)?.unwrap_or_else(|| implicit_record(txn));
    if rec.status == TxnStatus::Pending {
        rec.heartbeat = rec.heartbeat.max(now_wall);
        b.put(txn_key(&txn.anchor, txn.id), rec.encode());
    }
    Ok(txn_response(&rec))
}

pub fn push(
    engine: &BTree,
    pushee: &TxnMeta,
    kind: PushKind,
    now_wall: u64,
    b: &mut WriteBatch,
) -> Result<Response> {
    let mut rec = record(engine, pushee)?.unwrap_or_else(|| implicit_record(pushee));
    if rec.status != TxnStatus::Pending {
        return Ok(txn_response(&rec));
    }
    let abandoned = now_wall.saturating_sub(rec.heartbeat) > TXN_TIMEOUT_NANOS;
    match kind {
        PushKind::Abort { priority } if abandoned || priority > pushee.priority => {
            rec.status = TxnStatus::Aborted;
        }
        PushKind::Abort { .. } => return Ok(txn_response(&rec)), // pusher must wait
        PushKind::Timestamp { .. } if abandoned => rec.status = TxnStatus::Aborted,
        PushKind::Timestamp { ts, priority } => {
            if rec.min_commit_ts > ts {
                return Ok(txn_response(&rec)); // already above the reader
            }
            if priority <= pushee.priority {
                return Ok(txn_response(&rec)); // reader must wait
            }
            rec.min_commit_ts = ts.next();
        }
    }
    b.put(txn_key(&pushee.anchor, pushee.id), rec.encode());
    Ok(txn_response(&rec))
}

pub fn resolve(
    engine: &BTree,
    key: &[u8],
    txn_id: u128,
    commit: bool,
    commit_ts: Timestamp,
    b: &mut WriteBatch,
) -> Result<Response> {
    if let Some(i) = intent(engine, key)?
        && i.txn.id == txn_id
    {
        if commit {
            b.put(
                version_key(key, commit_ts),
                encode_value(i.value.as_deref()),
            );
        }
        b.delete(intent_key(key));
    }
    Ok(Response::Done)
}

/// The timestamp cache: the highest timestamp at which each key (or span) was
/// read on this leader. A write below it would change what that read saw, so it
/// is moved above. Old entries collapse into `low_water`.
#[derive(Debug, Default)]
pub struct TsCache {
    low_water: Timestamp,
    points: BTreeMap<Vec<u8>, Timestamp>,
    spans: Vec<(Vec<u8>, Vec<u8>, Timestamp)>,
}

const TSCACHE_POINTS: usize = 100_000;
const TSCACHE_SPANS: usize = 4_096;

impl TsCache {
    /// Forget everything; nothing may be written at or below `ts`. A new leader
    /// does this, since it does not know which reads the previous leader served.
    pub fn reset(&mut self, ts: Timestamp) {
        self.points.clear();
        self.spans.clear();
        self.low_water = self.low_water.max(ts);
    }

    pub fn record_point(&mut self, key: &[u8], ts: Timestamp) {
        let e = self.points.entry(key.to_vec()).or_insert(Timestamp::MIN);
        *e = (*e).max(ts);
        if self.points.len() > TSCACHE_POINTS {
            let max = self.points.values().copied().max().unwrap_or(ts);
            self.points.clear();
            self.low_water = self.low_water.max(max);
        }
    }

    pub fn record_span(&mut self, start: &[u8], end: &[u8], ts: Timestamp) {
        self.spans.push((start.to_vec(), end.to_vec(), ts));
        if self.spans.len() > TSCACHE_SPANS {
            let (_, _, old) = self.spans.remove(0);
            self.low_water = self.low_water.max(old);
        }
    }

    /// Highest read timestamp covering `key`.
    pub fn max_read(&self, key: &[u8]) -> Timestamp {
        let mut m = self
            .low_water
            .max(self.points.get(key).copied().unwrap_or(Timestamp::MIN));
        for (s, e, ts) in &self.spans {
            if key >= s.as_slice() && (e.is_empty() || key < e.as_slice()) {
                m = m.max(*ts);
            }
        }
        m
    }
}
