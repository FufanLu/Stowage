//! Messages between stores, and the client request/response protocol.

use std::collections::BTreeSet;

use stowage_common::NodeId;
use stowage_raft::Message;
use stowage_raft::codec::{decode_message, encode_message};

use stowage_common::Timestamp;

use crate::codec::{R, W};
use crate::mvcc::{TxnMeta, TxnStatus, get_ts, put_ts};
use crate::types::{PushKind, RangeId, RangeInfo};
use crate::{Error, Result};

/// Store-to-store traffic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerMsg {
    /// A Raft message for one range's group, with the sender's HLC time. Every
    /// receiver folds `ts` into its clock, so a newly elected leader's clock is at
    /// least as far as any timestamp its quorum saw from the previous leader.
    ///
    /// `generation` is the sender's descriptor generation for the range. A
    /// node that was removed from the range keeps a tombstone with the
    /// generation that removed it, and ignores messages from older ones: a
    /// replica created late by a split it applied long after the fact
    /// (a zombie with a stale membership) cannot recruit it as a voter.
    Raft {
        range: RangeId,
        msg: Message,
        ts: Timestamp,
        generation: u64,
    },
    /// Periodic gossip: every replica this store holds. Gives every node a
    /// routing table and the scheduler its view of the cluster.
    Heartbeat {
        node: NodeId,
        ranges: Vec<RangeInfo>,
    },
    /// Scheduler → range leader: change the range's voters to exactly this set.
    ChangeReplicas {
        range: RangeId,
        generation: u64,
        voters: BTreeSet<NodeId>,
    },
    /// Scheduler → range leader: hand leadership to `to`.
    TransferLeader { range: RangeId, to: NodeId },
    /// Leader → removed replica: you are no longer part of `range` as of `generation`.
    Gc { range: RangeId, generation: u64 },
}

impl PeerMsg {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = W::new();
        match self {
            PeerMsg::Raft {
                range,
                msg,
                ts,
                generation,
            } => {
                w.u8(1).u64(*range).bytes(&encode_message(msg));
                put_ts(&mut w, *ts);
                w.u64(*generation);
            }
            PeerMsg::Heartbeat { node, ranges } => {
                w.u8(2).u64(*node).u32(ranges.len() as u32);
                for r in ranges {
                    r.encode(&mut w);
                }
            }
            PeerMsg::ChangeReplicas {
                range,
                generation,
                voters,
            } => {
                w.u8(3)
                    .u64(*range)
                    .u64(*generation)
                    .u32(voters.len() as u32);
                for v in voters {
                    w.u64(*v);
                }
            }
            PeerMsg::TransferLeader { range, to } => {
                w.u8(4).u64(*range).u64(*to);
            }
            PeerMsg::Gc { range, generation } => {
                w.u8(5).u64(*range).u64(*generation);
            }
        }
        w.finish()
    }

    pub fn decode(b: &[u8]) -> Result<Self> {
        let mut r = R(b);
        let m = match r.u8()? {
            1 => {
                let range = r.u64()?;
                let msg = decode_message(&r.bytes()?).map_err(|e| Error::Codec(e.to_string()))?;
                PeerMsg::Raft {
                    range,
                    msg,
                    ts: get_ts(&mut r)?,
                    generation: r.u64()?,
                }
            }
            2 => {
                let node = r.u64()?;
                let n = r.u32()?;
                let ranges = (0..n)
                    .map(|_| RangeInfo::decode(&mut r))
                    .collect::<Result<_>>()?;
                PeerMsg::Heartbeat { node, ranges }
            }
            3 => {
                let (range, generation) = (r.u64()?, r.u64()?);
                let n = r.u32()?;
                let voters = (0..n).map(|_| r.u64()).collect::<Result<_>>()?;
                PeerMsg::ChangeReplicas {
                    range,
                    generation,
                    voters,
                }
            }
            4 => PeerMsg::TransferLeader {
                range: r.u64()?,
                to: r.u64()?,
            },
            5 => PeerMsg::Gc {
                range: r.u64()?,
                generation: r.u64()?,
            },
            k => return Err(Error::Codec(format!("peer message kind {k}"))),
        };
        r.done()?;
        Ok(m)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Get {
        key: Vec<u8>,
    },
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    /// Pairs in `[start, end)` within one range; the reply says where the range
    /// ends so the client can continue in the next one.
    Scan {
        start: Vec<u8>,
        end: Vec<u8>,
        limit: u32,
    },
    /// Which range holds `key`, and who leads it (from the routing table).
    Locate {
        key: Vec<u8>,
    },
    /// Every range this node knows about.
    Ranges,
    /// Admin: split the range containing `key` at `key`.
    Split {
        key: Vec<u8>,
    },
    /// Admin: this node's replicas, with their Raft state (debugging).
    Status,
    /// Transactional read at `ts`. `txn` sees its own intents; intents of the
    /// transactions in `skip` were pushed above `ts` and are ignored.
    TxnGet {
        key: Vec<u8>,
        ts: Timestamp,
        txn: Option<u128>,
        skip: Vec<u128>,
    },
    TxnScan {
        start: Vec<u8>,
        end: Vec<u8>,
        limit: u32,
        ts: Timestamp,
        txn: Option<u128>,
        skip: Vec<u128>,
    },
    /// Write an intent for `txn` (at `txn.write_ts`, or higher if a read got there first).
    TxnPut {
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        txn: TxnMeta,
    },
    /// A locking read (`SELECT … FOR UPDATE`): take the write intent on `key`
    /// now, holding its current value, and return that value.
    TxnLock {
        key: Vec<u8>,
        txn: TxnMeta,
    },
    /// Routed by `txn.anchor`: commit at `commit_ts`, or abort.
    EndTxn {
        txn: TxnMeta,
        commit: bool,
        commit_ts: Timestamp,
    },
    Resolve {
        key: Vec<u8>,
        txn_id: u128,
        commit: bool,
        commit_ts: Timestamp,
    },
    /// Routed by `pushee.anchor`.
    Push {
        pushee: TxnMeta,
        kind: PushKind,
    },
    Heartbeat {
        txn: TxnMeta,
    },
    /// Fails if `[start, end)` gained a committed version in `(from, to]`, or holds
    /// another transaction's intent at or below `to` (SSI read refresh).
    Refresh {
        start: Vec<u8>,
        end: Vec<u8>,
        from: Timestamp,
        to: Timestamp,
        txn: u128,
    },
    /// Every version committed in `(from, to]` in `[start, end)` (a changefeed
    /// window). Like a read at `to`: once answered, nothing can commit at or below
    /// `to` there any more, so `to` is a closed timestamp. Intents of the
    /// transactions in `skip` were pushed above `to`.
    Changes {
        start: Vec<u8>,
        end: Vec<u8>,
        from: Timestamp,
        to: Timestamp,
        limit: u32,
        skip: Vec<u128>,
    },
}

/// One committed write, as a changefeed reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub key: Vec<u8>,
    pub ts: Timestamp,
    /// `None`: deleted.
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Value(Option<Vec<u8>>),
    Done,
    Rows {
        pairs: Vec<(Vec<u8>, Vec<u8>)>,
        range_end: Vec<u8>,
    },
    Location(RangeInfo),
    Ranges(Vec<RangeInfo>),
    Status(Vec<String>),
    /// A transactional write landed at this timestamp.
    Written(Timestamp),
    /// Another transaction's intent is in the way.
    Conflict(TxnMeta),
    /// A newer committed version exists at this timestamp; write above it.
    WriteTooOld(Timestamp),
    /// A locking read took the intent at `ts`; `value` is the key's value.
    Locked {
        ts: Timestamp,
        value: Option<Vec<u8>>,
    },
    /// The state of a transaction record (reply to EndTxn, Push, Heartbeat).
    Txn {
        status: TxnStatus,
        commit_ts: Timestamp,
        min_commit_ts: Timestamp,
    },
    Refreshed(bool),
    /// A scan ran into another transaction's intent on `key`.
    ConflictAt {
        txn: TxnMeta,
        key: Vec<u8>,
    },
    /// A refresh covered its span only up to this range's end: `ok` for that part;
    /// refresh again from `resume`.
    RefreshedPart {
        ok: bool,
        resume: Vec<u8>,
    },
    /// Changes in the requested window; continue from `resume` unless it is empty.
    Changes {
        events: Vec<Change>,
        resume: Vec<u8>,
    },
    Error(KvError),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KvError {
    #[error("not the leader of range {range} (leader hint: {leader:?})")]
    NotLeader {
        range: RangeId,
        leader: Option<NodeId>,
    },
    #[error("no local replica for the key (routing hint: {hint:?})")]
    RangeNotFound { hint: Option<NodeId> },
    #[error("key moved to another range (split); retry")]
    KeyMoved,
    #[error("busy: {0}")]
    Busy(String),
    /// The write was proposed, but this node lost leadership (or the replica) before
    /// it applied. It may or may not commit later, so retrying could apply it twice.
    #[error("ambiguous result: the write may or may not have been applied")]
    Ambiguous,
}

impl Request {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = W::new();
        match self {
            Request::Get { key } => w.u8(1).bytes(key),
            Request::Put { key, value } => w.u8(2).bytes(key).bytes(value),
            Request::Delete { key } => w.u8(3).bytes(key),
            Request::Scan { start, end, limit } => w.u8(4).bytes(start).bytes(end).u32(*limit),
            Request::Locate { key } => w.u8(5).bytes(key),
            Request::Ranges => w.u8(6),
            Request::Split { key } => w.u8(7).bytes(key),
            Request::Status => w.u8(8),
            Request::TxnGet { key, ts, txn, skip } => {
                w.u8(9).bytes(key);
                put_ts(&mut w, *ts);
                put_txn_id(&mut w, *txn);
                put_ids(&mut w, skip);
                &mut w
            }
            Request::TxnScan {
                start,
                end,
                limit,
                ts,
                txn,
                skip,
            } => {
                w.u8(10).bytes(start).bytes(end).u32(*limit);
                put_ts(&mut w, *ts);
                put_txn_id(&mut w, *txn);
                put_ids(&mut w, skip);
                &mut w
            }
            Request::TxnPut { key, value, txn } => {
                w.u8(11).bytes(key).opt_bytes(value.as_deref());
                txn.encode(&mut w);
                &mut w
            }
            Request::TxnLock { key, txn } => {
                w.u8(18).bytes(key);
                txn.encode(&mut w);
                &mut w
            }
            Request::EndTxn {
                txn,
                commit,
                commit_ts,
            } => {
                w.u8(12);
                txn.encode(&mut w);
                w.u8(u8::from(*commit));
                put_ts(&mut w, *commit_ts);
                &mut w
            }
            Request::Resolve {
                key,
                txn_id,
                commit,
                commit_ts,
            } => {
                w.u8(13).bytes(key);
                put_txn_id(&mut w, Some(*txn_id));
                w.u8(u8::from(*commit));
                put_ts(&mut w, *commit_ts);
                &mut w
            }
            Request::Push { pushee, kind } => {
                w.u8(14);
                pushee.encode(&mut w);
                match kind {
                    PushKind::Timestamp { ts, priority } => {
                        w.u8(0);
                        put_ts(&mut w, *ts);
                        w.u32(*priority);
                    }
                    PushKind::Abort { priority } => {
                        w.u8(1).u32(*priority);
                    }
                }
                &mut w
            }
            Request::Heartbeat { txn } => {
                w.u8(15);
                txn.encode(&mut w);
                &mut w
            }
            Request::Refresh {
                start,
                end,
                from,
                to,
                txn,
            } => {
                w.u8(16).bytes(start).bytes(end);
                put_ts(&mut w, *from);
                put_ts(&mut w, *to);
                put_txn_id(&mut w, Some(*txn));
                &mut w
            }
            Request::Changes {
                start,
                end,
                from,
                to,
                limit,
                skip,
            } => {
                w.u8(17).bytes(start).bytes(end);
                put_ts(&mut w, *from);
                put_ts(&mut w, *to);
                w.u32(*limit).u32(skip.len() as u32);
                for s in skip {
                    put_txn_id(&mut w, Some(*s));
                }
                &mut w
            }
        };
        w.finish()
    }

    pub fn decode(b: &[u8]) -> Result<Self> {
        let mut r = R(b);
        let q = match r.u8()? {
            1 => Request::Get { key: r.bytes()? },
            2 => Request::Put {
                key: r.bytes()?,
                value: r.bytes()?,
            },
            3 => Request::Delete { key: r.bytes()? },
            4 => Request::Scan {
                start: r.bytes()?,
                end: r.bytes()?,
                limit: r.u32()?,
            },
            5 => Request::Locate { key: r.bytes()? },
            6 => Request::Ranges,
            7 => Request::Split { key: r.bytes()? },
            8 => Request::Status,
            9 => Request::TxnGet {
                key: r.bytes()?,
                ts: get_ts(&mut r)?,
                txn: get_txn_id(&mut r)?,
                skip: get_ids(&mut r)?,
            },
            10 => Request::TxnScan {
                start: r.bytes()?,
                end: r.bytes()?,
                limit: r.u32()?,
                ts: get_ts(&mut r)?,
                txn: get_txn_id(&mut r)?,
                skip: get_ids(&mut r)?,
            },
            11 => Request::TxnPut {
                key: r.bytes()?,
                value: r.opt_bytes()?,
                txn: TxnMeta::decode(&mut r)?,
            },
            18 => Request::TxnLock {
                key: r.bytes()?,
                txn: TxnMeta::decode(&mut r)?,
            },
            12 => Request::EndTxn {
                txn: TxnMeta::decode(&mut r)?,
                commit: r.u8()? == 1,
                commit_ts: get_ts(&mut r)?,
            },
            13 => Request::Resolve {
                key: r.bytes()?,
                txn_id: get_txn_id(&mut r)?.unwrap_or(0),
                commit: r.u8()? == 1,
                commit_ts: get_ts(&mut r)?,
            },
            14 => {
                let pushee = TxnMeta::decode(&mut r)?;
                let kind = match r.u8()? {
                    0 => PushKind::Timestamp {
                        ts: get_ts(&mut r)?,
                        priority: r.u32()?,
                    },
                    _ => PushKind::Abort { priority: r.u32()? },
                };
                Request::Push { pushee, kind }
            }
            15 => Request::Heartbeat {
                txn: TxnMeta::decode(&mut r)?,
            },
            16 => Request::Refresh {
                start: r.bytes()?,
                end: r.bytes()?,
                from: get_ts(&mut r)?,
                to: get_ts(&mut r)?,
                txn: get_txn_id(&mut r)?.unwrap_or(0),
            },
            17 => {
                let (start, end, from, to, limit) = (
                    r.bytes()?,
                    r.bytes()?,
                    get_ts(&mut r)?,
                    get_ts(&mut r)?,
                    r.u32()?,
                );
                let n = r.u32()?;
                let skip = (0..n)
                    .map(|_| get_txn_id(&mut r).map(|x| x.unwrap_or(0)))
                    .collect::<Result<_>>()?;
                Request::Changes {
                    start,
                    end,
                    from,
                    to,
                    limit,
                    skip,
                }
            }
            k => return Err(Error::Codec(format!("request kind {k}"))),
        };
        r.done()?;
        Ok(q)
    }

    /// The key that decides which range serves this request.
    pub fn routing_key(&self) -> Option<&[u8]> {
        match self {
            Request::Get { key }
            | Request::Put { key, .. }
            | Request::Delete { key }
            | Request::Locate { key }
            | Request::Split { key } => Some(key),
            Request::Scan { start, .. }
            | Request::TxnScan { start, .. }
            | Request::Refresh { start, .. }
            | Request::Changes { start, .. } => Some(start),
            Request::TxnGet { key, .. }
            | Request::TxnPut { key, .. }
            | Request::TxnLock { key, .. }
            | Request::Resolve { key, .. } => Some(key),
            Request::EndTxn { txn, .. } | Request::Heartbeat { txn } => Some(&txn.anchor),
            Request::Push { pushee, .. } => Some(&pushee.anchor),
            Request::Ranges | Request::Status => None,
        }
    }
}

impl Response {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = W::new();
        match self {
            Response::Value(v) => {
                w.u8(1).opt_bytes(v.as_deref());
            }
            Response::Done => {
                w.u8(2);
            }
            Response::Rows { pairs, range_end } => {
                w.u8(3).u32(pairs.len() as u32);
                for (k, v) in pairs {
                    w.bytes(k).bytes(v);
                }
                w.bytes(range_end);
            }
            Response::Location(info) => {
                w.u8(4);
                info.encode(&mut w);
            }
            Response::Ranges(rs) => {
                w.u8(5).u32(rs.len() as u32);
                for r in rs {
                    r.encode(&mut w);
                }
            }
            Response::Status(lines) => {
                w.u8(7).u32(lines.len() as u32);
                for l in lines {
                    w.bytes(l.as_bytes());
                }
            }
            Response::Written(ts) => {
                w.u8(8);
                put_ts(&mut w, *ts);
            }
            Response::Conflict(t) => {
                w.u8(9);
                t.encode(&mut w);
            }
            Response::WriteTooOld(ts) => {
                w.u8(10);
                put_ts(&mut w, *ts);
            }
            Response::Locked { ts, value } => {
                w.u8(16);
                put_ts(&mut w, *ts);
                w.opt_bytes(value.as_deref());
            }
            Response::Txn {
                status,
                commit_ts,
                min_commit_ts,
            } => {
                w.u8(11).u8(match status {
                    TxnStatus::Pending => 0,
                    TxnStatus::Committed => 1,
                    TxnStatus::Aborted => 2,
                });
                put_ts(&mut w, *commit_ts);
                put_ts(&mut w, *min_commit_ts);
            }
            Response::Refreshed(ok) => {
                w.u8(12).u8(u8::from(*ok));
            }
            Response::ConflictAt { txn, key } => {
                w.u8(13);
                txn.encode(&mut w);
                w.bytes(key);
            }
            Response::Changes { events, resume } => {
                w.u8(15).u32(events.len() as u32);
                for e in events {
                    w.bytes(&e.key);
                    put_ts(&mut w, e.ts);
                    w.opt_bytes(e.value.as_deref());
                }
                w.bytes(resume);
            }
            Response::RefreshedPart { ok, resume } => {
                w.u8(14).u8(u8::from(*ok)).bytes(resume);
            }
            Response::Error(e) => {
                w.u8(6);
                match e {
                    KvError::NotLeader { range, leader } => {
                        w.u8(1).u64(*range).u64(leader.unwrap_or(0))
                    }
                    KvError::RangeNotFound { hint } => w.u8(2).u64(hint.unwrap_or(0)),
                    KvError::KeyMoved => w.u8(3),
                    KvError::Busy(s) => w.u8(4).bytes(s.as_bytes()),
                    KvError::Ambiguous => w.u8(5),
                };
            }
        }
        w.finish()
    }

    pub fn decode(b: &[u8]) -> Result<Self> {
        let mut r = R(b);
        let opt = |x: u64| (x != 0).then_some(x);
        let resp = match r.u8()? {
            1 => Response::Value(r.opt_bytes()?),
            2 => Response::Done,
            3 => {
                let n = r.u32()?;
                let mut pairs = Vec::with_capacity(usize::try_from(n.min(65_536)).unwrap_or(0));
                for _ in 0..n {
                    pairs.push((r.bytes()?, r.bytes()?));
                }
                Response::Rows {
                    pairs,
                    range_end: r.bytes()?,
                }
            }
            4 => Response::Location(RangeInfo::decode(&mut r)?),
            5 => {
                let n = r.u32()?;
                Response::Ranges(
                    (0..n)
                        .map(|_| RangeInfo::decode(&mut r))
                        .collect::<Result<_>>()?,
                )
            }
            7 => {
                let n = r.u32()?;
                Response::Status(
                    (0..n)
                        .map(|_| r.bytes().map(|b| String::from_utf8_lossy(&b).into_owned()))
                        .collect::<Result<_>>()?,
                )
            }
            8 => Response::Written(get_ts(&mut r)?),
            9 => Response::Conflict(TxnMeta::decode(&mut r)?),
            10 => Response::WriteTooOld(get_ts(&mut r)?),
            16 => Response::Locked {
                ts: get_ts(&mut r)?,
                value: r.opt_bytes()?,
            },
            11 => {
                let status = match r.u8()? {
                    0 => TxnStatus::Pending,
                    1 => TxnStatus::Committed,
                    _ => TxnStatus::Aborted,
                };
                Response::Txn {
                    status,
                    commit_ts: get_ts(&mut r)?,
                    min_commit_ts: get_ts(&mut r)?,
                }
            }
            12 => Response::Refreshed(r.u8()? == 1),
            13 => Response::ConflictAt {
                txn: TxnMeta::decode(&mut r)?,
                key: r.bytes()?,
            },
            15 => {
                let n = r.u32()?;
                let mut events = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    events.push(Change {
                        key: r.bytes()?,
                        ts: get_ts(&mut r)?,
                        value: r.opt_bytes()?,
                    });
                }
                Response::Changes {
                    events,
                    resume: r.bytes()?,
                }
            }
            14 => Response::RefreshedPart {
                ok: r.u8()? == 1,
                resume: r.bytes()?,
            },
            6 => Response::Error(match r.u8()? {
                1 => KvError::NotLeader {
                    range: r.u64()?,
                    leader: opt(r.u64()?),
                },
                2 => KvError::RangeNotFound {
                    hint: opt(r.u64()?),
                },
                3 => KvError::KeyMoved,
                5 => KvError::Ambiguous,
                _ => KvError::Busy(String::from_utf8_lossy(&r.bytes()?).into_owned()),
            }),
            k => return Err(Error::Codec(format!("response kind {k}"))),
        };
        r.done()?;
        Ok(resp)
    }
}

fn put_txn_id(w: &mut W, id: Option<u128>) {
    match id {
        None => {
            w.u8(0);
        }
        Some(id) => {
            w.u8(1).u64((id >> 64) as u64).u64(id as u64);
        }
    }
}

fn get_txn_id(r: &mut R<'_>) -> Result<Option<u128>> {
    Ok(match r.u8()? {
        0 => None,
        _ => {
            let hi = r.u64()?;
            let lo = r.u64()?;
            Some((u128::from(hi) << 64) | u128::from(lo))
        }
    })
}

fn put_ids(w: &mut W, ids: &[u128]) {
    w.u32(ids.len() as u32);
    for id in ids {
        w.u64((id >> 64) as u64).u64(*id as u64);
    }
}

fn get_ids(r: &mut R<'_>) -> Result<Vec<u128>> {
    let n = r.u32()?;
    (0..n.min(1024))
        .map(|_| {
            let hi = r.u64()?;
            let lo = r.u64()?;
            Ok((u128::from(hi) << 64) | u128::from(lo))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RangeDescriptor;
    use stowage_raft::Body;

    #[test]
    fn transactional_messages_roundtrip() {
        use crate::mvcc::{TxnMeta, TxnStatus};
        let ts = Timestamp::new(1_793_923_202_015_000_000, 7);
        let txn = TxnMeta {
            id: (5u128 << 96) | 0xdead_beef,
            anchor: b"alice".to_vec(),
            epoch: 3,
            write_ts: ts,
            start_wall: 99,
            priority: 1 << 21,
        };
        for q in [
            Request::TxnGet {
                key: b"k".to_vec(),
                ts,
                txn: Some(txn.id),
                skip: vec![1, 2 << 100],
            },
            Request::TxnGet {
                key: b"k".to_vec(),
                ts,
                txn: None,
                skip: vec![],
            },
            Request::TxnScan {
                start: b"a".to_vec(),
                end: b"z".to_vec(),
                limit: 9,
                ts,
                txn: Some(3),
                skip: vec![4],
            },
            Request::TxnPut {
                key: b"k".to_vec(),
                value: Some(b"v".to_vec()),
                txn: txn.clone(),
            },
            Request::TxnPut {
                key: b"k".to_vec(),
                value: None,
                txn: txn.clone(),
            },
            Request::TxnLock {
                key: b"k".to_vec(),
                txn: txn.clone(),
            },
            Request::EndTxn {
                txn: txn.clone(),
                commit: true,
                commit_ts: ts.next(),
            },
            Request::EndTxn {
                txn: txn.clone(),
                commit: false,
                commit_ts: ts,
            },
            Request::Resolve {
                key: b"k".to_vec(),
                txn_id: txn.id,
                commit: true,
                commit_ts: ts,
            },
            Request::Push {
                pushee: txn.clone(),
                kind: PushKind::Timestamp { ts, priority: 8 },
            },
            Request::Push {
                pushee: txn.clone(),
                kind: PushKind::Abort { priority: 8 },
            },
            Request::Heartbeat { txn: txn.clone() },
            Request::Changes {
                start: b"a".to_vec(),
                end: vec![],
                from: ts,
                to: ts.next(),
                limit: 100,
                skip: vec![1, 2 << 100],
            },
            Request::Refresh {
                start: b"a".to_vec(),
                end: b"a\0".to_vec(),
                from: ts,
                to: ts.next(),
                txn: txn.id,
            },
        ] {
            assert_eq!(Request::decode(&q.encode()).unwrap(), q);
        }
        for p in [
            Response::Written(ts),
            Response::WriteTooOld(ts),
            Response::Locked {
                ts,
                value: Some(b"v".to_vec()),
            },
            Response::Locked { ts, value: None },
            Response::Conflict(txn.clone()),
            Response::Txn {
                status: TxnStatus::Committed,
                commit_ts: ts,
                min_commit_ts: ts.next(),
            },
            Response::Txn {
                status: TxnStatus::Pending,
                commit_ts: Timestamp::MIN,
                min_commit_ts: ts,
            },
            Response::Txn {
                status: TxnStatus::Aborted,
                commit_ts: Timestamp::MIN,
                min_commit_ts: Timestamp::MIN,
            },
            Response::Refreshed(true),
            Response::Refreshed(false),
            Response::ConflictAt {
                txn: txn.clone(),
                key: b"k".to_vec(),
            },
            Response::RefreshedPart {
                ok: true,
                resume: b"m".to_vec(),
            },
            Response::Changes {
                events: vec![
                    Change {
                        key: b"a".to_vec(),
                        ts,
                        value: Some(b"1".to_vec()),
                    },
                    Change {
                        key: b"b".to_vec(),
                        ts: ts.next(),
                        value: None,
                    },
                ],
                resume: b"c".to_vec(),
            },
            Response::Value(Some(vec![0, 1, 2])),
        ] {
            assert_eq!(Response::decode(&p.encode()).unwrap(), p);
        }
    }

    #[test]
    fn roundtrips() {
        let info = RangeInfo {
            desc: RangeDescriptor {
                id: 3,
                start: b"a".to_vec(),
                end: vec![],
                replicas: [1, 2].into(),
                generation: 2,
            },
            leader: Some(2),
            term: 7,
            approx_bytes: 100,
        };
        for m in [
            PeerMsg::Raft {
                range: 3,
                msg: Message {
                    from: 1,
                    to: 2,
                    term: 3,
                    body: Body::TimeoutNow,
                },
                ts: Timestamp::new(5, 1),
                generation: 9,
            },
            PeerMsg::Heartbeat {
                node: 1,
                ranges: vec![info.clone()],
            },
            PeerMsg::ChangeReplicas {
                range: 3,
                generation: 2,
                voters: [1, 4].into(),
            },
            PeerMsg::TransferLeader { range: 3, to: 4 },
            PeerMsg::Gc {
                range: 3,
                generation: 5,
            },
        ] {
            assert_eq!(PeerMsg::decode(&m.encode()).unwrap(), m);
        }
        for q in [
            Request::Get { key: b"k".to_vec() },
            Request::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
            Request::Scan {
                start: b"a".to_vec(),
                end: vec![],
                limit: 10,
            },
            Request::Ranges,
        ] {
            assert_eq!(Request::decode(&q.encode()).unwrap(), q);
        }
        for p in [
            Response::Value(None),
            Response::Rows {
                pairs: vec![(b"a".to_vec(), b"1".to_vec())],
                range_end: b"m".to_vec(),
            },
            Response::Location(info),
            Response::Error(KvError::NotLeader {
                range: 3,
                leader: None,
            }),
            Response::Error(KvError::Busy("x".into())),
        ] {
            assert_eq!(Response::decode(&p.encode()).unwrap(), p);
        }
    }
}
