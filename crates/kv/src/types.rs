//! Ranges, commands, and how they are laid out in the storage engine.

use std::collections::BTreeSet;

use stowage_common::{NodeId, Timestamp};

use crate::codec::{R, W};
use crate::mvcc::{TxnMeta, get_ts, put_ts};
use crate::{Error, Result};

pub type RangeId = u64;

/// A contiguous span `[start, end)` of the user key space and the nodes that
/// replicate it. An empty `end` means "to the end of the key space".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeDescriptor {
    pub id: RangeId,
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub replicas: BTreeSet<NodeId>,
    /// Bumped by every split and every membership change. Between two
    /// descriptors of overlapping spans, the higher generation is the truth.
    pub generation: u64,
}

impl RangeDescriptor {
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && (self.end.is_empty() || key < self.end.as_slice())
    }

    pub fn overlaps(&self, other: &RangeDescriptor) -> bool {
        let a_before_b = !self.end.is_empty() && self.end.as_slice() <= other.start.as_slice();
        let b_before_a = !other.end.is_empty() && other.end.as_slice() <= self.start.as_slice();
        !a_before_b && !b_before_a
    }

    pub fn encode(&self, w: &mut W) {
        w.u64(self.id)
            .bytes(&self.start)
            .bytes(&self.end)
            .u64(self.generation);
        w.u32(self.replicas.len() as u32);
        for r in &self.replicas {
            w.u64(*r);
        }
    }

    pub fn decode(r: &mut R<'_>) -> Result<Self> {
        let (id, start, end, generation) = (r.u64()?, r.bytes()?, r.bytes()?, r.u64()?);
        let n = r.u32()?;
        let replicas = (0..n).map(|_| r.u64()).collect::<Result<_>>()?;
        Ok(Self {
            id,
            start,
            end,
            replicas,
            generation,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut w = W::new();
        self.encode(&mut w);
        w.finish()
    }

    pub fn from_bytes(b: &[u8]) -> Result<Self> {
        let mut r = R(b);
        let d = Self::decode(&mut r)?;
        r.done()?;
        Ok(d)
    }
}

impl std::fmt::Display for RangeDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let end = if self.end.is_empty() {
            "∞".to_owned()
        } else {
            format!("{:?}", String::from_utf8_lossy(&self.end))
        };
        write!(
            f,
            "r{} [{:?}, {end}) gen {} on {:?}",
            self.id,
            String::from_utf8_lossy(&self.start),
            self.generation,
            self.replicas
        )
    }
}

/// What a node knows about a range: its descriptor, who leads it, how big it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeInfo {
    pub desc: RangeDescriptor,
    pub leader: Option<NodeId>,
    pub term: u64,
    pub approx_bytes: u64,
}

impl RangeInfo {
    pub fn encode(&self, w: &mut W) {
        self.desc.encode(w);
        w.u64(self.leader.unwrap_or(0))
            .u64(self.term)
            .u64(self.approx_bytes);
    }
    pub fn decode(r: &mut R<'_>) -> Result<Self> {
        let desc = RangeDescriptor::decode(r)?;
        let leader = r.u64()?;
        Ok(Self {
            desc,
            leader: (leader != 0).then_some(leader),
            term: r.u64()?,
            approx_bytes: r.u64()?,
        })
    }
}

/// Identifies a client request a node proposed, so that only the proposing node
/// answers when the entry is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProposalId {
    pub node: NodeId,
    pub seq: u64,
}

/// How a transaction record is pushed by a conflicting reader or writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushKind {
    /// A reader at `ts` with `priority` ran into the intent: if the owner is
    /// abandoned it is aborted; if the reader's priority is higher the owner may
    /// no longer commit at or below `ts` (so the read can ignore the intent);
    /// otherwise the reader waits.
    Timestamp { ts: Timestamp, priority: u32 },
    /// A writer with `priority` ran into the intent: abort the owner if it is
    /// abandoned or has a lower priority.
    Abort { priority: u32 },
}

/// A command in a range's Raft log. Timestamps and wall-clock values are chosen by
/// the proposing leader and carried in the command, so applying it is a pure
/// function of the command and the engine state: every replica gets the same result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Write `value` (`None` deletes) at `ts`. Without `txn` it commits at once
    /// (at `ts`, or just above the newest version); with `txn` it lays down an
    /// intent owned by that transaction.
    Put {
        pid: ProposalId,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        ts: Timestamp,
        txn: Option<TxnMeta>,
    },
    /// A locking read for `txn`: lay down an intent holding the newest
    /// committed value (unchanged) at `ts` and return that value.
    Lock {
        pid: ProposalId,
        key: Vec<u8>,
        ts: Timestamp,
        txn: TxnMeta,
    },
    /// Commit (at `commit_ts`) or abort a transaction: update its record.
    EndTxn {
        pid: ProposalId,
        txn: TxnMeta,
        commit: bool,
        commit_ts: Timestamp,
        now_wall: u64,
    },
    /// Turn `txn_id`'s intent on `key` into a committed version, or remove it.
    Resolve {
        pid: ProposalId,
        key: Vec<u8>,
        txn_id: u128,
        commit: bool,
        commit_ts: Timestamp,
    },
    /// Push the owner of a conflicting intent.
    Push {
        pid: ProposalId,
        pushee: TxnMeta,
        kind: PushKind,
        now_wall: u64,
    },
    /// The coordinator is alive.
    Heartbeat {
        pid: ProposalId,
        txn: TxnMeta,
        now_wall: u64,
    },
    /// Split at `key`; the right half becomes range `new_id`. Applies only if the
    /// range is still at `generation` (otherwise it was already split or changed).
    Split {
        pid: ProposalId,
        key: Vec<u8>,
        new_id: RangeId,
        generation: u64,
    },
}

impl Command {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = W::new();
        let pid = self.pid();
        match self {
            Command::Put {
                key,
                value,
                ts,
                txn,
                ..
            } => {
                w.u8(1)
                    .u64(pid.node)
                    .u64(pid.seq)
                    .bytes(key)
                    .opt_bytes(value.as_deref());
                put_ts(&mut w, *ts);
                match txn {
                    None => w.u8(0),
                    Some(t) => {
                        w.u8(1);
                        t.encode(&mut w);
                        &mut w
                    }
                };
            }
            Command::EndTxn {
                txn,
                commit,
                commit_ts,
                now_wall,
                ..
            } => {
                w.u8(3).u64(pid.node).u64(pid.seq);
                txn.encode(&mut w);
                w.u8(u8::from(*commit));
                put_ts(&mut w, *commit_ts);
                w.u64(*now_wall);
            }
            Command::Resolve {
                key,
                txn_id,
                commit,
                commit_ts,
                ..
            } => {
                w.u8(4)
                    .u64(pid.node)
                    .u64(pid.seq)
                    .bytes(key)
                    .u64((txn_id >> 64) as u64)
                    .u64(*txn_id as u64);
                w.u8(u8::from(*commit));
                put_ts(&mut w, *commit_ts);
            }
            Command::Push {
                pushee,
                kind,
                now_wall,
                ..
            } => {
                w.u8(5).u64(pid.node).u64(pid.seq);
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
                w.u64(*now_wall);
            }
            Command::Heartbeat { txn, now_wall, .. } => {
                w.u8(6).u64(pid.node).u64(pid.seq);
                txn.encode(&mut w);
                w.u64(*now_wall);
            }
            Command::Lock { key, ts, txn, .. } => {
                w.u8(7).u64(pid.node).u64(pid.seq).bytes(key);
                put_ts(&mut w, *ts);
                txn.encode(&mut w);
            }
            Command::Split {
                pid,
                key,
                new_id,
                generation,
            } => {
                w.u8(2)
                    .u64(pid.node)
                    .u64(pid.seq)
                    .bytes(key)
                    .u64(*new_id)
                    .u64(*generation);
            }
        }
        w.finish()
    }

    pub fn decode(b: &[u8]) -> Result<Self> {
        let mut r = R(b);
        let kind = r.u8()?;
        let pid = ProposalId {
            node: r.u64()?,
            seq: r.u64()?,
        };
        let c = match kind {
            1 => {
                let key = r.bytes()?;
                let value = r.opt_bytes()?;
                let ts = get_ts(&mut r)?;
                let txn = match r.u8()? {
                    0 => None,
                    _ => Some(TxnMeta::decode(&mut r)?),
                };
                Command::Put {
                    pid,
                    key,
                    value,
                    ts,
                    txn,
                }
            }
            3 => {
                let txn = TxnMeta::decode(&mut r)?;
                let commit = r.u8()? == 1;
                Command::EndTxn {
                    pid,
                    txn,
                    commit,
                    commit_ts: get_ts(&mut r)?,
                    now_wall: r.u64()?,
                }
            }
            4 => {
                let key = r.bytes()?;
                let hi = r.u64()?;
                let lo = r.u64()?;
                let commit = r.u8()? == 1;
                Command::Resolve {
                    pid,
                    key,
                    txn_id: (u128::from(hi) << 64) | u128::from(lo),
                    commit,
                    commit_ts: get_ts(&mut r)?,
                }
            }
            5 => {
                let pushee = TxnMeta::decode(&mut r)?;
                let kind = match r.u8()? {
                    0 => PushKind::Timestamp {
                        ts: get_ts(&mut r)?,
                        priority: r.u32()?,
                    },
                    _ => PushKind::Abort { priority: r.u32()? },
                };
                Command::Push {
                    pid,
                    pushee,
                    kind,
                    now_wall: r.u64()?,
                }
            }
            6 => {
                let txn = TxnMeta::decode(&mut r)?;
                Command::Heartbeat {
                    pid,
                    txn,
                    now_wall: r.u64()?,
                }
            }
            7 => Command::Lock {
                pid,
                key: r.bytes()?,
                ts: get_ts(&mut r)?,
                txn: TxnMeta::decode(&mut r)?,
            },
            2 => Command::Split {
                pid,
                key: r.bytes()?,
                new_id: r.u64()?,
                generation: r.u64()?,
            },
            k => return Err(Error::Codec(format!("command kind {k}"))),
        };
        r.done()?;
        Ok(c)
    }

    pub fn pid(&self) -> ProposalId {
        match self {
            Command::Put { pid, .. }
            | Command::EndTxn { pid, .. }
            | Command::Resolve { pid, .. }
            | Command::Push { pid, .. }
            | Command::Heartbeat { pid, .. }
            | Command::Lock { pid, .. }
            | Command::Split { pid, .. } => *pid,
        }
    }
}

/// Payload of a range snapshot: its descriptor and every user pair in its span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotData {
    pub desc: RangeDescriptor,
    pub pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

impl SnapshotData {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = W::new();
        self.desc.encode(&mut w);
        w.u32(self.pairs.len() as u32);
        for (k, v) in &self.pairs {
            w.bytes(k).bytes(v);
        }
        w.finish()
    }
    pub fn decode(b: &[u8]) -> Result<Self> {
        let mut r = R(b);
        let desc = RangeDescriptor::decode(&mut r)?;
        let n = r.u32()?;
        let mut pairs = Vec::with_capacity(usize::try_from(n.min(65_536)).unwrap_or(0));
        for _ in 0..n {
            pairs.push((r.bytes()?, r.bytes()?));
        }
        r.done()?;
        Ok(Self { desc, pairs })
    }
}

/// Node-local metadata keys. User data lives under the [`crate::mvcc`] prefixes
/// (`'d'` versions, `'i'` intents, `'x'` transaction records); ranges partition
/// those by user key, so a split moves no data.
///
/// ```text
///  'l' ‖ "desc"    ‖ range_id (BE)     a local replica's descriptor
///  'l' ‖ "applied" ‖ range_id (BE)     its applied Raft index
///  'l' ‖ "tomb"    ‖ range_id (BE)     generation that removed this node (u64 LE)
///  'l' ‖ "rangeseq"                    this node's range-id allocator
///  'l' ‖ "incarnation"                 bumped at every open; prefixes proposal ids
/// ```
pub mod keys {
    fn local(kind: &str, id: u64) -> Vec<u8> {
        let mut k = vec![b'l'];
        k.extend_from_slice(kind.as_bytes());
        k.extend_from_slice(&id.to_be_bytes());
        k
    }
    pub fn desc(id: u64) -> Vec<u8> {
        local("desc", id)
    }
    pub fn applied(id: u64) -> Vec<u8> {
        local("applied", id)
    }
    /// Written when a replica that was a member of the range is destroyed: the
    /// generation that removed this node. Raft messages from older
    /// generations are ignored (see `PeerMsg::Raft`).
    pub fn tombstone(id: u64) -> Vec<u8> {
        local("tomb", id)
    }
    pub fn desc_prefix() -> Vec<u8> {
        b"ldesc".to_vec()
    }
    pub const RANGE_SEQ: &[u8] = b"lrangeseq";
    pub const INCARNATION: &[u8] = b"lincarnation";
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(start: &str, end: &str) -> RangeDescriptor {
        RangeDescriptor {
            id: 1,
            start: start.into(),
            end: end.into(),
            replicas: [1, 2, 3].into(),
            generation: 4,
        }
    }

    #[test]
    fn spans() {
        assert!(d("", "").contains(b"anything"));
        assert!(d("b", "d").contains(b"b") && d("b", "d").contains(b"c"));
        assert!(!d("b", "d").contains(b"d") && !d("b", "d").contains(b"a"));
        assert!(d("b", "d").overlaps(&d("c", "")));
        assert!(!d("b", "d").overlaps(&d("d", "f")));
        assert!(d("", "").overlaps(&d("x", "y")));
    }

    #[test]
    fn codecs_roundtrip() {
        let desc = d("a", "m");
        assert_eq!(RangeDescriptor::from_bytes(&desc.to_bytes()).unwrap(), desc);
        let pid = ProposalId { node: 2, seq: 9 };
        for c in [
            Command::Put {
                pid,
                key: b"k".to_vec(),
                value: Some(b"v".to_vec()),
                ts: Timestamp::new(9, 1),
                txn: Some(TxnMeta {
                    id: 7,
                    anchor: b"a".to_vec(),
                    epoch: 0,
                    write_ts: Timestamp::new(9, 1),
                    start_wall: 3,
                    priority: 5,
                }),
            },
            Command::Lock {
                pid,
                key: b"k".to_vec(),
                ts: Timestamp::new(9, 2),
                txn: TxnMeta {
                    id: 8,
                    anchor: b"k".to_vec(),
                    epoch: 1,
                    write_ts: Timestamp::new(9, 2),
                    start_wall: 4,
                    priority: 6,
                },
            },
            Command::Push {
                pid,
                pushee: TxnMeta {
                    id: 1,
                    anchor: vec![],
                    epoch: 0,
                    write_ts: Timestamp::MIN,
                    start_wall: 0,
                    priority: 0,
                },
                kind: PushKind::Abort { priority: 9 },
                now_wall: 4,
            },
            Command::Split {
                pid,
                key: b"m".to_vec(),
                new_id: 77,
                generation: 4,
            },
        ] {
            assert_eq!(Command::decode(&c.encode()).unwrap(), c);
        }
        let s = SnapshotData {
            desc,
            pairs: vec![(b"a".to_vec(), b"1".to_vec())],
        };
        assert_eq!(SnapshotData::decode(&s.encode()).unwrap(), s);
    }
}
