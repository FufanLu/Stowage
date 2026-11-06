//! Messages between stores, and the client request/response protocol.

use std::collections::BTreeSet;

use stowage_common::NodeId;
use stowage_raft::Message;
use stowage_raft::codec::{decode_message, encode_message};

use crate::codec::{R, W};
use crate::types::{RangeId, RangeInfo};
use crate::{Error, Result};

/// Store-to-store traffic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerMsg {
    /// A Raft message for one range's group.
    Raft { range: RangeId, msg: Message },
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
            PeerMsg::Raft { range, msg } => {
                w.u8(1).u64(*range).bytes(&encode_message(msg));
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
                PeerMsg::Raft { range, msg }
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
            Request::Scan { start, .. } => Some(start),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RangeDescriptor;
    use stowage_raft::Body;

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
