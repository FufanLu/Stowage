//! Ranges, commands, and how they are laid out in the storage engine.

use std::collections::BTreeSet;

use stowage_common::NodeId;

use crate::codec::{R, W};
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

/// A command in a range's Raft log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Atomic batch of puts (`Some`) and deletes (`None`) within one range.
    Write {
        pid: ProposalId,
        ops: Vec<(Vec<u8>, Option<Vec<u8>>)>,
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
        match self {
            Command::Write { pid, ops } => {
                w.u8(1).u64(pid.node).u64(pid.seq).u32(ops.len() as u32);
                for (k, v) in ops {
                    w.bytes(k).opt_bytes(v.as_deref());
                }
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
                let n = r.u32()?;
                let mut ops = Vec::with_capacity(usize::try_from(n.min(4096)).unwrap_or(0));
                for _ in 0..n {
                    ops.push((r.bytes()?, r.opt_bytes()?));
                }
                Command::Write { pid, ops }
            }
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
            Command::Write { pid, .. } | Command::Split { pid, .. } => *pid,
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

/// Engine key layout. Everything lives in one B+Tree per node:
///
/// ```text
///  'd' ‖ user_key                      user data (any range)
///  'l' ‖ "desc"    ‖ range_id (BE)     a local replica's descriptor
///  'l' ‖ "applied" ‖ range_id (BE)     its applied Raft index
///  'l' ‖ "rangeseq"                    this node's range-id allocator
///  'l' ‖ "incarnation"                 bumped at every open; prefixes proposal ids
/// ```
///
/// Ranges partition only the `'d'` space, so a split moves no data: both halves
/// keep reading the same keys from the same engine.
pub mod keys {
    pub const DATA: u8 = b'd';

    pub fn data(user_key: &[u8]) -> Vec<u8> {
        let mut k = Vec::with_capacity(1 + user_key.len());
        k.push(DATA);
        k.extend_from_slice(user_key);
        k
    }

    pub fn user(engine_key: &[u8]) -> &[u8] {
        &engine_key[1..]
    }

    /// Engine-key bounds of the user span `[start, end)`.
    pub fn span(start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let lo = data(start);
        let hi = if end.is_empty() {
            vec![DATA + 1]
        } else {
            data(end)
        };
        (lo, hi)
    }

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
            Command::Write {
                pid,
                ops: vec![(b"k".to_vec(), Some(b"v".to_vec())), (b"x".to_vec(), None)],
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

    #[test]
    fn data_keys_sort_inside_their_span() {
        let (lo, hi) = keys::span(b"", b"");
        assert!(keys::data(b"") >= lo && keys::data(b"\xff\xff") < hi);
        assert!(keys::desc(1) < lo || keys::desc(1) >= hi);
    }
}
