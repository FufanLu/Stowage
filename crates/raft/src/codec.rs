//! Binary encodings for durable Raft state (little-endian, CRC-framed log).

use std::collections::BTreeSet;

use crate::types::{
    Body, ConflictHint, Entry, EntryKind, HardState, Membership, Message, NodeId, Snapshot,
    SnapshotMeta,
};
use crate::{Error, Result};

/// `len u32 ‖ crc32c(payload) u32 ‖ payload`
pub fn encode_frame(out: &mut Vec<u8>, payload: &[u8]) {
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
    out.extend_from_slice(payload);
}

/// Decode one frame: `(payload, bytes consumed)`, or `None` if short or corrupt.
pub fn decode_frame(data: &[u8]) -> Option<(&[u8], usize)> {
    let h = data.get(..8)?;
    let len = usize::try_from(u32::from_le_bytes([h[0], h[1], h[2], h[3]])).ok()?;
    let crc = u32::from_le_bytes([h[4], h[5], h[6], h[7]]);
    let payload = data.get(8..8 + len)?;
    (crc32c::crc32c(payload) == crc).then_some((payload, 8 + len))
}

#[derive(Debug)]
pub struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.buf.len() < n {
            return Err(Error::Storage("truncated record".into()));
        }
        let (h, t) = self.buf.split_at(n);
        self.buf = t;
        Ok(h)
    }
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    pub fn u64(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_le_bytes(a))
    }
    pub fn bytes(&mut self) -> Result<Vec<u8>> {
        let n = usize::try_from(self.u32()?).map_err(|_| Error::Storage("length".into()))?;
        Ok(self.take(n)?.to_vec())
    }
    fn ids(&mut self) -> Result<BTreeSet<NodeId>> {
        let n = self.u32()?;
        (0..n).map(|_| self.u64()).collect()
    }
    pub fn done(&self) -> Result<()> {
        if self.buf.is_empty() {
            Ok(())
        } else {
            Err(Error::Storage("trailing bytes".into()))
        }
    }
}

fn put_ids(out: &mut Vec<u8>, ids: &BTreeSet<NodeId>) {
    out.extend_from_slice(&(ids.len() as u32).to_le_bytes());
    for id in ids {
        out.extend_from_slice(&id.to_le_bytes());
    }
}

fn put_membership(out: &mut Vec<u8>, m: &Membership) {
    put_ids(out, &m.voters);
    put_ids(out, &m.outgoing);
}

fn get_membership(r: &mut Reader<'_>) -> Result<Membership> {
    Ok(Membership {
        voters: r.ids()?,
        outgoing: r.ids()?,
    })
}

pub fn encode_entry(e: &Entry) -> Vec<u8> {
    let mut out = Vec::with_capacity(e.size());
    out.extend_from_slice(&e.index.to_le_bytes());
    out.extend_from_slice(&e.term.to_le_bytes());
    match &e.kind {
        EntryKind::Noop => out.push(0),
        EntryKind::Normal(d) => {
            out.push(1);
            out.extend_from_slice(&(d.len() as u32).to_le_bytes());
            out.extend_from_slice(d);
        }
        EntryKind::Config(m) => {
            out.push(2);
            put_membership(&mut out, m);
        }
    }
    out
}

pub fn decode_entry(buf: &[u8]) -> Result<Entry> {
    let mut r = Reader::new(buf);
    let index = r.u64()?;
    let term = r.u64()?;
    let kind = match r.u8()? {
        0 => EntryKind::Noop,
        1 => EntryKind::Normal(r.bytes()?),
        2 => EntryKind::Config(get_membership(&mut r)?),
        k => return Err(Error::Storage(format!("entry kind {k}"))),
    };
    r.done()?;
    Ok(Entry { term, index, kind })
}

pub fn encode_hard_state(hs: &HardState) -> Vec<u8> {
    let mut p = Vec::with_capacity(17);
    p.extend_from_slice(&hs.term.to_le_bytes());
    p.push(u8::from(hs.vote.is_some()));
    p.extend_from_slice(&hs.vote.unwrap_or(0).to_le_bytes());
    let mut out = Vec::new();
    encode_frame(&mut out, &p);
    out
}

pub fn decode_hard_state(buf: &[u8]) -> Result<HardState> {
    let (p, _) = decode_frame(buf).ok_or_else(|| Error::Storage("corrupt hard state".into()))?;
    let mut r = Reader::new(p);
    let term = r.u64()?;
    let has = r.u8()? == 1;
    let vote = r.u64()?;
    r.done()?;
    Ok(HardState {
        term,
        vote: has.then_some(vote),
    })
}

pub fn encode_snapshot(s: &Snapshot) -> Vec<u8> {
    let mut p = Vec::with_capacity(32 + s.data.len());
    p.extend_from_slice(&s.meta.index.to_le_bytes());
    p.extend_from_slice(&s.meta.term.to_le_bytes());
    put_membership(&mut p, &s.meta.membership);
    p.extend_from_slice(&(s.data.len() as u32).to_le_bytes());
    p.extend_from_slice(&s.data);
    let mut out = Vec::new();
    encode_frame(&mut out, &p);
    out
}

pub fn decode_snapshot(buf: &[u8]) -> Result<Snapshot> {
    let (p, _) = decode_frame(buf).ok_or_else(|| Error::Storage("corrupt snapshot".into()))?;
    let mut r = Reader::new(p);
    let index = r.u64()?;
    let term = r.u64()?;
    let membership = get_membership(&mut r)?;
    let data = r.bytes()?;
    r.done()?;
    Ok(Snapshot {
        meta: SnapshotMeta {
            index,
            term,
            membership,
        },
        data,
    })
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

/// Wire encoding of a Raft message.
pub fn encode_message(m: &Message) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(&m.from.to_le_bytes());
    out.extend_from_slice(&m.to.to_le_bytes());
    out.extend_from_slice(&m.term.to_le_bytes());
    let u64s = |out: &mut Vec<u8>, xs: &[u64]| {
        for x in xs {
            out.extend_from_slice(&x.to_le_bytes());
        }
    };
    match &m.body {
        Body::PreVote {
            last_index,
            last_term,
        } => {
            out.push(1);
            u64s(&mut out, &[*last_index, *last_term]);
        }
        Body::PreVoteResp { granted } => {
            out.push(2);
            out.push(u8::from(*granted));
        }
        Body::Vote {
            last_index,
            last_term,
            force,
        } => {
            out.push(3);
            u64s(&mut out, &[*last_index, *last_term]);
            out.push(u8::from(*force));
        }
        Body::VoteResp { granted } => {
            out.push(4);
            out.push(u8::from(*granted));
        }
        Body::Append {
            prev_index,
            prev_term,
            entries,
            commit,
        } => {
            out.push(5);
            u64s(&mut out, &[*prev_index, *prev_term, *commit]);
            out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            for e in entries {
                put_bytes(&mut out, &encode_entry(e));
            }
        }
        Body::AppendResp {
            prev_index,
            success,
            match_index,
            hint,
        } => {
            out.push(6);
            u64s(&mut out, &[*prev_index, *match_index]);
            out.push(u8::from(*success));
            match hint {
                None => out.push(0),
                Some(h) => {
                    out.push(1);
                    out.push(u8::from(h.term.is_some()));
                    u64s(&mut out, &[h.term.unwrap_or(0), h.first_index]);
                }
            }
        }
        Body::Heartbeat {
            commit,
            read_ctx,
            round,
        } => {
            out.push(7);
            u64s(&mut out, &[*commit, *read_ctx, *round]);
        }
        Body::HeartbeatResp { read_ctx, round } => {
            out.push(8);
            u64s(&mut out, &[*read_ctx, *round]);
        }
        Body::InstallSnapshot { snapshot } => {
            out.push(9);
            put_bytes(&mut out, &encode_snapshot(snapshot));
        }
        Body::TimeoutNow => out.push(10),
    }
    out
}

pub fn decode_message(buf: &[u8]) -> Result<Message> {
    let mut r = Reader::new(buf);
    let (from, to, term) = (r.u64()?, r.u64()?, r.u64()?);
    let bool8 = |r: &mut Reader<'_>| -> Result<bool> { Ok(r.u8()? != 0) };
    let body = match r.u8()? {
        1 => Body::PreVote {
            last_index: r.u64()?,
            last_term: r.u64()?,
        },
        2 => Body::PreVoteResp {
            granted: bool8(&mut r)?,
        },
        3 => Body::Vote {
            last_index: r.u64()?,
            last_term: r.u64()?,
            force: bool8(&mut r)?,
        },
        4 => Body::VoteResp {
            granted: bool8(&mut r)?,
        },
        5 => {
            let (prev_index, prev_term, commit) = (r.u64()?, r.u64()?, r.u64()?);
            let n = r.u32()?;
            let mut entries = Vec::with_capacity(usize::try_from(n.min(1024)).unwrap_or(0));
            for _ in 0..n {
                entries.push(decode_entry(&r.bytes()?)?);
            }
            Body::Append {
                prev_index,
                prev_term,
                entries,
                commit,
            }
        }
        6 => {
            let (prev_index, match_index) = (r.u64()?, r.u64()?);
            let success = bool8(&mut r)?;
            let hint = match r.u8()? {
                0 => None,
                _ => {
                    let has_term = bool8(&mut r)?;
                    let (t, first_index) = (r.u64()?, r.u64()?);
                    Some(ConflictHint {
                        term: has_term.then_some(t),
                        first_index,
                    })
                }
            };
            Body::AppendResp {
                prev_index,
                success,
                match_index,
                hint,
            }
        }
        7 => Body::Heartbeat {
            commit: r.u64()?,
            read_ctx: r.u64()?,
            round: r.u64()?,
        },
        8 => Body::HeartbeatResp {
            read_ctx: r.u64()?,
            round: r.u64()?,
        },
        9 => Body::InstallSnapshot {
            snapshot: decode_snapshot(&r.bytes()?)?,
        },
        10 => Body::TimeoutNow,
        k => return Err(Error::Storage(format!("message kind {k}"))),
    };
    r.done()?;
    Ok(Message {
        from,
        to,
        term,
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn roundtrips() {
        let m = Membership {
            voters: [1, 2, 4].into(),
            outgoing: [1, 2, 3].into(),
        };
        for e in [
            Entry {
                term: 3,
                index: 9,
                kind: EntryKind::Noop,
            },
            Entry {
                term: 3,
                index: 10,
                kind: EntryKind::Normal(b"put x".to_vec()),
            },
            Entry {
                term: 4,
                index: 11,
                kind: EntryKind::Config(m.clone()),
            },
        ] {
            assert_eq!(decode_entry(&encode_entry(&e)).unwrap(), e);
        }
        let hs = HardState {
            term: 7,
            vote: Some(3),
        };
        assert_eq!(decode_hard_state(&encode_hard_state(&hs)).unwrap(), hs);
        let s = Snapshot {
            meta: SnapshotMeta {
                index: 5,
                term: 2,
                membership: m,
            },
            data: b"kv".to_vec(),
        };
        assert_eq!(decode_snapshot(&encode_snapshot(&s)).unwrap(), s);
    }

    #[test]
    fn messages_roundtrip() {
        let e = Entry {
            term: 2,
            index: 7,
            kind: EntryKind::Normal(b"x".to_vec()),
        };
        let snap = Snapshot {
            meta: SnapshotMeta {
                index: 5,
                term: 5,
                membership: Membership::new([1, 2, 3]),
            },
            data: b"state".to_vec(),
        };
        let bodies = vec![
            Body::PreVote {
                last_index: 3,
                last_term: 1,
            },
            Body::PreVoteResp { granted: true },
            Body::Vote {
                last_index: 3,
                last_term: 1,
                force: true,
            },
            Body::VoteResp { granted: false },
            Body::Append {
                prev_index: 6,
                prev_term: 2,
                entries: vec![e.clone(), e],
                commit: 4,
            },
            Body::AppendResp {
                prev_index: 6,
                success: false,
                match_index: 0,
                hint: Some(ConflictHint {
                    term: Some(1),
                    first_index: 3,
                }),
            },
            Body::AppendResp {
                prev_index: 6,
                success: true,
                match_index: 8,
                hint: None,
            },
            Body::Heartbeat {
                commit: 9,
                read_ctx: 2,
                round: 11,
            },
            Body::HeartbeatResp {
                read_ctx: 2,
                round: 11,
            },
            Body::InstallSnapshot { snapshot: snap },
            Body::TimeoutNow,
        ];
        for body in bodies {
            let m = Message {
                from: 1,
                to: 2,
                term: 9,
                body,
            };
            assert_eq!(decode_message(&encode_message(&m)).unwrap(), m);
        }
    }

    proptest! {
        #[test]
        fn message_decoder_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..300)) {
            let _ = decode_message(&bytes);
        }

        #[test]
        fn decoders_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..200)) {
            let _ = decode_entry(&bytes);
            let _ = decode_hard_state(&bytes);
            let _ = decode_snapshot(&bytes);
            let _ = decode_frame(&bytes);
        }
    }
}
