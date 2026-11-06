//! Binary encodings for durable Raft state (little-endian, CRC-framed log).

use std::collections::BTreeSet;

use crate::types::{Entry, EntryKind, HardState, Membership, NodeId, Snapshot, SnapshotMeta};
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

    proptest! {
        #[test]
        fn decoders_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..200)) {
            let _ = decode_entry(&bytes);
            let _ = decode_hard_state(&bytes);
            let _ = decode_snapshot(&bytes);
            let _ = decode_frame(&bytes);
        }
    }
}
