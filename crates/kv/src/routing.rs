//! Every node's view of where ranges live, assembled from store heartbeats.
//!
//! This is soft state, rebuilt within one heartbeat interval after a restart.
//! When two descriptors cover overlapping spans, the higher generation wins,
//! because splits and membership changes always bump it.

use std::collections::BTreeMap;

use stowage_common::NodeId;

use crate::types::{RangeId, RangeInfo};

#[derive(Debug, Default)]
pub struct Routing {
    ranges: BTreeMap<RangeId, RangeInfo>,
    /// Tick of the last heartbeat from each store.
    seen: BTreeMap<NodeId, u64>,
}

impl Routing {
    pub fn store_seen(&mut self, node: NodeId, now: u64) {
        self.seen.insert(node, now);
    }

    /// Stores heard from within `timeout` ticks.
    pub fn live_stores(&self, now: u64, timeout: u64) -> Vec<NodeId> {
        self.seen
            .iter()
            .filter(|&(_, &t)| now.saturating_sub(t) <= timeout)
            .map(|(&n, _)| n)
            .collect()
    }

    pub fn update(&mut self, info: RangeInfo) {
        let id = info.desc.id;
        if let Some(cur) = self.ranges.get(&id) {
            let newer = (info.desc.generation, info.term) >= (cur.desc.generation, cur.term);
            if !newer {
                return;
            }
            // Same generation and term from a follower may lack the leader hint.
            if info.desc.generation == cur.desc.generation
                && info.term == cur.term
                && info.leader.is_none()
            {
                let mut merged = info;
                merged.leader = cur.leader;
                merged.approx_bytes = merged.approx_bytes.max(cur.approx_bytes);
                self.ranges.insert(id, merged);
                return;
            }
        }
        // Drop older descriptors that this one supersedes.
        let stale: Vec<RangeId> = self
            .ranges
            .values()
            .filter(|o| {
                o.desc.id != id
                    && o.desc.overlaps(&info.desc)
                    && o.desc.generation < info.desc.generation
            })
            .map(|o| o.desc.id)
            .collect();
        for s in stale {
            self.ranges.remove(&s);
        }
        let superseded = self.ranges.values().any(|o| {
            o.desc.id != id
                && o.desc.overlaps(&info.desc)
                && o.desc.generation > info.desc.generation
        });
        if !superseded {
            self.ranges.insert(id, info);
        }
    }

    pub fn forget(&mut self, id: RangeId) {
        self.ranges.remove(&id);
    }

    /// The range covering `key` with the highest generation.
    pub fn lookup(&self, key: &[u8]) -> Option<&RangeInfo> {
        self.ranges
            .values()
            .filter(|r| r.desc.contains(key))
            .max_by_key(|r| (r.desc.generation, r.term))
    }

    pub fn get(&self, id: RangeId) -> Option<&RangeInfo> {
        self.ranges.get(&id)
    }

    /// All known ranges, in key order.
    pub fn all(&self) -> Vec<RangeInfo> {
        let mut v: Vec<RangeInfo> = self.ranges.values().cloned().collect();
        v.sort_by(|a, b| a.desc.start.cmp(&b.desc.start));
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RangeDescriptor;

    fn info(id: u64, start: &str, end: &str, generation: u64, leader: Option<u64>) -> RangeInfo {
        RangeInfo {
            desc: RangeDescriptor {
                id,
                start: start.into(),
                end: end.into(),
                replicas: [1, 2, 3].into(),
                generation,
            },
            leader,
            term: 1,
            approx_bytes: 0,
        }
    }

    #[test]
    fn split_supersedes_parent() {
        let mut r = Routing::default();
        r.update(info(1, "", "", 1, Some(1)));
        assert_eq!(r.lookup(b"q").unwrap().desc.id, 1);
        // After the split: r1 = ["", "m"), r9 = ["m", ∞), both generation 2.
        r.update(info(9, "m", "", 2, Some(2)));
        assert_eq!(
            r.lookup(b"q").unwrap().desc.id,
            9,
            "the newer right half wins"
        );
        r.update(info(1, "", "m", 2, Some(1)));
        assert_eq!(r.lookup(b"a").unwrap().desc.id, 1);
        assert_eq!(r.all().len(), 2);
        // A late heartbeat with the old descriptor must not resurrect it.
        r.update(info(1, "", "", 1, Some(3)));
        assert_eq!(r.lookup(b"q").unwrap().desc.id, 9);
        assert_eq!(r.get(1).unwrap().desc.end, b"m");
    }

    #[test]
    fn follower_heartbeat_keeps_leader_hint() {
        let mut r = Routing::default();
        r.update(info(1, "", "", 1, Some(2)));
        r.update(info(1, "", "", 1, None));
        assert_eq!(r.lookup(b"x").unwrap().leader, Some(2));
    }
}
