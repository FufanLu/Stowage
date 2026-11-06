//! Hand-scripted scenarios for safety properties that random fault injection
//! rarely reaches. Each one drives a cluster message by message, in the spirit of
//! the scenario tests in etcd/raft. Mutation testing (see
//! `docs/illustrated/03-one-write-through-raft.md`) showed the random simulator
//! alone misses these three bugs; these tests catch all of them.

#![allow(clippy::unwrap_used)] // test code

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use stowage_common::os::Xoshiro256;
use stowage_raft::{
    Body, Config, DiskStorage, EntryKind, Membership, Message, Raft, ReadMode, Role,
};
use stowage_sim::{CrashMode, SimDisk};

struct Cluster {
    cfg: Box<dyn Fn(u64) -> Config>,
    members: Membership,
    nodes: BTreeMap<u64, Raft<DiskStorage>>,
    disks: BTreeMap<u64, Arc<SimDisk>>,
    queue: VecDeque<Message>,
    /// index -> (term, payload) applied by anyone (State Machine Safety).
    applied: BTreeMap<u64, (u64, Vec<u8>)>,
    /// node -> kv state from applying `k=v` commands
    kv: BTreeMap<u64, BTreeMap<String, String>>,
}

impl Cluster {
    fn new(n: u64, cfg: impl Fn(u64) -> Config + 'static) -> Self {
        let members = Membership::new(1..=n);
        let mut c = Self {
            cfg: Box::new(cfg),
            members,
            nodes: BTreeMap::new(),
            disks: BTreeMap::new(),
            queue: VecDeque::new(),
            applied: BTreeMap::new(),
            kv: BTreeMap::new(),
        };
        for id in 1..=n {
            c.disks
                .insert(id, Arc::new(SimDisk::new(CrashMode::DropUnsynced)));
            c.start(id);
        }
        c
    }

    fn start(&mut self, id: u64) {
        let storage = DiskStorage::open(self.disks[&id].clone(), self.members.clone()).unwrap();
        self.nodes
            .insert(id, Raft::new((self.cfg)(id), storage).unwrap());
        self.kv.insert(id, BTreeMap::new());
    }

    fn crash(&mut self, id: u64) {
        self.nodes.remove(&id);
        self.disks[&id].crash(&mut Xoshiro256::seeded(id));
        self.queue.retain(|m| m.to != id && m.from != id);
    }

    fn node(&mut self, id: u64) -> &mut Raft<DiskStorage> {
        self.nodes.get_mut(&id).unwrap()
    }

    /// Move every node's outgoing messages into the queue and apply committed
    /// entries, checking State Machine Safety.
    fn collect(&mut self) {
        for (&id, r) in &mut self.nodes {
            self.queue.extend(r.take_messages());
            let _ = r.take_snapshot();
            for e in r.take_committed() {
                let payload = match &e.kind {
                    EntryKind::Normal(d) => d.clone(),
                    EntryKind::Noop => b"noop".to_vec(),
                    EntryKind::Config(_) => b"config".to_vec(),
                };
                let prev = self
                    .applied
                    .entry(e.index)
                    .or_insert((e.term, payload.clone()));
                assert_eq!(
                    *prev,
                    (e.term, payload.clone()),
                    "node {id} applied a different entry at {}",
                    e.index
                );
                if let EntryKind::Normal(d) = &e.kind {
                    let s = String::from_utf8(d.clone()).unwrap();
                    let (k, v) = s.split_once('=').unwrap();
                    self.kv.get_mut(&id).unwrap().insert(k.into(), v.into());
                }
            }
        }
    }

    /// Deliver queued messages that `keep` accepts; drop the rest. Repeat until
    /// nothing is left.
    fn run(&mut self, keep: impl Fn(&Message) -> bool) {
        self.run_map(|m| keep(&m).then_some(m));
    }

    /// Like [`Cluster::run`], but `f` may also rewrite a message in flight.
    fn run_map(&mut self, f: impl Fn(Message) -> Option<Message>) {
        for _ in 0..1000 {
            self.collect();
            let Some(m) = self.queue.pop_front() else {
                return;
            };
            if let Some(m) = f(m)
                && self.nodes.contains_key(&m.to)
            {
                self.node(m.to).step(m).unwrap();
            }
        }
        panic!("messages did not quiesce");
    }

    /// Force an immediate election at `id` (as a leadership transfer would).
    fn elect(&mut self, id: u64, keep: impl Fn(&Message) -> bool) {
        let term = self.node(id).term();
        self.node(id)
            .step(Message {
                from: 0,
                to: id,
                term,
                body: Body::TimeoutNow,
            })
            .unwrap();
        self.run(keep);
    }

    /// Campaign until `id` wins: a restarted node's term may be stale, so its first
    /// attempt can lose and only teach it the current term.
    fn elect_until_leader(&mut self, id: u64, keep: impl Fn(&Message) -> bool + Copy) {
        for _ in 0..4 {
            self.elect(id, keep);
            if self.node(id).role() == Role::Leader {
                return;
            }
            if std::env::var_os("SCENARIO_DEBUG").is_some() {
                for (nid, r) in &self.nodes {
                    eprintln!(
                        "  after attempt: node {nid} {:?} term {} last {} commit {}",
                        r.role(),
                        r.term(),
                        r.last_index(),
                        r.commit()
                    );
                }
            }
        }
        panic!("node {id} could not win an election");
    }

    fn tick(&mut self, id: u64, n: u32) {
        for _ in 0..n {
            self.node(id).tick().unwrap();
        }
    }
}

fn base(id: u64) -> Config {
    let mut c = Config::new(id);
    c.max_append_bytes = 1; // one entry per Append: lets the test split replication
    c
}

fn between(a: &[u64], b: &[u64]) -> impl Fn(&Message) -> bool {
    let (a, b): (BTreeSet<u64>, BTreeSet<u64>) =
        (a.iter().copied().collect(), b.iter().copied().collect());
    move |m| {
        (a.contains(&m.from) && b.contains(&m.to)) || (b.contains(&m.from) && a.contains(&m.to))
    }
}

/// Raft paper, Figure 8: a leader must not commit an entry from an earlier term
/// by counting replicas, or a later leader can overwrite it.
#[test]
fn figure_8_old_term_entry_is_not_committed_by_counting() {
    let mut c = Cluster::new(5, base);
    let all = [1, 2, 3, 4, 5];
    // (a) S1 leads term 1; everyone has the no-op at index 1.
    c.elect(1, |_| true);
    assert_eq!(c.node(1).role(), Role::Leader);
    // S1 appends "x=1" at index 2 and replicates it to S2 only.
    c.node(1).propose(vec![b"x=1".to_vec()]).unwrap();
    c.run(between(&[1], &[2]));
    assert_eq!(c.node(2).last_index(), 2);

    // (b) S1 crashes. S5 wins term 2 with votes from S3, S4 and appends its
    // no-op at index 2, which reaches nobody.
    c.crash(1);
    c.elect(5, |m| {
        between(&[5], &[3, 4])(m) && matches!(m.body, Body::Vote { .. } | Body::VoteResp { .. })
    });
    assert_eq!(c.node(5).role(), Role::Leader);
    assert_eq!(c.node(5).last_index(), 2);

    // (c) S5 crashes; S1 restarts and wins term 3 (S2, S3, S4). It replicates
    // index 2 ("x=1", term 1) to S3, so that entry is now on a majority {1,2,3},
    // but S1's own term-3 no-op (index 3) reaches nobody.
    c.crash(5);
    c.start(1);
    let s1_peer = |m: &Message| {
        (m.from == 1 && [2, 3, 4].contains(&m.to)) || (m.to == 1 && [2, 3, 4].contains(&m.from))
    };
    c.elect_until_leader(1, |m| s1_peer(m) && !matches!(m.body, Body::Append { .. }));
    // Replicate, but lose index 3 (the term-3 no-op) from every Append in flight.
    // Heartbeats make S1 re-probe the followers whose probes were dropped above.
    for _ in 0..8 {
        c.tick(1, 2);
        c.run_map(|mut m| {
            if !s1_peer(&m) {
                return None;
            }
            if let Body::Append { entries, .. } = &mut m.body {
                entries.retain(|e| e.index < 3);
            }
            Some(m)
        });
    }
    assert_eq!(c.node(1).role(), Role::Leader);
    assert_eq!(c.node(3).last_index(), 2, "S3 must have exactly index 2");
    // The correct rule: index 2 is from term 1 != 3, so it is not committed yet.
    assert!(
        c.node(1).commit() < 2,
        "S1 committed an old-term entry by counting replicas"
    );

    // (d) S1 crashes; S5 restarts and wins term 4 (its last entry is from term 2,
    // newer than term 1), then overwrites index 2 everywhere. If (c) had committed
    // "x=1", it would now be lost: the oracle in `collect` would catch the two
    // different entries applied at index 2.
    c.crash(1);
    c.start(5);
    c.elect_until_leader(5, |_| true);
    assert_eq!(c.node(5).role(), Role::Leader);
    c.start(1);
    c.run(|_| true);
    c.tick(5, 4); // heartbeats bring S1 back into line
    c.run(|_| true);
    for id in all {
        assert_eq!(c.node(id).term(), c.node(5).term(), "node {id}");
    }
}

/// A vote must be durable before it is sent: after a crash a node must not vote
/// for a second candidate in the same term.
#[test]
fn vote_survives_a_crash() {
    let mut c = Cluster::new(3, |id| {
        let mut c = Config::new(id);
        c.pre_vote = false;
        c.check_quorum = false;
        c
    });
    // S1 campaigns in term 1 and S3 votes for it. Only votes are delivered: S3's
    // log stays empty, so it would happily vote for S2's equally empty log.
    c.elect(1, |m| {
        between(&[1], &[3])(m) && matches!(m.body, Body::Vote { .. } | Body::VoteResp { .. })
    });
    assert_eq!(c.node(3).term(), 1);
    // S3 crashes before anything else happens, then restarts.
    c.crash(3);
    c.start(3);
    // S2 also campaigns in term 1 (its term is still 0) and asks S3.
    let term = c.node(2).term();
    assert_eq!(term, 0);
    c.node(2)
        .step(Message {
            from: 0,
            to: 2,
            term,
            body: Body::TimeoutNow,
        })
        .unwrap();
    c.run(between(&[2], &[3]));
    // S1 got S3's vote in term 1; S2 must not also win term 1.
    let leaders: Vec<(u64, u64)> = c
        .nodes
        .iter()
        .filter(|(_, r)| r.role() == Role::Leader)
        .map(|(&id, r)| (id, r.term()))
        .collect();
    let in_term_1 = leaders.iter().filter(|(_, t)| *t == 1).count();
    assert!(in_term_1 <= 1, "two leaders in term 1: {leaders:?}");
}

/// Lease reads are only safe if followers that recently heard from the leader
/// refuse to elect anyone else. Build the partial partition that tempts them.
#[test]
fn lease_read_is_not_stale_under_partial_partition() {
    let mut c = Cluster::new(5, |id| {
        let mut c = Config::new(id);
        c.read_mode = ReadMode::Lease;
        c
    });
    c.elect(1, |_| true);
    c.node(1).propose(vec![b"x=old".to_vec()]).unwrap();
    c.run(|_| true);
    // A heartbeat round acknowledged by everyone gives S1 a fresh lease.
    c.tick(1, 2);
    c.run(|_| true);
    // Now S1 can reach only S2, S3. S4, S5 can reach everyone except S1.
    let cut = |m: &Message| {
        let pair = (m.from.min(m.to), m.from.max(m.to));
        !matches!(pair, (1, 4) | (1, 5))
    };
    // S4 times out (it no longer hears S1) and campaigns. S2 and S3 heard from S1
    // moments ago.
    c.tick(4, 25);
    c.run(cut);
    let usurper = [4, 5]
        .into_iter()
        .find(|&id| c.node(id).role() == Role::Leader);
    if let Some(u) = usurper {
        c.node(u).propose(vec![b"x=new".to_vec()]).unwrap();
        c.run(cut);
    }
    // S1 still believes its lease is valid and serves a read locally.
    c.node(1).read(7).unwrap();
    c.collect();
    let served = c.node(1).take_reads();
    let committed_new = c.applied.values().any(|(_, p)| p == b"x=new");
    if served.contains(&7) && committed_new {
        let seen = c.kv[&1].get("x").cloned();
        assert_eq!(
            seen.as_deref(),
            Some("new"),
            "stale lease read: S1 served x=old after x=new committed"
        );
    }
}

/// Found by the Multi-Raft simulation: a group whose log starts at a snapshot of
/// term 5 (as every range created by a split does) began writing entries of term 1
/// and 2. A replica holding only the snapshot then looked *more* up to date
/// (last term 5) than one holding acknowledged writes (last term 2), won an
/// election, and overwrote them.
#[test]
fn entries_after_an_initial_snapshot_use_a_higher_term() {
    use stowage_raft::types::SnapshotMeta;
    use stowage_raft::{MemStorage, Snapshot, Storage};
    let mut storage = MemStorage::new(Membership::new([1]));
    storage
        .install_snapshot(Snapshot {
            meta: SnapshotMeta {
                index: 5,
                term: 5,
                membership: Membership::new([1]),
            },
            data: Vec::new(),
        })
        .unwrap();
    let mut r = Raft::new(Config::new(1), storage).unwrap();
    assert!(
        r.term() >= 5,
        "term {} is below the snapshot's term 5",
        r.term()
    );
    for _ in 0..30 {
        r.tick().unwrap();
    }
    assert!(r.is_leader());
    let idx = r.propose(vec![b"x=1".to_vec()]).unwrap();
    assert!(
        r.storage().term(idx).unwrap() > 5,
        "new entry written in term {:?}",
        r.storage().term(idx)
    );
}
