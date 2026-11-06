//! A node's store: every range replica it holds, one shared storage engine, the
//! routing table, and (on the leader of range 1) the scheduler.
//!
//! Like [`stowage_raft::Raft`], a `Store` is a deterministic state machine:
//!
//! ```text
//!  tick()                     ─┐                       ┌─► take_peer_msgs()  → other stores
//!  handle_peer(from, PeerMsg) ─┼─► replicas, routing, ─┤
//!  handle_client(tok, Request)─┤   scheduler           └─► take_responses()  → clients
//!  flush()   (end of a batch) ─┘
//! ```
//!
//! The same code runs inside the simulator and in `stowd`'s tokio driver.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::Arc;

use stowage_common::{Disk, NodeId};
use stowage_raft::types::SnapshotMeta;
use stowage_raft::{
    Body, Config as RaftConfig, Entry, EntryKind, LogBatch, LogEngine, Membership, Message, Raft,
    ReadMode, SharedStorage, Snapshot, Storage as _,
};
use stowage_storage::{BTree, BTreeOptions, Engine as _, WriteBatch};

use crate::msg::{KvError, PeerMsg, Request, Response};
use crate::prefix_disk::PrefixDisk;
use crate::routing::Routing;
use crate::types::{Command, ProposalId, RangeDescriptor, RangeId, RangeInfo, SnapshotData, keys};
use crate::{Error, Result};

/// New ranges start their Raft log at this index/term (as in TiKV), so a replica
/// created by a split never needs a snapshot from its leader, and a replica
/// added later always does (its log would start below the leader's first index).
pub const INIT_INDEX: u64 = 5;
pub const INIT_TERM: u64 = 5;

/// Request handle chosen by the driver; echoed in the response.
pub type Token = u64;

#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub node: NodeId,
    /// Every node in the cluster (static membership of *nodes*; ranges move between them).
    pub peers: Vec<NodeId>,
    /// Voters of range 1 when the cluster is bootstrapped.
    pub initial_replicas: Vec<NodeId>,
    pub replication: usize,
    /// Split a range once its data exceeds this many bytes.
    pub split_bytes: u64,
    /// Snapshot and truncate a range's log every this many applied entries.
    pub compact_every: u64,
    pub heartbeat_ticks: u64,
    pub schedule_ticks: u64,
    /// A store unheard of for this long is treated as dead by the scheduler.
    pub store_timeout_ticks: u64,
    pub read_mode: ReadMode,
    pub engine: BTreeOptions,
    /// Enable automatic size-based splits and the scheduler.
    pub auto_split: bool,
    pub rebalance: bool,
}

impl StoreConfig {
    pub fn new(node: NodeId, peers: Vec<NodeId>) -> Self {
        let initial = peers.iter().copied().take(3).collect();
        Self {
            node,
            peers,
            initial_replicas: initial,
            replication: 3,
            split_bytes: 64 << 20,
            compact_every: 1000,
            heartbeat_ticks: 100,
            schedule_ticks: 50,
            store_timeout_ticks: 500,
            read_mode: ReadMode::ReadIndex,
            engine: BTreeOptions::default(),
            auto_split: true,
            rebalance: true,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StoreStats {
    pub proposals: u64,
    pub batches: u64,
    pub applied: u64,
    pub splits: u64,
    pub snapshots_installed: u64,
    pub snapshots_sent: u64,
    pub replicas_created: u64,
    pub replicas_destroyed: u64,
    pub schedule_ops: u64,
}

struct Replica {
    desc: RangeDescriptor,
    raft: Raft<SharedStorage>,
    applied: u64,
    batch: Vec<Command>,
    waiting: HashMap<u64, Token>,
    reads: BTreeMap<u64, (Token, Request)>,
    next_read: u64,
    approx_bytes: u64,
    since_compact: u64,
    split_pending_until: u64,
    destroy: bool,
    /// Created on demand for a range this node has no state for (it was added
    /// as a voter, or missed the split that created the range). It can vote and
    /// waits for a snapshot; it owns no data and is ignored by overlap checks.
    uninitialized: bool,
    created_tick: u64,
}

/// Peer messages and client responses, ready to send.
type Outputs = (Vec<(NodeId, PeerMsg)>, Vec<(Token, Response)>);

/// Output of [`Store::stage`].
#[derive(Debug)]
pub struct Staged {
    pub log: Option<LogBatch>,
    pub marks: Vec<(RangeId, u64)>,
    pub peer_msgs: Vec<(NodeId, PeerMsg)>,
    pub responses: Vec<(Token, Response)>,
}

pub struct Store {
    cfg: StoreConfig,
    engine: BTree,
    /// One Raft log for every replica on this node: one fsync per batch.
    log: LogEngine,
    // Boxed: `pump` takes replicas out of the map to borrow them alongside `self`.
    replicas: BTreeMap<RangeId, Box<Replica>>,
    /// Replicas that received input since the last pump.
    dirty: BTreeSet<RangeId>,
    routing: Routing,
    ticks: u64,
    next_seq: u64,
    range_seq: u64,
    peer_out: Vec<(NodeId, PeerMsg)>,
    client_out: Vec<(Token, Response)>,
    /// Ranges the scheduler changed recently, until this tick.
    sched_busy: BTreeMap<RangeId, u64>,
    stats: StoreStats,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("node", &self.cfg.node)
            .field("ranges", &self.replicas.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

fn raft_config(node: NodeId, range: RangeId, read_mode: ReadMode) -> RaftConfig {
    let mut c = RaftConfig::new(node);
    c.read_mode = read_mode;
    c.app_snapshots = true;
    c.async_durability = true;
    c.seed = node.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ range.wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    c
}

fn storage_err(e: impl std::fmt::Display) -> Error {
    Error::Storage(e.to_string())
}

impl Store {
    pub fn open(cfg: StoreConfig, disk: Arc<dyn Disk>) -> Result<Self> {
        let engine_disk: Arc<dyn Disk> = Arc::new(PrefixDisk::new(disk.clone(), "e."));
        let (engine, _) = BTree::open(engine_disk, cfg.engine.clone()).map_err(storage_err)?;
        let log = LogEngine::open(Arc::new(PrefixDisk::new(disk.clone(), "raft.")))
            .map_err(storage_err)?;
        let range_seq = engine
            .get(keys::RANGE_SEQ)
            .map_err(storage_err)?
            .map_or(0, |v| {
                u64::from_le_bytes(v.as_slice().try_into().unwrap_or([0; 8]))
            });
        let initialized = engine.get(b"lcluster").map_err(storage_err)?.is_some();
        // Proposal ids must never repeat across restarts: an entry proposed before
        // a crash can still commit afterwards, and must not be mistaken for a new
        // request that reused its id. Each incarnation gets its own id space.
        let incarnation = engine
            .get(keys::INCARNATION)
            .map_err(storage_err)?
            .map_or(0, |v| {
                u64::from_le_bytes(v.as_slice().try_into().unwrap_or([0; 8]))
            })
            + 1;
        let mut b = WriteBatch::new();
        b.put(
            keys::INCARNATION.to_vec(),
            incarnation.to_le_bytes().to_vec(),
        );
        engine.write(&b, true).map_err(storage_err)?;
        let mut store = Self {
            cfg,
            engine,
            log,
            replicas: BTreeMap::new(),
            dirty: BTreeSet::new(),
            routing: Routing::default(),
            ticks: 0,
            next_seq: incarnation << 32,
            range_seq,
            peer_out: Vec::new(),
            client_out: Vec::new(),
            sched_busy: BTreeMap::new(),
            stats: StoreStats::default(),
        };
        // Reopen every local replica from its persisted descriptor.
        let (lo, hi) = (keys::desc_prefix(), {
            let mut h = keys::desc_prefix();
            h.push(0xff);
            h
        });
        let descs: Vec<RangeDescriptor> = store
            .engine
            .iter(Bound::Included(&lo), Bound::Excluded(&hi))
            .map(|kv| {
                kv.map_err(storage_err)
                    .and_then(|(_, v)| RangeDescriptor::from_bytes(&v))
            })
            .collect::<Result<_>>()?;
        for d in descs {
            store.open_replica(d, false)?;
        }
        // Raft state without a descriptor: the engine lost an unsynced descriptor
        // in a crash (Raft's own files are always synced). Reopen it as
        // uninitialized; if its storage holds a snapshot, the pump reinstalls it.
        let orphans: BTreeSet<RangeId> = store
            .log
            .groups()
            .into_iter()
            .filter(|id| !store.replicas.contains_key(id))
            .collect();
        for id in orphans {
            store.open_uninitialized(id)?;
        }
        if !initialized {
            let mut b = WriteBatch::new();
            b.put(b"lcluster".to_vec(), b"1".to_vec());
            store.engine.write(&b, true).map_err(storage_err)?;
            if store.replicas.is_empty() && store.cfg.initial_replicas.contains(&store.cfg.node) {
                let desc = RangeDescriptor {
                    id: 1,
                    start: Vec::new(),
                    end: Vec::new(),
                    replicas: store.cfg.initial_replicas.iter().copied().collect(),
                    generation: 1,
                };
                store.open_replica(desc, true)?;
            }
        }
        Ok(store)
    }

    /// Open (or create, with `init`) the replica described by `desc`.
    fn open_replica(&mut self, desc: RangeDescriptor, init: bool) -> Result<()> {
        let id = desc.id;
        let membership = Membership::new(desc.replicas.iter().copied());
        let mut storage = self.log.storage(id, membership.clone());
        if init && storage.snapshot_meta().index == 0 {
            storage
                .install_snapshot(Snapshot {
                    meta: SnapshotMeta {
                        index: INIT_INDEX,
                        term: INIT_TERM,
                        membership,
                    },
                    data: Vec::new(),
                })
                .map_err(storage_err)?;
            let mut b = WriteBatch::new();
            b.put(keys::desc(id), desc.to_bytes());
            b.put(keys::applied(id), INIT_INDEX.to_le_bytes().to_vec());
            self.engine.write(&b, false).map_err(storage_err)?;
        }
        let applied = self
            .engine
            .get(&keys::applied(id))
            .map_err(storage_err)?
            .map_or(0, |v| {
                u64::from_le_bytes(v.as_slice().try_into().unwrap_or([0; 8]))
            });
        let raft = Raft::new(raft_config(self.cfg.node, id, self.cfg.read_mode), storage)
            .map_err(storage_err)?;
        let approx_bytes = self.measure(&desc)?;
        self.dirty.insert(id);
        self.replicas.insert(
            id,
            Box::new(Replica {
                desc,
                raft,
                applied,
                batch: Vec::new(),
                waiting: HashMap::new(),
                reads: BTreeMap::new(),
                next_read: 1,
                approx_bytes,
                since_compact: 0,
                split_pending_until: 0,
                destroy: false,
                uninitialized: false,
                created_tick: self.ticks,
            }),
        );
        self.stats.replicas_created += 1;
        Ok(())
    }

    /// A placeholder replica that will be filled by an incoming snapshot.
    fn open_uninitialized(&mut self, id: RangeId) -> Result<()> {
        let desc = RangeDescriptor {
            id,
            start: Vec::new(),
            end: Vec::new(),
            replicas: BTreeSet::new(),
            generation: 0,
        };
        // Never wipe existing Raft state here: it may hold votes and entries this
        // node already acknowledged (e.g. a snapshot installed just before a crash
        // that lost the engine's unsynced descriptor). Raft durability depends on it.
        let storage = self.log.storage(id, Membership::default());
        let raft = Raft::new(raft_config(self.cfg.node, id, self.cfg.read_mode), storage)
            .map_err(storage_err)?;
        self.dirty.insert(id);
        self.replicas.insert(
            id,
            Box::new(Replica {
                desc,
                raft,
                applied: 0,
                batch: Vec::new(),
                waiting: HashMap::new(),
                reads: BTreeMap::new(),
                next_read: 1,
                approx_bytes: 0,
                since_compact: 0,
                split_pending_until: 0,
                destroy: false,
                uninitialized: true,
                created_tick: self.ticks,
            }),
        );
        Ok(())
    }

    fn measure(&self, desc: &RangeDescriptor) -> Result<u64> {
        let (lo, hi) = keys::span(&desc.start, &desc.end);
        let mut n = 0u64;
        for kv in self.engine.iter(Bound::Included(&lo), Bound::Excluded(&hi)) {
            let (k, v) = kv.map_err(storage_err)?;
            n += (k.len() + v.len()) as u64;
        }
        Ok(n)
    }

    // ---- accessors --------------------------------------------------------------

    pub fn node(&self) -> NodeId {
        self.cfg.node
    }
    pub fn stats(&self) -> StoreStats {
        self.stats
    }
    pub fn routing(&self) -> &Routing {
        &self.routing
    }
    pub fn engine(&self) -> &BTree {
        &self.engine
    }
    /// Local replicas: (descriptor, is leader, term, applied index).
    pub fn local_ranges(&self) -> Vec<(RangeDescriptor, bool, u64, u64)> {
        self.replicas
            .values()
            .filter(|r| !r.uninitialized)
            .map(|r| (r.desc.clone(), r.raft.is_leader(), r.raft.term(), r.applied))
            .collect()
    }
    /// Every user pair this node stores for `desc`'s span.
    pub fn scan_local(&self, desc: &RangeDescriptor) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let (lo, hi) = keys::span(&desc.start, &desc.end);
        self.engine
            .iter(Bound::Included(&lo), Bound::Excluded(&hi))
            .map(|kv| {
                kv.map(|(k, v)| (keys::user(&k).to_vec(), v))
                    .map_err(storage_err)
            })
            .collect()
    }

    /// Ids of every replica, including uninitialized ones (debugging).
    pub fn replica_ids(&self) -> Vec<RangeId> {
        self.replicas.keys().copied().collect()
    }

    /// Debug view of one replica's Raft state: (role, term, commit, last index,
    /// snapshot index, [(peer, match, next)]).
    pub fn raft_debug(&self, id: RangeId) -> Option<String> {
        let r = self.replicas.get(&id)?;
        Some(format!(
            "uninit={} destroy={} applied={} desc={} {:?} term {} commit {} last {} snap {} progress {:?} stats {:?}",
            r.uninitialized,
            r.destroy,
            r.applied,
            r.desc,
            r.raft.role(),
            r.raft.term(),
            r.raft.commit(),
            r.raft.last_index(),
            r.raft.storage().snapshot_meta().index,
            r.raft.progress(),
            r.raft.stats()
        ))
    }

    /// Everything ready to leave the node, together with the log bytes it depends
    /// on. The driver must make `log` durable (write + fsync, possibly on another
    /// thread) **before** sending anything in it, then report back with
    /// [`Store::durable`]. Batches must be completed in order.
    pub fn stage(&mut self) -> Staged {
        let log = self.log.take_batch();
        let marks = self
            .replicas
            .iter()
            .map(|(&id, r)| (id, r.raft.last_index()))
            .collect();
        Staged {
            log,
            marks,
            peer_msgs: std::mem::take(&mut self.peer_out),
            responses: std::mem::take(&mut self.client_out),
        }
    }

    /// The log bytes of a staged batch are durable: leaders may now count their own
    /// copy of those entries towards a quorum.
    pub fn durable(&mut self, marks: &[(RangeId, u64)]) -> Result<()> {
        for &(id, index) in marks {
            if let Some(r) = self.replicas.get_mut(&id) {
                r.raft.mark_durable(index).map_err(storage_err)?;
                self.dirty.insert(id);
            }
        }
        self.pump()
    }

    /// Rewrite the shared log if it grew too large. Call only when no staged batch
    /// is still being written.
    pub fn maintain_log(&mut self) -> Result<()> {
        if self.log.needs_rewrite() {
            self.log.rewrite().map_err(storage_err)?;
        }
        Ok(())
    }

    /// Synchronous variant of stage → write → durable, repeated until nothing new
    /// needs writing (used by the simulator and tests).
    fn sync_now(&mut self) -> Result<Outputs> {
        let mut msgs = Vec::new();
        let mut resps = Vec::new();
        loop {
            let st = self.stage();
            msgs.extend(st.peer_msgs);
            resps.extend(st.responses);
            let Some(batch) = st.log else {
                if self.peer_out.is_empty() && self.client_out.is_empty() {
                    break;
                }
                continue;
            };
            batch.write().map_err(storage_err)?;
            self.durable(&st.marks)?;
        }
        self.maintain_log()?;
        Ok((msgs, resps))
    }

    /// Outgoing peer messages, after making the log durable (synchronously).
    pub fn take_peer_msgs(&mut self) -> Result<Vec<(NodeId, PeerMsg)>> {
        let (m, r) = self.sync_now()?;
        self.client_out.extend(r);
        Ok(m)
    }

    /// Client responses, after making the log durable (synchronously).
    pub fn take_responses(&mut self) -> Result<Vec<(Token, Response)>> {
        let (m, r) = self.sync_now()?;
        self.peer_out.extend(m);
        Ok(r)
    }

    pub fn log_fsyncs(&self) -> u64 {
        self.log.fsyncs()
    }

    // ---- inputs -------------------------------------------------------------------

    pub fn tick(&mut self) -> Result<()> {
        self.ticks += 1;
        for (&id, r) in &mut self.replicas {
            r.raft.tick().map_err(storage_err)?;
            self.dirty.insert(id);
        }
        let ttl = 2 * self.cfg.store_timeout_ticks;
        for r in self.replicas.values_mut() {
            // Nobody ever sent it a snapshot. Only reclaim it if it never voted:
            // forgetting a vote could let this node vote twice in one term.
            let pristine = r.raft.storage().hard_state().vote.is_none();
            if r.uninitialized && pristine && self.ticks.saturating_sub(r.created_tick) > ttl {
                r.destroy = true;
            }
        }
        if self.ticks % self.cfg.heartbeat_ticks == 1 {
            self.heartbeat();
        }
        if self.ticks.is_multiple_of(self.cfg.schedule_ticks) {
            self.schedule();
        }
        self.pump()
    }

    pub fn handle_peer(&mut self, from: NodeId, msg: PeerMsg) -> Result<()> {
        match msg {
            PeerMsg::Raft { range, msg } => self.raft_message(from, range, msg)?,
            PeerMsg::Heartbeat { node, ranges } => {
                self.routing.store_seen(node, self.ticks);
                for info in ranges {
                    self.routing.update(info);
                }
            }
            PeerMsg::ChangeReplicas {
                range,
                generation,
                voters,
            } => {
                if let Some(r) = self.replicas.get_mut(&range)
                    && r.raft.is_leader()
                    && r.desc.generation == generation
                {
                    let _ = r.raft.propose_membership(voters);
                    self.dirty.insert(range);
                }
            }
            PeerMsg::TransferLeader { range, to } => {
                if let Some(r) = self.replicas.get_mut(&range)
                    && r.raft.is_leader()
                {
                    let _ = r.raft.transfer_leadership(to);
                    self.dirty.insert(range);
                }
            }
            PeerMsg::Gc { range, generation } => {
                // The leader committed a configuration without us. Our descriptor
                // is older than that configuration, so this replica is stale.
                if let Some(r) = self.replicas.get_mut(&range)
                    && !r.raft.is_leader()
                    && r.desc.generation < generation
                {
                    r.destroy = true;
                    self.dirty.insert(range);
                }
            }
        }
        self.pump()
    }

    fn raft_message(&mut self, _from: NodeId, range: RangeId, msg: Message) -> Result<()> {
        if let Body::InstallSnapshot { snapshot } = &msg.body {
            // Never let a snapshot land on top of a span another local replica owns
            // (we are behind on a split or merge there; the leader will retry).
            let Ok(data) = SnapshotData::decode(&snapshot.data) else {
                return Ok(());
            };
            let overlaps = self
                .replicas
                .values()
                .any(|r| r.desc.id != range && !r.uninitialized && r.desc.overlaps(&data.desc));
            if overlaps {
                return Ok(());
            }
        }
        if !self.replicas.contains_key(&range) {
            let from_leader_or_candidate = matches!(
                msg.body,
                Body::PreVote { .. }
                    | Body::Vote { .. }
                    | Body::Append { .. }
                    | Body::Heartbeat { .. }
                    | Body::InstallSnapshot { .. }
            );
            if !from_leader_or_candidate {
                return Ok(());
            }
            self.open_uninitialized(range)?;
        }
        if let Some(r) = self.replicas.get_mut(&range) {
            r.raft.step(msg).map_err(storage_err)?;
            self.dirty.insert(range);
        }
        Ok(())
    }

    pub fn handle_client(&mut self, token: Token, req: Request) -> Result<()> {
        match &req {
            Request::Locate { key } => {
                let resp = match self.routing.lookup(key) {
                    Some(info) => Response::Location(info.clone()),
                    None => Response::Error(KvError::RangeNotFound { hint: None }),
                };
                self.client_out.push((token, resp));
                return Ok(());
            }
            Request::Ranges => {
                self.client_out
                    .push((token, Response::Ranges(self.routing.all())));
                return Ok(());
            }
            Request::Status => {
                let lines = self
                    .replicas
                    .keys()
                    .filter_map(|&id| self.raft_debug(id))
                    .collect();
                self.client_out.push((token, Response::Status(lines)));
                return Ok(());
            }
            _ => {}
        }
        let key = req.routing_key().unwrap_or_default().to_vec();
        let Some((&id, _)) = self
            .replicas
            .iter()
            .find(|(_, r)| !r.uninitialized && !r.destroy && r.desc.contains(&key))
        else {
            let hint = self.routing.lookup(&key).and_then(|i| i.leader);
            self.client_out
                .push((token, Response::Error(KvError::RangeNotFound { hint })));
            return Ok(());
        };
        let r = self
            .replicas
            .get_mut(&id)
            .ok_or_else(|| Error::Storage("replica vanished".into()))?;
        if !r.raft.is_leader() {
            let leader = r
                .raft
                .leader()
                .or_else(|| self.routing.get(id).and_then(|i| i.leader));
            self.client_out.push((
                token,
                Response::Error(KvError::NotLeader { range: id, leader }),
            ));
            return Ok(());
        }
        match req {
            Request::Get { .. } | Request::Scan { .. } => {
                let rid = r.next_read;
                r.next_read += 1;
                self.dirty.insert(id);
                r.reads.insert(rid, (token, req));
                if r.raft.read(rid).is_err() {
                    r.reads.remove(&rid);
                    self.client_out.push((
                        token,
                        Response::Error(KvError::NotLeader {
                            range: id,
                            leader: None,
                        }),
                    ));
                }
            }
            Request::Put { key, value } => {
                let pid = self.alloc_pid();
                self.push_command(
                    id,
                    token,
                    Command::Write {
                        pid,
                        ops: vec![(key, Some(value))],
                    },
                );
            }
            Request::Delete { key } => {
                let pid = self.alloc_pid();
                self.push_command(
                    id,
                    token,
                    Command::Write {
                        pid,
                        ops: vec![(key, None)],
                    },
                );
            }
            Request::Split { key } => {
                let r = &self.replicas[&id];
                if key == r.desc.start {
                    self.client_out.push((token, Response::Done));
                } else {
                    let generation = r.desc.generation;
                    let pid = self.alloc_pid();
                    let new_id = self.alloc_range_id()?;
                    self.push_command(
                        id,
                        token,
                        Command::Split {
                            pid,
                            key,
                            new_id,
                            generation,
                        },
                    );
                }
            }
            Request::Locate { .. } | Request::Ranges | Request::Status => {}
        }
        Ok(())
    }

    fn alloc_pid(&mut self) -> ProposalId {
        let seq = self.next_seq;
        self.next_seq += 1;
        ProposalId {
            node: self.cfg.node,
            seq,
        }
    }

    /// Cluster-unique: `(node << 40) | n`, with `n` persisted before use.
    fn alloc_range_id(&mut self) -> Result<RangeId> {
        self.range_seq += 1;
        let mut b = WriteBatch::new();
        b.put(
            keys::RANGE_SEQ.to_vec(),
            self.range_seq.to_le_bytes().to_vec(),
        );
        self.engine.write(&b, true).map_err(storage_err)?;
        Ok((self.cfg.node << 40) | self.range_seq)
    }

    fn push_command(&mut self, id: RangeId, token: Token, cmd: Command) {
        self.dirty.insert(id);
        if let Some(r) = self.replicas.get_mut(&id) {
            r.waiting.insert(cmd.pid().seq, token);
            r.batch.push(cmd);
        }
    }

    /// Propose everything queued since the last flush: one Raft entry batch (one
    /// fsync) per range. Drivers call this once per loop iteration.
    pub fn flush(&mut self) -> Result<()> {
        for r in self.replicas.values_mut() {
            if r.batch.is_empty() {
                continue;
            }
            let cmds = std::mem::take(&mut r.batch);
            self.dirty.insert(r.desc.id);
            self.stats.batches += 1;
            self.stats.proposals += cmds.len() as u64;
            let encoded = cmds.iter().map(Command::encode).collect();
            if r.raft.propose(encoded).is_err() {
                for c in cmds {
                    if let Some(t) = r.waiting.remove(&c.pid().seq) {
                        let leader = r.raft.leader();
                        self.client_out.push((
                            t,
                            Response::Error(KvError::NotLeader {
                                range: r.desc.id,
                                leader,
                            }),
                        ));
                    }
                }
            }
        }
        self.pump()
    }

    // ---- outputs: apply, reads, messages ---------------------------------------------

    fn pump(&mut self) -> Result<()> {
        // Pumping a replica can dirty others (a split creates one), so loop.
        while !self.dirty.is_empty() {
            let ids = std::mem::take(&mut self.dirty);
            for id in ids {
                self.pump_replica(id)?;
            }
        }
        let doomed: Vec<RangeId> = self
            .replicas
            .iter()
            .filter(|(_, r)| r.destroy)
            .map(|(&id, _)| id)
            .collect();
        for id in doomed {
            self.destroy_replica(id)?;
        }
        Ok(())
    }

    fn pump_replica(&mut self, id: RangeId) -> Result<()> {
        let Some(mut r) = self.replicas.remove(&id) else {
            return Ok(());
        };
        let res = self.pump_inner(&mut r);
        self.replicas.insert(id, r);
        res
    }

    fn pump_inner(&mut self, r: &mut Replica) -> Result<()> {
        if let Some(snap) = r.raft.take_snapshot()
            && !snap.data.is_empty()
            && snap.meta.index > r.applied
        {
            self.install_snapshot(r, &snap)?;
        }
        // An uninitialized replica has no state machine to apply to: it must first
        // install a snapshot (or be upgraded by the split that creates it). Its
        // committed entries stay in the Raft log until then.
        if !r.uninitialized {
            for e in r.raft.take_committed() {
                if e.index > r.applied {
                    self.apply(r, &e)?;
                }
            }
        }
        for rid in r.raft.take_reads() {
            if let Some((token, req)) = r.reads.remove(&rid) {
                let resp = self.serve_read(&r.desc, &req)?;
                self.client_out.push((token, resp));
            }
        }
        // Reads and writes queued at a replica that lost leadership will never
        // complete here; fail them so clients retry at the new leader.
        if !r.raft.is_leader() && (!r.reads.is_empty() || !r.waiting.is_empty()) {
            let leader = r.raft.leader();
            for (_, (t, _)) in std::mem::take(&mut r.reads) {
                self.client_out.push((
                    t,
                    Response::Error(KvError::NotLeader {
                        range: r.desc.id,
                        leader,
                    }),
                ));
            }
            for (_, t) in std::mem::take(&mut r.waiting) {
                self.client_out
                    .push((t, Response::Error(KvError::Ambiguous)));
            }
        }
        if r.since_compact >= self.cfg.compact_every
            && !r.uninitialized
            && r.raft.applied() > r.raft.storage().snapshot_meta().index
        {
            self.compact(r)?;
        }
        if self.cfg.auto_split
            && r.raft.is_leader()
            && r.approx_bytes > self.cfg.split_bytes
            && self.ticks >= r.split_pending_until
        {
            self.propose_split(r)?;
        }
        if r.raft.is_leader() && !r.uninitialized {
            for to in r.raft.take_snapshot_requests() {
                // Generated now, from the state as of the applied index.
                let pairs = self.scan_local(&r.desc)?;
                let data = SnapshotData {
                    desc: r.desc.clone(),
                    pairs,
                }
                .encode();
                r.raft.send_snapshot(to, data).map_err(storage_err)?;
                self.stats.snapshots_sent += 1;
            }
        }
        for m in r.raft.take_messages() {
            self.peer_out.push((
                m.to,
                PeerMsg::Raft {
                    range: r.desc.id,
                    msg: m,
                },
            ));
        }
        Ok(())
    }

    fn serve_read(&self, desc: &RangeDescriptor, req: &Request) -> Result<Response> {
        Ok(match req {
            Request::Get { key } => {
                if !desc.contains(key) {
                    Response::Error(KvError::KeyMoved)
                } else {
                    Response::Value(self.engine.get(&keys::data(key)).map_err(storage_err)?)
                }
            }
            Request::Scan { start, end, limit } => {
                if !desc.contains(start) {
                    return Ok(Response::Error(KvError::KeyMoved));
                }
                let hi_user: &[u8] = if desc.end.is_empty() || (!end.is_empty() && end < &desc.end)
                {
                    end
                } else {
                    &desc.end
                };
                let (lo, hi) = keys::span(start, hi_user);
                let pairs = self
                    .engine
                    .scan(Bound::Included(&lo), Bound::Excluded(&hi), *limit as usize)
                    .map_err(storage_err)?
                    .into_iter()
                    .map(|(k, v)| (keys::user(&k).to_vec(), v))
                    .collect();
                Response::Rows {
                    pairs,
                    range_end: desc.end.clone(),
                }
            }
            _ => Response::Done,
        })
    }

    fn apply(&mut self, r: &mut Replica, e: &Entry) -> Result<()> {
        self.stats.applied += 1;
        r.since_compact += 1;
        let id = r.desc.id;
        let mut b = WriteBatch::new();
        b.put(keys::applied(id), e.index.to_le_bytes().to_vec());
        let mut reply: Option<(u64, Response)> = None;
        let mut new_range: Option<RangeDescriptor> = None;
        match &e.kind {
            EntryKind::Noop => {}
            EntryKind::Config(m) => {
                let old = r.desc.replicas.clone();
                r.desc.replicas = m.all();
                r.desc.generation += 1;
                b.put(keys::desc(id), r.desc.to_bytes());
                if !m.is_joint() {
                    if r.raft.is_leader() {
                        for gone in old.difference(&m.voters) {
                            self.peer_out.push((
                                *gone,
                                PeerMsg::Gc {
                                    range: id,
                                    generation: r.desc.generation,
                                },
                            ));
                        }
                    }
                    if !m.voters.contains(&self.cfg.node) {
                        r.destroy = true;
                    }
                }
            }
            EntryKind::Normal(data) => match Command::decode(data)? {
                Command::Write { pid, ops } => {
                    if ops.iter().all(|(k, _)| r.desc.contains(k)) {
                        for (k, v) in ops {
                            let size = (k.len() + v.as_ref().map_or(0, Vec::len) + 1) as u64;
                            match v {
                                Some(v) => {
                                    r.approx_bytes += size;
                                    b.put(keys::data(&k), v);
                                }
                                None => {
                                    r.approx_bytes = r.approx_bytes.saturating_sub(size);
                                    b.delete(keys::data(&k));
                                }
                            }
                        }
                        reply = (pid.node == self.cfg.node).then_some((pid.seq, Response::Done));
                    } else {
                        reply = (pid.node == self.cfg.node)
                            .then_some((pid.seq, Response::Error(KvError::KeyMoved)));
                    }
                }
                Command::Split {
                    pid,
                    key,
                    new_id,
                    generation,
                } => {
                    if generation == r.desc.generation
                        && r.desc.contains(&key)
                        && key != r.desc.start
                    {
                        let right = RangeDescriptor {
                            id: new_id,
                            start: key.clone(),
                            end: r.desc.end.clone(),
                            replicas: r.desc.replicas.clone(),
                            generation: generation + 1,
                        };
                        r.desc.end = key;
                        r.desc.generation = generation + 1;
                        b.put(keys::desc(id), r.desc.to_bytes());
                        b.put(keys::desc(new_id), right.to_bytes());
                        b.put(keys::applied(new_id), INIT_INDEX.to_le_bytes().to_vec());
                        new_range = Some(right);
                        self.stats.splits += 1;
                    }
                    reply = (pid.node == self.cfg.node).then_some((pid.seq, Response::Done));
                }
            },
        }
        // A split creates the right half's Raft files (which are fsynced). Make the
        // descriptors durable first, so a crash can never leave Raft state for a
        // range the engine does not know about.
        self.engine
            .write(&b, new_range.is_some())
            .map_err(storage_err)?;
        r.applied = e.index;
        if let Some(right) = new_range {
            let was_leader = r.raft.is_leader();
            let right_id = right.id;
            // An uninitialized placeholder for this range (created when its leader
            // contacted us before we applied the split) is upgraded in place: its
            // files, including any vote it cast, are kept.
            if self
                .replicas
                .get(&right_id)
                .is_some_and(|r| r.uninitialized)
            {
                self.replicas.remove(&right_id);
            }
            if !self.replicas.contains_key(&right_id) {
                self.open_replica(right, true)?;
            }
            r.approx_bytes = self.measure(&r.desc)?;
            let right_size = match self.replicas.get(&right_id) {
                Some(nr) => self.measure(&nr.desc)?,
                None => 0,
            };
            if let Some(nr) = self.replicas.get_mut(&right_id) {
                nr.approx_bytes = right_size;
                // The left half's leader campaigns at once for the right half, so
                // it is writable without waiting out an election timeout.
                if was_leader {
                    let term = nr.raft.term();
                    nr.raft
                        .step(Message {
                            from: 0,
                            to: self.cfg.node,
                            term,
                            body: Body::TimeoutNow,
                        })
                        .map_err(storage_err)?;
                }
            }
        }
        if let Some((seq, resp)) = reply
            && let Some(t) = r.waiting.remove(&seq)
        {
            self.client_out.push((t, resp));
        }
        Ok(())
    }

    fn install_snapshot(&mut self, r: &mut Replica, snap: &Snapshot) -> Result<()> {
        let data = SnapshotData::decode(&snap.data)?;
        let (lo, hi) = keys::span(&data.desc.start, &data.desc.end);
        let mut b = WriteBatch::new();
        for kv in self.engine.iter(Bound::Included(&lo), Bound::Excluded(&hi)) {
            let (k, _) = kv.map_err(storage_err)?;
            b.delete(k);
        }
        let mut size = 0u64;
        for (k, v) in &data.pairs {
            size += (k.len() + v.len()) as u64;
            b.put(keys::data(k), v.clone());
        }
        b.put(keys::desc(data.desc.id), data.desc.to_bytes());
        b.put(
            keys::applied(data.desc.id),
            snap.meta.index.to_le_bytes().to_vec(),
        );
        // A snapshot can be large: write it in chunks the buffer pool can pin.
        let ops = b.ops().to_vec();
        for chunk in ops.chunks(256) {
            let mut c = WriteBatch::new();
            for op in chunk {
                match op {
                    stowage_storage::Op::Put { key, value } => c.put(key.clone(), value.clone()),
                    stowage_storage::Op::Delete { key } => c.delete(key.clone()),
                };
            }
            self.engine.write(&c, false).map_err(storage_err)?;
        }
        r.desc = data.desc;
        r.applied = snap.meta.index;
        r.approx_bytes = size;
        r.uninitialized = false;
        self.stats.snapshots_installed += 1;
        Ok(())
    }

    fn compact(&mut self, r: &mut Replica) -> Result<()> {
        r.since_compact = 0;
        // The engine must hold everything up to the snapshot index durably before
        // the log entries that produced it are discarded. The Raft snapshot itself
        // carries no data: lagging followers get one generated on demand.
        self.engine.sync().map_err(storage_err)?;
        let index = r.raft.applied();
        r.raft.compact(index, Vec::new()).map_err(storage_err)
    }

    fn propose_split(&mut self, r: &mut Replica) -> Result<()> {
        let pairs = self.scan_local(&r.desc)?;
        if pairs.len() < 2 {
            return Ok(());
        }
        let key = pairs[pairs.len() / 2].0.clone();
        if key == r.desc.start {
            return Ok(());
        }
        let pid = self.alloc_pid();
        let new_id = self.alloc_range_id()?;
        r.batch.push(Command::Split {
            pid,
            key,
            new_id,
            generation: r.desc.generation,
        });
        r.split_pending_until = self.ticks + 100;
        let cmds = std::mem::take(&mut r.batch);
        let _ = r.raft.propose(cmds.iter().map(Command::encode).collect());
        Ok(())
    }

    fn destroy_replica(&mut self, id: RangeId) -> Result<()> {
        let Some(r) = self.replicas.remove(&id) else {
            return Ok(());
        };
        let mut b = WriteBatch::new();
        if !r.uninitialized {
            let (lo, hi) = keys::span(&r.desc.start, &r.desc.end);
            let others_cover = self
                .replicas
                .values()
                .any(|o| !o.uninitialized && o.desc.overlaps(&r.desc));
            if !others_cover {
                for kv in self.engine.iter(Bound::Included(&lo), Bound::Excluded(&hi)) {
                    let (k, _) = kv.map_err(storage_err)?;
                    b.delete(k);
                }
            }
        }
        b.delete(keys::desc(id));
        b.delete(keys::applied(id));
        let ops = b.ops().to_vec();
        for chunk in ops.chunks(256) {
            let mut c = WriteBatch::new();
            for op in chunk {
                if let stowage_storage::Op::Delete { key } = op {
                    c.delete(key.clone());
                }
            }
            self.engine.write(&c, false).map_err(storage_err)?;
        }
        self.log.remove(id);
        for (_, (t, _)) in r.reads {
            self.client_out
                .push((t, Response::Error(KvError::RangeNotFound { hint: None })));
        }
        for (_, t) in r.waiting {
            self.client_out
                .push((t, Response::Error(KvError::Ambiguous)));
        }
        self.stats.replicas_destroyed += 1;
        Ok(())
    }

    // ---- gossip and scheduling ------------------------------------------------------------

    fn heartbeat(&mut self) {
        let infos: Vec<RangeInfo> = self
            .replicas
            .values()
            .filter(|r| !r.uninitialized)
            .map(|r| RangeInfo {
                desc: r.desc.clone(),
                leader: r.raft.leader(),
                term: r.raft.term(),
                approx_bytes: r.approx_bytes,
            })
            .collect();
        self.routing.store_seen(self.cfg.node, self.ticks);
        for i in &infos {
            self.routing.update(i.clone());
        }
        for &p in &self.cfg.peers {
            if p != self.cfg.node {
                self.peer_out.push((
                    p,
                    PeerMsg::Heartbeat {
                        node: self.cfg.node,
                        ranges: infos.clone(),
                    },
                ));
            }
        }
    }

    /// Runs only on the leader of range 1, which always exists (a split keeps the
    /// left half's id). Each round plans up to `OPS` replica moves and `OPS`
    /// leader transfers, each on a different range, updating its load counts as
    /// it goes; a range that was just changed cools down for a few rounds.
    fn schedule(&mut self) {
        const OPS: usize = 4;
        if !self.cfg.rebalance || !self.replicas.get(&1).is_some_and(|r| r.raft.is_leader()) {
            return;
        }
        let now = self.ticks;
        self.sched_busy.retain(|_, until| *until > now);
        let live: BTreeSet<NodeId> = self
            .routing
            .live_stores(now, self.cfg.store_timeout_ticks)
            .into_iter()
            .collect();
        let ranges: Vec<RangeInfo> = self
            .routing
            .all()
            .into_iter()
            .filter(|r| r.leader.is_some() && !self.sched_busy.contains_key(&r.desc.id))
            .collect();
        let mut load: BTreeMap<NodeId, i64> = live.iter().map(|&n| (n, 0)).collect();
        let mut leads: BTreeMap<NodeId, i64> = live.iter().map(|&n| (n, 0)).collect();
        for r in self.routing.all() {
            for n in &r.desc.replicas {
                if let Some(c) = load.get_mut(n) {
                    *c += 1;
                }
            }
            if let Some(l) = r.leader
                && let Some(c) = leads.get_mut(&l)
            {
                *c += 1;
            }
        }
        let least = |load: &BTreeMap<NodeId, i64>, exclude: &BTreeSet<NodeId>| {
            load.iter()
                .filter(|(n, _)| !exclude.contains(n))
                .min_by_key(|&(n, c)| (*c, *n))
                .map(|(&n, _)| n)
        };
        let mut plan: Vec<(NodeId, PeerMsg)> = Vec::new();
        let mut touched: BTreeSet<RangeId> = BTreeSet::new();

        // Replica moves: repair first (dead stores, under-replication), then balance.
        for r in &ranges {
            if plan.len() >= OPS {
                break;
            }
            let (Some(leader), reps) = (r.leader, &r.desc.replicas) else {
                continue;
            };
            let change = if let Some(&dead) = reps.iter().find(|n| !live.contains(n)) {
                least(&load, reps).map(|to| (Some(dead), to))
            } else if reps.len() < self.cfg.replication {
                least(&load, reps).map(|to| (None, to))
            } else {
                None
            };
            if let Some((from, to)) = change {
                let mut v = reps.clone();
                if let Some(f) = from {
                    v.remove(&f);
                }
                v.insert(to);
                *load.entry(to).or_default() += 1;
                plan.push((
                    leader,
                    PeerMsg::ChangeReplicas {
                        range: r.desc.id,
                        generation: r.desc.generation,
                        voters: v,
                    },
                ));
                touched.insert(r.desc.id);
            }
        }
        // Balance replica counts: among all (range, donor replica, recipient node)
        // moves that narrow a gap of 2 or more, take the widest gap first. Looking
        // at every pair, not just fullest/emptiest, avoids getting stuck when the
        // fullest store happens to share no movable range with the emptiest.
        for _ in plan.len()..OPS {
            let mut best: Option<(i64, &RangeInfo, NodeId, NodeId)> = None;
            for r in &ranges {
                if touched.contains(&r.desc.id) {
                    continue;
                }
                for &from in r.desc.replicas.iter().filter(|&&n| Some(n) != r.leader) {
                    for (&to, &c_to) in &load {
                        if r.desc.replicas.contains(&to) {
                            continue;
                        }
                        let gap = load.get(&from).copied().unwrap_or(0) - c_to;
                        if gap >= 2 && best.is_none_or(|(g, ..)| gap > g) {
                            best = Some((gap, r, from, to));
                        }
                    }
                }
            }
            let Some((_, r, from, to)) = best else { break };
            let mut v = r.desc.replicas.clone();
            v.remove(&from);
            v.insert(to);
            *load.entry(from).or_default() -= 1;
            *load.entry(to).or_default() += 1;
            touched.insert(r.desc.id);
            if let Some(l) = r.leader {
                plan.push((
                    l,
                    PeerMsg::ChangeReplicas {
                        range: r.desc.id,
                        generation: r.desc.generation,
                        voters: v,
                    },
                ));
            }
        }
        // Leader transfers are cheap (no data moves): even out leadership the same way.
        for _ in 0..OPS {
            let mut best: Option<(i64, &RangeInfo, NodeId, NodeId)> = None;
            for r in &ranges {
                let Some(l) = r.leader else { continue };
                if touched.contains(&r.desc.id) {
                    continue;
                }
                for &to in r
                    .desc
                    .replicas
                    .iter()
                    .filter(|&&n| n != l && live.contains(&n))
                {
                    let gap =
                        leads.get(&l).copied().unwrap_or(0) - leads.get(&to).copied().unwrap_or(0);
                    if gap >= 2 && best.is_none_or(|(g, ..)| gap > g) {
                        best = Some((gap, r, l, to));
                    }
                }
            }
            let Some((_, r, from, to)) = best else { break };
            *leads.entry(from).or_default() -= 1;
            *leads.entry(to).or_default() += 1;
            touched.insert(r.desc.id);
            plan.push((
                from,
                PeerMsg::TransferLeader {
                    range: r.desc.id,
                    to,
                },
            ));
        }
        for id in &touched {
            self.sched_busy
                .insert(*id, now + 3 * self.cfg.schedule_ticks);
        }
        for (to, msg) in plan {
            tracing::debug!(node = self.cfg.node, ?msg, "schedule");
            self.stats.schedule_ops += 1;
            if to == self.cfg.node {
                let _ = self.handle_peer(to, msg);
            } else {
                self.peer_out.push((to, msg));
            }
        }
    }
}
