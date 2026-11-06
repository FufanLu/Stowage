//! A replicated key-value store on Raft, run inside the deterministic simulator.
//! Shared by `tests/simulation.rs` and `examples/trace_put.rs`.
//!
//! - Each node is a [`KvNode`] actor: a `Raft<DiskStorage>` on its `SimDisk`, a
//!   `BTreeMap<u8, u64>` state machine, and a session table for exactly-once
//!   semantics under client retries.
//! - Clients ([`Clients`]) live in the test: they send requests to whichever node
//!   they believe leads, retry on timeouts or redirects with the same sequence
//!   number, and record a history for the linearizability checker.
//! - [`Oracle`] watches every node from the outside and asserts Raft's safety
//!   properties as the run proceeds.

#![allow(dead_code, clippy::unwrap_used)]

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;

use stowage_common::{NodeId, Rng};
use stowage_raft::{
    Config, DiskStorage, Entry, EntryKind, Membership, Message, Raft, ReadMode, Role,
};
use stowage_sim::linearizability::{Op, RegisterOp};
use stowage_sim::{Actor, CLIENT, Ctx, MS, NodeEnv, Simulation};

pub const TICK: u64 = 10 * MS;
const TICK_TIMER: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvOp {
    Put(u8, u64),
    Get(u8),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Ok(Option<u64>),
    NotLeader(Option<NodeId>),
}

#[derive(Clone)]
pub enum Msg {
    Raft(Message),
    Req {
        client: u64,
        seq: u64,
        op: KvOp,
    },
    Resp {
        client: u64,
        seq: u64,
        reply: Reply,
    },
    /// Admin: change voters (sent to the leader by the test).
    ChangeVoters(BTreeSet<NodeId>),
    /// Admin: hand leadership to a node.
    Transfer(NodeId),
}

/// Compact rendering for simulator traces (used by the illustrated docs).
impl std::fmt::Debug for Msg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Msg::Raft(m) => write!(f, "{}", short(m)),
            Msg::Req { client, seq, op } => write!(f, "Req(c{client}#{seq} {op:?})"),
            Msg::Resp { client, seq, reply } => write!(f, "Resp(c{client}#{seq} {reply:?})"),
            Msg::ChangeVoters(v) => write!(f, "ChangeVoters({v:?})"),
            Msg::Transfer(to) => write!(f, "Transfer(n{to})"),
        }
    }
}

pub fn short_kind(k: &EntryKind) -> String {
    match k {
        EntryKind::Noop => "noop".to_string(),
        EntryKind::Normal(d) => format!("cmd {}B", d.len()),
        EntryKind::Config(m) => format!("config{:?}", m.voters),
    }
}

pub fn short(m: &Message) -> String {
    use stowage_raft::Body;
    let kind = |k: &EntryKind| match k {
        EntryKind::Noop => "noop".to_string(),
        EntryKind::Normal(d) => format!("cmd {}B", d.len()),
        EntryKind::Config(m) => format!("config{:?}", m.voters),
    };
    let b = match &m.body {
        Body::PreVote {
            last_index,
            last_term,
        } => format!("PreVote(last {last_index}@t{last_term})"),
        Body::PreVoteResp { granted } => {
            format!("PreVoteResp({})", if *granted { "yes" } else { "no" })
        }
        Body::Vote {
            last_index,
            last_term,
            ..
        } => format!("Vote(last {last_index}@t{last_term})"),
        Body::VoteResp { granted } => format!("VoteResp({})", if *granted { "yes" } else { "no" }),
        Body::Append {
            prev_index,
            prev_term,
            entries,
            commit,
        } => {
            let es: Vec<String> = entries
                .iter()
                .map(|e| format!("{}@t{} {}", e.index, e.term, kind(&e.kind)))
                .collect();
            format!(
                "Append(prev {prev_index}@t{prev_term} [{}] commit {commit})",
                es.join(", ")
            )
        }
        Body::AppendResp {
            success: true,
            match_index,
            ..
        } => format!("AppendResp(ok match {match_index})"),
        Body::AppendResp { hint, .. } => format!("AppendResp(reject {hint:?})"),
        Body::Heartbeat {
            commit,
            read_ctx,
            round,
        } => format!("Heartbeat(commit {commit} ctx {read_ctx} round {round})"),
        Body::HeartbeatResp { read_ctx, round } => {
            format!("HeartbeatResp(ctx {read_ctx} round {round})")
        }
        Body::InstallSnapshot { snapshot } => format!("InstallSnapshot(@{})", snapshot.meta.index),
        Body::TimeoutNow => "TimeoutNow".to_string(),
    };
    format!("t{} {b}", m.term)
}

fn encode(client: u64, seq: u64, op: KvOp) -> Vec<u8> {
    let mut v = Vec::with_capacity(26);
    v.extend_from_slice(&client.to_le_bytes());
    v.extend_from_slice(&seq.to_le_bytes());
    match op {
        KvOp::Put(k, val) => {
            v.push(1);
            v.push(k);
            v.extend_from_slice(&val.to_le_bytes());
        }
        KvOp::Get(k) => {
            v.push(2);
            v.push(k);
        }
    }
    v
}

fn decode(b: &[u8]) -> (u64, u64, KvOp) {
    let u = |r: std::ops::Range<usize>| u64::from_le_bytes(b[r].try_into().unwrap());
    let (client, seq) = (u(0..8), u(8..16));
    let op = match b[16] {
        1 => KvOp::Put(b[17], u(18..26)),
        _ => KvOp::Get(b[17]),
    };
    (client, seq, op)
}

/// Global view used to check safety properties while the simulation runs.
#[derive(Debug, Default)]
pub struct Oracle {
    /// term -> leader elected in it (Election Safety).
    pub leaders: BTreeMap<u64, NodeId>,
    /// index -> (term, payload) of the entry first applied there (State Machine Safety).
    pub applied: BTreeMap<u64, (u64, Vec<u8>)>,
    pub applies: u64,
    pub snapshots_restored: u64,
}

impl Oracle {
    fn saw_role(&mut self, id: NodeId, role: Role, term: u64) {
        if role == Role::Leader {
            let prev = *self.leaders.entry(term).or_insert(id);
            assert_eq!(prev, id, "two leaders in term {term}: {prev} and {id}");
        }
    }

    fn saw_apply(&mut self, node: NodeId, e: &Entry) {
        self.applies += 1;
        let payload = match &e.kind {
            EntryKind::Normal(d) => d.clone(),
            EntryKind::Noop => b"noop".to_vec(),
            EntryKind::Config(m) => format!("{m:?}").into_bytes(),
        };
        let prev = self
            .applied
            .entry(e.index)
            .or_insert((e.term, payload.clone()));
        assert_eq!(
            *prev,
            (e.term, payload),
            "node {node} applied a different entry at index {}",
            e.index
        );
    }
}

pub type SharedOracle = Rc<RefCell<Oracle>>;

#[derive(Debug, Clone)]
pub struct NodeOpts {
    pub read_mode: ReadMode,
    pub snapshot_every: u64,
    pub initial: Membership,
}

pub struct KvNode {
    id: NodeId,
    pub raft: Raft<DiskStorage>,
    pub kv: BTreeMap<u8, u64>,
    /// client -> (last applied seq, its reply)
    sessions: HashMap<u64, (u64, Option<u64>)>,
    /// Requests this node proposed and must answer when applied.
    waiting: BTreeMap<(u64, u64), ()>,
    /// Read id -> (client, seq, key).
    reads: HashMap<u64, (u64, u64, u8)>,
    next_read: u64,
    opts: NodeOpts,
    oracle: SharedOracle,
    since_snapshot: u64,
}

impl std::fmt::Debug for KvNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvNode").field("raft", &self.raft).finish()
    }
}

impl KvNode {
    pub fn new(env: &NodeEnv, opts: NodeOpts, oracle: SharedOracle) -> Self {
        let storage = DiskStorage::open(env.disk(), opts.initial.clone()).unwrap();
        let mut cfg = Config::new(env.id);
        cfg.read_mode = opts.read_mode;
        cfg.seed = env.id.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ env.clock.skew() as u64;
        let raft = Raft::new(cfg, storage).unwrap();
        Self {
            id: env.id,
            raft,
            kv: BTreeMap::new(),
            sessions: HashMap::new(),
            waiting: BTreeMap::new(),
            reads: HashMap::new(),
            next_read: 1,
            opts,
            oracle,
            since_snapshot: 0,
        }
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&(self.kv.len() as u32).to_le_bytes());
        for (k, val) in &self.kv {
            v.push(*k);
            v.extend_from_slice(&val.to_le_bytes());
        }
        let mut sessions: Vec<_> = self.sessions.iter().collect();
        sessions.sort();
        v.extend_from_slice(&(sessions.len() as u32).to_le_bytes());
        for (c, (seq, r)) in sessions {
            v.extend_from_slice(&c.to_le_bytes());
            v.extend_from_slice(&seq.to_le_bytes());
            v.extend_from_slice(&r.map_or(u64::MAX, |x| x).to_le_bytes());
        }
        v
    }

    fn restore(&mut self, b: &[u8]) {
        self.kv.clear();
        self.sessions.clear();
        if b.is_empty() {
            return;
        }
        let u32_at = |p: usize| u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
        let u64_at = |p: usize| u64::from_le_bytes(b[p..p + 8].try_into().unwrap());
        let mut p = 4;
        for _ in 0..u32_at(0) {
            self.kv.insert(b[p], u64_at(p + 1));
            p += 9;
        }
        let n = u32_at(p);
        p += 4;
        for _ in 0..n {
            let r = u64_at(p + 16);
            self.sessions
                .insert(u64_at(p), (u64_at(p + 8), (r != u64::MAX).then_some(r)));
            p += 24;
        }
    }

    fn apply(&mut self, ctx: &mut Ctx<'_, Msg>, e: &Entry) {
        self.oracle.borrow_mut().saw_apply(self.id, e);
        let EntryKind::Normal(data) = &e.kind else {
            return;
        };
        let (client, seq, op) = decode(data);
        let last = self.sessions.get(&client).map_or(0, |s| s.0);
        let reply = if seq <= last {
            // A retry of something already applied: do not apply twice.
            if seq == last {
                self.sessions[&client].1
            } else {
                return;
            }
        } else {
            let r = match op {
                KvOp::Put(k, v) => {
                    self.kv.insert(k, v);
                    None
                }
                KvOp::Get(k) => self.kv.get(&k).copied(),
            };
            self.sessions.insert(client, (seq, r));
            r
        };
        if self.waiting.remove(&(client, seq)).is_some() {
            ctx.send(
                CLIENT,
                Msg::Resp {
                    client,
                    seq,
                    reply: Reply::Ok(reply),
                },
            );
        }
    }

    /// Drain Raft's outputs after every input.
    fn pump(&mut self, ctx: &mut Ctx<'_, Msg>) {
        if let Some(s) = self.raft.take_snapshot() {
            self.restore(&s.data);
            self.oracle.borrow_mut().snapshots_restored += 1;
        }
        for e in self.raft.take_committed() {
            self.apply(ctx, &e);
            self.since_snapshot += 1;
        }
        for id in self.raft.take_reads() {
            if let Some((client, seq, key)) = self.reads.remove(&id) {
                let v = self.kv.get(&key).copied();
                ctx.send(
                    CLIENT,
                    Msg::Resp {
                        client,
                        seq,
                        reply: Reply::Ok(v),
                    },
                );
            }
        }
        if self.opts.snapshot_every > 0 && self.since_snapshot >= self.opts.snapshot_every {
            self.since_snapshot = 0;
            let applied = self.raft.applied();
            let data = self.snapshot_bytes();
            self.raft.compact(applied, data).unwrap();
        }
        self.oracle
            .borrow_mut()
            .saw_role(self.id, self.raft.role(), self.raft.term());
        for m in self.raft.take_messages() {
            ctx.send(m.to, Msg::Raft(m));
        }
    }
}

impl Actor for KvNode {
    type Msg = Msg;

    fn on_start(&mut self, ctx: &mut Ctx<'_, Msg>) {
        ctx.set_timer(TICK, TICK_TIMER);
        self.pump(ctx);
    }

    fn on_message(&mut self, ctx: &mut Ctx<'_, Msg>, _from: NodeId, msg: Msg) {
        match msg {
            Msg::Raft(m) => self.raft.step(m).unwrap(),
            Msg::Req { client, seq, op } => {
                let not_leader = |r: &Raft<DiskStorage>| Reply::NotLeader(r.leader());
                if !self.raft.is_leader() {
                    let reply = not_leader(&self.raft);
                    ctx.send(CLIENT, Msg::Resp { client, seq, reply });
                } else {
                    match op {
                        KvOp::Get(key) => {
                            let id = self.next_read;
                            self.next_read += 1;
                            self.reads.insert(id, (client, seq, key));
                            if self.raft.read(id).is_err() {
                                self.reads.remove(&id);
                            }
                        }
                        KvOp::Put(..) => {
                            if self.raft.propose(vec![encode(client, seq, op)]).is_ok() {
                                self.waiting.insert((client, seq), ());
                            }
                        }
                    }
                }
            }
            Msg::ChangeVoters(v) => {
                let _ = self.raft.propose_membership(v);
            }
            Msg::Transfer(to) => {
                let _ = self.raft.transfer_leadership(to);
            }
            Msg::Resp { .. } => {}
        }
        self.pump(ctx);
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_, Msg>, _t: u64) {
        self.raft.tick().unwrap();
        ctx.set_timer(TICK, TICK_TIMER);
        self.pump(ctx);
    }
}

/// Test-side clients with retries and a recorded history.
#[derive(Debug)]
pub struct Clients {
    pub history: Vec<(u8, Op<RegisterOp, Option<u64>>)>,
    clients: Vec<ClientState>,
    nodes: Vec<NodeId>,
    keys: u8,
    pub completed: u64,
    pub redirects: u64,
    pub timeouts: u64,
}

#[derive(Debug)]
struct ClientState {
    id: u64,
    seq: u64,
    target: NodeId,
    /// (history index, op, last send time)
    pending: Option<(usize, KvOp, u64)>,
}

pub const RETRY_AFTER: u64 = 120 * MS;

impl Clients {
    pub fn new(n: u64, nodes: Vec<NodeId>, keys: u8) -> Self {
        Self {
            history: Vec::new(),
            clients: (1..=n)
                .map(|id| ClientState {
                    id,
                    seq: 0,
                    target: nodes[0],
                    pending: None,
                })
                .collect(),
            nodes,
            keys,
            completed: 0,
            redirects: 0,
            timeouts: 0,
        }
    }

    pub fn set_nodes(&mut self, nodes: Vec<NodeId>) {
        self.nodes = nodes;
    }

    pub fn idle(&self) -> bool {
        self.clients.iter().all(|c| c.pending.is_none())
    }

    /// Advance clients: consume replies, retry, and (if `issue`) start new ops.
    pub fn step(&mut self, sim: &mut Simulation<KvNode>, rng: &mut dyn Rng, issue: bool) {
        let now = sim.now();
        for (_, _, msg) in sim.take_client_messages() {
            let Msg::Resp { client, seq, reply } = msg else {
                continue;
            };
            let c = &mut self.clients[usize::try_from(client - 1).unwrap()];
            let Some((h, op, _)) = c.pending else {
                continue;
            };
            if seq != c.seq {
                continue; // reply to an older request
            }
            match reply {
                Reply::Ok(v) => {
                    let entry = &mut self.history[h].1;
                    entry.ret = Some(now);
                    entry.output = Some(match op {
                        KvOp::Put(..) => None,
                        KvOp::Get(_) => v,
                    });
                    c.pending = None;
                    self.completed += 1;
                }
                Reply::NotLeader(hint) => {
                    self.redirects += 1;
                    c.target = hint.filter(|h| self.nodes.contains(h)).unwrap_or_else(|| {
                        self.nodes
                            [usize::try_from(rng.gen_range(0, self.nodes.len() as u64)).unwrap()]
                    });
                    sim.send(c.target, Msg::Req { client, seq, op });
                    c.pending = Some((h, op, now));
                }
            }
        }
        for c in &mut self.clients {
            match c.pending {
                Some((h, op, sent)) if now - sent >= RETRY_AFTER => {
                    self.timeouts += 1;
                    c.target = self.nodes
                        [usize::try_from(rng.gen_range(0, self.nodes.len() as u64)).unwrap()];
                    sim.send(
                        c.target,
                        Msg::Req {
                            client: c.id,
                            seq: c.seq,
                            op,
                        },
                    );
                    c.pending = Some((h, op, now));
                }
                None if issue && rng.gen_bool(0.3) => {
                    c.seq += 1;
                    let key = u8::try_from(rng.gen_range(0, u64::from(self.keys))).unwrap();
                    let op = if rng.gen_bool(0.5) {
                        KvOp::Put(key, c.id * 1_000_000 + c.seq)
                    } else {
                        KvOp::Get(key)
                    };
                    let input = match op {
                        KvOp::Put(_, v) => RegisterOp::Put(v),
                        KvOp::Get(_) => RegisterOp::Get,
                    };
                    self.history.push((
                        key,
                        Op {
                            client: c.id,
                            invoke: now,
                            ret: None,
                            input,
                            output: None,
                        },
                    ));
                    sim.send(
                        c.target,
                        Msg::Req {
                            client: c.id,
                            seq: c.seq,
                            op,
                        },
                    );
                    c.pending = Some((self.history.len() - 1, op, now));
                }
                _ => {}
            }
        }
    }

    /// Per-key histories (linearizability is compositional).
    pub fn by_key(&self) -> BTreeMap<u8, Vec<Op<RegisterOp, Option<u64>>>> {
        let mut m: BTreeMap<u8, Vec<_>> = BTreeMap::new();
        for (k, op) in &self.history {
            m.entry(*k).or_default().push(op.clone());
        }
        m
    }
}

/// The current leader with the highest term, if any node believes it leads.
pub fn leader(sim: &Simulation<KvNode>) -> Option<NodeId> {
    sim.node_ids()
        .into_iter()
        .filter_map(|id| {
            sim.actor(id)
                .map(|a| (a.raft.term(), id, a.raft.is_leader()))
        })
        .filter(|&(_, _, l)| l)
        .max()
        .map(|(_, id, _)| id)
}
