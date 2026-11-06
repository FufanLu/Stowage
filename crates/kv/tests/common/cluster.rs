//! A Stowage KV cluster inside the deterministic simulator, plus routing clients
//! that record a history for the linearizability checker. Shared by
//! `tests/simulation.rs` and `examples/trace_split.rs`.

#![allow(dead_code, clippy::unwrap_used)]

use std::collections::BTreeMap;

use stowage_common::{NodeId, Rng};
use stowage_kv::{KvError, PeerMsg, Request, Response, Store, StoreConfig};
use stowage_sim::linearizability::{Op, RegisterOp};
use stowage_sim::{Actor, CLIENT, Ctx, MS, NodeEnv, Simulation};

pub const TICK: u64 = 10 * MS;

#[derive(Clone)]
pub enum Msg {
    Peer(PeerMsg),
    Client { token: u64, req: Request },
    Reply { token: u64, resp: Response },
}

impl std::fmt::Debug for Msg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Msg::Peer(PeerMsg::Raft { range, msg }) => write!(
                f,
                "{} t{} {}",
                short_range(*range),
                msg.term,
                short_body(&msg.body)
            ),
            Msg::Peer(PeerMsg::Heartbeat { node, ranges }) => {
                write!(f, "StoreHeartbeat(n{node}, {} ranges)", ranges.len())
            }
            Msg::Peer(m) => write!(f, "{m:?}"),
            Msg::Client { token, req } => write!(f, "Client#{token} {req:?}"),
            Msg::Reply { token, resp } => write!(f, "Reply#{token} {resp:?}"),
        }
    }
}

/// `r1`, or `r(n1)#3` for ids allocated by node 1 (`(node << 40) | n`).
pub fn short_range(id: u64) -> String {
    if id >> 40 == 0 {
        format!("r{id}")
    } else {
        format!("r(n{})#{}", id >> 40, id & ((1 << 40) - 1))
    }
}

fn short_body(b: &stowage_raft::Body) -> String {
    use stowage_kv::types::Command;
    use stowage_raft::{Body, EntryKind};
    let entry = |e: &stowage_raft::Entry| {
        let what = match &e.kind {
            EntryKind::Noop => "noop".to_string(),
            EntryKind::Config(m) if m.is_joint() => {
                format!("config joint {:?}→{:?}", m.outgoing, m.voters)
            }
            EntryKind::Config(m) => format!("config {:?}", m.voters),
            EntryKind::Normal(d) => match Command::decode(d) {
                Ok(Command::Write { ops, .. }) => format!(
                    "write {:?}",
                    ops.iter()
                        .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
                        .collect::<Vec<_>>()
                ),
                Ok(Command::Split { key, new_id, .. }) => format!(
                    "split at {:?} → {}",
                    String::from_utf8_lossy(&key),
                    short_range(new_id)
                ),
                Err(_) => "?".to_string(),
            },
        };
        format!("{}:{what}", e.index)
    };
    match b {
        Body::Append {
            prev_index,
            entries,
            commit,
            ..
        } => {
            format!(
                "Append(prev {prev_index}, [{}], commit {commit})",
                entries.iter().map(entry).collect::<Vec<_>>().join(", ")
            )
        }
        Body::AppendResp {
            success: true,
            match_index,
            ..
        } => format!("AppendResp(ok {match_index})"),
        Body::AppendResp { hint, .. } => format!("AppendResp(reject {hint:?})"),
        Body::InstallSnapshot { snapshot } => {
            let pairs = stowage_kv::types::SnapshotData::decode(&snapshot.data)
                .map_or(0, |d| d.pairs.len());
            format!(
                "InstallSnapshot(@{} t{}, {pairs} pairs, {} B)",
                snapshot.meta.index,
                snapshot.meta.term,
                snapshot.data.len()
            )
        }
        Body::Heartbeat { commit, .. } => format!("Heartbeat(commit {commit})"),
        Body::HeartbeatResp { .. } => "HeartbeatResp".to_string(),
        other => format!("{other:?}"),
    }
}

pub struct Node {
    pub store: Store,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.store.fmt(f)
    }
}

impl Node {
    pub fn new(env: &NodeEnv, cfg: &StoreConfig) -> Self {
        let mut cfg = cfg.clone();
        cfg.node = env.id;
        Self {
            store: Store::open(cfg, env.disk()).unwrap(),
        }
    }

    fn drain(&mut self, ctx: &mut Ctx<'_, Msg>) {
        self.store.flush().unwrap();
        for (to, m) in self.store.take_peer_msgs().unwrap() {
            ctx.send(to, Msg::Peer(m));
        }
        for (token, resp) in self.store.take_responses().unwrap() {
            ctx.send(CLIENT, Msg::Reply { token, resp });
        }
    }
}

impl Actor for Node {
    type Msg = Msg;

    fn on_start(&mut self, ctx: &mut Ctx<'_, Msg>) {
        ctx.set_timer(TICK, 0);
        self.drain(ctx);
    }

    fn on_message(&mut self, ctx: &mut Ctx<'_, Msg>, from: NodeId, msg: Msg) {
        match msg {
            Msg::Peer(m) => self.store.handle_peer(from, m).unwrap(),
            Msg::Client { token, req } => self.store.handle_client(token, req).unwrap(),
            Msg::Reply { .. } => {}
        }
        self.drain(ctx);
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_, Msg>, _t: u64) {
        self.store.tick().unwrap();
        ctx.set_timer(TICK, 0);
        self.drain(ctx);
    }
}

pub fn key(i: u64) -> Vec<u8> {
    format!("user{i:04}").into_bytes()
}

#[derive(Debug, Clone)]
struct Pending {
    hist: usize,
    req: Request,
    target: NodeId,
    sent: u64,
    attempt: u64,
}

#[derive(Debug)]
struct Client {
    id: u64,
    seq: u64,
    pending: Option<Pending>,
}

/// Clients that route by a shared leader cache, follow redirects, and retry.
/// Reads are retried freely; a write whose outcome is unknown (timeout or
/// `Ambiguous`) is recorded as never-returned instead of retried, because
/// retrying could apply it twice.
#[derive(Debug)]
pub struct Clients {
    clients: Vec<Client>,
    nodes: Vec<NodeId>,
    keys: u64,
    value_size: usize,
    /// key -> node believed to lead its range
    leader_cache: BTreeMap<Vec<u8>, NodeId>,
    pub history: Vec<(u64, Op<RegisterOp, Option<u64>>)>,
    /// Node that answered each history entry (for debugging).
    pub servers: Vec<Option<NodeId>>,
    /// Replica state at the answering node when the answer arrived.
    pub notes: Vec<String>,
    pub completed: u64,
    pub ambiguous: u64,
    pub redirects: u64,
    pub timeouts: u64,
}

pub const TIMEOUT: u64 = 150 * MS;

impl Clients {
    pub fn new(n: u64, nodes: Vec<NodeId>, keys: u64, value_size: usize) -> Self {
        Self {
            clients: (1..=n)
                .map(|id| Client {
                    id,
                    seq: 0,
                    pending: None,
                })
                .collect(),
            nodes,
            keys,
            value_size,
            leader_cache: BTreeMap::new(),
            history: Vec::new(),
            servers: Vec::new(),
            notes: Vec::new(),
            completed: 0,
            ambiguous: 0,
            redirects: 0,
            timeouts: 0,
        }
    }

    /// Keys of operations still waiting for an answer.
    pub fn pending_keys(&self) -> Vec<Vec<u8>> {
        self.clients
            .iter()
            .filter_map(|c| {
                c.pending
                    .as_ref()
                    .map(|p| p.req.routing_key().unwrap().to_vec())
            })
            .collect()
    }

    pub fn set_nodes(&mut self, nodes: Vec<NodeId>) {
        self.leader_cache.retain(|_, n| nodes.contains(n));
        self.nodes = nodes;
    }

    pub fn idle(&self) -> bool {
        self.clients.iter().all(|c| c.pending.is_none())
    }

    /// Unique per (client, request, attempt): a late reply to an earlier request
    /// must never be mistaken for the current one.
    fn token(client: u64, seq: u64, attempt: u64) -> u64 {
        (client << 48) | ((seq & 0xff_ffff) << 24) | (attempt & 0xff_ffff)
    }

    fn random_node(&self, rng: &mut dyn Rng) -> NodeId {
        self.nodes[usize::try_from(rng.gen_range(0, self.nodes.len() as u64)).unwrap()]
    }

    fn send(&mut self, sim: &mut Simulation<Node>, ci: usize, target: NodeId) {
        let now = sim.now();
        let c = &mut self.clients[ci];
        let p = c.pending.as_mut().unwrap();
        p.attempt += 1;
        p.target = target;
        p.sent = now;
        let token = Self::token(c.id, c.seq, p.attempt);
        sim.send(
            target,
            Msg::Client {
                token,
                req: p.req.clone(),
            },
        );
    }

    fn finish(&mut self, ci: usize, now: u64, output: Option<Option<u64>>) {
        let p = self.clients[ci].pending.take().unwrap();
        self.servers[p.hist] = Some(p.target);
        let op = &mut self.history[p.hist].1;
        match output {
            Some(out) => {
                op.ret = Some(now);
                op.output = Some(out);
                self.completed += 1;
            }
            None => self.ambiguous += 1, // stays ret = None: may or may not have happened
        }
    }

    pub fn step(&mut self, sim: &mut Simulation<Node>, rng: &mut dyn Rng, issue: bool) {
        let now = sim.now();
        for (_, _, msg) in sim.take_client_messages() {
            let Msg::Reply { token, resp } = msg else {
                continue;
            };
            let cid = token >> 48;
            let ci = usize::try_from(cid - 1).unwrap();
            let Some(p) = self.clients[ci].pending.clone() else {
                continue;
            };
            if token != Self::token(cid, self.clients[ci].seq, p.attempt) {
                continue; // a reply to an earlier request or attempt
            }
            let is_write = matches!(p.req, Request::Put { .. });
            let key = p.req.routing_key().unwrap().to_vec();
            if let Some(n) = sim.actor(p.target) {
                self.notes[p.hist] = n
                    .store
                    .local_ranges()
                    .iter()
                    .filter(|(d, ..)| d.contains(&key))
                    .map(|(d, l, t, a)| format!("{d} leader={l} term={t} applied={a}"))
                    .collect::<Vec<_>>()
                    .join("; ");
            }
            match resp {
                Response::Done => {
                    assert!(is_write, "Done for a read");
                    self.finish(ci, now, Some(None));
                }
                Response::Value(v) => {
                    assert!(!is_write, "Value for a write");
                    let v = v.map(|b| u64::from_le_bytes(b[..8].try_into().unwrap()));
                    self.finish(ci, now, Some(v));
                }
                Response::Error(KvError::Ambiguous) => self.finish(ci, now, None),
                Response::Error(KvError::NotLeader { leader, .. }) => {
                    self.redirects += 1;
                    let to = leader
                        .filter(|l| self.nodes.contains(l))
                        .unwrap_or_else(|| self.random_node(rng));
                    self.leader_cache.insert(key, to);
                    self.send(sim, ci, to);
                }
                Response::Error(KvError::RangeNotFound { hint }) => {
                    self.redirects += 1;
                    let to = hint
                        .filter(|l| self.nodes.contains(l))
                        .unwrap_or_else(|| self.random_node(rng));
                    self.leader_cache.insert(key, to);
                    self.send(sim, ci, to);
                }
                Response::Error(KvError::KeyMoved | KvError::Busy(_)) => {
                    let to = self.random_node(rng);
                    self.send(sim, ci, to);
                }
                other => panic!("unexpected reply {other:?} (write: {is_write})"),
            }
        }
        for ci in 0..self.clients.len() {
            match self.clients[ci].pending.clone() {
                Some(p) if now - p.sent >= TIMEOUT => {
                    self.timeouts += 1;
                    if matches!(p.req, Request::Put { .. }) {
                        self.finish(ci, now, None);
                    } else {
                        let to = self.random_node(rng);
                        self.send(sim, ci, to);
                    }
                }
                None if issue && rng.gen_bool(0.3) => {
                    let c = &mut self.clients[ci];
                    c.seq += 1;
                    let k = rng.gen_range(0, self.keys);
                    let value = c.id * 1_000_000 + c.seq;
                    let (req, input) = if rng.gen_bool(0.5) {
                        let mut v = value.to_le_bytes().to_vec();
                        v.resize(self.value_size.max(8), b'.');
                        (
                            Request::Put {
                                key: key(k),
                                value: v,
                            },
                            RegisterOp::Put(value),
                        )
                    } else {
                        (Request::Get { key: key(k) }, RegisterOp::Get)
                    };
                    self.history.push((
                        k,
                        Op {
                            client: c.id,
                            invoke: now,
                            ret: None,
                            input,
                            output: None,
                        },
                    ));
                    self.servers.push(None);
                    self.notes.push(String::new());
                    c.pending = Some(Pending {
                        hist: self.history.len() - 1,
                        req,
                        target: 0,
                        sent: now,
                        attempt: 0,
                    });
                    let to = self
                        .leader_cache
                        .get(&key(k))
                        .copied()
                        .unwrap_or_else(|| self.random_node(rng));
                    self.send(sim, ci, to);
                }
                _ => {}
            }
        }
    }

    pub fn servers_by_key(&self) -> BTreeMap<u64, Vec<(Option<NodeId>, String)>> {
        let mut m: BTreeMap<u64, Vec<_>> = BTreeMap::new();
        for (((k, _), s), n) in self.history.iter().zip(&self.servers).zip(&self.notes) {
            m.entry(*k).or_default().push((*s, n.clone()));
        }
        m
    }

    pub fn by_key(&self) -> BTreeMap<u64, Vec<Op<RegisterOp, Option<u64>>>> {
        let mut m: BTreeMap<u64, Vec<_>> = BTreeMap::new();
        for (k, op) in &self.history {
            m.entry(*k).or_default().push(op.clone());
        }
        m
    }
}
