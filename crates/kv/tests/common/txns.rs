//! Drives transaction coordinators against a simulated cluster: routes each
//! request by key, follows redirects, and resends on timeouts and ambiguity (every
//! transactional request is idempotent).

#![allow(dead_code, clippy::unwrap_used)]

/// (sim time, txn label, outgoing?, node, request or reply).
pub type LogLine = (u64, String, bool, NodeId, String);

use std::collections::BTreeMap;

use stowage_common::{NodeId, Rng, Timestamp};
use stowage_kv::txn::{Isolation, Outcome, Program, Step, TxnCoord};
use stowage_kv::{KvError, Request, Response};
use stowage_sim::{MS, Simulation};

use super::cluster::{Msg, Node};

const WALL_ORIGIN: u64 = 1_793_923_200 * 1_000_000_000;
const RESEND_AFTER: u64 = 150 * MS;

struct InFlight {
    coord: usize,
    tag: u64,
    req: Request,
    target: NodeId,
    sent: u64,
}

pub struct Finished {
    pub label: String,
    pub outcome: Outcome,
    pub started: u64,
    pub ended: u64,
}

pub struct TxnDriver {
    nodes: Vec<NodeId>,
    coords: Vec<Option<(String, TxnCoord, u64)>>,
    inflight: BTreeMap<u64, InFlight>,
    next_token: u64,
    leader_cache: BTreeMap<Vec<u8>, NodeId>,
    clock: Timestamp,
    next_id: u128,
    pub finished: Vec<Finished>,
    pub resends: u64,
    pub why: BTreeMap<String, u64>,
    /// When set, every request sent and reply received: (sim time, txn label,
    /// outgoing?, node, request or reply).
    pub log: Option<Vec<LogLine>>,
    /// Start transactions as interactive ones: the program is let through one
    /// step per driver step, the way a SQL session issues statements.
    pub interactive: bool,
    gates: BTreeMap<usize, std::sync::Arc<std::sync::atomic::AtomicBool>>,
}

impl TxnDriver {
    pub fn new(nodes: Vec<NodeId>) -> Self {
        Self {
            nodes,
            coords: Vec::new(),
            inflight: BTreeMap::new(),
            next_token: 1,
            leader_cache: BTreeMap::new(),
            clock: Timestamp::MIN,
            next_id: 1,
            finished: Vec::new(),
            resends: 0,
            why: BTreeMap::new(),
            log: None,
            interactive: false,
            gates: BTreeMap::new(),
        }
    }

    pub fn debug_running(&self) -> Vec<String> {
        self.coords
            .iter()
            .flatten()
            .map(|(l, c, s)| {
                format!(
                    "{l} since {s}: {c:?} inflight {:?}",
                    self.inflight
                        .values()
                        .filter(|f| self
                            .coords
                            .iter()
                            .position(|x| x.as_ref().is_some_and(|(_, y, _)| std::ptr::eq(y, c)))
                            == Some(f.coord))
                        .map(|f| (
                            f.tag,
                            format!("{:?}", f.req).chars().take(120).collect::<String>()
                        ))
                        .collect::<Vec<_>>()
                )
            })
            .collect()
    }

    pub fn running(&self) -> usize {
        self.coords.iter().filter(|c| c.is_some()).count()
    }

    fn now(&mut self, sim: &Simulation<Node>) -> Timestamp {
        let wall = WALL_ORIGIN + sim.now();
        self.clock = if wall > self.clock.wall {
            Timestamp::new(wall, 0)
        } else {
            self.clock.next()
        };
        self.clock
    }

    fn observe(&mut self, ts: Timestamp) {
        if ts > self.clock && ts.wall < Timestamp::MAX.wall {
            self.clock = ts;
        }
    }

    pub fn start(
        &mut self,
        sim: &Simulation<Node>,
        rng: &mut dyn Rng,
        label: impl Into<String>,
        iso: Isolation,
        program: Program,
    ) {
        let prio = u32::try_from(rng.gen_range(1, 1 << 20)).unwrap();
        self.start_with_priority(sim, rng, label, iso, prio, program);
    }

    pub fn start_with_priority(
        &mut self,
        sim: &Simulation<Node>,
        rng: &mut dyn Rng,
        label: impl Into<String>,
        iso: Isolation,
        prio: u32,
        program: Program,
    ) {
        let now = self.now(sim);
        let id = (self.next_id << 32) | u128::from(rng.next_u64() as u32);
        self.next_id += 1;
        let coord = if self.interactive {
            use std::sync::atomic::Ordering;
            let gate = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let g = gate.clone();
            let mut inner = program;
            self.gates.insert(self.coords.len(), gate);
            TxnCoord::interactive(
                id,
                now,
                prio,
                iso,
                Box::new(move |reads, done| {
                    if g.swap(false, Ordering::SeqCst) {
                        inner(reads, done)
                    } else {
                        Step::Wait
                    }
                }),
            )
        } else {
            TxnCoord::new(id, now, prio, iso, program)
        };
        self.coords.push(Some((label.into(), coord, sim.now())));
    }

    fn route(&self, req: &Request, rng: &mut dyn Rng) -> NodeId {
        req.routing_key()
            .and_then(|k| self.leader_cache.get(k).copied())
            .unwrap_or_else(|| {
                self.nodes[usize::try_from(rng.gen_range(0, self.nodes.len() as u64)).unwrap()]
            })
    }

    fn send(
        &mut self,
        sim: &mut Simulation<Node>,
        coord: usize,
        tag: u64,
        req: Request,
        target: NodeId,
    ) {
        let token = self.next_token;
        self.next_token += 1;
        if let Some(log) = &mut self.log {
            let label = self
                .coords
                .get(coord)
                .and_then(|c| c.as_ref())
                .map_or_else(String::new, |(l, _, _)| l.clone());
            log.push((sim.now(), label, true, target, brief_req(&req)));
        }
        sim.send(
            target,
            Msg::Client {
                token,
                req: req.clone(),
            },
        );
        self.inflight.insert(
            token,
            InFlight {
                coord,
                tag,
                req,
                target,
                sent: sim.now(),
            },
        );
    }

    pub fn step(&mut self, sim: &mut Simulation<Node>, rng: &mut dyn Rng) {
        let now = self.now(sim);
        for (_, from, msg) in sim.take_client_messages() {
            let Msg::Reply { token, resp } = msg else {
                continue;
            };
            let Some(f) = self.inflight.remove(&token) else {
                continue;
            };
            if let Some(log) = &mut self.log {
                let label = self
                    .coords
                    .get(f.coord)
                    .and_then(|c| c.as_ref())
                    .map_or_else(String::new, |(l, _, _)| l.clone());
                log.push((
                    sim.now(),
                    label,
                    false,
                    from,
                    format!("{} => {}", brief_req(&f.req), brief_resp(&resp)),
                ));
            }
            let key = f.req.routing_key().map(<[u8]>::to_vec);
            let redirect = |me: &mut Self,
                            sim: &mut Simulation<Node>,
                            to: Option<NodeId>,
                            rng: &mut dyn Rng| {
                let to = to.filter(|n| me.nodes.contains(n)).unwrap_or_else(|| {
                    me.nodes[usize::try_from(rng.gen_range(0, me.nodes.len() as u64)).unwrap()]
                });
                if let Some(k) = &key {
                    me.leader_cache.insert(k.clone(), to);
                }
                me.resends += 1;
                me.send(sim, f.coord, f.tag, f.req.clone(), to);
            };
            if let Response::Error(e) = &resp {
                let n = format!("{e:?}");
                *self
                    .why
                    .entry(n.split([' ', '(', '{']).next().unwrap().to_string())
                    .or_default() += 1;
            }
            match resp {
                Response::Error(KvError::NotLeader { leader, .. }) => {
                    redirect(self, sim, leader, rng)
                }
                Response::Error(KvError::RangeNotFound { hint }) => redirect(self, sim, hint, rng),
                Response::Error(KvError::KeyMoved | KvError::Ambiguous | KvError::Busy(_)) => {
                    redirect(self, sim, None, rng)
                }
                resp => {
                    if let Some(k) = &key {
                        self.leader_cache.insert(k.clone(), from);
                    }
                    match &resp {
                        Response::Written(ts) | Response::WriteTooOld(ts) => self.observe(*ts),
                        Response::Txn {
                            commit_ts,
                            min_commit_ts,
                            ..
                        } => {
                            self.observe(*commit_ts);
                            self.observe(*min_commit_ts);
                        }
                        _ => {}
                    }
                    if let Some(Some((_, c, _))) = self.coords.get_mut(f.coord) {
                        c.on_response(f.tag, &f.req, resp, now);
                    }
                }
            }
        }
        // Resend anything unanswered for too long (idempotent).
        let stale: Vec<u64> = self
            .inflight
            .iter()
            .filter(|(_, f)| sim.now() - f.sent >= RESEND_AFTER)
            .map(|(&t, _)| t)
            .collect();
        for t in stale {
            let f = self.inflight.remove(&t).unwrap();
            self.resends += 1;
            *self.why.entry("timeout".into()).or_default() += 1;
            let to =
                self.nodes[usize::try_from(rng.gen_range(0, self.nodes.len() as u64)).unwrap()];
            self.send(sim, f.coord, f.tag, f.req, to);
        }
        let now = self.now(sim);
        for i in 0..self.coords.len() {
            let Some((_, c, _)) = self.coords[i].as_mut() else {
                continue;
            };
            c.tick(now);
            if c.is_idle()
                && let Some(g) = self.gates.get(&i)
            {
                g.store(true, std::sync::atomic::Ordering::SeqCst);
                c.resume();
            }
            let out = c.take_requests();
            for o in out {
                let to = self.route(&o.req, rng);
                self.send(sim, i, o.tag, o.req, to);
            }
            let done = self.coords[i].as_ref().is_some_and(|(_, c, _)| c.is_done());
            if done {
                let (label, c, started) = self.coords[i].take().unwrap();
                self.inflight.retain(|_, f| f.coord != i);
                self.finished.push(Finished {
                    label,
                    outcome: c.outcome.clone().unwrap(),
                    started,
                    ended: sim.now(),
                });
            }
        }
    }
}

/// `u64` <-> bytes helpers for workloads.
pub fn enc(v: u64) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

pub fn dec(v: &Option<Vec<u8>>) -> u64 {
    v.as_ref()
        .map_or(0, |b| u64::from_le_bytes(b[..8].try_into().unwrap()))
}

/// Lists for list-append: comma-separated decimals.
pub fn enc_list(l: &[u64]) -> Vec<u8> {
    l.iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",")
        .into_bytes()
}

pub fn dec_list(v: &Option<Vec<u8>>) -> Vec<u64> {
    match v {
        None => Vec::new(),
        Some(b) if b.is_empty() => Vec::new(),
        Some(b) => String::from_utf8_lossy(b)
            .split(',')
            .map(|x| x.parse().unwrap())
            .collect(),
    }
}

fn txt(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn val(v: Option<&Vec<u8>>) -> String {
    match v {
        Some(b) if b.len() == 8 => dec(&Some(b.clone())).to_string(),
        Some(b) => txt(b),
        None => "∅".into(),
    }
}

/// One-line rendering of a transactional request (traces, docs).
pub fn brief_req(req: &Request) -> String {
    let id = |id: u128| format!("{:x}", id >> 32);
    match req {
        Request::TxnGet { key, ts, txn, skip } => {
            format!(
                "TxnGet {} @{ts} txn={}{}",
                txt(key),
                txn.map_or("-".into(), id),
                if skip.is_empty() {
                    String::new()
                } else {
                    format!(
                        " skip={:?}",
                        skip.iter().map(|s| id(*s)).collect::<Vec<_>>()
                    )
                }
            )
        }
        Request::TxnPut { key, value, txn } => format!(
            "TxnPut {}={} txn={} write_ts={} anchor={}",
            txt(key),
            val(value.as_ref()),
            id(txn.id),
            txn.write_ts,
            txt(&txn.anchor)
        ),
        Request::EndTxn {
            txn,
            commit,
            commit_ts,
        } => format!(
            "EndTxn txn={} {} @{commit_ts}",
            id(txn.id),
            if *commit { "COMMIT" } else { "ABORT" }
        ),
        Request::Resolve {
            key,
            txn_id,
            commit,
            commit_ts,
        } => format!(
            "Resolve {} txn={} {} @{commit_ts}",
            txt(key),
            id(*txn_id),
            if *commit { "commit" } else { "abort" }
        ),
        Request::Push { pushee, kind } => format!("Push txn={} {kind:?}", id(pushee.id)),
        Request::Heartbeat { txn } => format!("Heartbeat txn={}", id(txn.id)),
        Request::Refresh {
            start,
            from,
            to,
            txn,
            ..
        } => format!("Refresh {} ({from}, {to}] txn={}", txt(start), id(*txn)),
        other => format!("{other:?}"),
    }
}

pub fn brief_resp(resp: &Response) -> String {
    match resp {
        Response::Value(v) => format!("Value({})", val(v.as_ref())),
        Response::Conflict(t) => format!(
            "Conflict(intent of txn={:x} write_ts={} prio={})",
            t.id >> 32,
            t.write_ts,
            t.priority
        ),
        Response::Txn {
            status,
            commit_ts,
            min_commit_ts,
        } => format!("Txn {{ {status:?}, commit_ts={commit_ts}, min_commit_ts={min_commit_ts} }}"),
        other => format!("{other:?}"),
    }
}
