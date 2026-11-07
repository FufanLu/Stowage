//! Real traces for `docs/illustrated/05-a-transfer-transaction.md`.
//!
//! ```text
//! cargo run --release -p stowage-kv --example trace_txn
//! ```
//!
//! Three nodes, two ranges (split at "m"), accounts `alice` (range 1) and `rob`
//! (range 2). Every transactional request and reply is printed, together with the
//! engine pairs it created or removed on node 1.
//!
//! Part 1: a transfer that commits without interference.
//! Part 2: an audit reads `alice` just before transfer T2 writes it: the
//!         timestamp cache moves T2's write above the read, T2 refreshes and
//!         commits higher.
//! Part 3: high-priority H reads into low-priority L's intent and pushes L's
//!         timestamp; L refreshes and commits above H's read; H's own refresh
//!         then fails and H restarts.

#![allow(clippy::unwrap_used, dead_code)]

#[path = "../tests/common/cluster.rs"]
mod cluster;
#[path = "../tests/common/txns.rs"]
mod txns;

use std::collections::BTreeMap;

use cluster::{Msg, Node};
use stowage_common::Rng;
use stowage_common::os::Xoshiro256;
use stowage_kv::mvcc::{self, Intent, TxnRecord};
use stowage_kv::txn::{Isolation, Step, TxnOp};
use stowage_kv::{KvError, Request, Response, StoreConfig};
use stowage_sim::{MS, SEC, SimConfig, Simulation};
use txns::{TxnDriver, dec, enc};

fn cfg() -> StoreConfig {
    let mut c = StoreConfig::new(0, vec![1, 2, 3]);
    c.split_bytes = u64::MAX; // splits only when asked
    c.heartbeat_ticks = 20;
    c.rebalance = false;
    c
}

fn request(sim: &mut Simulation<Node>, token: u64, req: Request) -> Response {
    for to in [1, 2, 3, 1, 2, 3] {
        sim.send(
            to,
            Msg::Client {
                token,
                req: req.clone(),
            },
        );
        for _ in 0..2000 {
            sim.run_for(MS);
            for (_, _, m) in sim.take_client_messages() {
                if let Msg::Reply { token: tk, resp } = m
                    && tk == token
                {
                    if matches!(resp, Response::Error(KvError::NotLeader { .. })) {
                        break;
                    }
                    return resp;
                }
            }
        }
    }
    panic!("no leader answered");
}

fn hex(b: &[u8]) -> String {
    b.iter()
        .map(|x| format!("{x:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// A raw engine pair, explained.
fn explain(k: &[u8], v: &[u8]) -> String {
    match k[0] {
        mvcc::VERSION => {
            let (user, ts) = mvcc::decode_version_key(k).unwrap();
            let val =
                mvcc::decode_value(v).map_or("tombstone".into(), |b| dec(&Some(b)).to_string());
            format!(
                "version  {}@{ts} = {val}\n             key {}",
                String::from_utf8_lossy(&user),
                hex(k)
            )
        }
        mvcc::INTENT => {
            let user = mvcc::decode_intent_key(k).unwrap();
            let i = Intent::decode(v).unwrap();
            format!(
                "intent   {} = {} (txn {:x} write_ts {} prio {})\n             key {}",
                String::from_utf8_lossy(&user),
                i.value
                    .as_ref()
                    .map_or("delete".into(), |b| dec(&Some(b.clone())).to_string()),
                i.txn.id >> 32,
                i.txn.write_ts,
                i.txn.priority,
                hex(k)
            )
        }
        mvcc::TXN => {
            let r = TxnRecord::decode(v).unwrap();
            format!(
                "record   {:?} commit_ts {} min_commit_ts {}\n             key {}",
                r.status,
                r.commit_ts,
                r.min_commit_ts,
                hex(k)
            )
        }
        _ => format!("? {}", hex(k)),
    }
}

fn engine(sim: &Simulation<Node>) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let store = &sim.actor(1).unwrap().store;
    let mut m = BTreeMap::new();
    for (desc, ..) in store.local_ranges() {
        m.extend(store.scan_local(&desc).unwrap());
    }
    m
}

struct Tracer {
    seen_log: usize,
    state: BTreeMap<Vec<u8>, Vec<u8>>,
    t0: u64,
}

impl Tracer {
    fn flush(&mut self, sim: &Simulation<Node>, d: &mut TxnDriver) {
        let log = d.log.as_ref().unwrap();
        for (t, label, out, node, what) in &log[self.seen_log..] {
            let ms = (t - self.t0) as f64 / MS as f64;
            if *out {
                println!("{ms:7.1}ms {label:>8} ─► n{node}  {what}");
            } else {
                let what = what.split_once(" => ").map_or(what.as_str(), |(_, r)| r);
                println!("{ms:7.1}ms {label:>8} ◄─ n{node}  {what}");
            }
        }
        self.seen_log = log.len();
        let now = engine(sim);
        for (k, v) in &now {
            if self.state.get(k) != Some(v) {
                println!("           n1 engine + {}", explain(k, v));
            }
        }
        for (k, v) in &self.state {
            if !now.contains_key(k) {
                println!("           n1 engine − {}", explain(k, v));
            }
        }
        self.state = now;
    }
}

fn step(sim: &mut Simulation<Node>, d: &mut TxnDriver, rng: &mut Xoshiro256, tr: &mut Tracer) {
    sim.run_for(MS);
    d.step(sim, rng);
    tr.flush(sim, d);
}

fn until_done(
    sim: &mut Simulation<Node>,
    d: &mut TxnDriver,
    rng: &mut Xoshiro256,
    tr: &mut Tracer,
) {
    for _ in 0..5000 {
        step(sim, d, rng, tr);
        if d.running() == 0 {
            // Let the resolves apply on node 1 too.
            for _ in 0..20 {
                step(sim, d, rng, tr);
            }
            return;
        }
    }
    panic!("transactions did not finish");
}

fn transfer(from: &'static str, to: &'static str, amt: u64) -> stowage_kv::txn::Program {
    Box::new(move |reads, done| match done {
        0 => Step::Op(TxnOp::Get(from.into())),
        1 => Step::Op(TxnOp::Get(to.into())),
        2 => Step::Op(TxnOp::Put(from.into(), enc(dec(&reads[0]) - amt))),
        3 => Step::Op(TxnOp::Put(to.into(), enc(dec(&reads[1]) + amt))),
        _ => Step::Commit,
    })
}

fn wait_for(
    sim: &mut Simulation<Node>,
    d: &mut TxnDriver,
    rng: &mut Xoshiro256,
    tr: &mut Tracer,
    what: &str,
) {
    for _ in 0..5000 {
        step(sim, d, rng, tr);
        if d.log
            .as_ref()
            .unwrap()
            .iter()
            .any(|(_, _, out, _, s)| !out && s.contains(what))
        {
            return;
        }
    }
    panic!("never saw {what}");
}

fn main() {
    let mut sim = Simulation::new(
        5,
        SimConfig {
            net: stowage_sim::NetConfig::default(),
            crash_mode: stowage_sim::CrashMode::Torn,
            record_trace: false,
        },
        |env| Node::new(env, &cfg()),
    );
    for id in 1..=3 {
        sim.add_node(id);
    }
    sim.run_for(SEC);
    let r = request(&mut sim, 1, Request::Split { key: b"m".to_vec() });
    println!("split at \"m\": {r:?}");
    sim.run_for(SEC);
    let mut rng = Xoshiro256::seeded(7);
    let mut d = TxnDriver::new(vec![1, 2, 3]);
    d.log = Some(Vec::new());
    let mut tr = Tracer {
        seen_log: 0,
        state: engine(&sim),
        t0: sim.now(),
    };
    println!("\n== setup: alice = rob = 100 ==");
    d.start(
        &sim,
        &mut rng,
        "init",
        Isolation::Serializable,
        Box::new(|_, done| match done {
            0 => Step::Op(TxnOp::Put(b"alice".to_vec(), enc(100))),
            1 => Step::Op(TxnOp::Put(b"rob".to_vec(), enc(100))),
            _ => Step::Commit,
        }),
    );
    until_done(&mut sim, &mut d, &mut rng, &mut tr);

    println!("\n== part 1: transfer 30 alice → rob ==");
    tr.t0 = sim.now();
    d.start_with_priority(
        &sim,
        &mut rng,
        "T1",
        Isolation::Serializable,
        100,
        transfer("alice", "rob", 30),
    );
    until_done(&mut sim, &mut d, &mut rng, &mut tr);

    println!("\n== part 2: an audit reads alice just before T2 writes it ==");
    tr.t0 = sim.now();
    d.start_with_priority(
        &sim,
        &mut rng,
        "T2",
        Isolation::Serializable,
        100,
        transfer("alice", "rob", 10),
    );
    wait_for(&mut sim, &mut d, &mut rng, &mut tr, "Written"); // (matches init's replies at once)
    sim.run_for(2 * MS);
    d.start_with_priority(
        &sim,
        &mut rng,
        "audit",
        Isolation::Serializable,
        5000,
        Box::new(|_, done| match done {
            0 => Step::Op(TxnOp::Get(b"alice".to_vec())),
            1 => Step::Op(TxnOp::Get(b"rob".to_vec())),
            _ => Step::Commit,
        }),
    );
    until_done(&mut sim, &mut d, &mut rng, &mut tr);

    println!("\n== part 3: high-priority H reads into low-priority L's intent ==");
    tr.t0 = sim.now();
    let before = d.log.as_ref().unwrap().len();
    d.start_with_priority(
        &sim,
        &mut rng,
        "L",
        Isolation::Serializable,
        7,
        transfer("alice", "rob", 5),
    );
    for _ in 0..5000 {
        step(&mut sim, &mut d, &mut rng, &mut tr);
        if d.log.as_ref().unwrap()[before..]
            .iter()
            .any(|(_, l, out, _, s)| l == "L" && !out && s.contains("=> Written"))
        {
            break;
        }
    }
    d.start_with_priority(
        &sim,
        &mut rng,
        "H",
        Isolation::Serializable,
        9000,
        transfer("alice", "rob", 1),
    );
    until_done(&mut sim, &mut d, &mut rng, &mut tr);

    println!("\n== outcomes ==");
    for f in &d.finished {
        println!(
            "{:>6}: {:?} ({} ms)",
            f.label,
            f.outcome,
            (f.ended - f.started) / MS
        );
    }
    let _ = rng.next_u64();
}
