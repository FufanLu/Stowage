//! Transactions under the simulator, checked three ways:
//! - bank transfers must conserve money (every committed audit sees the total);
//! - list-append histories go through the Elle-style checker: under SSI no
//!   dependency cycle at all, under SI nothing but G2 (write skew);
//! - a hand-built write skew that SI must allow and SSI must prevent.

#![allow(clippy::unwrap_used)]

mod common;

use common::cluster::Node;
use common::txns::{TxnDriver, dec, dec_list, enc, enc_list};
use stowage_common::Rng;
use stowage_common::os::Xoshiro256;
use stowage_kv::StoreConfig;
use stowage_kv::txn::{Isolation, Outcome, Step, TxnOp};
use stowage_sim::elle::{self, Mop};
use stowage_sim::{CrashMode, MS, NetConfig, SEC, SimConfig, Simulation, for_each_seed};
use stowage_storage::BTreeOptions;

fn cluster(seed: u64, net: NetConfig) -> Simulation<Node> {
    let mut c = StoreConfig::new(0, vec![1, 2, 3]);
    c.split_bytes = 4_000;
    c.compact_every = 64;
    c.heartbeat_ticks = 20;
    c.max_offset_nanos = 20 * MS;
    c.engine = BTreeOptions {
        cache_pages: 256,
        checkpoint_wal_bytes: 4 << 20,
    };
    stowage_kv::store::divergence::enable();
    let mut sim = Simulation::new(
        seed,
        SimConfig {
            net,
            crash_mode: CrashMode::Torn,
            record_trace: false,
        },
        move |env| Node::new(env, &c),
    );
    for id in 1..=3 {
        sim.add_node(id);
    }
    sim
}

fn key(i: u64) -> Vec<u8> {
    format!("acct{i:02}").into_bytes()
}

fn run(sim: &mut Simulation<Node>, d: &mut TxnDriver, rng: &mut Xoshiro256, dur: u64) {
    let end = sim.now() + dur;
    while sim.now() < end {
        sim.run_for(MS);
        d.step(sim, rng);
    }
}

fn nemesis(sim: &mut Simulation<Node>, rng: &mut Xoshiro256) {
    let down: Vec<u64> = (1..=3).filter(|&i| !sim.is_up(i)).collect();
    match rng.gen_range(0, 4) {
        0 => {
            let lone = rng.gen_range(1, 4);
            sim.heal();
            sim.isolate(lone);
        }
        1 => sim.heal(),
        2 if down.is_empty() => sim.crash(rng.gen_range(1, 4)),
        _ => {
            for d in down {
                sim.restart(d);
            }
        }
    }
}

fn settle(sim: &mut Simulation<Node>, d: &mut TxnDriver, rng: &mut Xoshiro256) {
    sim.heal();
    for id in 1..=3 {
        if !sim.is_up(id) {
            sim.restart(id);
        }
    }
    for _ in 0..600 {
        run(sim, d, rng, 50 * MS);
        if d.running() == 0 {
            return;
        }
    }
    panic!(
        "{} transactions still running after healing:\n{}",
        d.running(),
        d.debug_running().join("\n")
    );
}

/// Debugging a failing seed: `STOWAGE_TXN_DUMP=1` prints every request and
/// reply, every committed version (node 1) and every committed transaction, so
/// a broken invariant can be traced to the transaction and version behind it.
/// `STOWAGE_TXN_CALM=1` turns the nemesis and message loss off.
fn dumping() -> bool {
    std::env::var("STOWAGE_TXN_DUMP").is_ok()
}

fn dump_history(sim: &Simulation<Node>, d: &TxnDriver) {
    if !dumping() {
        return;
    }
    let store = &sim.actor(1).unwrap().store;
    for (desc, ..) in store.local_ranges() {
        for (k, v) in store.scan_local(&desc).unwrap() {
            if k[0] == stowage_kv::mvcc::VERSION {
                let (u, ts) = stowage_kv::mvcc::decode_version_key(&k).unwrap();
                println!(
                    "V {} {ts:?} {:?}",
                    String::from_utf8_lossy(&u),
                    stowage_kv::mvcc::decode_value(&v).map(|b| dec(&Some(b)))
                );
            }
        }
    }
    for (t, l, out, n, w) in d.log.iter().flatten() {
        println!("L {t} {l} {} n{n} {w}", if *out { "->" } else { "<-" });
    }
    for f in &d.finished {
        if let Outcome::Committed {
            commit_ts,
            reads,
            restarts,
        } = &f.outcome
        {
            println!(
                "T {} {commit_ts:?} r={restarts} {:?} [{}..{}]",
                f.label,
                reads.iter().map(dec).collect::<Vec<_>>(),
                f.started,
                f.ended
            );
        }
    }
}

const ACCOUNTS: u64 = 5;
const START: u64 = 100;

fn bank(seed: u64, iso: Isolation) -> (usize, usize) {
    let calm = std::env::var("STOWAGE_TXN_CALM").is_ok();
    let net = NetConfig {
        drop_prob: if calm { 0.0 } else { 0.02 },
        ..NetConfig::default()
    };
    let mut sim = cluster(seed, net);
    let mut rng = Xoshiro256::seeded(seed ^ 0xBA4C);
    let mut d = TxnDriver::new(vec![1, 2, 3]);
    if dumping() {
        d.log = Some(Vec::new());
    }
    run(&mut sim, &mut d, &mut rng, SEC);
    d.start(
        &sim,
        &mut rng,
        "init",
        iso,
        Box::new(|_, done| {
            if done < ACCOUNTS as usize {
                Step::Op(TxnOp::Put(key(done as u64), enc(START)))
            } else {
                Step::Commit
            }
        }),
    );
    settle(&mut sim, &mut d, &mut rng);
    assert!(
        matches!(d.finished[0].outcome, Outcome::Committed { .. }),
        "init failed: {:?}",
        d.finished[0].outcome
    );
    for round in 0..400u64 {
        if round % 10 == 0 && !calm {
            nemesis(&mut sim, &mut rng);
        }
        while d.running() < 6 {
            if rng.gen_bool(0.3) {
                d.start(
                    &sim,
                    &mut rng,
                    "audit",
                    iso,
                    Box::new(|_, done| {
                        if done < ACCOUNTS as usize {
                            Step::Op(TxnOp::Get(key(done as u64)))
                        } else {
                            Step::Commit
                        }
                    }),
                );
            } else {
                let a = rng.gen_range(0, ACCOUNTS);
                let b = (a + rng.gen_range(1, ACCOUNTS)) % ACCOUNTS;
                let amt = rng.gen_range(1, 30);
                d.start(
                    &sim,
                    &mut rng,
                    "transfer",
                    iso,
                    Box::new(move |reads, done| match done {
                        0 => Step::Op(TxnOp::Get(key(a))),
                        1 => Step::Op(TxnOp::Get(key(b))),
                        2 if dec(&reads[0]) < amt => Step::Commit, // insufficient funds: read-only
                        2 => Step::Op(TxnOp::Put(key(a), enc(dec(&reads[0]) - amt))),
                        3 => Step::Op(TxnOp::Put(key(b), enc(dec(&reads[1]) + amt))),
                        _ => Step::Commit,
                    }),
                );
            }
        }
        run(&mut sim, &mut d, &mut rng, 100 * MS);
    }
    settle(&mut sim, &mut d, &mut rng);
    dump_history(&sim, &d);
    let mut audits = 0;
    let mut transfers = 0;
    for f in &d.finished {
        match (&f.label[..], &f.outcome) {
            ("audit", Outcome::Committed { reads, .. }) => {
                let total: u64 = reads.iter().map(dec).sum();
                assert_eq!(
                    total,
                    ACCOUNTS * START,
                    "seed {seed}: an audit saw {total}: {reads:?}"
                );
                audits += 1;
            }
            ("transfer", Outcome::Committed { .. }) => transfers += 1,
            (_, Outcome::GaveUp { restarts }) => {
                panic!("seed {seed}: a transaction gave up after {restarts} restarts")
            }
            _ => {}
        }
    }
    (audits, transfers)
}

#[test]
fn bank_conserves_money_under_ssi() {
    for_each_seed(&[1, 2], |seed| {
        let (audits, transfers) = bank(seed, Isolation::Serializable);
        assert!(
            audits > 20 && transfers > 50,
            "seed {seed}: {audits} audits, {transfers} transfers"
        );
    });
}

#[test]
fn bank_conserves_money_under_si() {
    for_each_seed(&[3, 4], |seed| {
        let (audits, transfers) = bank(seed, Isolation::Snapshot);
        assert!(
            audits > 20 && transfers > 50,
            "seed {seed}: {audits} audits, {transfers} transfers"
        );
    });
}

/// List-append over a few hot keys. Each mop is a Read (one Get) or an Append
/// (a Get of the list, then a Put of the list plus the value).
fn list_append(seed: u64, iso: Isolation) -> elle::Report {
    let calm = std::env::var("STOWAGE_TXN_CALM").is_ok();
    let net = NetConfig {
        drop_prob: if calm { 0.0 } else { 0.02 },
        ..NetConfig::default()
    };
    let mut sim = cluster(seed, net);
    let mut rng = Xoshiro256::seeded(seed ^ 0x11E);
    let mut d = TxnDriver::new(vec![1, 2, 3]);
    run(&mut sim, &mut d, &mut rng, SEC);
    #[allow(clippy::type_complexity)]
    let mut programs: Vec<(String, Vec<(bool, u64, u64)>)> = Vec::new();
    let mut next_value = 1u64;
    for round in 0..600u64 {
        if round % 20 == 0 && !calm {
            nemesis(&mut sim, &mut rng);
        }
        while d.running() < 6 {
            let n = rng.gen_range(1, 4);
            let mops: Vec<(bool, u64, u64)> = (0..n)
                .map(|_| {
                    let k = rng.gen_range(0, 4);
                    if rng.gen_bool(0.5) {
                        next_value += 1;
                        (true, k, next_value)
                    } else {
                        (false, k, 0)
                    }
                })
                .collect();
            let label = format!("t{}", programs.len());
            programs.push((label.clone(), mops.clone()));
            d.start(
                &sim,
                &mut rng,
                label,
                iso,
                Box::new(move |reads, done| {
                    // Walk the mops: a read costs one op, an append costs two.
                    let mut op = 0usize;
                    let mut r = 0usize;
                    for &(append, k, v) in &mops {
                        let kk = format!("list{k}").into_bytes();
                        if !append {
                            if done == op {
                                return Step::Op(TxnOp::Get(kk));
                            }
                            op += 1;
                            r += 1;
                        } else {
                            if done == op {
                                return Step::Op(TxnOp::Get(kk));
                            }
                            if done == op + 1 {
                                let mut l = dec_list(&reads[r]);
                                l.push(v);
                                return Step::Op(TxnOp::Put(kk, enc_list(&l)));
                            }
                            op += 2;
                            r += 1;
                        }
                    }
                    Step::Commit
                }),
            );
        }
        run(&mut sim, &mut d, &mut rng, 80 * MS);
    }
    settle(&mut sim, &mut d, &mut rng);
    // Final read of every key, so the version order is fully observed.
    d.start(
        &sim,
        &mut rng,
        "final",
        iso,
        Box::new(|_, done| {
            if done < 4 {
                Step::Op(TxnOp::Get(format!("list{done}").into_bytes()))
            } else {
                Step::Commit
            }
        }),
    );
    settle(&mut sim, &mut d, &mut rng);
    if dumping() {
        let mut restarts = 0;
        let (mut c, mut a) = (0, 0);
        let mut lat = Vec::new();
        for f in &d.finished {
            match &f.outcome {
                Outcome::Committed { restarts: r, .. } => {
                    c += 1;
                    restarts += r;
                    lat.push((f.ended - f.started) / MS);
                }
                _ => a += 1,
            }
        }
        lat.sort_unstable();
        println!("{:?}", d.why);
        println!(
            "committed {c} other {a} restarts {restarts} resends {} lat p50 {} p90 {} max {}",
            d.resends,
            lat[lat.len() / 2],
            lat[lat.len() * 9 / 10],
            lat[lat.len() - 1]
        );
    }
    let by_label: std::collections::HashMap<&str, &Vec<(bool, u64, u64)>> =
        programs.iter().map(|(l, m)| (l.as_str(), m)).collect();
    let mut history = Vec::new();
    for (i, f) in d.finished.iter().enumerate() {
        let (committed, reads) = match &f.outcome {
            Outcome::Committed { reads, .. } => (true, reads.clone()),
            Outcome::GaveUp { restarts } => {
                panic!("seed {seed}: gave up after {restarts} restarts")
            }
            Outcome::Aborted { reads } => (false, reads.clone()),
        };
        let ops = if f.label == "final" {
            (0..4)
                .map(|k| Mop::Read(k, dec_list(&reads[k as usize])))
                .collect()
        } else {
            let mops = by_label[f.label.as_str()];
            mops.iter()
                .enumerate()
                .map(|(j, &(append, k, v))| {
                    if append {
                        Mop::Append(k, v)
                    } else {
                        Mop::Read(k, dec_list(&reads[j]))
                    }
                })
                .collect()
        };
        history.push(elle::Txn {
            id: i,
            committed,
            ops,
        });
    }
    elle::check(&history)
}

fn report(seed: u64, iso: &str, r: &elle::Report) {
    if dumping() {
        let mut kinds: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
        for a in &r.anomalies {
            if let elle::Anomaly::Cycle { kind, .. } = a {
                *kinds.entry(kind).or_default() += 1;
            }
        }
        println!(
            "ELLE {iso} seed {seed}: {} committed, edges {:?}, cycles {kinds:?}",
            r.committed, r.edges
        );
    }
}

#[test]
fn list_append_is_serializable_under_ssi() {
    for_each_seed(&[11, 12], |seed| {
        let r = list_append(seed, Isolation::Serializable);
        report(seed, "SSI", &r);
        assert!(
            r.committed > 120,
            "seed {seed}: only {} committed",
            r.committed
        );
        assert!(
            r.edges.get(&elle::Edge::Rw).copied().unwrap_or(0) > 0,
            "no anti-dependencies: workload too tame"
        );
        assert!(
            r.violates_serializable().is_empty(),
            "seed {seed}: {:?}",
            r.anomalies
        );
    });
}

#[test]
fn list_append_under_si_shows_at_most_write_skew() {
    for_each_seed(&[21, 22], |seed| {
        let r = list_append(seed, Isolation::Snapshot);
        report(seed, "SI", &r);
        assert!(
            r.committed > 120,
            "seed {seed}: only {} committed",
            r.committed
        );
        assert!(
            r.violates_si().is_empty(),
            "seed {seed}: {:?}",
            r.violates_si()
        );
    });
}

/// Two doctors on call: each checks that both are on call, then takes itself off.
/// Under SI both commit (write skew) and nobody is left; SSI must stop one.
fn doctors(iso: Isolation) -> (u64, usize) {
    let mut sim = cluster(5, NetConfig::default());
    let mut rng = Xoshiro256::seeded(5);
    let mut d = TxnDriver::new(vec![1, 2, 3]);
    run(&mut sim, &mut d, &mut rng, SEC);
    d.start(
        &sim,
        &mut rng,
        "init",
        iso,
        Box::new(|_, done| match done {
            0 => Step::Op(TxnOp::Put(b"alice".to_vec(), enc(1))),
            1 => Step::Op(TxnOp::Put(b"bob".to_vec(), enc(1))),
            _ => Step::Commit,
        }),
    );
    settle(&mut sim, &mut d, &mut rng);
    for me in ["alice", "bob"] {
        d.start(
            &sim,
            &mut rng,
            me,
            iso,
            Box::new(move |reads, done| match done {
                0 => Step::Op(TxnOp::Get(b"alice".to_vec())),
                1 => Step::Op(TxnOp::Get(b"bob".to_vec())),
                2 if dec(&reads[0]) + dec(&reads[1]) >= 2 => {
                    Step::Op(TxnOp::Put(me.as_bytes().to_vec(), enc(0)))
                }
                _ => Step::Commit,
            }),
        );
    }
    settle(&mut sim, &mut d, &mut rng);
    d.start(
        &sim,
        &mut rng,
        "check",
        iso,
        Box::new(|_, done| match done {
            0 => Step::Op(TxnOp::Get(b"alice".to_vec())),
            1 => Step::Op(TxnOp::Get(b"bob".to_vec())),
            _ => Step::Commit,
        }),
    );
    settle(&mut sim, &mut d, &mut rng);
    let Outcome::Committed { reads, .. } = &d.finished.last().unwrap().outcome else {
        panic!()
    };
    let restarts = d
        .finished
        .iter()
        .map(|f| {
            if let Outcome::Committed { restarts, .. } = f.outcome {
                restarts as usize
            } else {
                0
            }
        })
        .sum();
    (dec(&reads[0]) + dec(&reads[1]), restarts)
}

#[test]
fn write_skew_si_allows_ssi_prevents() {
    let (on_call_si, _) = doctors(Isolation::Snapshot);
    assert_eq!(
        on_call_si, 0,
        "SI should let both doctors leave (write skew)"
    );
    let (on_call_ssi, restarts) = doctors(Isolation::Serializable);
    assert_eq!(on_call_ssi, 1, "SSI must keep one doctor on call");
    assert!(
        restarts >= 1,
        "SSI prevented the skew by restarting a transaction"
    );
}
