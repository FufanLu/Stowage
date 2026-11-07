//! Multi-Raft under the simulator: automatic splits, rebalancing, dead-store
//! replacement, partitions and crashes. Every run ends with three checks:
//! 1. the client history is linearizable per key;
//! 2. the ranges tile the key space exactly (no gaps, no overlaps);
//! 3. every replica of every range holds identical data.

#![allow(clippy::unwrap_used)] // test code

mod common;

use std::collections::BTreeMap;

use common::cluster::{Clients, Node};
use stowage_common::Rng;
use stowage_common::os::Xoshiro256;
use stowage_kv::{RangeDescriptor, StoreConfig};
use stowage_sim::linearizability::{Register, check};
use stowage_sim::{CrashMode, MS, NetConfig, SEC, SimConfig, Simulation, for_each_seed};
use stowage_storage::BTreeOptions;

struct World {
    sim: Simulation<Node>,
    clients: Clients,
    rng: Xoshiro256,
    events: Vec<String>,
}

fn config(nodes: u64, split_bytes: u64, rebalance: bool) -> StoreConfig {
    let mut c = StoreConfig::new(0, (1..=nodes).collect());
    c.split_bytes = split_bytes;
    c.compact_every = 64;
    c.heartbeat_ticks = 20;
    c.schedule_ticks = 10;
    c.store_timeout_ticks = 150;
    c.rebalance = rebalance;
    c.engine = BTreeOptions {
        cache_pages: 256,
        checkpoint_wal_bytes: 4 << 20,
    };
    c
}

fn world(seed: u64, nodes: u64, cfg: StoreConfig, net: NetConfig, keys: u64) -> World {
    stowage_kv::store::divergence::enable();
    let mut sim = Simulation::new(
        seed,
        SimConfig {
            net,
            crash_mode: CrashMode::Torn,
            record_trace: false,
        },
        move |env| Node::new(env, &cfg),
    );
    for id in 1..=nodes {
        sim.add_node(id);
    }
    World {
        sim,
        clients: Clients::new(8, (1..=nodes).collect(), keys, 120),
        rng: Xoshiro256::seeded(seed ^ 0x5EED),
        events: Vec::new(),
    }
}

impl World {
    fn run(&mut self, dur: u64, issue: bool) {
        let end = self.sim.now() + dur;
        while self.sim.now() < end {
            self.sim.run_for(MS);
            self.clients.step(&mut self.sim, &mut self.rng, issue);
        }
    }

    fn nemesis(&mut self, max_down: usize) {
        let ids = self.sim.node_ids();
        let down: Vec<u64> = ids
            .iter()
            .copied()
            .filter(|&i| !self.sim.is_up(i))
            .collect();
        match self.rng.gen_range(0, 5) {
            0 => {
                let (mut a, mut b) = (Vec::new(), Vec::new());
                for &i in &ids {
                    if self.rng.gen_bool(0.5) {
                        a.push(i)
                    } else {
                        b.push(i)
                    }
                }
                self.sim.heal();
                self.sim.partition(&a, &b);
                self.events
                    .push(format!("{} partition {a:?} | {b:?}", self.sim.now() / MS));
            }
            1 => {
                self.sim.heal();
                self.events.push(format!("{} heal", self.sim.now() / MS));
            }
            2 if down.len() < max_down => {
                let up: Vec<u64> = ids.iter().copied().filter(|&i| self.sim.is_up(i)).collect();
                let v = up[usize::try_from(self.rng.gen_range(0, up.len() as u64)).unwrap()];
                self.sim.crash(v);
                self.events
                    .push(format!("{} crash n{v}", self.sim.now() / MS));
            }
            3 => {
                if let Some(&d) = down.first() {
                    self.sim.restart(d);
                    self.events
                        .push(format!("{} restart n{d}", self.sim.now() / MS));
                }
            }
            _ => {}
        }
    }

    fn settle(&mut self) {
        self.sim.heal();
        for id in self.sim.node_ids() {
            if !self.sim.is_up(id) {
                self.sim.restart(id);
            }
        }
        for _ in 0..300 {
            self.run(100 * MS, false);
            if self.clients.idle() {
                break;
            }
        }
        if !self.clients.idle() {
            for k in self.clients.pending_keys() {
                eprintln!("pending key {:?}", String::from_utf8_lossy(&k));
                for id in self.sim.node_ids() {
                    let Some(n) = self.sim.actor(id) else {
                        continue;
                    };
                    for (d, ..) in n.store.local_ranges() {
                        if d.contains(&k) {
                            eprintln!("  n{id}: {d} {:?}", n.store.raft_debug(d.id));
                        }
                    }
                }
            }
            panic!("operations still pending after healing");
        }
        // Let replication, splits and GC quiesce.
        self.run(3 * SEC, false);
    }

    /// The authoritative descriptor of every range: the one its leader holds.
    fn leader_descs(&self) -> Vec<(RangeDescriptor, u64)> {
        let mut best: BTreeMap<u64, (RangeDescriptor, u64)> = BTreeMap::new();
        for id in self.sim.node_ids() {
            let Some(n) = self.sim.actor(id) else {
                continue;
            };
            for (d, leader, _, _) in n.store.local_ranges() {
                if leader {
                    best.insert(d.id, (d, id));
                }
            }
        }
        let mut v: Vec<_> = best.into_values().collect();
        v.sort_by(|a, b| a.0.start.cmp(&b.0.start));
        v
    }

    fn check_all(&self) {
        let servers = self.clients.servers_by_key();
        for (k, ops) in self.clients.by_key() {
            if let Err(v) = check(&Register, &ops) {
                let stuck = ops[v.stuck_at].invoke;
                let mut near: Vec<_> = ops
                    .iter()
                    .zip(&servers[&k])
                    .filter(|(o, _)| o.invoke + 20 * SEC >= stuck && o.invoke <= stuck + 100 * MS)
                    .collect();
                near.sort_by_key(|(o, _)| o.invoke);
                for (o, srv) in near {
                    eprintln!(
                        "  c{} [{} .. {:?}] {:?} -> {:?} via {:?}",
                        o.client,
                        o.invoke / MS,
                        o.ret.map(|r| r / MS),
                        o.input,
                        o.output,
                        srv
                    );
                }
                for e in &self.events {
                    eprintln!("  event {e}");
                }
                let kk = common::cluster::key(k);
                for id in self.sim.node_ids() {
                    if let Some(n) = self.sim.actor(id) {
                        for (d, leader, term, applied) in n.store.local_ranges() {
                            if d.contains(&kk) {
                                let v = n
                                    .store
                                    .scan_local(&d)
                                    .unwrap()
                                    .into_iter()
                                    .find(|(key, _)| *key == kk)
                                    .map(|(_, v)| u64::from_le_bytes(v[..8].try_into().unwrap()));
                                eprintln!(
                                    "  n{id}: {d} leader={leader} term={term} applied={applied} value={v:?}"
                                );
                            }
                        }
                    }
                }
                panic!(
                    "key {k}: {} ops not linearizable; stuck at {:?}",
                    ops.len(),
                    ops[v.stuck_at]
                );
            }
        }
        let descs = self.leader_descs();
        assert!(!descs.is_empty(), "no range has a leader");
        assert!(
            descs[0].0.start.is_empty(),
            "first range starts at {:?}",
            descs[0].0
        );
        for w in descs.windows(2) {
            if w[0].0.end != w[1].0.start {
                let k = w[0].0.end.clone();
                for id in self.sim.node_ids() {
                    let Some(n) = self.sim.actor(id) else {
                        continue;
                    };
                    for rid in n.store.replica_ids() {
                        let dbg = n.store.raft_debug(rid).unwrap_or_default();
                        let local = n.store.local_ranges();
                        let covers = local.iter().any(|(d, ..)| d.id == rid && d.contains(&k));
                        if covers || dbg.contains("uninit=true") {
                            eprintln!("  n{id} r{rid}: {}", &dbg[..dbg.len().min(400)]);
                        }
                    }
                }
            }
            assert_eq!(
                w[0].0.end, w[1].0.start,
                "gap or overlap between {} and {}",
                w[0].0, w[1].0
            );
        }
        assert!(
            descs.last().unwrap().0.end.is_empty(),
            "last range is bounded"
        );
        for (d, leader) in &descs {
            let want = self
                .sim
                .actor(*leader)
                .unwrap()
                .store
                .scan_local(d)
                .unwrap();
            for r in &d.replicas {
                let n = self
                    .sim
                    .actor(*r)
                    .unwrap_or_else(|| panic!("replica n{r} of {d} is down"));
                if !n.store.local_ranges().iter().any(|(ld, ..)| ld.id == d.id) {
                    for (ld, l, t, a) in n.store.local_ranges() {
                        eprintln!("  n{r} has {ld} leader={l} term={t} applied={a}");
                    }
                    eprintln!(
                        "  leader n{leader} stats {:?}",
                        self.sim.actor(*leader).unwrap().store.stats()
                    );
                    eprintln!(
                        "  leader raft: {:?}",
                        self.sim.actor(*leader).unwrap().store.raft_debug(d.id)
                    );
                    eprintln!("  n{r} stats {:?}", n.store.stats());
                    eprintln!("  n{r} replica: {:?}", n.store.raft_debug(d.id));
                    panic!("n{r} is a voter of {d} but has no replica");
                }
                assert_eq!(
                    n.store.scan_local(d).unwrap(),
                    want,
                    "replica n{r} of {d} diverged"
                );
            }
        }
    }
}

#[test]
fn splits_under_chaos_stay_linearizable() {
    for_each_seed(&[1, 2, 3], |seed| {
        let net = NetConfig {
            drop_prob: 0.02,
            dup_prob: 0.01,
            ..NetConfig::default()
        };
        let mut w = world(seed, 3, config(3, 3_000, false), net, 60);
        w.run(2 * SEC, true);
        for _ in 0..20 {
            w.nemesis(1);
            w.run(500 * MS, true);
        }
        w.settle();
        w.check_all();
        let ranges = w.leader_descs().len();
        eprintln!(
            "seed {seed}: {} ops completed, {} ambiguous, {ranges} ranges, now {} ms",
            w.clients.completed,
            w.clients.ambiguous,
            w.sim.now() / MS
        );
        assert!(
            ranges >= 4,
            "seed {seed}: only {ranges} ranges after splitting"
        );
    });
}

#[test]
fn rebalancing_spreads_replicas_and_leaders() {
    for_each_seed(&[11, 12], |seed| {
        let mut w = world(seed, 5, config(5, 2_500, true), NetConfig::default(), 80);
        w.run(15 * SEC, true);
        w.settle();
        w.check_all();
        let descs = w.leader_descs();
        let mut load: BTreeMap<u64, usize> = (1..=5).map(|n| (n, 0)).collect();
        let mut leads: BTreeMap<u64, usize> = (1..=5).map(|n| (n, 0)).collect();
        for (d, l) in &descs {
            assert_eq!(d.replicas.len(), 3, "{d} is not 3-way replicated");
            for r in &d.replicas {
                *load.get_mut(r).unwrap() += 1;
            }
            *leads.get_mut(l).unwrap() += 1;
        }
        let spread =
            |m: &BTreeMap<u64, usize>| m.values().max().unwrap() - m.values().min().unwrap();
        assert!(descs.len() >= 5, "seed {seed}: only {} ranges", descs.len());
        assert!(spread(&load) <= 2, "seed {seed}: replica load {load:?}");
        // The scheduler acts on a leader imbalance of 2 or more, and only when some
        // range led by the busiest node has a replica on the idlest one, so a
        // snapshot can catch it one transfer short of even.
        assert!(spread(&leads) <= 3, "seed {seed}: leader load {leads:?}");
    });
}

#[test]
fn dead_store_is_replaced() {
    for_each_seed(&[21, 22], |seed| {
        let mut w = world(seed, 4, config(4, 4_000, true), NetConfig::default(), 40);
        w.run(4 * SEC, true);
        // Node 3 dies for good: its replicas must move to node 4.
        w.sim.crash(3);
        w.clients.set_nodes(vec![1, 2, 4]);
        w.run(10 * SEC, true);
        for _ in 0..300 {
            w.run(100 * MS, false);
            if w.clients.idle() {
                break;
            }
        }
        w.run(3 * SEC, false);
        let descs = w.leader_descs();
        for (d, _) in &descs {
            assert!(
                !d.replicas.contains(&3),
                "seed {seed}: {d} still has the dead store"
            );
            assert_eq!(d.replicas.len(), 3, "seed {seed}: {d}");
        }
        for (k, ops) in w.clients.by_key() {
            assert!(
                check(&Register, &ops).is_ok(),
                "seed {seed}: key {k} not linearizable"
            );
        }
    });
}
