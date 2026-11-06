//! Raft under the deterministic simulator: partitions, crashes, message loss and
//! duplication, membership changes, snapshots and leadership transfer, with the
//! client history checked for linearizability and Raft's safety properties
//! asserted continuously by the [`Oracle`].

#![allow(clippy::unwrap_used)] // test code

mod common;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use common::kv::{Clients, KvNode, Msg, NodeOpts, Oracle, SharedOracle, leader};
use stowage_common::Rng;
use stowage_common::os::Xoshiro256;
use stowage_raft::{Membership, ReadMode};
use stowage_sim::linearizability::{Register, check};
use stowage_sim::{CrashMode, MS, NetConfig, SEC, SimConfig, Simulation, for_each_seed};

struct World {
    sim: Simulation<KvNode>,
    oracle: SharedOracle,
    clients: Clients,
    rng: Xoshiro256,
}

fn world(seed: u64, n: u64, voters: &[u64], opts: NodeOpts, net: NetConfig) -> World {
    let oracle: SharedOracle = Rc::new(RefCell::new(Oracle::default()));
    let o = oracle.clone();
    let voters: BTreeSet<u64> = voters.iter().copied().collect();
    let mut sim = Simulation::new(
        seed,
        SimConfig {
            net,
            crash_mode: CrashMode::Torn,
            record_trace: false,
        },
        move |env| {
            let mut opts = opts.clone();
            // Nodes outside the initial voter set start empty and are added later.
            if !voters.contains(&env.id) {
                opts.initial = Membership::default();
            }
            KvNode::new(env, opts, o.clone())
        },
    );
    for id in 1..=n {
        sim.add_node(id);
    }
    World {
        sim,
        oracle,
        clients: Clients::new(8, (1..=n).collect(), 4),
        rng: Xoshiro256::seeded(seed ^ 0xC11E),
    }
}

fn opts(voters: &[u64], read_mode: ReadMode, snapshot_every: u64) -> NodeOpts {
    NodeOpts {
        read_mode,
        snapshot_every,
        initial: Membership::new(voters.iter().copied()),
    }
}

impl World {
    /// Run for `dur`, stepping clients every millisecond.
    fn run(&mut self, dur: u64, issue: bool) {
        let end = self.sim.now() + dur;
        while self.sim.now() < end {
            self.sim.run_for(MS);
            self.clients.step(&mut self.sim, &mut self.rng, issue);
        }
    }

    /// One random fault (or repair), Jepsen-nemesis style.
    fn nemesis(&mut self, max_down: usize) {
        let ids = self.sim.node_ids();
        let down: Vec<u64> = ids
            .iter()
            .copied()
            .filter(|&i| !self.sim.is_up(i))
            .collect();
        match self.rng.gen_range(0, 6) {
            0 => {
                // Random bisection.
                let mut a = Vec::new();
                let mut b = Vec::new();
                for &i in &ids {
                    if self.rng.gen_bool(0.5) {
                        a.push(i)
                    } else {
                        b.push(i)
                    }
                }
                self.sim.heal();
                self.sim.partition(&a, &b);
            }
            1 => {
                if let Some(l) = leader(&self.sim) {
                    self.sim.heal();
                    self.sim.isolate(l);
                }
            }
            2 => self.sim.heal(),
            3 if down.len() < max_down => {
                let up: Vec<u64> = ids.iter().copied().filter(|&i| self.sim.is_up(i)).collect();
                let victim = up[usize::try_from(self.rng.gen_range(0, up.len() as u64)).unwrap()];
                self.sim.crash(victim);
            }
            4 => {
                if let Some(&d) = down.first() {
                    self.sim.restart(d);
                }
            }
            _ => {}
        }
    }

    /// Leader churn: kill or cut off the leader often, restart quickly, and make
    /// non-transitive partitions where some nodes still see both sides. This is
    /// what it takes to hit Figure-8 histories, lost votes and stale lease reads.
    fn churn(&mut self) {
        let ids = self.sim.node_ids();
        let down: Vec<u64> = ids
            .iter()
            .copied()
            .filter(|&i| !self.sim.is_up(i))
            .collect();
        for d in down {
            if self.rng.gen_bool(0.7) {
                self.sim.restart(d);
            }
        }
        let leader = leader(&self.sim);
        match self.rng.gen_range(0, 5) {
            0 => {
                if let Some(l) = leader {
                    self.sim.crash(l);
                }
            }
            1 => {
                // Cut the leader off from a random subset of followers only.
                if let Some(l) = leader {
                    let others: Vec<u64> = ids
                        .iter()
                        .copied()
                        .filter(|&i| i != l && self.rng.gen_bool(0.5))
                        .collect();
                    self.sim.heal();
                    self.sim.partition(&[l], &others);
                }
            }
            2 => {
                let up: Vec<u64> = ids.iter().copied().filter(|&i| self.sim.is_up(i)).collect();
                if up.len() > ids.len() / 2 + 1 {
                    let v = up[usize::try_from(self.rng.gen_range(0, up.len() as u64)).unwrap()];
                    self.sim.crash(v);
                }
            }
            3 => self.sim.heal(),
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
        // Let every outstanding client operation finish.
        for _ in 0..200 {
            self.run(100 * MS, false);
            if self.clients.idle() {
                break;
            }
        }
        if !self.clients.idle() {
            for id in self.sim.node_ids() {
                if let Some(a) = self.sim.actor(id) {
                    eprintln!(
                        "n{id}: {:?} term {} commit {} applied {} last {} progress {:?}",
                        a.raft.role(),
                        a.raft.term(),
                        a.raft.commit(),
                        a.raft.applied(),
                        a.raft.last_index(),
                        a.raft.progress()
                    );
                }
            }
            eprintln!("{:?}", self.clients);
            panic!("client operations still pending after healing");
        }
    }

    fn check_linearizable(&self) {
        for (key, ops) in self.clients.by_key() {
            if let Err(v) = check(&Register, &ops) {
                let op = &ops[v.stuck_at];
                panic!(
                    "key {key}: history of {} ops is not linearizable; stuck at {:?}",
                    ops.len(),
                    op
                );
            }
        }
    }
}

fn chaos(seed: u64, read_mode: ReadMode) -> (u64, usize) {
    let voters = [1, 2, 3, 4, 5];
    let net = NetConfig {
        drop_prob: 0.03,
        dup_prob: 0.02,
        ..NetConfig::default()
    };
    let mut w = world(seed, 5, &voters, opts(&voters, read_mode, 64), net);
    w.run(SEC, true);
    for _ in 0..30 {
        w.nemesis(2);
        w.run(400 * MS, true);
    }
    w.settle();
    w.check_linearizable();
    let leaders = w.oracle.borrow().leaders.len();
    (w.clients.completed, leaders)
}

#[test]
fn linearizable_under_chaos_read_index() {
    for_each_seed(&[1, 2, 3, 4], |seed| {
        let (completed, terms) = chaos(seed, ReadMode::ReadIndex);
        assert!(
            completed > 300,
            "seed {seed}: only {completed} ops completed"
        );
        assert!(terms > 1, "seed {seed}: nemesis never caused a re-election");
    });
}

#[test]
fn linearizable_under_chaos_lease_reads() {
    for_each_seed(&[11, 12, 13, 14], |seed| {
        let (completed, _) = chaos(seed, ReadMode::Lease);
        assert!(
            completed > 300,
            "seed {seed}: only {completed} ops completed"
        );
    });
}

fn churn_run(seed: u64, read_mode: ReadMode) -> u64 {
    let voters = [1, 2, 3, 4, 5];
    let net = NetConfig {
        drop_prob: 0.10,
        dup_prob: 0.05,
        ..NetConfig::default()
    };
    let mut w = world(seed, 5, &voters, opts(&voters, read_mode, 0), net);
    w.run(SEC, true);
    for _ in 0..80 {
        w.churn();
        let pause = 60 * MS + w.rng.gen_range(0, 140 * MS);
        w.run(pause, true);
    }
    w.settle();
    w.check_linearizable();
    w.oracle.borrow().leaders.len() as u64
}

#[test]
fn linearizable_under_leader_churn_read_index() {
    for_each_seed(&[51, 52, 53, 54, 55, 56], |seed| {
        let terms = churn_run(seed, ReadMode::ReadIndex);
        assert!(terms > 10, "seed {seed}: only {terms} terms had a leader");
    });
}

#[test]
fn linearizable_under_leader_churn_lease_reads() {
    for_each_seed(&[61, 62, 63, 64, 65, 66], |seed| {
        churn_run(seed, ReadMode::Lease);
    });
}

#[test]
fn membership_change_under_load() {
    for_each_seed(&[21, 22, 23], |seed| {
        let mut w = world(
            seed,
            5,
            &[1, 2, 3],
            opts(&[1, 2, 3], ReadMode::ReadIndex, 32),
            NetConfig::default(),
        );
        w.run(SEC, true);
        // {1,2,3} -> {1,2,3,4,5} -> {3,4,5}, each by joint consensus.
        for target in [vec![1, 2, 3, 4, 5], vec![3, 4, 5]] {
            let want: BTreeSet<u64> = target.iter().copied().collect();
            let mut done = false;
            for _ in 0..50 {
                if let Some(l) = leader(&w.sim) {
                    let m = w.sim.actor(l).unwrap().raft.membership().clone();
                    if m.voters == want
                        && !m.is_joint()
                        && w.sim.actor(l).unwrap().raft.commit()
                            >= w.sim.actor(l).unwrap().raft.last_index()
                    {
                        done = true;
                        break;
                    }
                    if !m.is_joint() && m.voters != want {
                        w.sim.send(l, Msg::ChangeVoters(want.clone()));
                    }
                }
                w.run(100 * MS, true);
            }
            assert!(done, "seed {seed}: membership did not reach {want:?}");
            if let Some(l) = leader(&w.sim)
                && (l == 1 || l == 2)
            {
                w.sim.crash(l); // exercise re-election after the change
            }
            w.run(500 * MS, true);
        }
        // Removed nodes no longer get client traffic, and must not disrupt.
        w.clients.set_nodes(vec![3, 4, 5]);
        w.run(2 * SEC, true);
        w.settle();
        w.check_linearizable();
        let l = leader(&w.sim).expect("a leader");
        assert!(
            [3, 4, 5].contains(&l),
            "leader {l} is not in the final voter set"
        );
    });
}

#[test]
fn lagging_node_catches_up_by_snapshot() {
    for_each_seed(&[31, 32], |seed| {
        let voters = [1, 2, 3];
        let mut w = world(
            seed,
            3,
            &voters,
            opts(&voters, ReadMode::ReadIndex, 16),
            NetConfig::default(),
        );
        w.run(SEC, true);
        let l = leader(&w.sim).unwrap();
        let victim = voters.iter().copied().find(|&i| i != l).unwrap();
        w.sim.crash(victim);
        w.clients
            .set_nodes(voters.iter().copied().filter(|&i| i != victim).collect());
        w.run(5 * SEC, true); // the log is compacted many times meanwhile
        let restored_before = w.oracle.borrow().snapshots_restored;
        w.sim.restart(victim);
        w.clients.set_nodes(voters.to_vec());
        w.settle();
        w.run(SEC, false);
        let l = leader(&w.sim).unwrap();
        let (a, b) = (w.sim.actor(l).unwrap(), w.sim.actor(victim).unwrap());
        assert_eq!(a.kv, b.kv, "seed {seed}: lagging node did not converge");
        assert!(
            w.oracle.borrow().snapshots_restored > restored_before,
            "seed {seed}: no snapshot was installed"
        );
        w.check_linearizable();
    });
}

#[test]
fn leadership_transfer_moves_the_leader() {
    for_each_seed(&[41, 42, 43], |seed| {
        let voters = [1, 2, 3];
        let mut w = world(
            seed,
            3,
            &voters,
            opts(&voters, ReadMode::ReadIndex, 0),
            NetConfig::default(),
        );
        w.run(SEC, true);
        let from = leader(&w.sim).unwrap();
        let to = voters.iter().copied().find(|&i| i != from).unwrap();
        w.sim.send(from, Msg::Transfer(to));
        w.run(300 * MS, true);
        assert_eq!(
            leader(&w.sim),
            Some(to),
            "seed {seed}: transfer {from} -> {to} failed"
        );
        w.settle();
        w.check_linearizable();
    });
}

#[test]
fn minority_side_cannot_commit() {
    let voters = [1, 2, 3, 4, 5];
    let mut w = world(
        7,
        5,
        &voters,
        opts(&voters, ReadMode::ReadIndex, 0),
        NetConfig::default(),
    );
    w.run(SEC, true);
    let old = leader(&w.sim).unwrap();
    let follower = voters.iter().copied().find(|&i| i != old).unwrap();
    let minority = vec![old, follower];
    let majority: Vec<u64> = voters
        .iter()
        .copied()
        .filter(|i| !minority.contains(i))
        .collect();
    let commit_before = w.sim.actor(old).unwrap().raft.commit();
    w.sim.partition(&minority, &majority);
    w.run(2 * SEC, true);
    // The old leader stepped down (check-quorum) and committed nothing new.
    let o = w.sim.actor(old).unwrap();
    assert!(!o.raft.is_leader(), "minority leader kept leading");
    assert_eq!(
        o.raft.commit(),
        commit_before,
        "minority side committed entries"
    );
    let new = leader(&w.sim).unwrap();
    assert!(majority.contains(&new));
    w.settle();
    w.check_linearizable();
}
