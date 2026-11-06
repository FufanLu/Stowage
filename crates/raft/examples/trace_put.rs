//! Real traces for `docs/illustrated/03-one-write-through-raft.md`.
//!
//! ```text
//! cargo run -p stowage-raft --example trace_put
//! ```
//!
//! Three nodes on the simulator (0.5–5 ms links, fsync'd `DiskStorage` on each
//! node's `SimDisk`). Prints:
//! 1. the first election, message by message;
//! 2. one client `PUT` from arrival at the leader to the reply;
//! 3. one linearizable `GET` via ReadIndex;
//! 4. a minority partition: the old leader steps down and commits nothing.

#![allow(clippy::unwrap_used)]

#[path = "../tests/common/kv.rs"]
mod kv;

use std::cell::RefCell;
use std::rc::Rc;

use kv::{KvNode, KvOp, Msg, NodeOpts, Oracle, leader};
use stowage_raft::{Membership, ReadMode, Storage as _};
use stowage_sim::{MS, SimConfig, Simulation};

/// Print the simulator's trace between two instants (timers omitted).
fn print_window(sim: &Simulation<KvNode>, from: u64, to: u64, raft_only: bool) {
    for line in sim.trace() {
        let ms: f64 = line
            .trim_start()
            .split("ms")
            .next()
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let t = (ms * MS as f64) as u64;
        if t < from || t > to {
            continue;
        }
        if line.contains("timer") {
            continue;
        }
        if raft_only && line.contains("Heartbeat") {
            continue;
        }
        println!("{line}");
    }
}

fn main() {
    let oracle = Rc::new(RefCell::new(Oracle::default()));
    let o = oracle.clone();
    let mut sim = Simulation::new(
        3,
        SimConfig {
            record_trace: true,
            ..SimConfig::default()
        },
        move |env| {
            KvNode::new(
                env,
                NodeOpts {
                    read_mode: ReadMode::ReadIndex,
                    snapshot_every: 0,
                    initial: Membership::new([1, 2, 3]),
                },
                o.clone(),
            )
        },
    );
    for id in 1..=3 {
        sim.add_node(id);
    }

    // 1. First election.
    sim.run_until(2_000 * MS, |s| leader(s).is_some());
    let elected_at = sim.now();
    let l = leader(&sim).unwrap();
    sim.run_for(15 * MS);
    println!(
        "== 1. first election: n{l} leads at {:.3} ms",
        elected_at as f64 / MS as f64
    );
    print_window(&sim, 0, sim.now(), false);

    // 2. One PUT.
    sim.run_for(200 * MS);
    let t0 = sim.now();
    sim.send(
        l,
        Msg::Req {
            client: 1,
            seq: 1,
            op: KvOp::Put(7, 42),
        },
    );
    let mut reply_at = None;
    for _ in 0..200 {
        sim.run_for(MS);
        for (t, from, m) in sim.take_client_messages() {
            println!(
                "\n== 2. PUT k7=42 answered by n{from} at {:.3} ms ({:?})",
                t as f64 / MS as f64,
                m
            );
            reply_at = Some(t);
        }
        if reply_at.is_some() {
            break;
        }
    }
    let t1 = reply_at.unwrap();
    sim.run_for(25 * MS);
    println!(
        "latency: {:.3} ms (client → leader → quorum fsync → apply → client)",
        (t1 - t0) as f64 / MS as f64
    );
    print_window(&sim, t0, sim.now(), true);
    for id in 1..=3 {
        let a = sim.actor(id).unwrap();
        println!(
            "n{id}: commit {} applied {} last {} log fsyncs {} kv {:?}",
            a.raft.commit(),
            a.raft.applied(),
            a.raft.last_index(),
            a.raft.storage().fsyncs,
            a.kv
        );
    }

    // 3. One linearizable GET via ReadIndex.
    sim.run_for(100 * MS);
    let t2 = sim.now();
    sim.send(
        l,
        Msg::Req {
            client: 1,
            seq: 2,
            op: KvOp::Get(7),
        },
    );
    let mut got = None;
    for _ in 0..200 {
        sim.run_for(MS);
        if let Some((t, from, m)) = sim.take_client_messages().pop() {
            got = Some((t, from, m));
            break;
        }
    }
    let (t3, from, m) = got.unwrap();
    println!(
        "\n== 3. GET k7 answered by n{from} at {:.3} ms: {:?}; latency {:.3} ms, no log entry written",
        t3 as f64 / MS as f64,
        m,
        (t3 - t2) as f64 / MS as f64
    );
    print_window(&sim, t2, t3, false);

    // 4. Minority partition.
    sim.run_for(100 * MS);
    let follower = [1, 2, 3].into_iter().find(|&i| i != l).unwrap();
    let other = [1, 2, 3]
        .into_iter()
        .find(|&i| i != l && i != follower)
        .unwrap();
    let t4 = sim.now();
    let commit_before = sim.actor(l).unwrap().raft.commit();
    sim.isolate(l);
    sim.send(
        l,
        Msg::Req {
            client: 2,
            seq: 1,
            op: KvOp::Put(7, 99),
        },
    );
    sim.run_for(600 * MS);
    let a = sim.actor(l).unwrap();
    let new_leader = leader(&sim);
    println!(
        "\n== 4. n{l} isolated at {:.0} ms with an uncommitted PUT k7=99",
        t4 as f64 / MS as f64
    );
    println!(
        "n{l}: role {:?} term {} commit {} (was {commit_before}) last {} — the PUT sits uncommitted",
        a.raft.role(),
        a.raft.term(),
        a.raft.commit(),
        a.raft.last_index()
    );
    println!(
        "majority side: new leader {new_leader:?}; n{follower} term {} n{other} term {}",
        sim.actor(follower).unwrap().raft.term(),
        sim.actor(other).unwrap().raft.term()
    );
    let replies = sim.take_client_messages();
    println!("replies to client 2 while isolated: {}", replies.len());
    let lines: Vec<&String> = sim
        .trace()
        .iter()
        .filter(|l| {
            let ms: f64 = l
                .trim_start()
                .split("ms")
                .next()
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            (ms * MS as f64) as u64 >= t4
                && (l.contains("PreVote")
                    || l.contains("Vote(")
                    || l.contains("partition")
                    || l.contains("cut"))
        })
        .take(14)
        .collect();
    for l in lines {
        println!("{l}");
    }
    sim.heal();
    sim.run_for(300 * MS);
    let a = sim.actor(l).unwrap();
    let e3 = a.raft.storage().entries(3, 4, usize::MAX);
    println!(
        "after healing: n{l} role {:?} term {} commit {} last {} kv {:?}; its index 3 is now {:?}",
        a.raft.role(),
        a.raft.term(),
        a.raft.commit(),
        a.raft.last_index(),
        a.kv,
        e3.first().map(|e| (e.term, kv::short_kind(&e.kind)))
    );
    println!(
        "\noracle: {} terms with a leader, {} entries applied cluster-wide",
        oracle.borrow().leaders.len(),
        oracle.borrow().applies
    );
}
