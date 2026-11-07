//! Real traces for `docs/illustrated/04-a-range-splits.md`.
//!
//! ```text
//! cargo run --release -p stowage-kv --example trace_split
//! ```
//!
//! Part 1: three nodes, one range; a manual split, message by message.
//! Part 2: a fourth node joins; the scheduler moves a replica onto it.
//! Part 3: a client with a stale route finds the new leader.

#![allow(clippy::unwrap_used)]

#[path = "../tests/common/cluster.rs"]
mod cluster;

use cluster::{Msg, Node, short_range};
use stowage_kv::{Request, Response, StoreConfig};
use stowage_sim::{CLIENT, MS, SEC, SimConfig, Simulation};

fn cfg(nodes: u64, rebalance: bool) -> StoreConfig {
    let mut c = StoreConfig::new(0, (1..=nodes).collect());
    c.split_bytes = u64::MAX; // splits only when asked
    c.heartbeat_ticks = 20;
    c.schedule_ticks = 10;
    c.rebalance = rebalance;
    c
}

fn request(sim: &mut Simulation<Node>, to: u64, token: u64, req: Request) -> (u64, u64, Response) {
    sim.send(to, Msg::Client { token, req });
    for _ in 0..2000 {
        sim.run_for(MS);
        for (t, from, m) in sim.take_client_messages() {
            if let Msg::Reply { token: tk, resp } = m
                && tk == token
            {
                return (t, from, resp);
            }
        }
    }
    panic!("no reply");
}

fn print_trace(sim: &Simulation<Node>, from: u64, to: u64, filter: impl Fn(&str) -> bool) {
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
        if t >= from && t <= to && !line.contains("timer") && filter(line) {
            println!("{line}");
        }
    }
}

fn ranges(sim: &Simulation<Node>) {
    for id in sim.node_ids() {
        let Some(n) = sim.actor(id) else { continue };
        let mut v: Vec<String> = n
            .store
            .local_ranges()
            .into_iter()
            .map(|(d, leader, term, applied)| {
                format!(
                    "{} [{:?},{}) gen {} {}{:?} applied {applied} term {term}",
                    short_range(d.id),
                    String::from_utf8_lossy(&d.start),
                    if d.end.is_empty() {
                        "∞".into()
                    } else {
                        format!("{:?}", String::from_utf8_lossy(&d.end))
                    },
                    d.generation,
                    if leader { "LEADER " } else { "" },
                    d.replicas,
                )
            })
            .collect();
        v.sort();
        println!("  n{id}: {}", v.join(" | "));
    }
}

fn main() {
    // ---- Part 1 ------------------------------------------------------------------
    let mut sim = Simulation::new(
        4,
        SimConfig {
            record_trace: true,
            ..SimConfig::default()
        },
        {
            let c = cfg(3, false);
            move |env| Node::new(env, &c)
        },
    );
    for id in 1..=3 {
        sim.add_node(id);
    }
    sim.run_for(SEC);
    let leader = (1..=3)
        .find(|&i| {
            sim.actor(i)
                .unwrap()
                .store
                .local_ranges()
                .iter()
                .any(|(_, l, ..)| *l)
        })
        .unwrap();
    for (i, k) in ["apple", "banana", "melon", "peach", "zucchini"]
        .iter()
        .enumerate()
    {
        request(
            &mut sim,
            leader,
            100 + i as u64,
            Request::Put {
                key: k.as_bytes().to_vec(),
                value: b"1".to_vec(),
            },
        );
    }
    println!("== 1. before: one range, leader n{leader}");
    ranges(&sim);

    let t0 = sim.now();
    let (t1, from, resp) = request(&mut sim, leader, 1, Request::Split { key: b"m".to_vec() });
    sim.run_for(60 * MS);
    println!(
        "\n== split at \"m\": {resp:?} from n{from} after {:.1} ms",
        (t1 - t0) as f64 / MS as f64
    );
    print_trace(&sim, t0, sim.now(), |l| {
        !l.contains("Heartbeat") && !l.contains("StoreHeartbeat")
    });
    println!("\nafter:");
    ranges(&sim);
    let right = sim
        .actor(leader)
        .unwrap()
        .store
        .local_ranges()
        .into_iter()
        .find(|(d, ..)| d.start == b"m")
        .unwrap()
        .0;
    let (t2, from2, resp2) = request(
        &mut sim,
        leader,
        2,
        Request::Get {
            key: b"peach".to_vec(),
        },
    );
    println!(
        "\nGET peach (now in {}) from n{from2}: {resp2:?} at {:.1} ms",
        short_range(right.id),
        t2 as f64 / MS as f64
    );
    for id in 1..=3 {
        let n = sim.actor(id).unwrap();
        let keys: Vec<String> = n
            .store
            .scan_local(&right)
            .unwrap()
            .iter()
            .filter_map(|(k, _)| stowage_kv::mvcc::decode_version_key(k).ok())
            .map(|(k, _)| String::from_utf8_lossy(&k).into_owned())
            .collect();
        println!(
            "  n{id} holds {} keys for {} — no data moved: {keys:?}",
            keys.len(),
            short_range(right.id)
        );
    }

    // ---- Part 2 ------------------------------------------------------------------
    println!("\n== 2. a fourth node joins; the scheduler on r1's leader rebalances");
    let mut sim = Simulation::new(
        9,
        SimConfig {
            record_trace: true,
            ..SimConfig::default()
        },
        {
            let c = cfg(4, true);
            move |env| Node::new(env, &c)
        },
    );
    for id in 1..=4 {
        sim.add_node(id);
    }
    sim.run_for(SEC);
    let l1 = (1..=4)
        .find(|&i| {
            sim.actor(i)
                .unwrap()
                .store
                .local_ranges()
                .iter()
                .any(|(d, l, ..)| d.id == 1 && *l)
        })
        .unwrap();
    for (i, k) in ["apple", "banana", "melon", "peach", "zucchini"]
        .iter()
        .enumerate()
    {
        request(
            &mut sim,
            l1,
            100 + i as u64,
            Request::Put {
                key: k.as_bytes().to_vec(),
                value: b"1".to_vec(),
            },
        );
    }
    request(&mut sim, l1, 1, Request::Split { key: b"m".to_vec() });
    sim.run_for(200 * MS);
    println!("before (n4 holds nothing):");
    ranges(&sim);
    let t3 = sim.now();
    sim.run_for(3 * SEC);
    println!("\nscheduler and membership traffic:");
    print_trace(&sim, t3, sim.now(), |l| {
        l.contains("ChangeReplicas")
            || l.contains("TransferLeader")
            || l.contains("config")
            || l.contains("InstallSnapshot")
            || l.contains("Gc")
    });
    println!("\nafter:");
    ranges(&sim);
    for id in 1..=4 {
        println!("  n{id} stats: {:?}", sim.actor(id).unwrap().store.stats());
    }

    // ---- Part 3 ------------------------------------------------------------------
    println!("\n== 3. routing: a client asks the wrong node");
    let wrong = (1..=4)
        .find(|&i| {
            !sim.actor(i)
                .unwrap()
                .store
                .local_ranges()
                .iter()
                .any(|(d, l, ..)| d.contains(b"peach") && *l)
        })
        .unwrap();
    let t4 = sim.now();
    let (_, f1, r1) = request(
        &mut sim,
        wrong,
        7,
        Request::Get {
            key: b"peach".to_vec(),
        },
    );
    println!("GET peach → n{wrong}: {r1:?} (from n{f1})");
    let (_, f2, r2) = request(
        &mut sim,
        wrong,
        8,
        Request::Locate {
            key: b"peach".to_vec(),
        },
    );
    println!("Locate peach → n{wrong}: {r2:?} (from n{f2})");
    if let Response::Location(info) = r2
        && let Some(l) = info.leader
    {
        let (t5, f3, r3) = request(
            &mut sim,
            l,
            9,
            Request::Get {
                key: b"peach".to_vec(),
            },
        );
        println!(
            "GET peach → n{l}: {r3:?} (from n{f3}), {:.1} ms after the first attempt",
            (t5 - t4) as f64 / MS as f64
        );
    }
    let _ = CLIENT;
}
