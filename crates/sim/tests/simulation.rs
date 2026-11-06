//! Properties the rest of Stowage relies on: same seed => same run, partitions and
//! crashes behave like the real thing, and disk durability follows POSIX rules.

#![allow(clippy::unwrap_used)] // test code: a failed unwrap is a failed test

use std::sync::Arc;

use stowage_common::{Clock as _, DiskFile, NodeId};
use stowage_sim::{
    Actor, CLIENT, Ctx, MS, NetConfig, NodeEnv, SEC, SimConfig, Simulation, for_each_seed,
};

/// Each node broadcasts a heartbeat every 10ms and counts heartbeats received per
/// peer. It also persists a counter of client `Incr` requests, synced on each write.
#[derive(Debug)]
struct Node {
    id: NodeId,
    peers: Vec<NodeId>,
    beats_from: [u64; 4],
    counter_file: Arc<dyn DiskFile>,
    counter: u64,
}

#[derive(Debug, Clone)]
enum Msg {
    Beat(u64),
    Incr,
}

const BEAT: u64 = 1;

impl Node {
    fn new(env: &NodeEnv) -> Self {
        let disk = env.disk();
        let fresh = !disk.exists("counter").unwrap();
        let counter_file = disk.open("counter").unwrap();
        if fresh {
            disk.sync_dir().unwrap();
        }
        let mut buf = [0u8; 8];
        let counter = match counter_file.read_exact_at(&mut buf, 0) {
            Ok(()) => u64::from_le_bytes(buf),
            Err(_) => 0,
        };
        Self {
            id: env.id,
            peers: (1..=3).filter(|&p| p != env.id).collect(),
            beats_from: [0; 4],
            counter_file,
            counter,
        }
    }
}

impl Actor for Node {
    type Msg = Msg;

    fn on_start(&mut self, ctx: &mut Ctx<'_, Msg>) {
        let jitter = ctx.rng().gen_range(0, MS);
        ctx.set_timer(10 * MS + jitter, BEAT);
    }

    fn on_message(&mut self, _ctx: &mut Ctx<'_, Msg>, from: NodeId, msg: Msg) {
        match msg {
            Msg::Beat(sender) => {
                assert_eq!(sender, from, "beat must carry its sender");
                self.beats_from[usize::try_from(from).unwrap()] += 1;
            }
            Msg::Incr => {
                assert_eq!(from, CLIENT);
                self.counter += 1;
                self.counter_file
                    .write_at(&self.counter.to_le_bytes(), 0)
                    .unwrap();
                self.counter_file.sync().unwrap();
            }
        }
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_, Msg>, timer: u64) {
        assert_eq!(timer, BEAT);
        for &p in &self.peers {
            ctx.send(p, Msg::Beat(self.id));
        }
        ctx.set_timer(10 * MS, BEAT);
    }
}

fn cluster(seed: u64, net: NetConfig) -> Simulation<Node> {
    let mut sim = Simulation::new(
        seed,
        SimConfig {
            net,
            ..SimConfig::default()
        },
        Node::new,
    );
    for id in 1..=3 {
        sim.add_node(id);
    }
    sim
}

fn lossy() -> NetConfig {
    NetConfig {
        drop_prob: 0.05,
        dup_prob: 0.05,
        ..NetConfig::default()
    }
}

#[test]
fn same_seed_same_history_different_seed_different_history() {
    for_each_seed(&[1, 2, 3], |seed| {
        let run = |s| {
            let mut sim = cluster(s, lossy());
            sim.run_for(2 * SEC);
            (sim.fingerprint(), sim.stats())
        };
        let (fa, sa) = run(seed);
        let (fb, sb) = run(seed);
        assert_eq!(fa, fb, "seed {seed} is not deterministic");
        assert_eq!(sa, sb);
        let (fc, _) = run(seed.wrapping_add(1));
        assert_ne!(fa, fc, "different seeds produced identical histories");
        assert!(
            sa.dropped > 0 && sa.duplicated > 0,
            "lossy net had no faults: {sa:?}"
        );
    });
}

#[test]
fn partition_blocks_traffic_until_healed() {
    for_each_seed(&[7, 8], |seed| {
        let mut sim = cluster(seed, NetConfig::default());
        sim.run_for(SEC);
        sim.isolate(3);
        sim.run_for(20 * MS); // drain beats already in flight
        let before = sim.actor(1).unwrap().beats_from[3];
        sim.run_for(SEC);
        assert_eq!(
            sim.actor(1).unwrap().beats_from[3],
            before,
            "beat crossed a partition"
        );
        assert!(sim.stats().partitioned > 0);
        // The majority side keeps talking.
        let b12 = sim.actor(1).unwrap().beats_from[2];
        assert!(b12 > 150, "1<-2 beats: {b12}");

        sim.heal();
        assert!(sim.run_until(SEC, |s| s.actor(1).unwrap().beats_from[3] > before));
    });
}

#[test]
fn crash_loses_timers_and_restart_recovers_synced_state() {
    for_each_seed(&[11, 12], |seed| {
        let mut sim = cluster(seed, NetConfig::default());
        for _ in 0..5 {
            sim.send(2, Msg::Incr);
        }
        sim.run_for(100 * MS);
        assert_eq!(sim.actor(2).unwrap().counter, 5);

        sim.crash(2);
        assert!(!sim.is_up(2));
        let fired = sim.stats().timers_fired;
        sim.send(2, Msg::Incr); // lost: node is down
        sim.run_for(SEC);
        let delta = sim.stats().timers_fired - fired;
        // Only nodes 1 and 3 keep beating: ~100 timers each per second.
        assert!(
            (190..=210).contains(&delta),
            "timers fired while 2 was down: {delta}"
        );

        sim.restart(2);
        assert_eq!(
            sim.actor(2).unwrap().counter,
            5,
            "synced writes must survive"
        );
        sim.run_for(100 * MS);
        assert!(sim.actor(1).unwrap().beats_from[2] > 0);
    });
}

#[test]
fn clock_skew_moves_wall_clock_only() {
    let mut sim = cluster(5, NetConfig::default());
    sim.set_clock_skew(3, 250 * MS as i64);
    let c1 = sim.env(1).unwrap().clock.clone();
    let c3 = sim.env(3).unwrap().clock.clone();
    assert_eq!(c3.wall_nanos() - c1.wall_nanos(), 250 * MS);
    assert_eq!(c3.monotonic_nanos(), c1.monotonic_nanos());
}

#[test]
fn trace_is_human_readable() {
    let mut sim = Simulation::new(
        42,
        SimConfig {
            record_trace: true,
            ..SimConfig::default()
        },
        Node::new,
    );
    sim.add_node(1);
    sim.add_node(2);
    sim.run_for(25 * MS);
    let trace = sim.trace().join("\n");
    assert!(trace.contains("start n1"), "{trace}");
    assert!(trace.contains("timer n1 #1"), "{trace}");
    assert!(trace.contains("recv n1->n2 Beat(1)"), "{trace}");
}
