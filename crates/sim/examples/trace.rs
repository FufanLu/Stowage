//! Prints a short, real simulation trace for `docs/illustrated/A-deterministic-simulation.md`.
//!
//! Node 1 pings node 2 every 10ms; node 2 answers. The network drops 20% of
//! messages. At 30ms we partition 1 from 2, at 50ms we heal, at 60ms node 2 crashes,
//! and at 70ms it restarts.
//!
//! ```text
//! cargo run -p stowage-sim --example trace -- 42
//! ```

use stowage_common::NodeId;
use stowage_sim::{Actor, Ctx, MS, NetConfig, SimConfig, Simulation};

#[derive(Debug, Clone)]
enum Msg {
    Ping(u32),
    Pong(u32),
}

#[derive(Debug, Default)]
struct Node {
    next: u32,
    pongs: u32,
    last_pong: u32,
}

impl Actor for Node {
    type Msg = Msg;

    fn on_start(&mut self, ctx: &mut Ctx<'_, Msg>) {
        if ctx.node() == 1 {
            ctx.set_timer(10 * MS, 0);
        }
    }

    fn on_message(&mut self, ctx: &mut Ctx<'_, Msg>, from: NodeId, msg: Msg) {
        match msg {
            Msg::Ping(n) => ctx.send(from, Msg::Pong(n)),
            Msg::Pong(n) => {
                self.pongs += 1;
                self.last_pong = n;
            }
        }
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_, Msg>, _timer: u64) {
        self.next += 1;
        ctx.send(2, Msg::Ping(self.next));
        ctx.set_timer(10 * MS, 0);
    }
}

fn run(seed: u64) -> Simulation<Node> {
    let config = SimConfig {
        net: NetConfig {
            drop_prob: 0.2,
            ..NetConfig::default()
        },
        record_trace: true,
        ..SimConfig::default()
    };
    let mut sim = Simulation::new(seed, config, |_env| Node::default());
    sim.add_node(1);
    sim.add_node(2);
    sim.run_for(30 * MS);
    sim.partition(&[1], &[2]);
    sim.run_for(20 * MS);
    sim.heal();
    sim.run_for(10 * MS);
    sim.crash(2);
    sim.run_for(10 * MS);
    sim.restart(2);
    sim.run_for(30 * MS);
    sim
}

fn main() {
    let seed = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(42);
    let a = run(seed);
    for line in a.trace() {
        println!("{line}");
    }
    let b = run(seed);
    let c = run(seed + 1);
    println!("\nstats            {:?}", a.stats());
    if let Some(n) = a.actor(1) {
        println!("pongs at node 1  {} (last: Pong({}))", n.pongs, n.last_pong);
    }
    println!("fingerprint      seed {seed}: {:016x}", a.fingerprint());
    println!(
        "fingerprint      seed {seed}: {:016x} (second run)",
        b.fingerprint()
    );
    println!(
        "fingerprint      seed {}: {:016x}",
        seed + 1,
        c.fingerprint()
    );
}
