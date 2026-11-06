//! The event loop: nodes, network, timers, faults and the trace.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fmt::{self, Debug, Write as _};
use std::sync::Arc;

use stowage_common::os::Xoshiro256;
use stowage_common::{Clock, Disk, NodeId, Rng};

use crate::MS;
use crate::clock::{SimClock, SimTime};
use crate::disk::{CrashMode, SimDisk};

/// Pseudo node id for messages injected by the test itself (a client).
pub const CLIENT: NodeId = 0;

/// Caller-chosen timer identifier, echoed back to [`Actor::on_timer`].
pub type TimerId = u64;

/// A component under simulation. Handlers must be deterministic given their inputs:
/// all time, randomness and I/O must come from `ctx` or from the [`NodeEnv`] the
/// actor was built with.
pub trait Actor {
    type Msg: Clone + Debug;

    fn on_start(&mut self, _ctx: &mut Ctx<'_, Self::Msg>) {}
    fn on_message(&mut self, ctx: &mut Ctx<'_, Self::Msg>, from: NodeId, msg: Self::Msg);
    fn on_timer(&mut self, _ctx: &mut Ctx<'_, Self::Msg>, _timer: TimerId) {}
}

/// Everything a node keeps across crashes (its disk) plus its clock.
#[derive(Debug, Clone)]
pub struct NodeEnv {
    pub id: NodeId,
    pub disk: Arc<SimDisk>,
    pub clock: Arc<SimClock>,
}

impl NodeEnv {
    pub fn disk(&self) -> Arc<dyn Disk> {
        self.disk.clone()
    }
    pub fn clock(&self) -> Arc<dyn Clock> {
        self.clock.clone()
    }
}

/// Network behaviour. Latency is uniform in `min_latency..=max_latency`, which also
/// produces reordering.
#[derive(Debug, Clone)]
pub struct NetConfig {
    pub min_latency: u64,
    pub max_latency: u64,
    pub drop_prob: f64,
    pub dup_prob: f64,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            min_latency: MS / 2,
            max_latency: 5 * MS,
            drop_prob: 0.0,
            dup_prob: 0.0,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SimConfig {
    pub net: NetConfig,
    pub crash_mode: CrashMode,
    /// Keep the textual trace (for docs and debugging). The fingerprint is always kept.
    pub record_trace: bool,
}

/// Counters for assertions and docs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SimStats {
    pub sent: u64,
    pub delivered: u64,
    pub dropped: u64,
    pub duplicated: u64,
    pub partitioned: u64,
    pub timers_fired: u64,
    pub crashes: u64,
}

/// Handle passed to actor callbacks.
pub struct Ctx<'a, M> {
    node: NodeId,
    now: u64,
    rng: &'a mut Xoshiro256,
    effects: &'a mut Vec<Effect<M>>,
}

impl<M> Debug for Ctx<'_, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ctx")
            .field("node", &self.node)
            .field("now", &self.now)
            .finish_non_exhaustive()
    }
}

impl<M> Ctx<'_, M> {
    pub fn node(&self) -> NodeId {
        self.node
    }
    /// Virtual monotonic time in nanoseconds.
    pub fn now(&self) -> u64 {
        self.now
    }
    pub fn rng(&mut self) -> &mut dyn Rng {
        self.rng
    }
    pub fn send(&mut self, to: NodeId, msg: M) {
        self.effects.push(Effect::Send { to, msg });
    }
    /// Fire [`Actor::on_timer`] with `id` after `delay` nanoseconds, unless the node
    /// crashes first.
    pub fn set_timer(&mut self, delay: u64, id: TimerId) {
        self.effects.push(Effect::Timer { delay, id });
    }
}

enum Effect<M> {
    Send { to: NodeId, msg: M },
    Timer { delay: u64, id: TimerId },
}

#[derive(Debug)]
enum Event<M> {
    Deliver {
        from: NodeId,
        to: NodeId,
        msg: M,
    },
    Timer {
        node: NodeId,
        incarnation: u32,
        id: TimerId,
    },
}

struct Scheduled<M> {
    at: u64,
    seq: u64,
    event: Event<M>,
}

impl<M> PartialEq for Scheduled<M> {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}
impl<M> Eq for Scheduled<M> {}
impl<M> PartialOrd for Scheduled<M> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<M> Ord for Scheduled<M> {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.at, self.seq).cmp(&(other.at, other.seq))
    }
}

struct Slot<A> {
    actor: Option<A>,
    incarnation: u32,
    env: NodeEnv,
}

type Factory<A> = Box<dyn FnMut(&NodeEnv) -> A>;

/// A simulated cluster of actors of type `A`.
pub struct Simulation<A: Actor> {
    seed: u64,
    config: SimConfig,
    time: SimTime,
    rng: Xoshiro256,
    queue: BinaryHeap<Reverse<Scheduled<A::Msg>>>,
    seq: u64,
    nodes: BTreeMap<NodeId, Slot<A>>,
    factory: Factory<A>,
    /// Unordered pairs `(min, max)` that cannot talk.
    cut: BTreeSet<(NodeId, NodeId)>,
    stats: SimStats,
    fingerprint: u64,
    trace: Vec<String>,
}

impl<A: Actor> Debug for Simulation<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Simulation")
            .field("seed", &self.seed)
            .field("now", &self.time.now())
            .field("nodes", &self.nodes.keys().collect::<Vec<_>>())
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

impl<A: Actor> Simulation<A> {
    /// `factory` builds (or rebuilds, after a crash) the actor for a node. It should
    /// recover any persistent state from `env.disk`.
    pub fn new(seed: u64, config: SimConfig, factory: impl FnMut(&NodeEnv) -> A + 'static) -> Self {
        Self {
            seed,
            config,
            time: SimTime::default(),
            rng: Xoshiro256::seeded(seed),
            queue: BinaryHeap::new(),
            seq: 0,
            nodes: BTreeMap::new(),
            factory: Box::new(factory),
            cut: BTreeSet::new(),
            stats: SimStats::default(),
            fingerprint: FNV_OFFSET,
            trace: Vec::new(),
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }
    pub fn now(&self) -> u64 {
        self.time.now()
    }
    pub fn stats(&self) -> SimStats {
        self.stats
    }
    /// Hash of every event processed so far. Equal seeds must give equal fingerprints.
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }
    /// The recorded trace (empty unless [`SimConfig::record_trace`]).
    pub fn trace(&self) -> &[String] {
        &self.trace
    }
    pub fn net_mut(&mut self) -> &mut NetConfig {
        &mut self.config.net
    }
    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }
    pub fn env(&self, id: NodeId) -> Option<&NodeEnv> {
        self.nodes.get(&id).map(|s| &s.env)
    }
    /// The live actor, or `None` if the node is down or unknown.
    pub fn actor(&self, id: NodeId) -> Option<&A> {
        self.nodes.get(&id).and_then(|s| s.actor.as_ref())
    }
    pub fn actor_mut(&mut self, id: NodeId) -> Option<&mut A> {
        self.nodes.get_mut(&id).and_then(|s| s.actor.as_mut())
    }
    pub fn is_up(&self, id: NodeId) -> bool {
        self.actor(id).is_some()
    }

    /// Add and start a node. Ids must be non-zero (0 is [`CLIENT`]).
    pub fn add_node(&mut self, id: NodeId) {
        assert_ne!(id, CLIENT, "node id 0 is reserved for the client");
        assert!(!self.nodes.contains_key(&id), "node {id} already exists");
        let env = NodeEnv {
            id,
            disk: Arc::new(SimDisk::new(self.config.crash_mode)),
            clock: Arc::new(SimClock::new(self.time.clone())),
        };
        let actor = (self.factory)(&env);
        self.nodes.insert(
            id,
            Slot {
                actor: Some(actor),
                incarnation: 0,
                env,
            },
        );
        self.record(format_args!("start n{id}"));
        self.with_actor(id, |a, ctx| a.on_start(ctx));
    }

    /// Kill the node's process. Pending timers die with it; its disk keeps only
    /// what the crash mode lets survive.
    pub fn crash(&mut self, id: NodeId) {
        let slot = self.nodes.get_mut(&id).expect("unknown node");
        if slot.actor.take().is_none() {
            return;
        }
        slot.incarnation += 1;
        slot.env.disk.crash(&mut self.rng);
        self.stats.crashes += 1;
        self.record(format_args!("crash n{id}"));
    }

    /// Start a crashed node again from its disk.
    pub fn restart(&mut self, id: NodeId) {
        let slot = self.nodes.get_mut(&id).expect("unknown node");
        assert!(slot.actor.is_none(), "node {id} is running");
        slot.actor = Some((self.factory)(&slot.env));
        self.record(format_args!("restart n{id}"));
        self.with_actor(id, |a, ctx| a.on_start(ctx));
    }

    /// Cut every link between `side_a` and `side_b`.
    pub fn partition(&mut self, side_a: &[NodeId], side_b: &[NodeId]) {
        for &a in side_a {
            for &b in side_b {
                if a != b {
                    self.cut.insert((a.min(b), a.max(b)));
                }
            }
        }
        self.record(format_args!("partition {side_a:?} | {side_b:?}"));
    }

    /// Cut `id` off from every other node (the client can still reach it).
    pub fn isolate(&mut self, id: NodeId) {
        let others: Vec<NodeId> = self.node_ids().into_iter().filter(|&n| n != id).collect();
        self.partition(&[id], &others);
    }

    pub fn heal(&mut self) {
        self.cut.clear();
        self.record(format_args!("heal"));
    }

    pub fn set_clock_skew(&mut self, id: NodeId, skew_nanos: i64) {
        self.nodes[&id].env.clock.set_skew(skew_nanos);
        self.record(format_args!("skew n{id} {skew_nanos}ns"));
    }

    /// Deliver `msg` to `to` as if sent by [`CLIENT`], with normal network latency.
    pub fn send(&mut self, to: NodeId, msg: A::Msg) {
        self.route(CLIENT, to, msg);
    }

    /// Process the next event. Returns `false` when the queue is empty.
    pub fn step(&mut self) -> bool {
        let Some(Reverse(next)) = self.queue.pop() else {
            return false;
        };
        self.time.advance_to(next.at);
        match next.event {
            Event::Deliver { from, to, msg } => {
                if self.blocked(from, to) {
                    self.stats.partitioned += 1;
                    self.record(format_args!("cut n{from}->n{to} {msg:?}"));
                } else if self.is_up(to) {
                    self.stats.delivered += 1;
                    self.record(format_args!("recv n{from}->n{to} {msg:?}"));
                    self.with_actor(to, |a, ctx| a.on_message(ctx, from, msg));
                } else {
                    self.stats.dropped += 1;
                    self.record(format_args!("lost n{from}->n{to} (down) {msg:?}"));
                }
            }
            Event::Timer {
                node,
                incarnation,
                id,
            } => {
                let alive = self
                    .nodes
                    .get(&node)
                    .is_some_and(|s| s.actor.is_some() && s.incarnation == incarnation);
                if alive {
                    self.stats.timers_fired += 1;
                    self.record(format_args!("timer n{node} #{id}"));
                    self.with_actor(node, |a, ctx| a.on_timer(ctx, id));
                }
            }
        }
        true
    }

    /// Run until virtual time reaches `now + duration` (or the queue drains).
    pub fn run_for(&mut self, duration: u64) {
        let deadline = self.now() + duration;
        while self.queue.peek().is_some_and(|Reverse(s)| s.at <= deadline) {
            self.step();
        }
        self.time.advance_to(deadline.max(self.now()));
    }

    /// Run until `cond` holds, checking after every event. Gives up after `timeout`
    /// of virtual time and returns whether the condition was met.
    pub fn run_until(&mut self, timeout: u64, mut cond: impl FnMut(&Self) -> bool) -> bool {
        let deadline = self.now() + timeout;
        loop {
            if cond(self) {
                return true;
            }
            let due = self.queue.peek().is_some_and(|Reverse(s)| s.at <= deadline);
            if !due {
                self.time.advance_to(deadline.max(self.now()));
                return cond(self);
            }
            self.step();
        }
    }

    fn with_actor(&mut self, id: NodeId, f: impl FnOnce(&mut A, &mut Ctx<'_, A::Msg>)) {
        let mut effects = Vec::new();
        {
            let Some(actor) = self.nodes.get_mut(&id).and_then(|s| s.actor.as_mut()) else {
                return;
            };
            let mut ctx = Ctx {
                node: id,
                now: self.time.now(),
                rng: &mut self.rng,
                effects: &mut effects,
            };
            f(actor, &mut ctx);
        }
        let incarnation = self.nodes[&id].incarnation;
        for effect in effects {
            match effect {
                Effect::Send { to, msg } => self.route(id, to, msg),
                Effect::Timer { delay, id: timer } => {
                    let at = self.now() + delay;
                    self.schedule(
                        at,
                        Event::Timer {
                            node: id,
                            incarnation,
                            id: timer,
                        },
                    );
                }
            }
        }
    }

    fn route(&mut self, from: NodeId, to: NodeId, msg: A::Msg) {
        self.stats.sent += 1;
        if self.blocked(from, to) {
            self.stats.partitioned += 1;
            self.record(format_args!("cut n{from}->n{to} {msg:?}"));
            return;
        }
        let net = &self.config.net;
        if self.rng.gen_bool(net.drop_prob) {
            self.stats.dropped += 1;
            self.record(format_args!("drop n{from}->n{to} {msg:?}"));
            return;
        }
        let copies = if self.rng.gen_bool(net.dup_prob) {
            self.stats.duplicated += 1;
            self.record(format_args!("dup n{from}->n{to} {msg:?}"));
            2
        } else {
            1
        };
        for _ in 0..copies {
            let net = &self.config.net;
            let latency = self.rng.gen_range(net.min_latency, net.max_latency + 1);
            let at = self.now() + latency;
            self.schedule(
                at,
                Event::Deliver {
                    from,
                    to,
                    msg: msg.clone(),
                },
            );
        }
    }

    fn blocked(&self, a: NodeId, b: NodeId) -> bool {
        a != CLIENT && b != CLIENT && self.cut.contains(&(a.min(b), a.max(b)))
    }

    fn schedule(&mut self, at: u64, event: Event<A::Msg>) {
        self.seq += 1;
        self.queue.push(Reverse(Scheduled {
            at,
            seq: self.seq,
            event,
        }));
    }

    fn record(&mut self, what: fmt::Arguments<'_>) {
        let mut line = String::new();
        let _ = write!(line, "{:>9.3}ms {what}", self.now() as f64 / MS as f64);
        for b in line.bytes() {
            self.fingerprint = (self.fingerprint ^ u64::from(b)).wrapping_mul(FNV_PRIME);
        }
        if self.config.record_trace {
            self.trace.push(line);
        }
    }
}
