//! Linearizability checking (Wing & Gong, with Lowe's memoization as in Porcupine).
//!
//! A history is a set of operations, each with an invocation time, a return time
//! (or none, if the client never heard back) and an observed output. The history is
//! *linearizable* if every operation can be assigned a single instant between its
//! invocation and return such that, replayed in that order against a sequential
//! [`Model`], every operation produces the output the client observed.
//!
//! The search walks the history in real-time order and tries to "linearize" each
//! pending call; on a dead end it backtracks. A cache of
//! `(set of linearized ops, model state)` pairs prunes states already explored,
//! which keeps realistic histories (a handful of concurrent clients) fast.
//!
//! Operations that never returned may have taken effect at any point after their
//! invocation — or not at all. They are modelled with a return time of +∞ and an
//! unknown output; the model must accept any output for them.

use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

/// A sequential specification.
pub trait Model {
    type State: Clone + Eq + Hash + Debug;
    type Input: Debug;
    type Output: Debug;

    fn init(&self) -> Self::State;

    /// Apply `input` to `state`. `output` is `None` for operations whose result was
    /// never observed. Return the new state if `output` is consistent with it.
    fn step(
        &self,
        state: &Self::State,
        input: &Self::Input,
        output: Option<&Self::Output>,
    ) -> Option<Self::State>;
}

/// One client operation.
#[derive(Debug, Clone)]
pub struct Op<I, O> {
    pub client: u64,
    pub invoke: u64,
    /// `None`: the client never got an answer (crash, timeout).
    pub ret: Option<u64>,
    pub input: I,
    pub output: Option<O>,
}

/// Why a history is not linearizable: the longest sequence the search managed to
/// linearize, and the operation whose return it could not get past at that depth.
#[derive(Debug)]
pub struct Violation {
    pub linearized: Vec<usize>,
    pub stuck_at: usize,
}

#[derive(Clone, Copy)]
enum Kind {
    Call,
    Return,
}

#[derive(Clone, Copy)]
struct Event {
    op: usize,
    kind: Kind,
    /// Index of the matching return event (for calls).
    pair: usize,
    prev: usize,
    next: usize,
}

const NIL: usize = usize::MAX;

/// Check `history` against `model`.
pub fn check<M: Model>(model: &M, history: &[Op<M::Input, M::Output>]) -> Result<(), Violation> {
    let n = history.len();
    if n == 0 {
        return Ok(());
    }
    // Build events sorted by time; at equal times calls come first, so operations
    // that touch at an instant are treated as concurrent.
    let mut order: Vec<(u64, u8, usize)> = Vec::with_capacity(2 * n);
    for (i, op) in history.iter().enumerate() {
        order.push((op.invoke, 0, i));
        order.push((op.ret.unwrap_or(u64::MAX), 1, i));
    }
    order.sort_unstable();
    let mut events: Vec<Event> = Vec::with_capacity(2 * n + 1);
    // Sentinel head at index 0.
    events.push(Event {
        op: NIL,
        kind: Kind::Call,
        pair: NIL,
        prev: NIL,
        next: 1,
    });
    let mut call_of = vec![NIL; n];
    let mut ret_of = vec![NIL; n];
    for (pos, &(_, k, op)) in order.iter().enumerate() {
        let idx = pos + 1;
        let kind = if k == 0 { Kind::Call } else { Kind::Return };
        match kind {
            Kind::Call => call_of[op] = idx,
            Kind::Return => ret_of[op] = idx,
        }
        events.push(Event {
            op,
            kind,
            pair: NIL,
            prev: idx - 1,
            next: if pos + 1 < order.len() { idx + 1 } else { NIL },
        });
    }
    for op in 0..n {
        events[call_of[op]].pair = ret_of[op];
    }

    let words = n.div_ceil(64);
    let mut linearized = vec![0u64; words];
    let mut cache: HashSet<(Vec<u64>, M::State)> = HashSet::new();
    let mut stack: Vec<(usize, M::State)> = Vec::new();
    let mut state = model.init();
    let mut entry = events[0].next;
    let mut best: Vec<usize> = Vec::new();
    let mut best_stuck = NIL;

    let lift = |events: &mut Vec<Event>, call: usize| {
        let ret = events[call].pair;
        for e in [call, ret] {
            let (p, nx) = (events[e].prev, events[e].next);
            events[p].next = nx;
            if nx != NIL {
                events[nx].prev = p;
            }
        }
    };
    let unlift = |events: &mut Vec<Event>, call: usize| {
        let ret = events[call].pair;
        for e in [ret, call] {
            let (p, nx) = (events[e].prev, events[e].next);
            events[p].next = e;
            if nx != NIL {
                events[nx].prev = e;
            }
        }
    };

    while events[0].next != NIL {
        if entry == NIL {
            // Walked off the end without being able to linearize anything: the
            // remaining ops are all pending calls that could not apply. Backtrack.
            match stack.pop() {
                Some((call, prev_state)) => {
                    let op = events[call].op;
                    linearized[op / 64] &= !(1 << (op % 64));
                    state = prev_state;
                    unlift(&mut events, call);
                    entry = events[call].next;
                    continue;
                }
                None => break,
            }
        }
        let ev = events[entry];
        match ev.kind {
            Kind::Call => {
                let op = &history[ev.op];
                let next_state = model.step(&state, &op.input, op.output.as_ref());
                let mut advanced = false;
                if let Some(ns) = next_state {
                    let mut lin = linearized.clone();
                    lin[ev.op / 64] |= 1 << (ev.op % 64);
                    if cache.insert((lin.clone(), ns.clone())) {
                        stack.push((entry, std::mem::replace(&mut state, ns)));
                        linearized = lin;
                        lift(&mut events, entry);
                        entry = events[0].next;
                        advanced = true;
                        if stack.len() > best.len() {
                            best = stack.iter().map(|(c, _)| events[*c].op).collect();
                        }
                    }
                }
                if !advanced {
                    entry = ev.next;
                }
            }
            Kind::Return => {
                // An op returned before we could linearize it: backtrack.
                if stack.len() >= best.len() {
                    best_stuck = ev.op;
                }
                let Some((call, prev_state)) = stack.pop() else {
                    return Err(Violation {
                        linearized: best,
                        stuck_at: best_stuck,
                    });
                };
                let op = events[call].op;
                linearized[op / 64] &= !(1 << (op % 64));
                state = prev_state;
                unlift(&mut events, call);
                entry = events[call].next;
            }
        }
    }
    // Only pending (never-returned) calls may remain unlinearized: they can be
    // treated as never having taken effect.
    let mut e = events[0].next;
    while e != NIL {
        if history[events[e].op].ret.is_some() {
            return Err(Violation {
                linearized: best,
                stuck_at: events[e].op,
            });
        }
        e = events[e].next;
    }
    Ok(())
}

/// A single register: `Put(v)` / `Get -> v`. Use it per key (linearizability is
/// compositional, so a key-value history can be checked key by key).
#[derive(Debug, Clone, Copy, Default)]
pub struct Register;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterOp {
    Put(u64),
    Get,
}

impl Model for Register {
    type State = Option<u64>;
    type Input = RegisterOp;
    /// `Get` observes the value; `Put` observes nothing (`None` = acknowledged).
    type Output = Option<u64>;

    fn init(&self) -> Self::State {
        None
    }

    fn step(
        &self,
        state: &Self::State,
        input: &RegisterOp,
        output: Option<&Option<u64>>,
    ) -> Option<Self::State> {
        match input {
            RegisterOp::Put(v) => Some(Some(*v)),
            RegisterOp::Get => match output {
                None => Some(*state), // a read nobody saw changes nothing
                Some(seen) => (seen == state).then_some(*state),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(
        client: u64,
        invoke: u64,
        ret: Option<u64>,
        input: RegisterOp,
        output: Option<Option<u64>>,
    ) -> Op<RegisterOp, Option<u64>> {
        Op {
            client,
            invoke,
            ret,
            input,
            output,
        }
    }

    #[test]
    fn sequential_history_is_linearizable() {
        let h = vec![
            op(1, 0, Some(1), RegisterOp::Put(1), Some(None)),
            op(1, 2, Some(3), RegisterOp::Get, Some(Some(1))),
            op(1, 4, Some(5), RegisterOp::Put(2), Some(None)),
            op(1, 6, Some(7), RegisterOp::Get, Some(Some(2))),
        ];
        assert!(check(&Register, &h).is_ok());
    }

    #[test]
    fn concurrent_read_may_see_either_value() {
        // Put(2) overlaps both reads; one reader sees 1, the other 2: fine.
        let h = vec![
            op(1, 0, Some(1), RegisterOp::Put(1), Some(None)),
            op(2, 2, Some(10), RegisterOp::Put(2), Some(None)),
            op(3, 3, Some(4), RegisterOp::Get, Some(Some(1))),
            op(4, 5, Some(6), RegisterOp::Get, Some(Some(2))),
        ];
        assert!(check(&Register, &h).is_ok());
    }

    #[test]
    fn stale_read_after_newer_read_is_a_violation() {
        // Reader 3 saw 2, then (strictly later) reader 4 saw the older 1.
        let h = vec![
            op(1, 0, Some(1), RegisterOp::Put(1), Some(None)),
            op(2, 2, Some(10), RegisterOp::Put(2), Some(None)),
            op(3, 3, Some(4), RegisterOp::Get, Some(Some(2))),
            op(4, 5, Some(6), RegisterOp::Get, Some(Some(1))),
        ];
        let v = check(&Register, &h).unwrap_err();
        assert_eq!(v.stuck_at, 3);
    }

    #[test]
    fn read_of_unwritten_value_is_a_violation() {
        let h = vec![
            op(1, 0, Some(1), RegisterOp::Put(1), Some(None)),
            op(2, 2, Some(3), RegisterOp::Get, Some(Some(7))),
        ];
        assert!(check(&Register, &h).is_err());
    }

    #[test]
    fn pending_write_may_or_may_not_have_happened() {
        // Put(9) never returned. A later read seeing 9 is fine...
        let seen = vec![
            op(1, 0, Some(1), RegisterOp::Put(1), Some(None)),
            op(2, 2, None, RegisterOp::Put(9), None),
            op(3, 5, Some(6), RegisterOp::Get, Some(Some(9))),
        ];
        assert!(check(&Register, &seen).is_ok());
        // ...and so is a read seeing 1.
        let unseen = vec![
            op(1, 0, Some(1), RegisterOp::Put(1), Some(None)),
            op(2, 2, None, RegisterOp::Put(9), None),
            op(3, 5, Some(6), RegisterOp::Get, Some(Some(1))),
        ];
        assert!(check(&Register, &unseen).is_ok());
        // But not 9 then 1: once observed, the pending write has happened.
        let flip = vec![
            op(1, 0, Some(1), RegisterOp::Put(1), Some(None)),
            op(2, 2, None, RegisterOp::Put(9), None),
            op(3, 5, Some(6), RegisterOp::Get, Some(Some(9))),
            op(4, 7, Some(8), RegisterOp::Get, Some(Some(1))),
        ];
        assert!(check(&Register, &flip).is_err());
    }

    #[test]
    fn many_concurrent_clients_check_quickly() {
        // 8 clients, 2000 ops, each read returns the value of the latest write that
        // completed before it was invoked (a correct register): must pass.
        let mut h = Vec::new();
        let mut t = 0u64;
        let mut last = None;
        for i in 0..2000u64 {
            let c = i % 8;
            if i % 3 == 0 {
                h.push(op(c, t, Some(t + 5), RegisterOp::Put(i), Some(None)));
                last = Some(i);
            } else {
                h.push(op(c, t + 6, Some(t + 7), RegisterOp::Get, Some(last)));
            }
            t += 8;
        }
        assert!(check(&Register, &h).is_ok());
    }
}
