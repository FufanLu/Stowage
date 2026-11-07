//! The transaction coordinator, as a state machine with no I/O.
//!
//! A [`TxnCoord`] runs one transaction *program* against the cluster. It emits
//! requests (each routed by its key) and consumes their responses; a driver does
//! the routing, retries and waiting. The simulator and the async client drive the
//! same code.
//!
//! ```text
//!   begin: read_ts = write_ts = now
//!   Get k ─► TxnGet@read_ts ─► Value ──────────────────────────────────► next op
//!                          └─► Conflict(T) ─► Push T past read_ts ─┬─► T pending: skip T's intents, retry
//!                                                                  └─► T done: Resolve its intent, retry
//!   Put k ─► TxnPut@write_ts ─► Written(ts) : write_ts = max(write_ts, ts)
//!                           ├─► WriteTooOld : SI → restart · SSI → write above it
//!                           └─► Conflict(T) ─► Push T (abort if our priority wins), else back off
//!   commit: SSI and write_ts > read_ts ─► Refresh every read (read_ts, write_ts] ─► fail: restart
//!           EndTxn(commit at write_ts) ─► Committed ─► Resolve every intent ─► done
//!                                     ├─► Pending(min_commit_ts) : a reader pushed us; move up, refresh, retry
//!                                     └─► Aborted : someone aborted us ─► restart
//! ```
//!
//! Every request a coordinator sends is idempotent (an intent rewritten by its own
//! transaction, a record whose status only moves forward), so a driver may resend
//! any of them after a timeout or an ambiguous reply.

use std::collections::VecDeque;

use stowage_common::Timestamp;

use crate::msg::{Request, Response};
use crate::mvcc::{TxnMeta, TxnStatus};
use crate::types::PushKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    /// Snapshot isolation: reads from one snapshot, first committer wins on
    /// write-write conflicts. Allows write skew (G2).
    Snapshot,
    /// Serializable (SSI): additionally refreshes every read up to the commit
    /// timestamp, so no read-write anti-dependency can form a cycle.
    Serializable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnOp {
    Get(Vec<u8>),
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

/// What a program wants next, given the results of its reads so far (one entry
/// per `Get`, in order).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    Op(TxnOp),
    Commit,
    Abort,
}

/// A transaction body: given the reads so far and how many ops this attempt has
/// completed, the next step. Called again from the start if the transaction
/// restarts, so it must be a pure function of its arguments.
pub type Program = Box<dyn FnMut(&[Option<Vec<u8>>], usize) -> Step + Send>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Committed {
        commit_ts: Timestamp,
        reads: Vec<Option<Vec<u8>>>,
        restarts: u32,
    },
    /// The program chose to abort (with the reads that made it decide).
    Aborted { reads: Vec<Option<Vec<u8>>> },
    /// Too many restarts.
    GaveUp { restarts: u32 },
}

/// One outstanding request, identified by a tag the driver echoes back.
#[derive(Debug, Clone)]
pub struct Outgoing {
    pub tag: u64,
    pub req: Request,
}

#[derive(Debug)]
enum Phase {
    /// Executing program step `ops_done`; `waiting` is the outstanding request.
    Running,
    /// Waiting for a pushed transaction's fate before retrying the current op.
    Pushing {
        other: TxnMeta,
        key: Vec<u8>,
    },
    /// Resolving the intent we ran into, then retrying the current op.
    ResolvingConflict,
    /// Backing off after losing a write-write conflict, until `until_wall`.
    Backoff {
        until_wall: u64,
    },
    Refreshing {
        remaining: usize,
        ok: bool,
    },
    Committing,
    /// Resolving our own intents after commit/abort; `then` decides what follows.
    Cleaning {
        remaining: usize,
        then: After,
    },
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum After {
    Finish,
    Restart,
    AbortedByProgram,
}

pub const MAX_RESTARTS: u32 = 30;

pub struct TxnCoord {
    id_seed: u128,
    isolation: Isolation,
    program: Program,
    meta: TxnMeta,
    read_ts: Timestamp,
    has_anchor: bool,
    reads: Vec<Option<Vec<u8>>>,
    read_keys: Vec<Vec<u8>>,
    written: Vec<Vec<u8>>,
    skip: Vec<u128>,
    current: Option<TxnOp>,
    ops_done: usize,
    phase: Phase,
    next_tag: u64,
    /// Tags this attempt still waits for. Replies to anything else (an earlier
    /// attempt, a resent duplicate) are ignored: a stale read must never feed the
    /// current attempt.
    live: std::collections::BTreeSet<u64>,
    outbox: VecDeque<Outgoing>,
    restarts: u32,
    pub outcome: Option<Outcome>,
    last_heartbeat_wall: u64,
    backoffs: u32,
}

impl std::fmt::Debug for TxnCoord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnCoord")
            .field("id", &self.meta.id)
            .field("read_ts", &self.read_ts)
            .field("write_ts", &self.meta.write_ts)
            .field("phase", &self.phase)
            .field("restarts", &self.restarts)
            .finish_non_exhaustive()
    }
}

impl TxnCoord {
    /// `id` must be unique; `priority` breaks write-write conflicts.
    pub fn new(
        id: u128,
        now: Timestamp,
        priority: u32,
        isolation: Isolation,
        program: Program,
    ) -> Self {
        let mut c = Self {
            id_seed: id,
            isolation,
            program,
            meta: TxnMeta {
                id,
                anchor: Vec::new(),
                epoch: 0,
                write_ts: now,
                start_wall: now.wall,
                priority,
            },
            read_ts: now,
            has_anchor: false,
            reads: Vec::new(),
            read_keys: Vec::new(),
            written: Vec::new(),
            skip: Vec::new(),
            current: None,
            ops_done: 0,
            phase: Phase::Running,
            next_tag: 1,
            live: std::collections::BTreeSet::new(),
            outbox: VecDeque::new(),
            restarts: 0,
            outcome: None,
            last_heartbeat_wall: now.wall,
            backoffs: 0,
        };
        c.advance();
        c
    }

    pub fn meta(&self) -> &TxnMeta {
        &self.meta
    }

    pub fn is_done(&self) -> bool {
        matches!(self.phase, Phase::Done)
    }

    /// Requests to send now.
    pub fn take_requests(&mut self) -> Vec<Outgoing> {
        self.outbox.drain(..).collect()
    }

    fn send(&mut self, req: Request) -> u64 {
        let tag = self.next_tag;
        self.next_tag += 1;
        self.live.insert(tag);
        self.outbox.push_back(Outgoing { tag, req });
        tag
    }

    /// Ask the program for its next step and issue it.
    fn advance(&mut self) {
        let step = (self.program)(&self.reads, self.ops_done);
        match step {
            Step::Op(op) => {
                self.current = Some(op);
                self.issue_current();
            }
            Step::Commit => self.commit(),
            Step::Abort => self.clean(false, After::AbortedByProgram),
        }
    }

    fn issue_current(&mut self) {
        self.phase = Phase::Running;
        match self.current.clone() {
            Some(TxnOp::Get(key)) => {
                self.send(Request::TxnGet {
                    key,
                    ts: self.read_ts,
                    txn: Some(self.meta.id),
                    skip: self.skip.clone(),
                });
            }
            Some(TxnOp::Put(key, value)) => self.issue_put(key, Some(value)),
            Some(TxnOp::Delete(key)) => self.issue_put(key, None),
            None => {}
        }
    }

    fn issue_put(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        if !self.has_anchor {
            // The transaction record lives with the first key we write.
            self.meta.anchor = key.clone();
            self.has_anchor = true;
        }
        self.send(Request::TxnPut {
            key,
            value,
            txn: self.meta.clone(),
        });
    }

    fn commit(&mut self) {
        if self.written.is_empty() {
            // Read-only: nothing to make visible, no record needed.
            self.finish();
            return;
        }
        if self.meta.write_ts > self.read_ts {
            // Committing above the snapshot: nothing we wrote (SI: first committer
            // wins) or, under SSI, read may have changed in (read_ts, write_ts].
            self.start_refresh();
        } else {
            self.end_txn();
        }
    }

    fn start_refresh(&mut self) {
        let mut keys = self.written.clone();
        if self.isolation == Isolation::Serializable {
            for k in &self.read_keys {
                if !keys.contains(k) {
                    keys.push(k.clone());
                }
            }
        }
        if keys.is_empty() {
            self.read_ts = self.meta.write_ts;
            self.end_txn();
            return;
        }
        self.phase = Phase::Refreshing {
            remaining: keys.len(),
            ok: true,
        };
        for k in keys {
            let mut end = k.clone();
            end.push(0); // the smallest key after k: a one-key span
            self.send(Request::Refresh {
                start: k,
                end,
                from: self.read_ts,
                to: self.meta.write_ts,
                txn: self.meta.id,
            });
        }
    }

    fn end_txn(&mut self) {
        self.phase = Phase::Committing;
        self.send(Request::EndTxn {
            txn: self.meta.clone(),
            commit: true,
            commit_ts: self.meta.write_ts,
        });
    }

    /// Resolve every intent we wrote (commit or abort), then `then`.
    fn clean(&mut self, commit: bool, then: After) {
        let keys = self.written.clone();
        if keys.is_empty() && then != After::Finish {
            self.after_clean(then);
            return;
        }
        if then != After::Finish && self.has_anchor {
            // Make the abort authoritative before removing intents.
            self.send(Request::EndTxn {
                txn: self.meta.clone(),
                commit: false,
                commit_ts: self.meta.write_ts,
            });
        }
        self.phase = Phase::Cleaning {
            remaining: keys.len() + usize::from(then != After::Finish && self.has_anchor),
            then,
        };
        for k in keys {
            self.send(Request::Resolve {
                key: k,
                txn_id: self.meta.id,
                commit,
                commit_ts: self.meta.write_ts,
            });
        }
        if let Phase::Cleaning { remaining: 0, then } = self.phase {
            self.after_clean(then);
        }
    }

    fn after_clean(&mut self, then: After) {
        match then {
            After::Finish => self.finish(),
            After::AbortedByProgram => {
                self.phase = Phase::Done;
                self.outcome = Some(Outcome::Aborted {
                    reads: self.reads.clone(),
                });
            }
            After::Restart => self.restart_now(),
        }
    }

    fn finish(&mut self) {
        self.phase = Phase::Done;
        self.outcome = Some(Outcome::Committed {
            commit_ts: if self.written.is_empty() {
                self.read_ts
            } else {
                self.meta.write_ts
            },
            reads: self.reads.clone(),
            restarts: self.restarts,
        });
    }

    fn restart(&mut self) {
        self.clean(false, After::Restart);
    }

    /// Start over as a new transaction (new id, new timestamps, higher priority).
    fn restart_now(&mut self) {
        self.restarts += 1;
        if self.restarts > MAX_RESTARTS {
            self.phase = Phase::Done;
            self.outcome = Some(Outcome::GaveUp {
                restarts: self.restarts,
            });
            return;
        }
        let now = self.meta.write_ts.max(self.read_ts).next();
        self.live.clear();
        self.outbox.clear();
        self.meta = TxnMeta {
            id: self.id_seed.wrapping_add(u128::from(self.restarts) << 96),
            anchor: Vec::new(),
            epoch: self.restarts,
            write_ts: now,
            // A fresh record: pushers judge liveness from it.
            start_wall: now.wall,
            // Losing repeatedly raises priority, so a transaction is not starved.
            priority: self.meta.priority.saturating_add(1 << 20),
        };
        self.read_ts = now;
        self.has_anchor = false;
        self.reads.clear();
        self.read_keys.clear();
        self.written.clear();
        self.skip.clear();
        self.current = None;
        self.ops_done = 0;
        self.backoffs = 0;
        self.last_heartbeat_wall = now.wall;
        self.advance();
    }

    /// Feed the wall clock: retries after backoff and keeps the record alive.
    pub fn tick(&mut self, now: Timestamp) {
        if let Phase::Backoff { until_wall } = self.phase
            && now.wall >= until_wall
        {
            self.issue_current();
        }
        if self.has_anchor
            && !self.is_done()
            && now.wall.saturating_sub(self.last_heartbeat_wall) > 500_000_000
        {
            self.last_heartbeat_wall = now.wall;
            self.send(Request::Heartbeat {
                txn: self.meta.clone(),
            });
        }
    }

    /// A response to the request tagged `tag`. Routing errors never reach here:
    /// the driver resends those.
    pub fn on_response(&mut self, tag: u64, req: &Request, resp: Response, now: Timestamp) {
        if !self.live.remove(&tag) {
            return;
        }
        if matches!(req, Request::Heartbeat { .. }) {
            if let Response::Txn {
                status: TxnStatus::Aborted,
                ..
            } = resp
                && !self.is_done()
                && !matches!(self.phase, Phase::Cleaning { .. })
            {
                // Someone aborted us while we were running.
                self.restart();
            }
            return;
        }
        match std::mem::replace(&mut self.phase, Phase::Running) {
            Phase::Running => self.on_op_response(req, resp, now),
            Phase::Pushing { other, key } => match resp {
                Response::Txn {
                    status: TxnStatus::Pending,
                    min_commit_ts,
                    ..
                } => {
                    let lost = match req {
                        Request::Push {
                            kind: PushKind::Abort { .. },
                            ..
                        } => true,
                        Request::Push {
                            kind: PushKind::Timestamp { ts, .. },
                            ..
                        } => min_commit_ts <= *ts,
                        _ => true,
                    };
                    if lost {
                        // We lost the conflict: wait for the winner.
                        self.backoffs += 1;
                        if self.backoffs > 50 {
                            self.restart();
                        } else {
                            self.phase = Phase::Backoff {
                                until_wall: now.wall + 20_000_000,
                            };
                        }
                    } else {
                        // Pushed above our read: its intents are invisible to us.
                        self.skip.push(other.id);
                        self.issue_current();
                    }
                }
                Response::Txn {
                    status, commit_ts, ..
                } => {
                    self.phase = Phase::ResolvingConflict;
                    self.send(Request::Resolve {
                        key,
                        txn_id: other.id,
                        commit: status == TxnStatus::Committed,
                        commit_ts,
                    });
                }
                _ => self.issue_current(),
            },
            Phase::ResolvingConflict => self.issue_current(),
            Phase::Backoff { until_wall } => self.phase = Phase::Backoff { until_wall },
            Phase::Refreshing { remaining, ok } => {
                let ok = ok && matches!(resp, Response::Refreshed(true));
                if remaining > 1 {
                    self.phase = Phase::Refreshing {
                        remaining: remaining - 1,
                        ok,
                    };
                } else if ok {
                    self.read_ts = self.meta.write_ts;
                    self.end_txn();
                } else {
                    self.restart();
                }
            }
            Phase::Committing => match resp {
                Response::Txn {
                    status: TxnStatus::Committed,
                    commit_ts,
                    ..
                } => {
                    self.meta.write_ts = commit_ts;
                    self.clean(true, After::Finish);
                }
                Response::Txn {
                    status: TxnStatus::Aborted,
                    ..
                } => self.restart(),
                Response::Txn {
                    status: TxnStatus::Pending,
                    min_commit_ts,
                    ..
                } => {
                    // A reader pushed our commit timestamp.
                    self.meta.write_ts = self.meta.write_ts.max(min_commit_ts);
                    self.commit();
                }
                _ => self.end_txn(),
            },
            Phase::Cleaning { remaining, then } => {
                if remaining > 1 {
                    self.phase = Phase::Cleaning {
                        remaining: remaining - 1,
                        then,
                    };
                } else {
                    self.after_clean(then);
                }
            }
            Phase::Done => self.phase = Phase::Done,
        }
    }

    fn on_op_response(&mut self, req: &Request, resp: Response, _now: Timestamp) {
        match (self.current.clone(), resp) {
            (Some(TxnOp::Get(key)), Response::Value(v)) => {
                self.reads.push(v);
                self.read_keys.push(key);
                self.ops_done += 1;
                self.advance();
            }
            (Some(TxnOp::Put(key, _) | TxnOp::Delete(key)), Response::Written(ts)) => {
                self.meta.write_ts = self.meta.write_ts.max(ts);
                if !self.written.contains(&key) {
                    self.written.push(key);
                }
                self.ops_done += 1;
                self.advance();
            }
            (Some(TxnOp::Put(key, _) | TxnOp::Delete(key)), Response::WriteTooOld(ts)) => {
                match self.isolation {
                    // First committer wins: someone committed this key after our snapshot.
                    Isolation::Snapshot => self.restart(),
                    // We read this key, so refreshing past that write will fail: don't wait for it.
                    Isolation::Serializable if self.read_keys.contains(&key) => self.restart(),
                    Isolation::Serializable => {
                        self.meta.write_ts = self.meta.write_ts.max(ts.next());
                        self.issue_current();
                    }
                }
            }
            (Some(op), Response::Conflict(other)) => {
                let key = match &op {
                    TxnOp::Get(k) | TxnOp::Put(k, _) | TxnOp::Delete(k) => k.clone(),
                };
                let kind = match op {
                    TxnOp::Get(_) => PushKind::Timestamp {
                        ts: self.read_ts,
                        priority: self.meta.priority,
                    },
                    _ => PushKind::Abort {
                        priority: self.meta.priority,
                    },
                };
                self.phase = Phase::Pushing {
                    other: other.clone(),
                    key,
                };
                self.send(Request::Push {
                    pushee: other,
                    kind,
                });
            }
            _ => {
                // Anything else (e.g. a reply that does not fit): retry the op.
                let _ = req;
                self.issue_current();
            }
        }
    }
}
