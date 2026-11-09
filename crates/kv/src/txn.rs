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
    /// A locking read (`SELECT … FOR UPDATE`): takes the key's write intent now
    /// and reads the newest committed value. For read-modify-write of a hot key:
    /// conflicting transactions queue behind the intent instead of all reading,
    /// then restarting when one of them commits first. Its result counts as a
    /// `Get` in [`Reads`].
    GetForUpdate(Vec<u8>),
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    /// Up to `limit` live keys in `[start, end)` (`end` empty: unbounded), across
    /// as many ranges as it takes.
    Scan {
        start: Vec<u8>,
        end: Vec<u8>,
        limit: u32,
    },
}

/// The result of one op, in [`TxnCoord::results`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpResult {
    Value(Option<Vec<u8>>),
    Rows(Vec<(Vec<u8>, Vec<u8>)>),
    Written,
}

/// What a program wants next, given the results of its reads so far (one entry
/// per `Get`, in order).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    Op(TxnOp),
    Commit,
    Abort,
    /// Interactive transactions only: nothing to do until [`TxnCoord::resume`].
    Wait,
}

/// A transaction body: given the reads so far and how many ops this attempt has
/// completed, the next step. Called again from the start if the transaction
/// restarts, so it must be a pure function of its arguments.
pub type Program = Box<dyn FnMut(&Reads<'_>, usize) -> Step + Send>;

/// What a program sees: indexing gives the `Get` results in order (`reads[0]` is
/// the first `Get`); [`Reads::results`] has every op's result, scans included.
#[derive(Debug, Clone, Copy)]
pub struct Reads<'a> {
    gets: &'a [Option<Vec<u8>>],
    pub results: &'a [OpResult],
}

impl std::ops::Deref for Reads<'_> {
    type Target = [Option<Vec<u8>>];
    fn deref(&self) -> &Self::Target {
        self.gets
    }
}

impl Reads<'_> {
    /// Rows returned by the `n`-th op, if it was a scan.
    pub fn rows(&self, n: usize) -> Option<&[(Vec<u8>, Vec<u8>)]> {
        match self.results.get(n) {
            Some(OpResult::Rows(r)) => Some(r),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Committed {
        commit_ts: Timestamp,
        reads: Vec<Option<Vec<u8>>>,
        /// Every op's result, in order (scans included).
        results: Vec<OpResult>,
        restarts: u32,
    },
    /// The program chose to abort (with the reads that made it decide).
    Aborted { reads: Vec<Option<Vec<u8>>> },
    /// Too many restarts.
    GaveUp { restarts: u32 },
    /// An interactive transaction had to restart. Its earlier results may no longer
    /// hold, so the client must run it again (Postgres' SQLSTATE 40001).
    Retry { reason: &'static str },
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
    /// Interactive: the program has nothing queued.
    Idle,
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

/// Lost pushes a writer sits through before its priority may abort the holder
/// of the intent in its way. Each comes after up to ~80 ms in the
/// leaseholder's lock queue plus a 20 ms backoff, so this is about 100 ms:
/// long enough for a queue on a hot key to take turns, short enough that
/// waits stacking up do not starve other transactions into restarts.
pub const WOUND_AFTER: u32 = 1;

pub struct TxnCoord {
    id_seed: u128,
    isolation: Isolation,
    program: Program,
    meta: TxnMeta,
    read_ts: Timestamp,
    has_anchor: bool,
    reads: Vec<Option<Vec<u8>>>,
    /// One entry per completed op of this attempt.
    pub results: Vec<OpResult>,
    read_keys: Vec<Vec<u8>>,
    /// Spans read by scans, as `[start, end)` (`end` empty: unbounded).
    read_spans: Vec<(Vec<u8>, Vec<u8>)>,
    /// The scan in progress: where it started and the rows so far.
    scan_from: Vec<u8>,
    scan_rows: Vec<(Vec<u8>, Vec<u8>)>,
    interactive: bool,
    retry_reason: &'static str,
    written: Vec<Vec<u8>>,
    /// Keys whose intent a locking read took before anything else of ours
    /// was there: their newest committed version is the one we read, and
    /// nobody can commit above it before us, so the commit-time check for a
    /// newer version in (read_ts, write_ts] does not apply to them.
    locked: Vec<Vec<u8>>,
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
            .field("current", &self.current)
            .field("live", &self.live)
            .field("ops_done", &self.ops_done)
            .field("interactive", &self.interactive)
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
            results: Vec::new(),
            read_keys: Vec::new(),
            read_spans: Vec::new(),
            scan_from: Vec::new(),
            scan_rows: Vec::new(),
            interactive: false,
            retry_reason: "",
            written: Vec::new(),
            locked: Vec::new(),
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

    /// An interactive transaction: ops are supplied one at a time with
    /// [`Self::resume`] (through `program` returning [`Step::Wait`] until then),
    /// and a restart ends it with [`Outcome::Retry`] instead of re-running.
    pub fn interactive(
        id: u128,
        now: Timestamp,
        priority: u32,
        isolation: Isolation,
        program: Program,
    ) -> Self {
        let mut c = Self::new(id, now, priority, isolation, Box::new(|_, _| Step::Wait));
        c.interactive = true;
        c.program = program;
        c
    }

    /// Interactive: ask the program again (it has queued a step).
    pub fn resume(&mut self) {
        if matches!(self.phase, Phase::Idle) {
            self.advance();
        }
    }

    /// Interactive: the last op finished and the program has nothing queued.
    pub fn is_idle(&self) -> bool {
        matches!(self.phase, Phase::Idle)
    }

    pub fn read_ts(&self) -> Timestamp {
        self.read_ts
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
        let reads = Reads {
            gets: &self.reads,
            results: &self.results,
        };
        let step = (self.program)(&reads, self.ops_done);
        match step {
            Step::Op(op) => {
                self.current = Some(op);
                self.issue_current();
            }
            Step::Commit => self.commit(),
            Step::Abort => self.clean(false, After::AbortedByProgram),
            Step::Wait => self.phase = Phase::Idle,
        }
    }

    fn op_done(&mut self, r: OpResult) {
        self.results.push(r);
        self.ops_done += 1;
        self.advance();
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
            Some(TxnOp::GetForUpdate(key)) => {
                if !self.has_anchor {
                    self.meta.anchor = key.clone();
                    self.has_anchor = true;
                }
                self.send(Request::TxnLock {
                    key,
                    txn: self.meta.clone(),
                });
            }
            Some(TxnOp::Put(key, value)) => self.issue_put(key, Some(value)),
            Some(TxnOp::Delete(key)) => self.issue_put(key, None),
            Some(TxnOp::Scan { start, end, limit }) => {
                if self.scan_rows.is_empty() && self.scan_from.is_empty() {
                    self.scan_from = start.clone();
                }
                let want =
                    limit.saturating_sub(u32::try_from(self.scan_rows.len()).unwrap_or(u32::MAX));
                self.send(Request::TxnScan {
                    start,
                    end,
                    limit: want,
                    ts: self.read_ts,
                    txn: Some(self.meta.id),
                    skip: self.skip.clone(),
                });
            }
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
        let mut keys: Vec<Vec<u8>> = self
            .written
            .iter()
            .filter(|k| !self.locked.contains(k))
            .cloned()
            .collect();
        if self.isolation == Isolation::Serializable {
            for k in &self.read_keys {
                if !keys.contains(k) {
                    keys.push(k.clone());
                }
            }
        }
        let mut spans: Vec<(Vec<u8>, Vec<u8>)> = keys
            .into_iter()
            .map(|k| {
                let mut end = k.clone();
                end.push(0); // the smallest key after k: a one-key span
                (k, end)
            })
            .collect();
        if self.isolation == Isolation::Serializable {
            spans.extend(self.read_spans.iter().cloned());
        }
        if spans.is_empty() {
            self.read_ts = self.meta.write_ts;
            self.end_txn();
            return;
        }
        self.phase = Phase::Refreshing {
            remaining: spans.len(),
            ok: true,
        };
        for (start, end) in spans {
            self.send_refresh(start, end);
        }
    }

    fn send_refresh(&mut self, start: Vec<u8>, end: Vec<u8>) {
        self.send(Request::Refresh {
            start,
            end,
            from: self.read_ts,
            to: self.meta.write_ts,
            txn: self.meta.id,
        });
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
            results: self.results.clone(),
            restarts: self.restarts,
        });
    }

    fn restart(&mut self) {
        self.restart_because("aborted by a conflicting transaction");
    }

    fn restart_because(&mut self, reason: &'static str) {
        self.retry_reason = reason;
        self.clean(false, After::Restart);
    }

    /// Start over as a new transaction (new id, new timestamps, higher priority).
    fn restart_now(&mut self) {
        if self.interactive {
            self.phase = Phase::Done;
            self.outcome = Some(Outcome::Retry {
                reason: self.retry_reason,
            });
            return;
        }
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
        self.results.clear();
        self.read_keys.clear();
        self.read_spans.clear();
        self.scan_from.clear();
        self.scan_rows.clear();
        self.written.clear();
        self.locked.clear();
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
                            self.restart_because("waited too long for a conflicting transaction");
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
                if let (
                    Response::RefreshedPart { ok: part, resume },
                    Request::Refresh { end, .. },
                ) = (&resp, req)
                {
                    // That range covered its part; the rest lives further right.
                    self.phase = Phase::Refreshing {
                        remaining,
                        ok: ok && *part,
                    };
                    self.send_refresh(resume.clone(), end.clone());
                    return;
                }
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
                    self.restart_because(
                        "read refresh failed: a concurrent write invalidated a read",
                    );
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
                } => self.restart_because("aborted by a conflicting transaction before commit"),
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
            Phase::Idle => self.phase = Phase::Idle,
        }
    }

    fn on_op_response(&mut self, req: &Request, resp: Response, _now: Timestamp) {
        match (self.current.clone(), resp) {
            (Some(TxnOp::Get(key)), Response::Value(v)) => {
                self.reads.push(v.clone());
                self.read_keys.push(key);
                self.op_done(OpResult::Value(v));
            }
            (Some(TxnOp::GetForUpdate(key)), Response::Locked { ts, value }) => {
                // Not a snapshot read: the intent keeps the key unchanged up to
                // our commit, so it is not refreshed (not in `read_keys`).
                self.meta.write_ts = self.meta.write_ts.max(ts);
                if !self.written.contains(&key) {
                    self.written.push(key.clone());
                    self.locked.push(key);
                }
                self.reads.push(value.clone());
                self.op_done(OpResult::Value(value));
            }
            (Some(TxnOp::GetForUpdate(_)), Response::WriteTooOld(ts)) => {
                // Nothing read yet from this key: just lock above the newer version.
                self.meta.write_ts = self.meta.write_ts.max(ts.next());
                self.issue_current();
            }
            (Some(TxnOp::Put(key, _) | TxnOp::Delete(key)), Response::Written(ts)) => {
                self.meta.write_ts = self.meta.write_ts.max(ts);
                if !self.written.contains(&key) {
                    self.written.push(key);
                }
                self.op_done(OpResult::Written);
            }
            (Some(TxnOp::Scan { end, limit, .. }), Response::Rows { pairs, range_end }) => {
                self.scan_rows.extend(pairs);
                let full = self.scan_rows.len() >= limit as usize;
                let last_range = range_end.is_empty() || (!end.is_empty() && range_end >= end);
                if full || last_range {
                    // What was read: up to `end`, or just past the last row if the
                    // limit stopped us first.
                    let span_end = match self.scan_rows.last() {
                        Some((k, _)) if full => {
                            let mut e = k.clone();
                            e.push(0);
                            e
                        }
                        _ => end,
                    };
                    let start = std::mem::take(&mut self.scan_from);
                    self.read_spans.push((start, span_end));
                    let rows = std::mem::take(&mut self.scan_rows);
                    self.op_done(OpResult::Rows(rows));
                } else {
                    // Continue in the next range.
                    if let Some(TxnOp::Scan { start, .. }) = &mut self.current {
                        *start = range_end;
                    }
                    self.issue_current();
                }
            }
            (Some(TxnOp::Scan { .. }), Response::ConflictAt { txn: other, key }) => {
                let kind = PushKind::Timestamp {
                    ts: self.read_ts,
                    priority: self.meta.priority,
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
            (Some(TxnOp::Put(key, _) | TxnOp::Delete(key)), Response::WriteTooOld(ts)) => {
                match self.isolation {
                    // First committer wins: someone committed this key after our snapshot.
                    Isolation::Snapshot => {
                        self.restart_because("write-write conflict: a newer version was committed")
                    }
                    // We read this key, so refreshing past that write will fail: don't wait for it.
                    Isolation::Serializable if self.read_keys.contains(&key) => {
                        self.restart_because("a key this transaction read was overwritten");
                    }
                    Isolation::Serializable => {
                        self.meta.write_ts = self.meta.write_ts.max(ts.next());
                        self.issue_current();
                    }
                }
            }
            (Some(op), Response::Conflict(other)) => {
                let key = match &op {
                    TxnOp::Get(k)
                    | TxnOp::GetForUpdate(k)
                    | TxnOp::Put(k, _)
                    | TxnOp::Delete(k) => k.clone(),
                    TxnOp::Scan { start, .. } => start.clone(),
                };
                let kind = match op {
                    TxnOp::Get(_) => PushKind::Timestamp {
                        ts: self.read_ts,
                        priority: self.meta.priority,
                    },
                    // Wait first: only after WOUND_AFTER lost rounds may our
                    // priority abort the holder. A queue on a hot key then
                    // drains in turn instead of aborting whoever holds the
                    // lock, while a deadlock (each waiting for the other) is
                    // still broken by priority after ~100 ms. Priority 0
                    // never wins; the push still reports a finished or
                    // abandoned holder.
                    _ => PushKind::Abort {
                        priority: if self.backoffs >= WOUND_AFTER {
                            self.meta.priority
                        } else {
                            0
                        },
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
