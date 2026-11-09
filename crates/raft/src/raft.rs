//! The Raft state machine.
//!
//! [`Raft`] performs no I/O of its own except through its [`Storage`] (which is
//! durable on return). The driver feeds it inputs and collects outputs:
//!
//! ```text
//!  inputs                         Raft                          outputs
//!  ──────                         ────                          ───────
//!  tick()             ─┐                                    ┌─► take_messages()  → network
//!  step(Message)      ─┼─►  roles, terms, log, progress  ───┼─► take_snapshot()  → restore state machine
//!  propose(data)      ─┤    (persisted before replying)     ├─► take_committed() → apply in order
//!  read(id)           ─┤                                    └─► take_reads()     → serve reads
//!  propose_membership ─┘
//! ```
//!
//! Everything beyond the paper's core that a production deployment needs is here:
//! - **pre-vote** (thesis §9.6): rejoining nodes cannot disrupt a healthy leader;
//! - **check-quorum + leader stickiness**: a leader that loses its quorum steps down,
//!   and followers that heard from a leader recently refuse to vote;
//! - **ReadIndex** and **lease** reads (§6.4);
//! - **pipelined replication** with in-flight limits and fast conflict backtracking;
//! - **snapshots** with `InstallSnapshot`;
//! - **joint-consensus membership changes** (§4.3) and **leadership transfer** (§3.10).

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use stowage_common::Rng;
use stowage_common::os::Xoshiro256;

use crate::storage::Storage;
use crate::types::{
    Body, ConflictHint, Entry, EntryKind, HardState, Index, Membership, Message, NodeId, Snapshot,
    SnapshotMeta, Term,
};
use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    /// Confirm leadership with a heartbeat round per batch of reads. Safe under any
    /// clock behaviour.
    ReadIndex,
    /// Serve reads locally while the leader holds a lease: a quorum acknowledged a
    /// heartbeat sent less than `election_ticks - lease_margin_ticks` ago. Relies on
    /// bounded clock *rate* drift between nodes, and requires `check_quorum`.
    Lease,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub id: NodeId,
    /// Followers start an election after a random timeout in
    /// `[election_ticks, 2 * election_ticks)`.
    pub election_ticks: u32,
    pub heartbeat_ticks: u32,
    /// Upper bound on entry bytes in one `Append`.
    pub max_append_bytes: usize,
    /// Pipelined `Append`s outstanding per follower.
    pub max_inflight: usize,
    pub pre_vote: bool,
    pub check_quorum: bool,
    pub read_mode: ReadMode,
    pub lease_margin_ticks: u32,
    /// Seeds the election-timeout jitter (deterministic in simulation).
    pub seed: u64,
    /// Snapshots for lagging followers are produced by the application on demand
    /// (see [`Raft::take_snapshot_requests`] / [`Raft::send_snapshot`]) instead of
    /// being read from `Storage`. A snapshot generated at send time always reflects
    /// the current state, never a stale one taken at the last compaction.
    pub app_snapshots: bool,
    /// The driver writes the log in the background and reports completion with
    /// [`Raft::mark_durable`]. A leader then counts its own log towards a quorum
    /// only up to the durable index: it may replicate entries to followers
    /// *while* writing them locally (thesis §10.2.1) without ever committing an
    /// entry that a majority has not persisted.
    pub async_durability: bool,
    /// Ticks without any reply before a follower that was sent a snapshot is
    /// considered stalled and sent another one. Much longer than the Append
    /// stall timeout: a snapshot of a large range takes time to ship and
    /// install, and an impatient leader resending it (generating each copy)
    /// turns one lagging follower into a snapshot storm.
    pub snapshot_retry_ticks: u32,
}

impl Config {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            election_ticks: 10,
            heartbeat_ticks: 2,
            max_append_bytes: 256 << 10,
            max_inflight: 16,
            pre_vote: true,
            check_quorum: true,
            read_mode: ReadMode::ReadIndex,
            lease_margin_ticks: 2,
            seed: id,
            app_snapshots: false,
            async_durability: false,
            snapshot_retry_ticks: 50,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplState {
    /// One `Append` at a time until the follower's log position is found.
    Probe,
    /// Optimistically stream entries, up to `max_inflight` messages.
    Replicate,
    /// Waiting for an `InstallSnapshot` at this index to be acknowledged.
    Snapshot(Index),
}

#[derive(Debug, Clone)]
struct Progress {
    next: Index,
    matched: Index,
    state: ReplState,
    probe_sent: bool,
    /// Last index of each in-flight `Append`.
    inflight: VecDeque<Index>,
    recent_active: bool,
    /// Tick of the last `Append`/snapshot sent, and of the last reply to one. A
    /// follower that is behind while both are two heartbeat intervals old has lost
    /// a message: fall back to probing.
    last_send_tick: u64,
    last_resp_tick: u64,
    /// Latest heartbeat round this peer acknowledged (for leases).
    acked_round: u64,
    /// In this probe cycle, already probed at the compaction boundary (see
    /// `send_append`), so the next miss means "send a snapshot".
    tried_boundary: bool,
}

impl Progress {
    fn new(next: Index) -> Self {
        Self {
            next,
            matched: 0,
            state: ReplState::Probe,
            probe_sent: false,
            inflight: VecDeque::new(),
            recent_active: true,
            last_send_tick: 0,
            last_resp_tick: 0,
            acked_round: 0,
            tried_boundary: false,
        }
    }
    fn reset_probe(&mut self) {
        self.state = ReplState::Probe;
        self.next = self.matched + 1;
        self.probe_sent = false;
        self.inflight.clear();
        self.tried_boundary = false;
    }
}

#[derive(Debug)]
struct PendingRead {
    id: u64,
    ctx: u64,
    index: Index,
    acks: BTreeSet<NodeId>,
}

/// Counters for docs, metrics and tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RaftStats {
    pub elections: u64,
    pub pre_votes: u64,
    pub terms_led: u64,
    pub appends_sent: u64,
    pub entries_sent: u64,
    pub snapshots_sent: u64,
    pub rejections: u64,
    pub votes_refused_in_lease: u64,
    pub reads_lease: u64,
    pub reads_index: u64,
}

pub struct Raft<S: Storage> {
    cfg: Config,
    store: S,
    term: Term,
    vote: Option<NodeId>,
    role: Role,
    leader: Option<NodeId>,
    commit: Index,
    applied: Index,
    membership: Membership,
    /// Log index of the entry that set `membership` (snapshot index if none).
    membership_index: Index,
    progress: BTreeMap<NodeId, Progress>,
    votes: BTreeMap<NodeId, bool>,
    election_elapsed: u32,
    heartbeat_elapsed: u32,
    randomized_timeout: u32,
    rng: Xoshiro256,
    now_tick: u64,
    hb_round: u64,
    /// Tick at which each recent heartbeat round was sent.
    hb_round_tick: BTreeMap<u64, u64>,
    read_seq: u64,
    pending_reads: VecDeque<PendingRead>,
    /// Reads waiting for this term's first commit.
    reads_before_commit: Vec<u64>,
    ready_reads: Vec<(u64, Index)>,
    transfer_target: Option<NodeId>,
    msgs: Vec<Message>,
    pending_snapshot: Option<Snapshot>,
    /// Followers that need an application-generated snapshot.
    snapshot_requests: BTreeSet<NodeId>,
    /// This node's log is durable up to here (only meaningful with `async_durability`).
    durable_index: Index,
    /// May this node campaign? An application replica with no state machine
    /// yet (waiting for a snapshot) votes but must not lead.
    promotable: bool,
    /// Term or vote changed and must be persisted before any output is taken.
    hs_dirty: bool,
    stats: RaftStats,
}

impl<S: Storage> std::fmt::Debug for Raft<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Raft")
            .field("id", &self.cfg.id)
            .field("role", &self.role)
            .field("term", &self.term)
            .field("leader", &self.leader)
            .field("commit", &self.commit)
            .field("last", &self.store.last_index())
            .finish_non_exhaustive()
    }
}

impl<S: Storage> Raft<S> {
    pub fn new(cfg: Config, store: S) -> Result<Self> {
        let HardState { term, vote } = store.hard_state();
        let snap_index = store.snapshot_meta().index;
        // A log that starts from a snapshot of term T must only ever get entries of
        // terms >= T, or "last term" comparisons in elections invert: a replica
        // holding nothing but the snapshot would look more up to date than one with
        // newer entries written in a lower term.
        let (term, vote) = if store.snapshot_meta().term > term {
            (store.snapshot_meta().term, None)
        } else {
            (term, vote)
        };
        let mut r = Self {
            rng: Xoshiro256::seeded(cfg.seed),
            cfg,
            term,
            vote,
            role: Role::Follower,
            leader: None,
            commit: snap_index,
            applied: snap_index,
            membership: Membership::default(),
            membership_index: 0,
            progress: BTreeMap::new(),
            votes: BTreeMap::new(),
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            randomized_timeout: 0,
            now_tick: 0,
            hb_round: 0,
            hb_round_tick: BTreeMap::new(),
            read_seq: 0,
            pending_reads: VecDeque::new(),
            reads_before_commit: Vec::new(),
            ready_reads: Vec::new(),
            transfer_target: None,
            msgs: Vec::new(),
            pending_snapshot: (snap_index > 0).then(|| store.snapshot()).transpose()?,
            store,
            snapshot_requests: BTreeSet::new(),
            durable_index: 0,
            promotable: true,
            hs_dirty: false,
            stats: RaftStats::default(),
        };
        r.recompute_membership();
        r.reset_election_timer();
        r.durable_index = r.store.last_index(); // whatever storage returned on open is durable
        Ok(r)
    }

    // ---- accessors ----------------------------------------------------------------

    pub fn id(&self) -> NodeId {
        self.cfg.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn term(&self) -> Term {
        self.term
    }
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    pub fn commit(&self) -> Index {
        self.commit
    }
    pub fn applied(&self) -> Index {
        self.applied
    }

    /// Tell a freshly opened node how far its state machine already got (it
    /// persists its own applied index). Entries up to there were committed, or
    /// they would not have been applied, so the commit index moves too. Without
    /// this, `applied` restarts at the snapshot index, and a snapshot sent before
    /// it catches up would be labelled older than the state it carries.
    pub fn restore_applied(&mut self, index: Index) {
        let index = index.min(self.store.last_index());
        self.applied = self.applied.max(index);
        self.commit = self.commit.max(index);
    }
    pub fn last_index(&self) -> Index {
        self.store.last_index()
    }
    pub fn membership(&self) -> &Membership {
        &self.membership
    }
    pub fn storage(&self) -> &S {
        &self.store
    }
    pub fn storage_mut(&mut self) -> &mut S {
        &mut self.store
    }
    pub fn stats(&self) -> RaftStats {
        self.stats
    }
    /// `(peer, match index)` as the leader sees it.
    pub fn progress(&self) -> Vec<(NodeId, Index, Index)> {
        self.progress
            .iter()
            .map(|(&id, p)| (id, p.matched, p.next))
            .collect()
    }

    /// Allow or forbid campaigning (see the `promotable` field). Forbidding it
    /// also gives up leadership this node holds.
    pub fn set_promotable(&mut self, promotable: bool) -> Result<()> {
        self.promotable = promotable;
        if !promotable && self.role != Role::Follower {
            let term = self.term;
            self.become_follower(term, None)?;
        }
        Ok(())
    }

    fn last_term(&self) -> Term {
        self.store.term(self.store.last_index()).unwrap_or(0)
    }
    fn is_voter(&self) -> bool {
        self.membership.contains(self.cfg.id)
    }
    fn in_lease(&self) -> bool {
        self.cfg.check_quorum
            && self.leader.is_some()
            && self.election_elapsed < self.cfg.election_ticks
    }

    // ---- outputs --------------------------------------------------------------------

    pub fn take_messages(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.msgs)
    }

    /// A snapshot the state machine must restore before applying further entries.
    pub fn take_snapshot(&mut self) -> Option<Snapshot> {
        let s = self.pending_snapshot.take()?;
        self.applied = self.applied.max(s.meta.index);
        Some(s)
    }

    /// Newly committed entries, in order. They count as applied once returned.
    pub fn take_committed(&mut self) -> Vec<Entry> {
        if self.pending_snapshot.is_some() || self.commit <= self.applied {
            return Vec::new();
        }
        let entries = self
            .store
            .entries(self.applied + 1, self.commit + 1, usize::MAX);
        if let Some(last) = entries.last() {
            self.applied = last.index;
        }
        entries
    }

    /// The driver has made this node's log durable up to `index`.
    pub fn mark_durable(&mut self, index: Index) -> Result<()> {
        let index = index.min(self.store.last_index());
        if index > self.durable_index {
            self.durable_index = index;
            self.durable(Self::maybe_commit)?;
        }
        Ok(())
    }

    fn self_match(&self) -> Index {
        let last = self.store.last_index();
        if self.cfg.async_durability {
            last.min(self.durable_index)
        } else {
            last
        }
    }

    /// Followers waiting for a snapshot (only with `app_snapshots`). Answer each
    /// with [`Raft::send_snapshot`].
    pub fn take_snapshot_requests(&mut self) -> Vec<NodeId> {
        std::mem::take(&mut self.snapshot_requests)
            .into_iter()
            .collect()
    }

    /// Send `to` a snapshot of the state machine as of the current applied index;
    /// `data` must be exactly that state.
    pub fn send_snapshot(&mut self, to: NodeId, data: Vec<u8>) -> Result<()> {
        if self.role != Role::Leader {
            return Ok(());
        }
        let index = self.applied;
        let Some(term) = self.store.term(index) else {
            return Err(Error::Storage(format!("no term for applied index {index}")));
        };
        let membership = self.membership_at(index);
        let now = self.now_tick;
        if let Some(pr) = self.progress.get_mut(&to) {
            pr.state = ReplState::Snapshot(index);
            pr.last_send_tick = now;
        }
        self.stats.snapshots_sent += 1;
        self.send(
            to,
            Body::InstallSnapshot {
                snapshot: Snapshot {
                    meta: SnapshotMeta {
                        index,
                        term,
                        membership,
                    },
                    data,
                },
            },
        );
        Ok(())
    }

    /// Reads (by caller id) that may now be served from the state machine: the
    /// leader's leadership was confirmed and everything up to the read index has
    /// been applied.
    pub fn take_reads(&mut self) -> Vec<u64> {
        let applied = self.applied;
        let mut out = Vec::new();
        self.ready_reads.retain(|&(id, idx)| {
            if idx <= applied {
                out.push(id);
                false
            } else {
                true
            }
        });
        out
    }

    // ---- inputs: time ---------------------------------------------------------------

    pub fn tick(&mut self) -> Result<()> {
        self.durable(Self::tick_inner)
    }

    fn tick_inner(&mut self) -> Result<()> {
        self.now_tick += 1;
        self.election_elapsed += 1;
        if self.role == Role::Leader {
            self.heartbeat_elapsed += 1;
            if self.election_elapsed >= self.cfg.election_ticks {
                self.election_elapsed = 0;
                self.transfer_target = None;
                if self.cfg.check_quorum {
                    let mut active: BTreeSet<NodeId> = self
                        .progress
                        .iter()
                        .filter(|(_, p)| p.recent_active)
                        .map(|(&id, _)| id)
                        .collect();
                    active.insert(self.cfg.id);
                    if !self.membership.has_quorum(&active) {
                        tracing::debug!(
                            id = self.cfg.id,
                            term = self.term,
                            "lost quorum, stepping down"
                        );
                        self.become_follower(self.term, None)?;
                        return Ok(());
                    }
                    for (&id, p) in &mut self.progress {
                        p.recent_active = id == self.cfg.id;
                    }
                }
            }
            if self.heartbeat_elapsed >= self.cfg.heartbeat_ticks {
                self.heartbeat_elapsed = 0;
                let ctx = self.pending_reads.back().map_or(0, |r| r.ctx);
                self.broadcast_heartbeat(ctx);
            }
        } else if self.election_elapsed >= self.randomized_timeout
            && self.is_voter()
            && self.promotable
        {
            self.campaign(self.cfg.pre_vote, false)?;
        }
        Ok(())
    }

    // ---- inputs: client requests ----------------------------------------------------

    /// Append commands to the log (one fsync for the whole batch) and start
    /// replicating them. Returns the index of the last one.
    pub fn propose(&mut self, batch: Vec<Vec<u8>>) -> Result<Index> {
        self.check_can_propose()?;
        self.append_local(batch.into_iter().map(EntryKind::Normal).collect())
    }

    /// Change the voter set by joint consensus: first `C_old,new`, then — once that
    /// commits — `C_new` automatically.
    pub fn propose_membership(&mut self, voters: BTreeSet<NodeId>) -> Result<Index> {
        self.check_can_propose()?;
        if self.membership.is_joint() || self.membership_index > self.commit {
            return Err(Error::ConfigChangeInProgress);
        }
        if voters.is_empty() {
            return Err(Error::InvalidMembership("empty voter set".into()));
        }
        let joint = Membership {
            voters,
            outgoing: self.membership.voters.clone(),
        };
        self.append_local(vec![EntryKind::Config(joint)])
    }

    /// Queue a linearizable read identified by `id`; it becomes available through
    /// [`Raft::take_reads`].
    pub fn read(&mut self, id: u64) -> Result<()> {
        if self.role != Role::Leader {
            return Err(Error::NotLeader {
                leader: self.leader,
            });
        }
        if self.store.term(self.commit) != Some(self.term) {
            // Our no-op is not committed yet, so `commit` may lag behind entries
            // committed by earlier leaders.
            self.reads_before_commit.push(id);
            return Ok(());
        }
        self.start_read(id);
        Ok(())
    }

    fn start_read(&mut self, id: u64) {
        if self.cfg.read_mode == ReadMode::Lease && self.lease_valid() {
            self.stats.reads_lease += 1;
            self.ready_reads.push((id, self.commit));
            return;
        }
        self.stats.reads_index += 1;
        let me: BTreeSet<NodeId> = [self.cfg.id].into();
        if self.membership.has_quorum(&me) {
            self.ready_reads.push((id, self.commit));
            return;
        }
        self.read_seq += 1;
        let ctx = self.read_seq;
        self.pending_reads.push_back(PendingRead {
            id,
            ctx,
            index: self.commit,
            acks: me,
        });
        self.broadcast_heartbeat(ctx);
    }

    /// The leader's lease is valid if a quorum acknowledged a heartbeat round sent
    /// less than `election_ticks - margin` ticks ago. Followers refuse to vote for
    /// `election_ticks` after hearing from us, so no other leader can exist yet.
    fn lease_valid(&self) -> bool {
        if !self.cfg.check_quorum {
            return false;
        }
        let horizon = u64::from(
            self.cfg
                .election_ticks
                .saturating_sub(self.cfg.lease_margin_ticks),
        );
        let fresh: BTreeSet<NodeId> = self
            .progress
            .iter()
            .filter(|&(&id, p)| {
                id == self.cfg.id
                    || self
                        .hb_round_tick
                        .get(&p.acked_round)
                        .is_some_and(|&sent| self.now_tick < sent + horizon)
            })
            .map(|(&id, _)| id)
            .collect();
        self.membership.has_quorum(&fresh)
    }

    /// Hand leadership to `to` once its log has caught up.
    pub fn transfer_leadership(&mut self, to: NodeId) -> Result<()> {
        if self.role != Role::Leader {
            return Err(Error::NotLeader {
                leader: self.leader,
            });
        }
        if to == self.cfg.id || !self.membership.voters.contains(&to) {
            return Err(Error::InvalidMembership(format!("cannot transfer to {to}")));
        }
        self.transfer_target = Some(to);
        self.election_elapsed = 0; // the transfer is abandoned after an election timeout
        if self
            .progress
            .get(&to)
            .is_some_and(|p| p.matched == self.store.last_index())
        {
            self.send(to, Body::TimeoutNow);
        } else {
            self.send_append(to, false)?;
        }
        Ok(())
    }

    /// The state machine has a snapshot covering everything up to `index`
    /// (`<= applied`): store it and drop the log prefix.
    /// How far the log may be compacted without cutting off a follower that is
    /// catching up: the applied index, lowered on a leader to the match index of
    /// every recently active follower within `max_lag` entries of it. A
    /// follower compacted past needs a snapshot; one that just installed a
    /// snapshot and is streaming the entries after it would otherwise need
    /// another, and another, while the leader keeps writing. A follower further
    /// behind than `max_lag`, or silent, does not hold the log back.
    pub fn compaction_floor(&self, max_lag: u64) -> Index {
        let mut floor = self.applied;
        if self.role == Role::Leader {
            for (&id, p) in &self.progress {
                // A follower receiving a snapshot will continue from its index,
                // and may be too busy installing it to answer heartbeats (so
                // not "recently active"): keep what follows the snapshot.
                // Only a follower that is streaming entries holds the log: one
                // stuck probing (rejecting) may have lost what it once matched
                // (its replica was destroyed and recreated), and needs a
                // snapshot anyway.
                let (at, active) = match p.state {
                    ReplState::Snapshot(i) if i > 0 => (i, true),
                    ReplState::Replicate => (p.matched, p.recent_active),
                    _ => (p.matched, false),
                };
                if id != self.cfg.id && active && self.applied.saturating_sub(at) <= max_lag {
                    floor = floor.min(at);
                }
            }
        }
        floor
    }

    pub fn compact(&mut self, index: Index, data: Vec<u8>) -> Result<()> {
        if index > self.applied || index <= self.store.snapshot_meta().index {
            return Ok(());
        }
        let term = self
            .store
            .term(index)
            .ok_or_else(|| Error::Storage(format!("no term for {index}")))?;
        let membership = self.membership_at(index);
        self.store.compact(Snapshot {
            meta: SnapshotMeta {
                index,
                term,
                membership,
            },
            data,
        })
    }

    fn check_can_propose(&self) -> Result<()> {
        if self.role != Role::Leader {
            return Err(Error::NotLeader {
                leader: self.leader,
            });
        }
        if self.transfer_target.is_some() {
            return Err(Error::TransferInProgress);
        }
        Ok(())
    }

    // ---- inputs: messages -------------------------------------------------------------

    pub fn step(&mut self, m: Message) -> Result<()> {
        self.durable(|r| r.step_inner(m))
    }

    fn step_inner(&mut self, m: Message) -> Result<()> {
        tracing::trace!(id = self.cfg.id, from = m.from, term = m.term, my_term = self.term, role = ?self.role, body = ?std::mem::discriminant(&m.body), "step");
        if m.term > self.term {
            match &m.body {
                Body::PreVote { .. } => {}
                Body::PreVoteResp { granted: true } if self.role == Role::PreCandidate => {}
                Body::Vote { force: false, .. } if self.in_lease() => {
                    // Leader stickiness: we heard from a live leader recently.
                    self.stats.votes_refused_in_lease += 1;
                    return Ok(());
                }
                body => {
                    let leader = matches!(
                        body,
                        Body::Append { .. } | Body::Heartbeat { .. } | Body::InstallSnapshot { .. }
                    )
                    .then_some(m.from);
                    self.become_follower(m.term, leader)?;
                }
            }
        } else if m.term < self.term {
            match &m.body {
                // Tell a stale leader (e.g. healed from a partition) about the new term.
                Body::Append { .. } | Body::Heartbeat { .. } => self.send(
                    m.from,
                    Body::AppendResp {
                        prev_index: 0,
                        success: false,
                        match_index: 0,
                        hint: None,
                    },
                ),
                Body::PreVote { .. } => self.send(m.from, Body::PreVoteResp { granted: false }),
                _ => {}
            }
            return Ok(());
        }

        match m.body {
            Body::PreVote {
                last_index,
                last_term,
            } => {
                let granted = !self.in_lease() && self.up_to_date(last_index, last_term);
                tracing::trace!(id = self.cfg.id, from = m.from, granted, "pre-vote request");
                if !granted && self.in_lease() {
                    self.stats.votes_refused_in_lease += 1;
                }
                let term = if granted { m.term } else { self.term };
                self.msgs.push(Message {
                    from: self.cfg.id,
                    to: m.from,
                    term,
                    body: Body::PreVoteResp { granted },
                });
            }
            Body::Vote {
                last_index,
                last_term,
                ..
            } => {
                let can_vote =
                    self.vote == Some(m.from) || (self.vote.is_none() && self.leader.is_none());
                let granted = can_vote && self.up_to_date(last_index, last_term);
                if granted {
                    self.vote = Some(m.from);
                    self.persist_hard_state()?;
                    self.election_elapsed = 0;
                }
                self.send(m.from, Body::VoteResp { granted });
            }
            Body::PreVoteResp { granted } if self.role == Role::PreCandidate => {
                self.tally(m.from, granted, true)?;
            }
            Body::VoteResp { granted } if self.role == Role::Candidate => {
                self.tally(m.from, granted, false)?;
            }
            Body::Append {
                prev_index,
                prev_term,
                entries,
                commit,
            } => {
                self.accept_leader(m.from)?;
                self.handle_append(m.from, prev_index, prev_term, entries, commit)?;
            }
            Body::Heartbeat {
                commit,
                read_ctx,
                round,
            } => {
                self.accept_leader(m.from)?;
                // The leader sends min(our match, its commit), so this never exceeds
                // what we have.
                if commit > self.commit && commit <= self.store.last_index() {
                    self.commit = commit;
                }
                self.send(m.from, Body::HeartbeatResp { read_ctx, round });
            }
            Body::InstallSnapshot { snapshot } => {
                self.accept_leader(m.from)?;
                self.handle_snapshot(m.from, snapshot)?;
            }
            Body::AppendResp {
                prev_index,
                success,
                match_index,
                hint,
            } if self.role == Role::Leader => {
                self.handle_append_resp(m.from, prev_index, success, match_index, hint)?;
            }
            Body::HeartbeatResp { read_ctx, round } if self.role == Role::Leader => {
                self.handle_heartbeat_resp(m.from, read_ctx, round)?;
            }
            Body::TimeoutNow if self.is_voter() && self.promotable && self.role != Role::Leader => {
                self.campaign(false, true)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn accept_leader(&mut self, from: NodeId) -> Result<()> {
        if self.role != Role::Follower {
            self.become_follower(self.term, Some(from))?;
        }
        self.leader = Some(from);
        self.election_elapsed = 0;
        Ok(())
    }

    fn up_to_date(&self, last_index: Index, last_term: Term) -> bool {
        let my_term = self.last_term();
        last_term > my_term || (last_term == my_term && last_index >= self.store.last_index())
    }

    // ---- elections ----------------------------------------------------------------------

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        let et = u64::from(self.cfg.election_ticks);
        self.randomized_timeout = u32::try_from(et + self.rng.gen_range(0, et)).unwrap_or(u32::MAX);
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) -> Result<()> {
        if term > self.term {
            self.term = term;
            self.vote = None;
            self.persist_hard_state()?;
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.votes.clear();
        self.progress.clear();
        self.pending_reads.clear();
        self.reads_before_commit.clear();
        self.transfer_target = None;
        self.reset_election_timer();
        Ok(())
    }

    fn campaign(&mut self, pre: bool, force: bool) -> Result<()> {
        self.leader = None;
        self.votes.clear();
        self.votes.insert(self.cfg.id, true);
        self.reset_election_timer();
        let msg_term = if pre {
            self.role = Role::PreCandidate;
            self.stats.pre_votes += 1;
            self.term + 1
        } else {
            self.role = Role::Candidate;
            self.term += 1;
            self.vote = Some(self.cfg.id);
            self.persist_hard_state()?;
            self.stats.elections += 1;
            self.term
        };
        tracing::debug!(id = self.cfg.id, term = msg_term, pre, "campaign");
        let me: BTreeSet<NodeId> = [self.cfg.id].into();
        if self.membership.has_quorum(&me) {
            return if pre {
                self.campaign(false, force)
            } else {
                self.become_leader()
            };
        }
        let (last_index, last_term) = (self.store.last_index(), self.last_term());
        for peer in self.membership.all() {
            if peer == self.cfg.id {
                continue;
            }
            let body = if pre {
                Body::PreVote {
                    last_index,
                    last_term,
                }
            } else {
                Body::Vote {
                    last_index,
                    last_term,
                    force,
                }
            };
            self.msgs.push(Message {
                from: self.cfg.id,
                to: peer,
                term: msg_term,
                body,
            });
        }
        Ok(())
    }

    fn tally(&mut self, from: NodeId, granted: bool, pre: bool) -> Result<()> {
        tracing::trace!(
            id = self.cfg.id,
            from,
            granted,
            pre,
            term = self.term,
            "vote response"
        );
        if !self.membership.contains(from) {
            return Ok(());
        }
        self.votes.insert(from, granted);
        let yes: BTreeSet<NodeId> = self
            .votes
            .iter()
            .filter(|(_, g)| **g)
            .map(|(&id, _)| id)
            .collect();
        let no: BTreeSet<NodeId> = self
            .votes
            .iter()
            .filter(|(_, g)| !**g)
            .map(|(&id, _)| id)
            .collect();
        if self.membership.has_quorum(&yes) {
            if pre {
                self.campaign(false, false)?;
            } else {
                self.become_leader()?;
            }
        } else if !self.membership.quorum_possible(&no) {
            self.become_follower(self.term, None)?;
        }
        Ok(())
    }

    fn become_leader(&mut self) -> Result<()> {
        tracing::debug!(id = self.cfg.id, term = self.term, "became leader");
        self.role = Role::Leader;
        self.leader = Some(self.cfg.id);
        self.stats.terms_led += 1;
        self.votes.clear();
        self.heartbeat_elapsed = 0;
        self.election_elapsed = 0;
        let next = self.store.last_index() + 1;
        self.progress = self
            .membership
            .all()
            .into_iter()
            .map(|id| (id, Progress::new(next)))
            .collect();
        if let Some(me) = self.progress.get_mut(&self.cfg.id) {
            me.matched = next - 1;
        }
        self.append_local(vec![EntryKind::Noop])?;
        Ok(())
    }

    /// Mark term/vote for persistence. Every public entry point flushes it before
    /// returning, i.e. before the driver can take (and send) any message that
    /// depends on it. Coalescing matters: answering a vote request from a higher
    /// term changes the term *and* the vote, which would otherwise be two durable
    /// writes.
    fn persist_hard_state(&mut self) -> Result<()> {
        self.hs_dirty = true;
        Ok(())
    }

    fn flush_hard_state(&mut self) -> Result<()> {
        if self.hs_dirty {
            self.store.set_hard_state(HardState {
                term: self.term,
                vote: self.vote,
            })?;
            self.hs_dirty = false;
        }
        Ok(())
    }

    /// Run `f` and then persist any term/vote change it made.
    fn durable<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let r = f(self);
        self.flush_hard_state()?;
        r
    }

    // ---- replication: leader side ---------------------------------------------------------

    fn append_local(&mut self, kinds: Vec<EntryKind>) -> Result<Index> {
        let mut index = self.store.last_index();
        let entries: Vec<Entry> = kinds
            .into_iter()
            .map(|kind| {
                index += 1;
                Entry {
                    term: self.term,
                    index,
                    kind,
                }
            })
            .collect();
        if entries
            .iter()
            .any(|e| matches!(e.kind, EntryKind::Config(_)))
        {
            self.store.append(&entries)?;
            self.recompute_membership();
            self.sync_progress_with_membership();
        } else {
            self.store.append(&entries)?;
        }
        if let Some(me) = self.progress.get_mut(&self.cfg.id) {
            me.matched = index;
            me.next = index + 1;
        }
        self.maybe_commit()?;
        self.broadcast_append()?;
        Ok(index)
    }

    fn sync_progress_with_membership(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let all = self.membership.all();
        let next = self.store.last_index() + 1;
        for id in &all {
            self.progress
                .entry(*id)
                .or_insert_with(|| Progress::new(next));
        }
        self.progress.retain(|id, _| all.contains(id));
    }

    fn broadcast_append(&mut self) -> Result<()> {
        let peers: Vec<NodeId> = self
            .progress
            .keys()
            .copied()
            .filter(|&p| p != self.cfg.id)
            .collect();
        for p in peers {
            while self.send_append(p, false)? {}
        }
        Ok(())
    }

    /// Send the next `Append` to `peer` if its state allows. Returns whether a
    /// message carrying entries was sent (so callers can keep pipelining).
    fn send_append(&mut self, peer: NodeId, allow_empty: bool) -> Result<bool> {
        let last = self.store.last_index();
        let commit = self.commit;
        let max_bytes = self.cfg.max_append_bytes;
        let max_inflight = self.cfg.max_inflight;
        let Some(pr) = self.progress.get_mut(&peer) else {
            return Ok(false);
        };
        match pr.state {
            ReplState::Snapshot(_) => return Ok(false),
            ReplState::Probe if pr.probe_sent => return Ok(false),
            ReplState::Replicate if pr.inflight.len() >= max_inflight => return Ok(false),
            _ => {}
        }
        if pr.next > last && !allow_empty && pr.state == ReplState::Replicate {
            return Ok(false);
        }
        let snap_index = self.store.snapshot_meta().index;
        if pr.next <= snap_index && !pr.tried_boundary {
            // The follower seems to need entries we compacted. Before paying for a
            // snapshot, check whether it already has our log's first entry (e.g. a
            // range replica created by the same split, or one that only lagged
            // briefly): probe at the compaction boundary once per probe cycle.
            pr.tried_boundary = true;
            pr.next = snap_index + 1;
        }
        let prev = pr.next - 1;
        let Some(prev_term) = self.store.term(prev) else {
            // The entries this follower needs were compacted: send the snapshot.
            if self.cfg.app_snapshots {
                pr.state = ReplState::Snapshot(0);
                pr.last_send_tick = self.now_tick;
                self.snapshot_requests.insert(peer);
                return Ok(false);
            }
            let snapshot = self.store.snapshot()?;
            pr.state = ReplState::Snapshot(snapshot.meta.index);
            pr.last_send_tick = self.now_tick;
            self.stats.snapshots_sent += 1;
            self.send(peer, Body::InstallSnapshot { snapshot });
            return Ok(false);
        };
        let entries = if pr.next <= last {
            self.store.entries(pr.next, last + 1, max_bytes)
        } else {
            Vec::new()
        };
        let sent_entries = !entries.is_empty();
        match pr.state {
            ReplState::Replicate => {
                if let Some(e) = entries.last() {
                    pr.next = e.index + 1;
                    pr.inflight.push_back(e.index);
                }
            }
            ReplState::Probe => pr.probe_sent = true,
            ReplState::Snapshot(_) => {}
        }
        let probing = pr.state == ReplState::Probe;
        pr.last_send_tick = self.now_tick;
        self.stats.appends_sent += 1;
        self.stats.entries_sent += entries.len() as u64;
        self.send(
            peer,
            Body::Append {
                prev_index: prev,
                prev_term,
                entries,
                commit,
            },
        );
        Ok(sent_entries && !probing)
    }

    fn broadcast_heartbeat(&mut self, read_ctx: u64) {
        self.hb_round += 1;
        let round = self.hb_round;
        self.hb_round_tick.insert(round, self.now_tick);
        while self.hb_round_tick.len() > 64 {
            self.hb_round_tick.pop_first();
        }
        let peers: Vec<(NodeId, Index)> = self
            .progress
            .iter()
            .filter(|&(&id, _)| id != self.cfg.id)
            .map(|(&id, p)| (id, p.matched))
            .collect();
        for (peer, matched) in peers {
            let commit = self.commit.min(matched);
            self.send(
                peer,
                Body::Heartbeat {
                    commit,
                    read_ctx,
                    round,
                },
            );
        }
    }

    fn handle_append_resp(
        &mut self,
        from: NodeId,
        prev_index: Index,
        success: bool,
        match_index: Index,
        hint: Option<ConflictHint>,
    ) -> Result<()> {
        let last = self.store.last_index();
        let now = self.now_tick;
        let Some(pr) = self.progress.get_mut(&from) else {
            return Ok(());
        };
        pr.recent_active = true;
        pr.last_resp_tick = now;
        if success {
            pr.tried_boundary = false;
            if match_index > pr.matched {
                pr.matched = match_index;
            }
            pr.next = pr.next.max(pr.matched + 1);
            while pr.inflight.front().is_some_and(|&i| i <= pr.matched) {
                pr.inflight.pop_front();
            }
            match pr.state {
                ReplState::Probe => {
                    pr.state = ReplState::Replicate;
                    pr.probe_sent = false;
                    pr.inflight.clear();
                    pr.next = pr.matched + 1;
                }
                ReplState::Snapshot(i) if pr.matched >= i => {
                    pr.state = ReplState::Replicate;
                    pr.next = pr.matched + 1;
                }
                _ => {}
            }
            let caught_up = pr.matched == last;
            self.maybe_commit()?;
            if self.role != Role::Leader {
                return Ok(());
            }
            if caught_up && self.transfer_target == Some(from) {
                self.send(from, Body::TimeoutNow);
            }
            while self.send_append(from, false)? {}
        } else {
            if prev_index < pr.matched {
                return Ok(()); // stale rejection
            }
            if matches!(pr.state, ReplState::Snapshot(_)) {
                // A rejection of an Append sent before the snapshot. Probing
                // again would find the entries compacted and request another
                // snapshot, then another for each rejection still in flight
                // (a snapshot storm). The snapshot's acknowledgement moves the
                // follower on; `snapshot_retry_ticks` covers a lost one.
                return Ok(());
            }
            self.stats.rejections += 1;
            let next = match hint {
                Some(ConflictHint {
                    term: Some(t),
                    first_index,
                }) => self
                    .last_index_of_term(t, prev_index)
                    .map_or(first_index, |i| i + 1),
                Some(ConflictHint {
                    term: None,
                    first_index,
                }) => first_index,
                None => prev_index,
            };
            let Some(pr) = self.progress.get_mut(&from) else {
                return Ok(());
            };
            if let Some(ConflictHint {
                term: None,
                first_index,
            }) = hint
                && prev_index == pr.matched
                && first_index <= pr.matched
            {
                // A probe exactly at the entry we counted as matched was
                // rejected because the follower's log ends before it: its
                // replica was destroyed and recreated empty (removed, then
                // added back). Its old match index means nothing now; without
                // resetting it we would probe at `matched` forever. (Requiring
                // `prev_index == matched` ignores a reordered rejection of an
                // older probe, sent before the follower caught up.)
                pr.matched = first_index.saturating_sub(1);
            }
            // Keep "already probed at the boundary": a rejection of that probe
            // means the follower really needs a snapshot.
            let tried = pr.tried_boundary;
            pr.reset_probe();
            pr.tried_boundary = tried;
            pr.next = next.min(prev_index.max(1)).max(pr.matched + 1);
            self.send_append(from, true)?;
        }
        Ok(())
    }

    /// The last index (≤ `upto`) whose term is `t`, if the leader has any entry of
    /// term `t`.
    fn last_index_of_term(&self, t: Term, upto: Index) -> Option<Index> {
        let mut i = upto.min(self.store.last_index());
        while i >= self.store.first_index() {
            match self.store.term(i) {
                Some(x) if x == t => return Some(i),
                Some(x) if x < t => return None,
                _ => {}
            }
            i -= 1;
        }
        None
    }

    fn handle_heartbeat_resp(&mut self, from: NodeId, read_ctx: u64, round: u64) -> Result<()> {
        let last = self.store.last_index();
        let Some(pr) = self.progress.get_mut(&from) else {
            return Ok(());
        };
        pr.recent_active = true;
        pr.acked_round = pr.acked_round.max(round);
        let quiet = u64::from(2 * self.cfg.heartbeat_ticks);
        // Behind, and both the last reply and the last send are two heartbeat
        // intervals old. (Comparing send and reply ticks with each other is not
        // enough: a reply and the next send often fall in the same tick.) A
        // snapshot is different: the follower keeps answering heartbeats while
        // it receives and installs one, so only the time since it was sent
        // counts, and that is allowed to be long.
        let stalled = pr.matched < last
            && match pr.state {
                ReplState::Snapshot(_) => {
                    self.now_tick >= pr.last_send_tick + u64::from(self.cfg.snapshot_retry_ticks)
                }
                _ => {
                    self.now_tick >= pr.last_resp_tick + quiet
                        && self.now_tick >= pr.last_send_tick + quiet
                }
            };
        if stalled {
            // An Append, a snapshot, or a reply to one was lost: find the
            // follower's position again (resending the snapshot if still needed).
            pr.reset_probe();
            self.send_append(from, true)?;
        }
        if read_ctx > 0 {
            for r in &mut self.pending_reads {
                if r.ctx <= read_ctx {
                    r.acks.insert(from);
                }
            }
            while self
                .pending_reads
                .front()
                .is_some_and(|r| self.membership.has_quorum(&r.acks))
            {
                if let Some(r) = self.pending_reads.pop_front() {
                    self.ready_reads.push((r.id, r.index));
                }
            }
        }
        Ok(())
    }

    fn maybe_commit(&mut self) -> Result<()> {
        if self.role != Role::Leader {
            return Ok(());
        }
        let progress = &self.progress;
        let me = self.cfg.id;
        let mine = self.self_match();
        let candidate = self.membership.committed_index(&|id| {
            if id == me {
                mine
            } else {
                progress.get(&id).map_or(0, |p| p.matched)
            }
        });
        // Only entries of the current term are committed by counting replicas
        // (Raft §5.4.2, "Figure 8"); earlier ones commit indirectly.
        if candidate > self.commit && self.store.term(candidate) == Some(self.term) {
            let first_in_term = self.store.term(self.commit) != Some(self.term);
            self.commit = candidate;
            if first_in_term {
                for id in std::mem::take(&mut self.reads_before_commit) {
                    self.start_read(id);
                }
            }
            self.on_commit_advanced()?;
        }
        Ok(())
    }

    fn on_commit_advanced(&mut self) -> Result<()> {
        if self.membership_index > self.commit {
            return Ok(());
        }
        if self.membership.is_joint() {
            // C_old,new is committed: move to C_new.
            let next = Membership {
                voters: self.membership.voters.clone(),
                outgoing: BTreeSet::new(),
            };
            self.append_local(vec![EntryKind::Config(next)])?;
        } else if !self.membership.voters.contains(&self.cfg.id) {
            // C_new committed without us: step down.
            self.become_follower(self.term, None)?;
        }
        Ok(())
    }

    // ---- replication: follower side ---------------------------------------------------------

    fn handle_append(
        &mut self,
        from: NodeId,
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
    ) -> Result<()> {
        // Everything up to our commit index is already known to match the leader.
        let (prev_index, prev_term, entries) = if prev_index < self.commit {
            let skip = usize::try_from(self.commit - prev_index).unwrap_or(usize::MAX);
            if skip >= entries.len() {
                let reply = Body::AppendResp {
                    prev_index,
                    success: true,
                    match_index: self.commit,
                    hint: None,
                };
                self.send(from, reply);
                return Ok(());
            }
            let commit_term = self.store.term(self.commit).unwrap_or(0);
            (self.commit, commit_term, entries[skip..].to_vec())
        } else {
            (prev_index, prev_term, entries)
        };

        if self.store.term(prev_index) != Some(prev_term) {
            let last = self.store.last_index();
            let hint = if prev_index > last {
                ConflictHint {
                    term: None,
                    first_index: last + 1,
                }
            } else {
                let t = self.store.term(prev_index).unwrap_or(0);
                let mut first = prev_index;
                while first > self.commit + 1 && self.store.term(first - 1) == Some(t) {
                    first -= 1;
                }
                ConflictHint {
                    term: Some(t),
                    first_index: first,
                }
            };
            self.send(
                from,
                Body::AppendResp {
                    prev_index,
                    success: false,
                    match_index: 0,
                    hint: Some(hint),
                },
            );
            return Ok(());
        }

        let last_new = prev_index + entries.len() as u64;
        // Skip entries we already have; append from the first missing/conflicting one.
        let mut start = entries.len();
        for (k, e) in entries.iter().enumerate() {
            match self.store.term(e.index) {
                Some(t) if t == e.term => continue,
                Some(_) if e.index <= self.commit => {
                    return Err(Error::Storage(format!(
                        "leader {from} conflicts with committed entry {}",
                        e.index
                    )));
                }
                _ => {
                    start = k;
                    break;
                }
            }
        }
        if start < entries.len() {
            let truncates = entries[start].index <= self.store.last_index();
            if truncates {
                self.durable_index = self.durable_index.min(entries[start].index - 1);
            }
            let has_config = entries[start..]
                .iter()
                .any(|e| matches!(e.kind, EntryKind::Config(_)));
            self.store.append(&entries[start..])?;
            if truncates || has_config {
                self.recompute_membership();
            }
        }
        let new_commit = leader_commit.min(last_new);
        if new_commit > self.commit {
            self.commit = new_commit;
        }
        self.send(
            from,
            Body::AppendResp {
                prev_index,
                success: true,
                match_index: last_new,
                hint: None,
            },
        );
        Ok(())
    }

    fn handle_snapshot(&mut self, from: NodeId, snap: Snapshot) -> Result<()> {
        let idx = snap.meta.index;
        if idx > self.commit {
            if self.store.term(idx) == Some(snap.meta.term) {
                // We already have the entry: just fast-forward the commit index.
                self.commit = idx;
            } else {
                self.store.install_snapshot(snap.clone())?;
                self.commit = idx;
                self.applied = self.applied.min(idx);
                self.pending_snapshot = Some(snap);
                self.recompute_membership();
            }
        }
        let match_index = self.commit;
        self.send(
            from,
            Body::AppendResp {
                prev_index: match_index,
                success: true,
                match_index,
                hint: None,
            },
        );
        Ok(())
    }

    // ---- membership ---------------------------------------------------------------------------

    /// The latest configuration in the log takes effect immediately.
    fn recompute_membership(&mut self) {
        let (m, i) = self.config_at(self.store.last_index());
        self.membership = m;
        self.membership_index = i;
    }

    /// The configuration in effect at log index `index`.
    pub fn membership_at(&self, index: Index) -> Membership {
        self.config_at(index).0
    }

    fn config_at(&self, index: Index) -> (Membership, Index) {
        let first = self.store.first_index();
        let mut i = index;
        while i >= first {
            if let Some(e) = self.store.entries(i, i + 1, usize::MAX).pop()
                && let EntryKind::Config(m) = e.kind
            {
                return (m, i);
            }
            i -= 1;
        }
        let meta = self.store.snapshot_meta();
        (meta.membership.clone(), meta.index)
    }

    fn send(&mut self, to: NodeId, body: Body) {
        self.msgs.push(Message {
            from: self.cfg.id,
            to,
            term: self.term,
            body,
        });
    }
}
