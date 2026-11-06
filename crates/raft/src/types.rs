//! Raft's wire and log types.

use std::collections::BTreeSet;

pub use stowage_common::NodeId;

pub type Term = u64;
pub type Index = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryKind {
    /// Appended by every new leader so it can commit entries of earlier terms
    /// (Raft §5.4.2) and serve reads (§6.4).
    Noop,
    /// Application command.
    Normal(Vec<u8>),
    /// A new cluster configuration. Takes effect as soon as it is in the log, not
    /// when it commits (§4.1 / §4.3 of the Raft thesis).
    Config(Membership),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub term: Term,
    pub index: Index,
    pub kind: EntryKind,
}

impl Entry {
    /// Approximate encoded size, for batching limits.
    pub fn size(&self) -> usize {
        24 + match &self.kind {
            EntryKind::Noop => 0,
            EntryKind::Normal(d) => d.len(),
            EntryKind::Config(m) => 8 * (m.voters.len() + m.outgoing.len()),
        }
    }
}

/// Voting members. During a joint-consensus change both `voters` (the new set) and
/// `outgoing` (the old set) must agree: every decision needs a majority of each.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Membership {
    pub voters: BTreeSet<NodeId>,
    pub outgoing: BTreeSet<NodeId>,
}

impl Membership {
    pub fn new(voters: impl IntoIterator<Item = NodeId>) -> Self {
        Self {
            voters: voters.into_iter().collect(),
            outgoing: BTreeSet::new(),
        }
    }

    pub fn is_joint(&self) -> bool {
        !self.outgoing.is_empty()
    }

    /// Every node that votes in either half.
    pub fn all(&self) -> BTreeSet<NodeId> {
        self.voters.union(&self.outgoing).copied().collect()
    }

    pub fn contains(&self, id: NodeId) -> bool {
        self.voters.contains(&id) || self.outgoing.contains(&id)
    }

    /// Does `yes` contain a majority of every half?
    pub fn has_quorum(&self, yes: &BTreeSet<NodeId>) -> bool {
        majority(&self.voters, yes) && (self.outgoing.is_empty() || majority(&self.outgoing, yes))
    }

    /// Can `yes` still reach a quorum if every undecided node agrees?
    pub fn quorum_possible(&self, no: &BTreeSet<NodeId>) -> bool {
        let possible = |set: &BTreeSet<NodeId>| {
            let against = set.intersection(no).count();
            set.is_empty() || (set.len() - against) * 2 > set.len()
        };
        possible(&self.voters) && possible(&self.outgoing)
    }

    /// The highest index replicated on a quorum, given each node's match index.
    pub fn committed_index(&self, matched: &dyn Fn(NodeId) -> Index) -> Index {
        let half = |set: &BTreeSet<NodeId>| -> Index {
            if set.is_empty() {
                return Index::MAX;
            }
            let mut m: Vec<Index> = set.iter().map(|&id| matched(id)).collect();
            m.sort_unstable_by(|a, b| b.cmp(a));
            m[set.len() / 2] // the (n/2+1)-th largest
        };
        half(&self.voters).min(half(&self.outgoing))
    }
}

fn majority(set: &BTreeSet<NodeId>, yes: &BTreeSet<NodeId>) -> bool {
    set.intersection(yes).count() * 2 > set.len()
}

/// Durable vote state. Must be persisted before answering any message.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HardState {
    pub term: Term,
    pub vote: Option<NodeId>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub index: Index,
    pub term: Term,
    pub membership: Membership,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub meta: SnapshotMeta,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub from: NodeId,
    pub to: NodeId,
    pub term: Term,
    pub body: Body,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    /// Pre-vote (thesis §9.6): "would you vote for me in `term`?" Sent with the
    /// *next* term but changes nobody's term, so a node rejoining after a partition
    /// cannot depose a healthy leader.
    PreVote {
        last_index: Index,
        last_term: Term,
    },
    PreVoteResp {
        granted: bool,
    },
    /// `force`: sent after a leadership transfer's `TimeoutNow`; overrides leader
    /// stickiness on the voters.
    Vote {
        last_index: Index,
        last_term: Term,
        force: bool,
    },
    VoteResp {
        granted: bool,
    },
    Append {
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        commit: Index,
    },
    AppendResp {
        /// The `prev_index` of the request this answers (to discard stale replies).
        prev_index: Index,
        success: bool,
        /// On success: the last index now known to match the leader.
        match_index: Index,
        /// On failure: where the follower thinks the leader should retry from.
        hint: Option<ConflictHint>,
    },
    Heartbeat {
        commit: Index,
        read_ctx: u64,
        round: u64,
    },
    HeartbeatResp {
        read_ctx: u64,
        round: u64,
    },
    InstallSnapshot {
        snapshot: Snapshot,
    },
    /// Leadership transfer: "start an election now".
    TimeoutNow,
}

/// Fast log backtracking (thesis §5.3 optimisation): instead of retrying one index
/// at a time, the follower reports the term at `prev_index` and the first index it
/// holds of that term, or its last index if the log is too short.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConflictHint {
    pub term: Option<Term>,
    pub first_index: Index,
}
