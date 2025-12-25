//! This module contains Raft algorithm and RPCs types.

/// A term is a period during which a leader is elected and operates.
pub type Term = usize;

/// Unique Raft server identifier across the cluster.
pub type ServerId = usize;

/// Index is an unsigned integer that increases monotonically and wraps around
/// on overflow.
pub type Index = usize;

/// LogEntry defines a single entry in the replicated Raft log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub index: Index,
    pub term: Term,
    pub data: Vec<u8>,
}

/// RPC requests and response.
pub mod rpc {
    use super::*;

    /// RPC call invoked by leader to replicate log entries; also used as heart
    /// beat to maintain authority.
    #[derive(Debug, Clone)]
    pub struct AppendEntriesRequest {
        /// Leader's term.
        pub term: Term,
        /// So follower can redirect clients.
        pub leader_id: ServerId,
        /// Index of log entry immediately preceding new ones.
        pub prev_log_index: Index,
        /// Term of prev_log_index entry.
        pub prev_log_term: Term,
        /// Log entries to store (empty for heartbeat; may send more than one for
        /// efficiency).
        pub entries: Vec<LogEntry>,
        /// Leader's commit index.
        pub leader_commit: Index,
    }

    /// Result of [AppendEntriesRequest] RPC call.
    #[derive(Debug, Clone)]
    pub struct AppendEntriesResponse {
        /// Current term, for leader to update itself.
        pub term: Term,
        /// True if follower contained entry matching prev_log_index and
        /// prev_log_term.
        pub success: bool,

        /// Follower that produced the response.
        pub follower_id: ServerId,
    }

    /// RPC call invoked by candidates to gather votes.
    #[derive(Debug, Clone)]
    pub struct RequestVoteRequest {
        /// Candidate's term.
        pub term: Term,
        /// Candidate requesting vote.
        pub candidate_id: ServerId,
        /// Index of candidate's last log entry.
        pub last_log_index: Index,
        /// Term of candidate's last log entry.
        pub last_log_term: Term,
    }

    /// Result of [RequestVoteRequest] RPC call.
    #[derive(Debug, Clone)]
    pub struct RequestVoteResponse {
        /// Current term, for leader to update itself.
        pub term: Term,
        /// True means candidate received vote;
        pub vote_granted: bool,
    }
}

/// Raft server persistent state that must be updated on stable storage before
/// responding to RPC.
#[derive(Debug, Default, Clone)]
pub struct ServerPersistentState {
    /// Latest term node has seen (initialized to 0 on first boot, increases
    /// monotonically).
    pub current_term: Term,
    /// Node id that received vote in current term.
    pub voted_for: Option<usize>,
    /// Log entries; each entry contains command for state machine, and term
    /// when entry was received by leader (first index is 1).
    pub log: Log,
}

/// Log entries; each entry contains command for state machine, and term when
/// entry was received by leader (first index is 1).
#[derive(Debug, Clone)]
pub struct Log {
    entries: Vec<LogEntry>,

    offset: usize,
}

impl Default for Log {
    fn default() -> Self {
        Self {
            entries: Default::default(),
            offset: 1, // Log index starts at 1.
        }
    }
}

impl Log {
    pub fn get(&self, index: usize) -> Option<&LogEntry> {
        if index < self.offset {
            return None;
        }

        self.entries.get(index - self.offset)
    }

    pub fn truncate(&mut self, len: usize) {
        self.entries.truncate(len - self.offset)
    }

    pub fn last(&self) -> Option<&LogEntry> {
        self.entries.last()
    }

    pub fn append(&mut self, mut other: Vec<LogEntry>) {
        self.entries.append(&mut other);
    }

    pub fn slice(&self, from: usize, to: Option<usize>) -> &[LogEntry] {
        match to {
            Some(to) => &self.entries[from - self.offset..to - self.offset],
            None => &self.entries[from - self.offset..],
        }
    }

    pub fn last_index(&self) -> Index {
        self.entries.last().map(|e| e.index).unwrap_or(0)
    }

    pub fn last_term(&self) -> Term {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }
}

/// Raft server volatile state that is present regardless of its role (leader,
/// follower, or candidate).
#[derive(Debug, Default)]
pub(crate) struct ServerVolatileState {
    /// Index of highest log entry known to be committed (initialized to 0,
    /// increases monotonically).
    pub commit_index: Index,
    /// Index of highest log entry applied to state machine (initialized to 0,
    /// increases monotonically).
    pub last_applied: Index,
}

/// Raft server volatile state when candidate during election.
#[derive(Debug, Default)]
pub(crate) struct CandidateVolatileState {
    pub granted_vote: usize,
    pub rejected_vote: usize,
}

/// Raft server volatile state when leading the cluster.
#[derive(Debug)]
pub(crate) struct LeaderVolatileState {
    pub followers: Vec<Follower>,
}

impl LeaderVolatileState {
    pub fn new<'a>(last_log_index: Index, ids: impl Iterator<Item = &'a ServerId>) -> Self {
        let followers = ids
            .map(|id| Follower {
                id: *id,
                next_index: last_log_index + 1,
                match_index: 0,
            })
            .collect();

        Self { followers }
    }

    /// Returns index of log committed by majority of followers.
    pub fn majority_commit_index(&self) -> Index {
        if self.followers.is_empty() {
            return 0;
        }

        // Compute median.
        let mut match_indexes: Vec<Index> = self.followers.iter().map(|e| e.match_index).collect();
        match_indexes.sort();

        let half = match_indexes.len() / 2;
        if match_indexes.len().is_multiple_of(2) && match_indexes.len() > 2 {
            // Suppose there is 4 followers with following commit indexes:
            // [1, 2, 3, 4] we want to return 3 because leader has committed 3.
            match_indexes[half + 1]
        } else {
            match_indexes[half]
        }
    }
}

#[derive(Debug)]
pub(crate) struct Follower {
    /// Unique identifier across whole Raft cluster.
    pub id: ServerId,
    /// Index of the next log entry to send to that server (initialized to
    /// leader last log index + 1)
    pub next_index: Index,
    /// Index of highest log entry known to be replicated on server (initialized
    /// to 0, increases monotonically).
    pub match_index: Index,
}
