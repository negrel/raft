//! This module contains Raft protocol RPC types.

/// A term is a period during which a leader is elected and operates.
pub type Term = usize;

/// Unique Raft server identifier across the cluster.
pub type ServerId = usize;

/// Index is an unsigned integer that increases monotonically and wraps around
/// on overflow.
pub type Index = usize;

/// LogEntry defines a single entry in the replicated Raft log.
#[derive(Debug, Clone)]
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
    #[derive(Debug)]
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
    #[derive(Debug)]
    pub struct AppendEntriesResponse {
        /// Current term, for leader to update itself.
        pub term: Term,
        /// True if follower contained entry matching prev_log_index and
        /// prev_log_term.
        pub success: bool,
    }

    /// RPC call invoked by candidates to gather votes.
    #[derive(Debug)]
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
    #[derive(Debug)]
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
    pub log: Vec<LogEntry>,
}

impl ServerPersistentState {
    fn find_log(&self, log_idx: Index, log_term: Term) -> Option<usize> {
        if let Some(f) = self.log.first() {
            if log_idx < f.index {
                // This branch should never happen as we keep all log entries in
                // memory.
                todo!("TODO: return None once snapshotting is implemented");
            } else if log_idx > f.index {
                let diff = log_idx - f.index;
                if let Some(entry) = self.log.get(f.index + diff)
                    && entry.term == log_term
                {
                    return Some(diff);
                }
            } else if f.term == log_term {
                return Some(0);
            }
        }

        None
    }
}

/// Raft server volatile state that is present regardless of its role (leader,
/// follower, or candidate).
#[derive(Default)]
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
#[derive(Debug, Default)]
pub(crate) struct LeaderVolatileState {
    pub followers: Vec<Follower>,
}

#[derive(Debug)]
pub(crate) struct Follower {
    // Unique identifier across whole Raft cluster.
    pub id: ServerId,
    /// Index of the next log entry to send to that server (initialized to
    /// leader last log index + 1)
    pub next_index: Index,
    /// Index of highest log entry known to be replicated on server (initialized
    /// to 0, increases monotonically).
    pub match_index: Index,
}
