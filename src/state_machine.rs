use std::{
    cmp::min,
    ops::Range,
    time::{Duration, Instant},
};

use crate::{
    CandidateVolatileState, LeaderVolatileState, ServerId, ServerPersistentState,
    ServerVolatileState, rpc,
};

/// Raft server configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Unique server identifier across whole cluster.
    id: ServerId,
    /// Duration between each heartbeat. This should be an order of magnitude
    /// lower than election_timeout.0.
    heartbeat: Duration,
    /// Min and max election timeout.
    election_timeout: Range<Duration>,
    /// Peers servers in Raft cluster.
    peers: Vec<ServerId>,
}

/// State enumerates possible state of a Raft server.
#[derive(Debug, Default)]
pub(crate) enum State {
    #[default]
    Follower,
    Candidate(CandidateVolatileState),
    Leader(LeaderVolatileState),
}

pub enum Either<L, R> {
    Left(L),
    Right(R),
}

/// A Raft server within a cluster.
#[derive(Debug)]
pub struct Server {
    /// Server state that must be updated on stable storage before responding to
    /// RPC.
    persistent: ServerPersistentState,
    /// Server state that can be lost on crash.
    volatile: ServerVolatileState,

    /// Raft state.
    state: State,
    /// Server configuration.
    config: Config,
    /// Instant of last received / sent RPC request / response.
    last_rpc: Instant,
    /// Election timeout for current term.
    election_timeout: Duration,
}

impl Server {
    pub fn new(cfg: Config, now: Instant) -> Self {
        let election_timeout = cfg.election_timeout.start;

        Self {
            persistent: ServerPersistentState::default(),
            volatile: ServerVolatileState::default(),
            state: State::default(),
            config: cfg,
            last_rpc: now,
            election_timeout,
        }
    }

    /// Check for heartbeat or election timeout.
    pub fn tick(&mut self, now: Instant) -> TickAction {
        let since_last_rpc = now.duration_since(self.last_rpc);

        match &mut self.state {
            State::Leader(..) => {
                if since_last_rpc > self.config.heartbeat {
                    let reqs = self.heartbeat();
                    self.last_rpc = now;
                    return TickAction::Heartbeat(reqs);
                }
            }
            // Followers and Candidates.
            State::Follower | State::Candidate(..) => {
                if since_last_rpc > self.election_timeout {
                    // Update election timeout.
                    self.election_timeout = Self::random_election_timeout(
                        since_last_rpc,
                        self.config.election_timeout.clone(),
                    );

                    return TickAction::StartElection;
                }
            }
        }

        TickAction::None
    }

    /// Start an election to elect a new leader. This function returns non empty
    /// vector if server is a candidate.
    pub fn start_election(&mut self, candidate: bool) -> Vec<(ServerId, rpc::RequestVoteRequest)> {
        let mut requests = Vec::new();

        if candidate {
            // Overflow occurs after thousand years.
            self.persistent.current_term = self.persistent.current_term.strict_add(1);

            // Make RequestVote RPC call to each peer.
            for id in &self.config.peers {
                requests.push((
                    *id,
                    rpc::RequestVoteRequest {
                        term: self.persistent.current_term,
                        candidate_id: self.config.id,
                        last_log_index: self.persistent.log.last_index(),
                        last_log_term: self.persistent.log.last_term(),
                    },
                ));
            }

            // Transition to candidate.
            self.state = State::Candidate(CandidateVolatileState::default());
        } else {
            // Transition to follower.
            self.state = State::Follower;
        }

        requests
    }

    /// Process RequestVote RPC call.
    pub fn handle_vote_request(
        &self,
        req: rpc::RequestVoteRequest,
    ) -> (rpc::RequestVoteRequest, rpc::RequestVoteResponse) {
        let reject = rpc::RequestVoteResponse {
            term: self.persistent.current_term,
            vote_granted: false,
        };
        let grant = rpc::RequestVoteResponse {
            term: self.persistent.current_term,
            vote_granted: true,
        };

        // 1. Reply false if term < currentTerm.
        if req.term < self.persistent.current_term {
            return (req, reject);
        }

        // 2. If votedFor is null or candidateId, and candidate's log is at
        // least as up-to-date as receiver's log, grant vote.

        // votedFor != candidateId, don't grant vote.
        if let Some(id) = self.persistent.voted_for
            && id != req.candidate_id
        {
            return (req, reject);
        }

        // candidate's log is out of date, don't grant vote.
        if self.persistent.log.last_index() > req.last_log_index
            || self.persistent.log.last_term() > req.last_log_term
        {
            return (req, reject);
        }

        // Grant vote.
        (req, grant)
    }

    /// Process RequestVote RPC response.
    pub fn handle_vote_response(&mut self, resp: rpc::RequestVoteResponse) {
        let quorum = self.config.peers.len() / 2;
        if let State::Candidate(state) = &mut self.state {
            if resp.vote_granted {
                state.granted_vote += 1;
                if state.granted_vote > quorum {
                    // Transition to leader.
                    self.state = State::Leader(LeaderVolatileState::new(
                        self.persistent.log.last_index(),
                        self.config.peers.iter(),
                    ));
                }
            } else {
                state.rejected_vote += 1;
                if state.rejected_vote > quorum {
                    // Transition to follower.
                    self.state = State::Follower;
                }
            }
        }
    }

    /// Process AppendEntries RPC request and return response to be send.
    pub fn handle_append_entries_request(
        &mut self,
        req: rpc::AppendEntriesRequest,
    ) -> (rpc::AppendEntriesRequest, rpc::AppendEntriesResponse) {
        let reject = rpc::AppendEntriesResponse {
            follower_id: self.config.id,
            term: self.persistent.current_term,
            success: false,
        };
        let success = rpc::AppendEntriesResponse {
            follower_id: self.config.id,
            term: self.persistent.current_term,
            success: true,
        };

        // 1. Reply false if term < currentTerm.
        if req.term < self.persistent.current_term {
            return (req, reject);
        }

        // 2. Reply false if log doesn't contain an entry at prevLogIndex whose
        // term matches prevLogTerm.
        if let Some(entry) = self.persistent.log.get(req.prev_log_index) {
            // 3. If an existing entry conflicts with a new one (same index but
            // different terms), delete the existing entry and all that follow
            // it.
            if entry.term != req.prev_log_term {
                self.persistent.log.truncate(req.prev_log_index);
            }
        } else if req.prev_log_index == 0 {
            // First entry.
            self.persistent.log.append(req.entries.clone());
        } else {
            return (req, reject);
        }

        // 4. Append any new entries not already in the log.
        if let Some(last) = self.persistent.log.last()
            && last.index != req.prev_log_index
        {
            self.persistent.log.truncate(req.prev_log_index);
        }
        let index_of_last_new_entry = req.entries.last().map(|e| e.index).unwrap_or(0);
        self.persistent.log.append(req.entries.clone());

        // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit,
        // index of last new entry).
        if req.leader_commit > self.volatile.commit_index {
            self.volatile.commit_index = min(req.leader_commit, index_of_last_new_entry)
        }

        (req, success)
    }

    /// Process AppendEntries RPC response and return another request to send
    /// if follower rejected entries.
    pub fn handle_append_entries_response(
        &mut self,
        resp: rpc::AppendEntriesResponse,
    ) -> Option<(ServerId, rpc::AppendEntriesRequest)> {
        // If not leader ignore response.
        let state = match &mut self.state {
            State::Leader(leader) => leader,
            _ => return None,
        };

        if let Some(follower) = state
            .followers
            .iter_mut()
            .find(|f| f.id == resp.follower_id)
        {
            if resp.success {
                follower.next_index = self.persistent.log.last_index() + 1;

                // If there exists an N such that N > commitIndex, a majority of
                // matchIndex[i] >= N, and log[N].term == currentTerm:
                // set commitIndex = N
                let majority_commit_index = state.majority_commit_index();
                if majority_commit_index > self.volatile.commit_index
                    && self
                        .persistent
                        .log
                        .get(majority_commit_index)
                        .map(|e| e.term)
                        == Some(self.persistent.current_term)
                {
                    self.volatile.commit_index = majority_commit_index;
                }

                None
            } else {
                // Decrement and try again.
                follower.next_index -= 1;
                if follower.next_index == 0 {
                    log::error!("raft follower refused first log");
                    follower.next_index = 1;
                    return None;
                }

                let prev_log_index = follower.next_index - 1;
                Some((
                    follower.id,
                    rpc::AppendEntriesRequest {
                        term: self.persistent.current_term,
                        leader_id: self.config.id,
                        prev_log_index,
                        prev_log_term: self
                            .persistent
                            .log
                            .get(prev_log_index)
                            .map(|e| e.term)
                            .unwrap_or(0),
                        entries: self
                            .persistent
                            .log
                            .slice(follower.next_index, None)
                            .to_vec(),
                        leader_commit: self.volatile.commit_index,
                    },
                ))
            }
        } else {
            // Unknown follower.
            None
        }
    }

    fn heartbeat(&self) -> Vec<(ServerId, rpc::AppendEntriesRequest)> {
        match &self.state {
            State::Leader { .. } => {
                let mut actions = Vec::new();
                for id in &self.config.peers {
                    actions.push((
                        *id,
                        rpc::AppendEntriesRequest {
                            term: self.persistent.current_term,
                            leader_id: self.config.id,
                            prev_log_index: self.persistent.log.last_index(),
                            prev_log_term: self.persistent.log.last_term(),
                            entries: Vec::new(),
                            leader_commit: self.volatile.commit_index,
                        },
                    ));
                }
                actions
            }
            _ => unreachable!(),
        }
    }

    /// Update election timeout with a random duration within configured range.
    fn random_election_timeout(seed: Duration, range: Range<Duration>) -> Duration {
        let r = range.end - range.start;
        range.start + Duration::from_secs_f64(seed.as_secs_f64() % r.as_secs_f64())
    }
}

/// User action to do before interacting with the state machine again.
#[derive(Debug)]
pub enum Action {
    /// Server received no message for a moment, start a new election.
    StartElection,

    /// Make a RequestVote RPC call.
    RequestVote((ServerId, rpc::RequestVoteRequest)),
    /// Response to RequestVote RPC call.
    Vote((rpc::RequestVoteRequest, rpc::RequestVoteResponse)),

    /// Make an AppendEntries RPC call.
    AppendEntries((ServerId, rpc::AppendEntriesRequest)),
    /// Response to AppendEntries RPC call.
    AckEntries((ServerId, rpc::AppendEntriesResponse)),
}

/// Action to be done after a tick.
pub enum TickAction {
    /// Node is the leader and must send heartbeat RPC request to maintain
    /// authority.
    Heartbeat(Vec<(ServerId, rpc::AppendEntriesRequest)>),
    /// Node is a follower/candidate and a new election is started.
    StartElection,
    /// Nothing to be done.
    None,
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{self, Arc, mpsc},
        thread,
    };

    use super::*;

    /// Event defines a server event such as RPC calls, crash and more.
    #[derive(Debug)]
    enum Event {
        // Update server clock.
        Tick(Instant),
        // Sets whether server is a candidate on next elections.
        CandidateOnElection(bool),
        // Process RequestVote RPC call.
        RequestVote(rpc::RequestVoteRequest),
        // Process RequestVote RPC response.
        Vote(rpc::RequestVoteResponse),
        // Process AppendEntries RPC call.
        AppendEntries(rpc::AppendEntriesRequest),
        // Process AppendEntries RPC response.
        AppendEntriesResponse(rpc::AppendEntriesResponse),
        // Crash server and ignore all events until recover.
        Crash,
        // Recover from previous crash and accept new events.
        Recover,
    }

    /// Step define a test case step.
    #[derive(Debug)]
    enum Step {
        // Sleep for duration, no tick happens between during sleep.
        Sleep(Duration),
        // Sleep until election timeout, sending tick to all servers.
        ElectionTimeout,
        // Send a tick to all servers.
        Tick,
        // Send event to given server.
        Event((ServerId, Event)),
    }

    /// Cluster defines an in-memory cluster of Raft [Server].
    type Cluster = (
        Vec<Server>,
        Vec<(mpsc::Sender<Event>, mpsc::Receiver<Event>)>,
    );

    const HEARTBEAT: Duration = Duration::from_millis(3);
    const ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(15);
    const ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(30);

    fn new_server(id: ServerId, peers: Vec<ServerId>) -> Server {
        let cfg = Config {
            id,
            heartbeat: HEARTBEAT,
            election_timeout: ELECTION_TIMEOUT_MIN..ELECTION_TIMEOUT_MAX,
            peers,
        };
        Server::new(cfg.clone(), Instant::now())
    }

    fn new_cluster(size: usize) -> Cluster {
        let peers: Vec<ServerId> = (0..size).collect();
        (0..size)
            .map(|i| new_server(i, peers.clone().into_iter().filter(|j| *j != i).collect()))
            .map(|s| (s, mpsc::channel()))
            .collect()
    }

    fn test_case(cluster_size: usize, steps: Vec<Step>) -> Vec<Server> {
        // Setup servers state machine.
        let (mut servers, mut channels) = new_cluster(cluster_size);

        // Setup channel for thread communication.
        let txs: Vec<mpsc::Sender<Event>> = channels.iter().map(|(tx, _)| tx.clone()).collect();

        // Spawn server threads.
        let mut threads = Vec::new();
        let barrier = Arc::new(sync::Barrier::new(cluster_size));
        for _ in 0..cluster_size {
            let node = servers.pop().unwrap();
            let txs = txs.clone();
            let rx = channels.pop().unwrap().1;
            let barrier = barrier.clone();
            threads.push(thread::spawn(move || node_loop(node, txs, rx, barrier)))
        }

        // Execute test steps.
        for (i, step) in steps.into_iter().enumerate() {
            match step {
                Step::Sleep(duration) => thread::sleep(duration),
                Step::ElectionTimeout => {
                    let now = Instant::now();
                    while now.elapsed() < ELECTION_TIMEOUT_MAX {
                        for tx in &txs {
                            tx.send(Event::Tick(Instant::now())).unwrap();
                        }
                        thread::sleep(HEARTBEAT);
                    }
                }
                Step::Tick => {
                    for tx in &txs {
                        tx.send(Event::Tick(Instant::now())).unwrap();
                    }
                }
                Step::Event((id, event)) => {
                    let tx = txs[id].clone();
                    tx.send(event).unwrap();
                }
            }
            thread::yield_now();
        }

        // Join threads.
        threads.into_iter().map(|t| t.join().unwrap()).collect()
    }

    /// Event loop of a single Raft node.
    fn node_loop(
        mut node: Server,
        txs: Vec<mpsc::Sender<Event>>,
        rx: mpsc::Receiver<Event>,
        barrier: Arc<sync::Barrier>,
    ) -> Server {
        let mut is_crashed = false;
        let mut is_candidate = false;

        while let Ok(event) = rx.recv_timeout(3 * HEARTBEAT) {
            // While crashed ignore all events.
            if is_crashed {
                if let Event::Recover = event {
                    // Reset node state.
                    node = new_server(node.config.id, node.config.peers);
                    is_crashed = false;
                }
                continue;
            }

            match event {
                Event::Tick(instant) => match node.tick(instant) {
                    TickAction::Heartbeat(items) => {
                        for (remote_id, req) in items {
                            txs[remote_id].send(Event::AppendEntries(req)).unwrap();
                        }
                    }
                    TickAction::StartElection => {
                        for (remote_id, req) in node.start_election(is_candidate) {
                            txs[remote_id].send(Event::RequestVote(req)).unwrap();
                        }
                    }
                    TickAction::None => {}
                },
                Event::CandidateOnElection(candidate) => {
                    is_candidate = candidate;
                }
                Event::RequestVote(req) => {
                    let (req, resp) = node.handle_vote_request(req);
                    txs[req.candidate_id].send(Event::Vote(resp)).unwrap();
                }
                Event::Vote(resp) => {
                    node.handle_vote_response(resp);
                }
                Event::Crash => {
                    is_crashed = true;
                }
                Event::Recover => unreachable!(),
                Event::AppendEntries(req) => {
                    let (req, resp) = node.handle_append_entries_request(req);
                    txs[req.leader_id]
                        .send(Event::AppendEntriesResponse(resp))
                        .unwrap();
                }
                Event::AppendEntriesResponse(resp) => {
                    if let Some((remote_id, req)) = node.handle_append_entries_response(resp) {
                        txs[remote_id].send(Event::AppendEntries(req)).unwrap()
                    }
                }
            };
        }

        barrier.wait();

        node
    }

    #[test]
    fn single_tick() {
        let servers = test_case(3, vec![Step::Tick]);
        assert!(servers.len() == 3);

        let mut followers = 0;
        for srv in servers {
            match srv.state {
                State::Follower => followers += 1,
                State::Leader(_) => unreachable!(),
                State::Candidate(_) => unreachable!(),
            }
        }

        assert_eq!(followers, 3);
    }

    #[test]
    fn election() {
        let servers = test_case(
            3,
            vec![
                Step::Tick,
                Step::Event((1, Event::CandidateOnElection(true))),
                Step::ElectionTimeout,
                Step::Tick,
                Step::Sleep(HEARTBEAT),
                Step::Tick,
                Step::Sleep(HEARTBEAT),
            ],
        );
        assert!(servers.len() == 3);

        let mut followers = Vec::new();
        let mut leaders = Vec::new();

        for srv in servers {
            match srv.state {
                State::Follower => followers.push(srv),
                State::Leader(_) => leaders.push(srv),
                State::Candidate(_) => unreachable!(),
            }
        }

        assert_eq!(followers.len(), 2);
        assert_eq!(leaders.len(), 1);
        assert_eq!(leaders[0].config.id, 1);
    }

    #[test]
    fn election_after_crash() {
        let servers = test_case(
            3,
            vec![
                Step::Tick,
                Step::Event((1, Event::CandidateOnElection(true))),
                Step::ElectionTimeout,
                Step::Tick,
                Step::Sleep(HEARTBEAT),
                Step::Tick,
                Step::Sleep(HEARTBEAT),
                Step::Event((1, Event::Crash)), // Crash leader.
                Step::Event((2, Event::CandidateOnElection(true))),
                Step::ElectionTimeout, // Start new election.
                Step::Tick,
                Step::Sleep(HEARTBEAT),
                Step::Tick,
                Step::Sleep(HEARTBEAT),
                Step::Event((1, Event::Recover)), // Recover previous leader.
                Step::Tick,
                Step::Sleep(HEARTBEAT),
                Step::Tick,
                Step::Sleep(HEARTBEAT),
            ],
        );
        assert!(servers.len() == 3);

        let mut followers = Vec::new();
        let mut leaders = Vec::new();

        for srv in servers {
            match srv.state {
                State::Follower => followers.push(srv),
                State::Leader(_) => leaders.push(srv),
                State::Candidate(_) => unreachable!(),
            }
        }

        assert_eq!(followers.len(), 2);
        assert_eq!(leaders.len(), 1);
        assert_eq!(leaders[0].config.id, 2);
    }
}
