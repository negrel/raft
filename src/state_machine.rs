use std::{
    ops::Range,
    time::{Duration, Instant},
};

use crate::{
    CandidateVolatileState, Index, LeaderVolatileState, ServerId, ServerPersistentState,
    ServerVolatileState, Term, rpc,
};

/// Raft server configuration.
#[derive(Clone)]
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
    pub fn tick(&mut self, now: Instant) -> Vec<Action> {
        let since_last_rpc = now.duration_since(self.last_rpc);
        let mut actions = Vec::new();

        match &mut self.state {
            State::Leader(..) => {
                if since_last_rpc > self.config.heartbeat {
                    self.heartbeat(&mut actions);
                    self.last_rpc = now;
                }
            }
            // Followers and Candidates.
            State::Follower | State::Candidate(..) => {
                if since_last_rpc > self.election_timeout {
                    actions.push(Action::StartElection);

                    // Update election timeout.
                    self.election_timeout = Self::random_election_timeout(
                        since_last_rpc,
                        self.config.election_timeout.clone(),
                    );
                }
            }
        }

        actions
    }

    /// Start an election.
    pub fn start_election(&mut self, candidate: bool) -> Vec<Action> {
        let mut actions = Vec::new();

        if candidate {
            // Overflow occurs after thousand years.
            self.persistent.current_term = self.persistent.current_term.strict_add(1);

            // Make RequestVote RPC call.
            for id in &self.config.peers {
                actions.push(Action::RequestVote((
                    *id,
                    rpc::RequestVoteRequest {
                        term: self.persistent.current_term,
                        candidate_id: self.config.id,
                        last_log_index: self.last_log_index(),
                        last_log_term: self.last_log_term(),
                    },
                )));
            }

            // Transition to candidate.
            self.state = State::Candidate(CandidateVolatileState::default());
        } else {
            // Transition to follower.
            self.state = State::Follower;
        }

        actions
    }

    /// Process RequestVote RPC call.
    pub fn request_vote(&self, req: rpc::RequestVoteRequest) -> Vec<Action> {
        let reject = rpc::RequestVoteResponse {
            term: self.persistent.current_term,
            vote_granted: false,
        };
        let grant = rpc::RequestVoteResponse {
            term: self.persistent.current_term,
            vote_granted: true,
        };

        let mut actions = Vec::new();

        // 1. Reply false if term < currentTerm.
        if req.term < self.persistent.current_term {
            actions.push(Action::Vote((req, reject)));
            return actions;
        }

        // 2. If votedFor is null or candidateId, and candidate's log is at
        // least as up-to-date as receiver's log, grant vote.
        if let Some(id) = self.persistent.voted_for
            && id != req.candidate_id
        {
            actions.push(Action::Vote((req, reject)));
            return actions;
        }
        if self.last_log_index() > req.last_log_index || self.last_log_term() > req.last_log_term {
            actions.push(Action::Vote((req, reject)));
            return actions;
        }

        actions.push(Action::Vote((req, grant)));
        actions
    }

    /// Process RequestVote RPC response.
    pub fn vote_response(&mut self, resp: rpc::RequestVoteResponse) {
        let quorum = self.config.peers.len() / 2;
        if let State::Candidate(state) = &mut self.state {
            if resp.vote_granted {
                state.granted_vote += 1;
                if state.granted_vote > quorum {
                    // Transition to leader.
                    self.state = State::Leader(LeaderVolatileState::default());
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

    fn heartbeat(&self, actions: &mut Vec<Action>) {
        match &self.state {
            State::Leader { .. } => {
                for id in &self.config.peers {
                    actions.push(Action::AppendEntries((
                        *id,
                        rpc::AppendEntriesRequest {
                            term: self.persistent.current_term,
                            leader_id: self.config.id,
                            prev_log_index: self.last_log_index(),
                            prev_log_term: self.last_log_term(),
                            entries: Vec::new(),
                            leader_commit: self.volatile.commit_index,
                        },
                    )));
                }
            }
            _ => unreachable!(),
        }
    }

    fn last_log_index(&self) -> Index {
        self.persistent.log.last().map(|e| e.index).unwrap_or(0)
    }

    fn last_log_term(&self) -> Term {
        self.persistent.log.last().map(|e| e.term).unwrap_or(0)
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
    /// Server timed out, start a new election.
    StartElection,

    /// Save state on persistent storage.
    SaveState(ServerPersistentState),

    /// Make a RequestVote RPC call.
    RequestVote((ServerId, rpc::RequestVoteRequest)),
    /// Response to RequestVote RPC call.
    Vote((rpc::RequestVoteRequest, rpc::RequestVoteResponse)),

    /// Make an AppendEntries RPC call.
    AppendEntries((ServerId, rpc::AppendEntriesRequest)),
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{self, Arc, mpsc},
        thread,
    };

    use super::*;

    /// Event is define a server event.
    #[derive(Debug)]
    enum Event {
        Tick(Instant),
        CandidateOnElection(bool),
        RequestVote(rpc::RequestVoteRequest),
        Vote(rpc::RequestVoteResponse),
    }

    /// Step define a test case step.
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
        for step in steps {
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
        }

        // Join threads.
        threads.into_iter().map(|t| t.join().unwrap()).collect()
    }

    fn node_loop(
        mut node: Server,
        txs: Vec<mpsc::Sender<Event>>,
        rx: mpsc::Receiver<Event>,
        barrier: Arc<sync::Barrier>,
    ) -> Server {
        let mut is_candidate = false;
        let mut actions;

        while let Ok(event) = rx.recv_timeout(3 * HEARTBEAT) {
            actions = match event {
                Event::Tick(instant) => node.tick(instant),
                Event::CandidateOnElection(candidate) => {
                    is_candidate = candidate;
                    Vec::new()
                }
                Event::RequestVote(req) => node.request_vote(req),
                Event::Vote(resp) => {
                    node.vote_response(resp);
                    Vec::new()
                }
            };

            while !actions.is_empty() {
                let mut new_actions = Vec::new();
                for a in actions {
                    let mut v = match a {
                        Action::StartElection => node.start_election(is_candidate),
                        Action::SaveState(_) => todo!(),
                        Action::RequestVote((remote_id, req)) => {
                            txs[remote_id].send(Event::RequestVote(req)).unwrap();
                            Vec::new()
                        }
                        Action::Vote((req, resp)) => {
                            txs[req.candidate_id].send(Event::Vote(resp)).unwrap();
                            Vec::new()
                        }
                        Action::AppendEntries(_) => Vec::new(),
                    };
                    new_actions.append(&mut v);
                }
                actions = new_actions;
            }
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

        let mut followers = 0;
        let mut leaders = 0;

        for srv in servers {
            match srv.state {
                State::Follower => followers += 1,
                State::Leader(_) => leaders += 1,
                State::Candidate(_) => unreachable!(),
            }
        }

        assert_eq!(followers, 2);
        assert_eq!(leaders, 1);
    }
}
