mod proto;
mod state_machine;

pub use proto::*;
pub use state_machine::*;

// impl StateMachine {
//     /// Process AppendEntries RPC call from leader.
//     pub fn append_entries(&mut self, args: AppendEntriesRpcArgs) -> AppendEntriesRpcResult {
//         let s = &mut self.inner.state;
//         let ko = AppendEntriesRpcResult {
//             term: s.persistent.current_term,
//             success: false,
//         };
//
//         // 1. Reply false if term < current_term.
//         if args.term < s.persistent.current_term {
//             return ko;
//         }
//
//         let prev_log_i = s
//             .persistent
//             .find_log(args.prev_log_index, args.prev_log_term);
//
//         // 2. Reply false if log doesn't contain an entry at prev_log_index
//         // whose term match prev_log_term.
//         if prev_log_i.is_none() {
//             return ko;
//         }
//
//         let mut i = prev_log_i.unwrap();
//         for entry in &args.entries {
//             i += 1;
//             if let Some(other) = s.persistent.log.get(i) {
//                 // 3. If an existing entry conflicts with a new one (same index
//                 // but different terms), delete the existing entry and all that
//                 // follow it.
//                 if other.term != entry.term {
//                     s.persistent.log.truncate(i);
//                 }
//             } else {
//                 // 4. Append any new entries not already in the log.
//                 s.persistent.log.push(entry.clone());
//             }
//         }
//
//         // 5. If leader_commit > commit_index, set
//         // commit_index = min(leader_commit, index of last new entry).
//         if args.leader_commit > s.volatile.commit_index {
//             s.volatile.commit_index = std::cmp::min(
//                 args.entries
//                     .last()
//                     .map(|e| e.index)
//                     .unwrap_or(args.leader_commit),
//                 args.leader_commit,
//             );
//         }
//
//         AppendEntriesRpcResult {
//             term: s.persistent.current_term,
//             success: true,
//         }
//     }
//
//     /// Process RequestVote RPC call.
//     pub fn request_vote(&mut self, args: RequestVoteRpcArgs) -> RequestVoteRpcResult {
//         let s = &mut self.inner.state;
//         let ko = RequestVoteRpcResult {
//             term: s.persistent.current_term,
//             vote_granted: false,
//         };
//
//         // 1. Reply false if term < current_term.
//         if args.term < s.persistent.current_term {
//             return ko;
//         }
//
//         // If voted_for is null or candidate_id, and candidate's log is at least
//         // as up-to-date as receiver's log, grant vote.
//         if s.persistent.voted_for.is_some() && s.persistent.voted_for != Some(args.candidate_id) {
//             return ko;
//         }
//         if let Some(last) = s.persistent.log.last() {
//             if last.index > args.last_log_index {
//                 return ko;
//             }
//             if last.term > args.last_log_term {
//                 return ko;
//             }
//         }
//
//         RequestVoteRpcResult {
//             term: s.persistent.current_term,
//             vote_granted: true,
//         }
//     }
// }
//
// impl StateMachine<NormalOperationState<Leader>> {
//     pub fn heartbeat(&mut self) -> AppendEntriesRpcArgs {
//         self.append_entries(Vec::new())
//     }
//
//     /// Initialize arguments of AppendEntries RPC call.
//     pub fn append_entries(&self, entries: Vec<LogEntry>) -> AppendEntriesRpcArgs {
//         let s = &self.inner.state;
//         let prev_log = s.persistent.log.last();
//         AppendEntriesRpcArgs {
//             term: s.persistent.current_term,
//             leader_id: s.id,
//             prev_log_index: prev_log.map(|e| e.index).unwrap_or(0),
//             prev_log_term: prev_log.map(|e| e.term).unwrap_or(0),
//             entries,
//             leader_commit: s.volatile.commit_index,
//         }
//     }
//
//     pub fn append_entries_result(
//         self,
//         result: AppendEntriesRpcResult,
//     ) -> Result<Self, StateMachine<NormalOperationState<Follower>>> {
//         let s = self.inner.state;
//         if result.term > s.persistent.current_term {
//             Err(StateMachine {
//                 inner: NormalOperationState {
//                     state: Follower {
//                         persistent: s.persistent,
//                         volatile: s.volatile,
//                     },
//                 },
//             })
//         } else {
//             todo!("");
//         }
//     }
// }
