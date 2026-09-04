use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId {
    id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    term: u64,
    command: Command,
}

#[derive(Debug, Clone)]
pub enum Command {
    Set(String, String),
    Delete(String),
}

pub struct RaftNode {
    id: NodeId,
    peers: Vec<NodeId>,

    // Persistent
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,

    // Volatile
    commit_index: usize,
    last_applied: usize,

    // Leader-only
    next_index: HashMap<NodeId, usize>,
    match_index: HashMap<NodeId, usize>,

    role: Role,
    votes_received: HashSet<NodeId>,

    timeout: u64,
}

struct AppendEntries {
    term: u64,
    leader_id: NodeId,

    prev_log_index: usize,
    prev_log_term: u64,

    entries: Vec<LogEntry>,

    leader_commit: usize,
}

struct AppendEntriesResponse {
    term: u64,
    success: bool,
}

pub struct RequestVote {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: usize,
    pub last_log_term: u64,
}

pub struct RequestVoteResponse {
    pub term: u64,
    pub voter_id: NodeId,
    pub vote_granted: bool,
}

impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        Self {
            id,
            peers,

            current_term: 0,
            voted_for: None,

            // index 0 is a dummy entry
            log: vec![LogEntry {
                term: 0,
                command: Command::Set(String::new(), String::new()),
            }],

            commit_index: 0,
            last_applied: 0,

            next_index: HashMap::new(),
            match_index: HashMap::new(),

            role: Role::Follower,
            votes_received: HashSet::new(),

            timeout: 0,
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;

        for &peer in &self.peers {
            self.next_index.insert(peer, self.log.len());
            self.match_index.insert(peer, 0);
        }
    }

    fn start_election(&mut self) -> RequestVote {
        self.role = Role::Candidate;

        self.current_term += 1;

        self.voted_for = Some(self.id);

        self.votes_received.clear();
        self.votes_received.insert(self.id);

        let last_log_index = self.log.len() - 1;
        let last_log_term = self.log[last_log_index].term;

        RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index,
            last_log_term,
        }
    }

    fn handle_vote_response(&mut self, response: RequestVoteResponse) {
        if response.term > self.current_term {
            self.current_term = response.term;
            self.role = Role::Follower;
            self.voted_for = None;
            return;
        }

        if self.role != Role::Candidate {
            return;
        }

        if response.term < self.current_term {
            return;
        }

        if response.vote_granted {
            self.votes_received.insert(response.voter_id);
        }

        if self.votes_received.len() >= (self.peers.len() + 1) / 2 + 1 {
            self.become_leader();
        }
    }

    fn handle_request_vote(&mut self, request: RequestVote) -> RequestVoteResponse {
        if request.term < self.current_term {
            return RequestVoteResponse {
                term: self.current_term,
                voter_id: self.id,
                vote_granted: false,
            };
        }

        if request.term > self.current_term {
            self.current_term = request.term;
            self.role = Role::Follower;
            self.voted_for = None;
        }

        let can_vote = match self.voted_for {
            None => true,
            Some(id) => id == request.candidate_id,
        };

        if !can_vote {
            return RequestVoteResponse {
                term: self.current_term,
                voter_id: self.id,
                vote_granted: false,
            };
        }

        let last_log_index = self.log.len() - 1;
        let last_log_term = self.log[last_log_index].term;

        let log_is_up_to_date = request.last_log_term > last_log_term
            || (request.last_log_term == last_log_term && request.last_log_index >= last_log_index);

        if !log_is_up_to_date {
            return RequestVoteResponse {
                term: self.current_term,
                voter_id: self.id,
                vote_granted: false,
            };
        }

        self.voted_for = Some(request.candidate_id);

        RequestVoteResponse {
            term: self.current_term,
            voter_id: self.id,
            vote_granted: true,
        }
    }

    fn make_append_entries(&self, peer: NodeId) -> AppendEntries {
        let next_index = self.next_index[&peer];

        let prev_log_index = next_index - 1;
        let prev_log_term = self.log[prev_log_index].term;

        let entries = self.log[next_index..].to_vec();

        AppendEntries {
            term: self.current_term,
            leader_id: self.id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        }
    }

    fn handle_append_entries_response(
        &mut self,
        peer: NodeId,
        sent: AppendEntries,
        response: AppendEntriesResponse,
    ) {
        if response.term > self.current_term {
            self.current_term = response.term;
            self.role = Role::Follower;
            self.voted_for = None;
            return;
        }

        if self.role != Role::Leader {
            return;
        }

        if response.success {
            let replicated_until = sent.prev_log_index + sent.entries.len();

            self.match_index.insert(peer, replicated_until);
            self.next_index.insert(peer, replicated_until + 1);

            let mut new_commit_index = self.commit_index;

            for i in (self.commit_index + 1)..self.log.len() {
                let count = self
                    .match_index
                    .values()
                    .filter(|&&index| index >= i)
                    .count()
                    + 1;

                if count >= (self.peers.len() + 1) / 2 + 1 && self.log[i].term == self.current_term
                {
                    new_commit_index = i;
                }
            }

            if new_commit_index > self.commit_index {
                self.commit_index = new_commit_index;
            }
        } else {
            let next_index = self.next_index.get_mut(&peer).unwrap();
            if *next_index > 1 {
                *next_index -= 1;
            }
        }
    }
    fn handle_append_entries(&mut self, entries: AppendEntries) -> AppendEntriesResponse {
        if entries.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
            };
        }

        if entries.term > self.current_term {
            self.current_term = entries.term;
            self.voted_for = None;
        }

        self.role = Role::Follower;

        let prev_log_index = entries.prev_log_index;

        if prev_log_index >= self.log.len()
            || self.log[prev_log_index].term != entries.prev_log_term
        {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
            };
        }

        let mut index = entries.prev_log_index + 1;

        for incoming_entry in entries.entries {
            if index < self.log.len() {
                if self.log[index].term != incoming_entry.term {
                    self.log.truncate(index);
                }
            }

            if index >= self.log.len() {
                self.log.push(incoming_entry);
            }

            index += 1;
        }

        self.commit_index = std::cmp::min(entries.leader_commit, self.log.len() - 1);

        AppendEntriesResponse {
            term: self.current_term,
            success: true,
        }
    }
}
