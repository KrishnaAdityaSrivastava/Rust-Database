use std::time::{Duration, Instant};

use rand::RngExt;

use super::{
    message::{Message, RequestVote, RequestVoteResponse},
    node::{RaftNode, Role},
};

impl RaftNode {
    pub fn become_leader(&mut self) {
        self.role = Role::Leader;

        for &peer in &self.peers {
            self.next_index.insert(peer, self.log.len());
            self.match_index.insert(peer, 0);

            let entries = self.make_append_entries(peer);

            self.outbox.push((peer, Message::AppendEntries(entries)));
        }

        // Leader should send heartbeats periodically.
        self.heartbeat_deadline = Instant::now() + Duration::from_millis(100);

        if super::is_log_enabled() {
            println!("Node {} became LEADER", self.id.id);
        }
    }

    pub fn start_election(&mut self) {
        self.role = Role::Candidate;

        self.current_term += 1;

        self.voted_for = Some(self.id);

        self.votes_received.clear();
        self.votes_received.insert(self.id);

        let cluster_size = self.peers.len() + 1;
        let majority = cluster_size / 2 + 1;

        if self.votes_received.len() >= majority {
            self.become_leader();
            return;
        }

        // Start a fresh election timeout.
        self.reset_election_timeout();

        let last_log_index = self.log.len() - 1;
        let last_log_term = self.log[last_log_index].term;

        let request = RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index,
            last_log_term,
        };

        if super::is_log_enabled() {
            println!(
                "Node {} started election for term {}",
                self.id.id, self.current_term
            );
        }

        for &peer in &self.peers {
            self.outbox
                .push((peer, Message::RequestVote(request.clone())));
        }
    }

    pub fn handle_vote_response(&mut self, response: RequestVoteResponse) {
        // A newer term always wins.
        if response.term > self.current_term {
            self.step_down(response.term);
            return;
        }

        // Ignore responses from old terms.
        if response.term < self.current_term {
            return;
        }

        // We only care about votes while we're a candidate.
        if self.role != Role::Candidate {
            return;
        }

        if response.vote_granted {
            self.votes_received.insert(response.voter_id);
        }

        let cluster_size = self.peers.len() + 1;
        let majority = cluster_size / 2 + 1;

        if self.votes_received.len() >= majority {
            self.become_leader();
        }
    }

    pub fn handle_request_vote(&mut self, request: RequestVote) -> RequestVoteResponse {
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

        // Only reset because we actually granted the vote.
        self.reset_election_timeout();

        if super::is_log_enabled() {
            println!(
                "Node {} voted for Node {} in term {}",
                self.id.id, request.candidate_id.id, self.current_term
            );
        }

        RequestVoteResponse {
            term: self.current_term,
            voter_id: self.id,
            vote_granted: true,
        }
    }

    pub fn reset_election_timeout(&mut self) {
        let millis = rand::rng().random_range(150..=300);

        self.election_deadline = Instant::now() + Duration::from_millis(millis);
    }
}
