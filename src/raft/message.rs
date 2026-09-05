use crate::raft::RaftNode;

use super::node::{Command, LogEntry, NodeId};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: NodeId,

    pub prev_log_index: usize,
    pub prev_log_term: u64,

    pub entries: Vec<LogEntry>,

    pub leader_commit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub follower_id: NodeId,
    pub success: bool,
    pub match_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: NodeId,

    pub last_log_index: usize,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub voter_id: NodeId,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),

    AppendEntries(AppendEntries),
    AppendEntriesResponse(AppendEntriesResponse),
    
    ClientCommand(Command),
    Timeout,
}

impl RaftNode {
    pub fn handle_message(&mut self, message: Message) {
        match message {
            Message::RequestVote(request) => {
                let candidate_id = request.candidate_id;
                let response = self.handle_request_vote(request);
                self.outbox.push((candidate_id, Message::RequestVoteResponse(response)));
            }

            Message::RequestVoteResponse(response) => {
                self.handle_vote_response(response);
            }

            Message::AppendEntries(entries) => {
                let leader_id = entries.leader_id;
                let response = self.handle_append_entries(entries);
                self.outbox.push((leader_id, Message::AppendEntriesResponse(response)));
            }

            Message::AppendEntriesResponse(response) => {
                self.handle_append_entries_response(response);
            }
            
            Message::ClientCommand(cmd) => {
                if self.role == crate::raft::Role::Leader {
                    self.append_command(cmd);
                } else {
                    println!("Node {} is not leader, ignoring command", self.id.id);
                }
            }
            
            Message::Timeout => {
                self.start_election();
            }
        }
    }
}
