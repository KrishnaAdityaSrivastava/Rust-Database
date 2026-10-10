use std::time::Instant;

use serde::{Deserialize, Serialize};

use super::node::{LogEntry, NodeId, RaftNode};
use super::request::{Responder, StoreOperation, StoreRequest};
use crate::wal::log_record::Command;

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
pub struct ClientRequest {
    pub request_id: u64,
    pub client_id: NodeId,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientQuery {
    pub request_id: u64,
    pub client_id: NodeId,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusQuery {
    pub request_id: u64,
    pub client_id: NodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientResponse {
    pub request_id: u64,
    pub success: bool,
    pub value: Option<crate::wal::log_record::Value>,
    pub message: String,
    pub leader_id: Option<NodeId>,
    pub node_id: NodeId,
    pub role: String,
    pub term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),

    AppendEntries(AppendEntries),
    AppendEntriesResponse(AppendEntriesResponse),

    ClientCommand(Command),
    ClientRequest(ClientRequest),
    ClientQuery(ClientQuery),
    StatusQuery(StatusQuery),
    ClientResponse(ClientResponse),

    //remove this branch later
    Timeout,
}


impl RaftNode {
    pub fn handle_message(&mut self, message: Message, now: Instant) {
        match message {
            Message::RequestVote(request) => {
                let candidate_id = request.candidate_id;

                let response = self.handle_request_vote(request, now);

                self.outbox.push((
                    candidate_id,
                    Message::RequestVoteResponse(response),
                ));
            }

            Message::RequestVoteResponse(response) => {
                self.handle_vote_response(response, now);
            }

            Message::AppendEntries(entries) => {
                let leader_id = entries.leader_id;

                let response = self.handle_append_entries(entries, now);

                self.outbox.push((
                    leader_id,
                    Message::AppendEntriesResponse(response),
                ));
            }

            Message::AppendEntriesResponse(response) => {
                self.handle_append_entries_response(response, now);
            }

            Message::ClientCommand(cmd) => {
                if self.role == crate::raft::Role::Leader {
                    self.append_command(cmd);
                } else if let Some(leader_id) = self.leader_id {
                    if super::is_log_enabled() {
                        println!(
                            "Node {} is not leader, forwarding command to leader Node {}",
                            self.id.id, leader_id.id
                        );
                    }

                    self.outbox.push((
                        leader_id,
                        Message::ClientCommand(cmd),
                    ));
                } else if super::is_log_enabled() {
                    println!(
                        "Node {} is not leader and no leader is known yet, dropping command",
                        self.id.id
                    );
                }
            }

            Message::ClientRequest(req) => {
                let operation = match req.command {
                    Command::Set { key, value } => StoreOperation::Set { key, value },
                    Command::Delete { key } => StoreOperation::Delete { key },
                };
                let store_req = StoreRequest {
                    operation,
                    responder: Responder::RaftClient {
                        client_id: req.client_id,
                        request_id: req.request_id,
                    },
                };
                self.handle_store_request(store_req, now);
            }

            Message::ClientQuery(query) => {
                let store_req = StoreRequest {
                    operation: StoreOperation::Get { key: query.key },
                    responder: Responder::RaftClient {
                        client_id: query.client_id,
                        request_id: query.request_id,
                    },
                };
                self.handle_store_request(store_req, now);
            }

            Message::StatusQuery(query) => {
                let store_req = StoreRequest {
                    operation: StoreOperation::Status,
                    responder: Responder::RaftClient {
                        client_id: query.client_id,
                        request_id: query.request_id,
                    },
                };
                self.handle_store_request(store_req, now);
            }

            Message::ClientResponse(_) => {}

            Message::Timeout => {
                self.start_election(now);
            }
        }
    }
}