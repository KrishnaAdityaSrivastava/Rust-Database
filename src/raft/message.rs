use crate::{raft::RaftNode};
use crate::wal::log_record::Command;
use super::node::{LogEntry, NodeId};
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
                } else if let Some(leader_id) = self.leader_id {
                    if super::is_log_enabled() {
                        println!(
                            "Node {} is not leader, forwarding command to leader Node {}",
                            self.id.id, leader_id.id
                        );
                    }
                    self.outbox.push((leader_id, Message::ClientCommand(cmd)));
                } else if super::is_log_enabled() {
                    println!(
                        "Node {} is not leader and no leader is known yet, dropping command",
                        self.id.id
                    );
                }
            }

            Message::ClientRequest(req) => {
                let client_id = req.client_id;
                let req_id = req.request_id;
                if super::is_log_enabled() {
                    println!(
                        "[NODE {}] Received ClientRequest ({:?}) from Client {}",
                        self.id.id, req.command, client_id.id
                    );
                }
                if self.role == crate::raft::Role::Leader {
                    self.append_command(req.command.clone());
                    self.outbox.push((
                        client_id,
                        Message::ClientResponse(ClientResponse {
                            request_id: req_id,
                            success: true,
                            value: None,
                            message: format!("Command processed on Leader Node {}", self.id.id),
                            leader_id: Some(self.id),
                            node_id: self.id,
                            role: format!("{:?}", self.role),
                            term: self.current_term,
                        }),
                    ));
                } else if let Some(leader_id) = self.leader_id {
                    if super::is_log_enabled() {
                        println!(
                            "[NODE {}] Forwarding client request to Leader Node {}",
                            self.id.id, leader_id.id
                        );
                    }
                    // Inform client that request was forwarded to leader
                    self.outbox.push((
                        client_id,
                        Message::ClientResponse(ClientResponse {
                            request_id: req_id,
                            success: true,
                            value: None,
                            message: format!(
                                "Node {} (Follower) forwarded request to Leader Node {}",
                                self.id.id, leader_id.id
                            ),
                            leader_id: Some(leader_id),
                            node_id: self.id,
                            role: format!("{:?}", self.role),
                            term: self.current_term,
                        }),
                    ));
                    // Forward actual request to Leader
                    self.outbox.push((leader_id, Message::ClientRequest(req)));
                } else {
                    if super::is_log_enabled() {
                        println!(
                            "[NODE {}] Rejecting client request: no leader known yet",
                            self.id.id
                        );
                    }
                    self.outbox.push((
                        client_id,
                        Message::ClientResponse(ClientResponse {
                            request_id: req_id,
                            success: false,
                            value: None,
                            message: format!(
                                "Node {} is not leader and no leader is currently known",
                                self.id.id
                            ),
                            leader_id: None,
                            node_id: self.id,
                            role: format!("{:?}", self.role),
                            term: self.current_term,
                        }),
                    ));
                }
            }

            Message::ClientQuery(query) => {
                if super::is_log_enabled() {
                    println!(
                        "[NODE {}] Received ClientQuery for key '{}' from Client {}",
                        self.id.id, query.key, query.client_id.id
                    );
                }
                let val = self.db.get(&query.key).unwrap_or(None);
                self.outbox.push((
                    query.client_id,
                    Message::ClientResponse(ClientResponse {
                        request_id: query.request_id,
                        success: true,
                        value: val,
                        message: format!("Read from Node {}", self.id.id),
                        leader_id: self.leader_id,
                        node_id: self.id,
                        role: format!("{:?}", self.role),
                        term: self.current_term,
                    }),
                ));
            }

            Message::StatusQuery(query) => {
                if super::is_log_enabled() {
                    println!(
                        "[NODE {}] Received StatusQuery from Client {}",
                        self.id.id, query.client_id.id
                    );
                }
                self.outbox.push((
                    query.client_id,
                    Message::ClientResponse(ClientResponse {
                        request_id: query.request_id,
                        success: true,
                        value: None,
                        message: format!("Node status OK"),
                        leader_id: self.leader_id,
                        node_id: self.id,
                        role: format!("{:?}", self.role),
                        term: self.current_term,
                    }),
                ));
            }

            Message::ClientResponse(_) => {}

            Message::Timeout => {
                self.start_election();
            }
        }
    }
}
