use serde::{Deserialize, Serialize};

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use crate::{
    Database,
    database::Config,
    wal::log_record::{Command, LogRecord, Value},
};

use super::message::Message;
use super::request::{Responder, StoreOperation, StoreRequest};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId {
    pub id: u64,
}

impl From<u64> for NodeId {
    fn from(id: u64) -> Self {
        NodeId { id }
    }
}

impl From<NodeId> for u64 {
    fn from(node_id: NodeId) -> Self {
        node_id.id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub record: LogRecord,
}

pub trait ElectionTimeout {
    fn next_timeout(&mut self) -> Duration;
}

pub struct RaftNode {
    pub db: Database,
    pub id: NodeId,
    pub peers: Vec<NodeId>,

    // Persistent state
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,

    // Volatile state
    pub commit_index: usize,
    pub last_applied: usize,

    // Leader-only state
    pub next_index: HashMap<NodeId, usize>,
    pub match_index: HashMap<NodeId, usize>,

    // Election state
    pub role: Role,
    pub votes_received: HashSet<NodeId>,
    pub leader_id: Option<NodeId>,

    // Networking
    pub outbox: Vec<(NodeId, Message)>,

    // Election timeout
    pub election_deadline: Instant,
    pub heartbeat_deadline: Instant,

    pub election_timeout: Option<Duration>,

    pub pending_requests: HashMap<(usize, u64), Responder>,
}

impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>, config: impl Into<Config>) -> Self {
        #[allow(deprecated)]
        let dir = tempfile::tempdir().unwrap().into_path();
        Self::new_in_dir(dir, id, peers, config)
    }

    pub fn new_in_dir(
        dir: impl AsRef<std::path::Path>,
        id: NodeId,
        peers: Vec<NodeId>,
        config: impl Into<Config>,
    ) -> Self {
        let config = config.into();
        let now = Instant::now();

        let mut node = Self {
            id,
            peers,
            current_term: 0,
            voted_for: None,
            log: vec![LogEntry {
                term: 0,
                record: LogRecord::new(
                    0,
                    Command::Set {
                        key: String::new(),
                        value: Value::String(String::new()),
                    },
                ),
            }],
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            role: Role::Follower,
            votes_received: HashSet::new(),
            leader_id: None,
            outbox: Vec::new(),
            election_deadline: now,
            heartbeat_deadline: now,
            election_timeout: None,
            db: Database::open_in_dir(dir, config).unwrap(),
            pending_requests: HashMap::new(),
        };

        node.reset_election_timeout(now);

        node
    }

    pub fn handle_store_request(&mut self, request: StoreRequest, _now: Instant) {
        match request.operation {
            StoreOperation::Set { key, value } => {
                self.submit_store_write(Command::Set { key, value }, request.responder);
            }
            StoreOperation::Delete { key } => {
                self.submit_store_write(Command::Delete { key }, request.responder);
            }
            StoreOperation::Get { key } => {
                let (success, value, message) = match self.get(&key) {
                    Ok(val) => (true, val, format!("Read from Node {}", self.id.id)),
                    Err(err) => (false, None, err.to_string()),
                };
                let leader_id = self.leader_id;
                let current_term = self.current_term;
                let id = self.id;
                let role = self.role;
                request.responder.send(
                    success,
                    value,
                    message,
                    leader_id,
                    current_term,
                    id,
                    role,
                    &mut self.outbox,
                );
            }
            StoreOperation::Status => {
                let leader_id = self.leader_id;
                let current_term = self.current_term;
                let id = self.id;
                let role = self.role;
                request.responder.send(
                    true,
                    None,
                    format!("Node status OK (role: {:?})", self.role),
                    leader_id,
                    current_term,
                    id,
                    role,
                    &mut self.outbox,
                );
            }
        }
    }

    fn submit_store_write(&mut self, command: Command, responder: Responder) {
        if self.role != Role::Leader {
            let msg = if let Some(leader) = self.leader_id {
                format!("Not leader; current leader is Node {}", leader.id)
            } else {
                format!("Node {} is not leader and no leader is currently known", self.id.id)
            };
            let leader_id = self.leader_id;
            let current_term = self.current_term;
            let id = self.id;
            let role = self.role;
            responder.send(
                false,
                None,
                msg,
                leader_id,
                current_term,
                id,
                role,
                &mut self.outbox,
            );
            return;
        }

        let index = self.append_command(command);
        let term = self.log[index].term;

        if self.last_applied >= index {
            let leader_id = self.leader_id;
            let id = self.id;
            let role = self.role;
            responder.send(
                true,
                None,
                format!("Write committed and applied at log index {index}"),
                leader_id.or(Some(id)),
                term,
                id,
                role,
                &mut self.outbox,
            );
        } else {
            self.pending_requests.insert((index, term), responder);
        }
    }

    pub fn submit_command(&mut self, cmd: Command, now: Instant) {
        self.handle_message(Message::ClientCommand(cmd), now);
    }

    pub fn get(&self, key: &str) -> std::io::Result<Option<Value>> {
        self.db.get(key)
    }

    pub fn tick(&mut self, now: Instant) {
        match self.role {
            Role::Follower | Role::Candidate => {
                if now >= self.election_deadline {
                    self.start_election(now);
                }
            }

            Role::Leader => {
                if now >= self.heartbeat_deadline {
                    self.send_heartbeats();

                    self.heartbeat_deadline = now + Duration::from_millis(100);
                }
            }
        }
    }

    pub fn send_heartbeats(&mut self) {
        let peers = self.peers.clone();

        for peer in peers {
            let request = self.make_append_entries(peer);

            self.outbox.push((peer, Message::AppendEntries(request)));
        }
    }

    pub fn step_down(&mut self, new_term: u64, now: Instant) {
        self.current_term = new_term;
        self.role = Role::Follower;
        self.voted_for = None;
        self.leader_id = None;
        self.reset_election_timeout(now);

        let id = self.id;
        let role = self.role;
        for (_, responder) in self.pending_requests.drain() {
            responder.send(
                false,
                None,
                "Leadership changed before confirmation; write outcome may be uncertain".into(),
                None,
                new_term,
                id,
                role,
                &mut self.outbox,
            );
        }
    }
}
