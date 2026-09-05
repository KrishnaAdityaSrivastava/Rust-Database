use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use super::message::Message;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId {
    pub id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub command: Command,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Set(String, String),
    Delete(String),
}

pub struct RaftNode {
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

    // Networking
    pub outbox: Vec<(NodeId, Message)>,

    // Election timeout
    pub election_deadline: Instant,
    pub heartbeat_deadline: Instant,
}

impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        let mut node = Self {
            id,
            peers,

            current_term: 0,
            voted_for: None,

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

            outbox: Vec::new(),

            election_deadline: Instant::now(),
            heartbeat_deadline: Instant::now(),
        };

        node.reset_election_timeout();

        node
    }
    pub fn tick(&mut self) {
        let now = Instant::now();

        match self.role {
            Role::Follower | Role::Candidate => {
                if now >= self.election_deadline {
                    self.start_election();
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

    pub fn step_down(&mut self, new_term: u64) {
        self.current_term = new_term;
        self.role = Role::Follower;
        self.voted_for = None;
        self.reset_election_timeout();
    }
}
