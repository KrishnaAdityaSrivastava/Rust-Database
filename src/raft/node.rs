use serde::{Deserialize, Serialize};

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    time::{Duration, Instant},
};

use crate::{
    Database,
    database::Config,
    wal::log_record::{Command, LogRecord, Value},
};

use super::log_store::{MetaState, RaftLogStore, RaftMeta};
use super::message::Message;
use super::network::PeerChange;
use super::request::{Responder, StoreOperation, StoreRequest};
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
pub enum EntryPayload {
    Command(LogRecord),
    AddNode { id: NodeId, addr: String },
    RemoveNode { id: NodeId },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub payload: EntryPayload,
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

    // Channel for sending peer management commands to the network layer
    pub peer_changes_tx: Option<UnboundedSender<PeerChange>>,

    // Persistent Raft log store (None only in unit-test / tempdir mode)
    pub log_store: Option<RaftLogStore>,

    // Durable metadata: commit_index, current_term, voted_for.
    pub meta_store: Option<RaftMeta>,

    // Directory used for this node's data (kept for diagnostics)
    pub data_dir: PathBuf,
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
        let dir = dir.as_ref();
        let data_dir = dir.to_path_buf();

        // -----------------------------------------------------------------
        // Open the persistent Raft log store and replay any previously
        // persisted entries.  On a fresh node the file is empty and we fall
        // back to the sentinel dummy entry.
        // -----------------------------------------------------------------
        let log_store_path = dir.join("raft_log.bin");
        let (log_store, persisted_log) = match RaftLogStore::open(&log_store_path) {
            Ok(result) => {
                let (store, entries) = result;
                (Some(store), entries)
            }
            Err(e) => {
                eprintln!(
                    "[Node {}] Failed to open Raft log store at {}: {e}. \
                     Starting with empty log.",
                    id.id,
                    log_store_path.display()
                );
                (None, Vec::new())
            }
        };

        // Open the metadata store (commit_index / current_term / voted_for).
        let meta_path = dir.join("raft_meta.bin");
        let (meta_store, MetaState { commit_index: persisted_commit, current_term: persisted_term, voted_for: persisted_vote }) =
            match RaftMeta::open(&meta_path) {
                Ok((store, state)) => (Some(store), state),
                Err(e) => {
                    eprintln!(
                        "[Node {}] Failed to open Raft meta store at {}: {e}.",
                        id.id,
                        meta_path.display()
                    );
                    (None, MetaState::default())
                }
            };


        // If the store had persisted entries, use them; otherwise start with
        // the dummy sentinel that all Raft implementations require at index 0.
        let log = if persisted_log.is_empty() {
            vec![LogEntry {
                term: 0,
                payload: EntryPayload::Command(LogRecord::new(
                    0,
                    Command::Set {
                        key: String::new(),
                        value: Value::String(String::new()),
                    },
                )),
            }]
        } else {
            persisted_log
        };

        // Reconstruct the peer list from committed log entries.
        // We only apply AddNode / RemoveNode — Command entries are skipped
        // because the LSM/WAL database already has that data on disk.
        // The caller-supplied `peers` list is the initial bootstrap set;
        // any dynamic membership changes are layered on top from the log.
        let mut recovered_peers = peers;
        let safe_commit = persisted_commit.min(log.len().saturating_sub(1));
        for entry in log.iter().skip(1).take(safe_commit) {
            match &entry.payload {
                EntryPayload::AddNode { id: peer_id, .. } => {
                    if *peer_id != id && !recovered_peers.contains(peer_id) {
                        recovered_peers.push(*peer_id);
                    }
                }
                EntryPayload::RemoveNode { id: peer_id } => {
                    recovered_peers.retain(|p| p != peer_id);
                }
                EntryPayload::Command(_) => {}
            }
        }

        let voted_for_node = persisted_vote.map(|v| NodeId { id: v });

        let mut node = Self {
            id,
            peers: recovered_peers,
            current_term: persisted_term,
            voted_for: voted_for_node,
            log,
            commit_index: safe_commit,
            last_applied: safe_commit,
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
            peer_changes_tx: None,
            log_store,
            meta_store,
            data_dir,
        };

        node.reset_election_timeout(now);

        node
    }

    /// Persist the current `commit_index`, `current_term`, and `voted_for` to
    /// the metadata file so that they survive a crash.
    ///
    /// This is a best-effort write; failures are logged but not propagated so
    /// that a disk hiccup does not abort a running Raft operation.
    pub fn persist_meta(&mut self) {
        if let Some(store) = &mut self.meta_store {
            let voted_for = self.voted_for.map(|n| n.id);
            if let Err(e) = store.save(self.commit_index, self.current_term, voted_for) {
                eprintln!(
                    "[Node {}] Failed to persist Raft metadata: {e}",
                    self.id.id
                );
            }
        }
    }

    /// Append a `LogEntry` to both the in-memory log and the on-disk store.
    ///
    /// All code paths that grow the log (leader appends, follower replication)
    /// **must** go through this method so that the disk state stays in sync.
    pub fn push_log_entry(&mut self, entry: LogEntry) {
        if let Some(store) = &mut self.log_store {
            if let Err(e) = store.append(&entry) {
                eprintln!(
                    "[Node {}] Failed to persist log entry to disk: {e}. \
                     Continuing with in-memory log only.",
                    self.id.id
                );
            }
        }
        self.log.push(entry);
    }

    /// Truncate the log (and the on-disk store) at `from` (inclusive).
    ///
    /// Called when a follower detects a conflict with the leader's entries.
    pub fn truncate_log(&mut self, from: usize) {
        if let Some(store) = &mut self.log_store {
            if let Err(e) = store.truncate_from(from) {
                eprintln!(
                    "[Node {}] Failed to truncate Raft log store at index {from}: {e}.",
                    self.id.id
                );
            }
        }
        self.log.truncate(from);
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
                    self.peers.clone(),
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
                    self.peers.clone(),
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
                format!(
                    "Node {} is not leader and no leader is currently known",
                    self.id.id
                )
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
                self.peers.clone(),
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
                self.peers.clone(),
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
        // Persist new term and cleared vote immediately — these must survive a crash.
        self.persist_meta();

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
                self.peers.clone(),
                &mut self.outbox,
            );
        }
    }

    pub fn restart(&mut self, now: Instant) {
        self.role = Role::Follower;
        self.leader_id = None;
        self.votes_received.clear();
        self.outbox.clear();
        self.next_index.clear();
        self.match_index.clear();
        self.pending_requests.clear();
        self.reset_election_timeout(now);
    }

    // pub fn append_node_to_cluster(&mut self, id: NodeId) {
    //     self.peers.push(id);
    // }

    pub fn append_membership_change(&mut self, payload: EntryPayload) {
        if self.role != Role::Leader {
            return;
        }

        self.push_log_entry(LogEntry {
            term: self.current_term,
            payload,
        });

        if self.peers.is_empty() {
            self.update_commit_index();
        } else {
            self.send_heartbeats();
        }
    }
    /// Add a new peer to the cluster. Must be called on the leader; the change is replicated via the Raft log.
    pub fn add_peer(&mut self, id: NodeId, addr: String) {
        if self.role != Role::Leader {
            // Only the leader can initiate membership changes.
            return;
        }
        // Use the existing membership change mechanism to replicate the addition.
        self.append_membership_change(EntryPayload::AddNode { id, addr });
    }

    /// Remove an existing peer from the cluster. Must be called on the leader; the change is replicated via the Raft log.
    pub fn remove_peer(&mut self, id: NodeId) {
        if self.role != Role::Leader {
            return;
        }
        self.append_membership_change(EntryPayload::RemoveNode { id });
    }



}
