use std::time::Instant;

use crate::{
    lsm::wal::log_record::{Command, LogRecord},
    raft::network::PeerChange,
    raft::node::EntryPayload,
};

use super::{
    message::{AppendEntries, AppendEntriesResponse, Message},
    node::{LogEntry, NodeId, RaftNode, Role},
};

impl RaftNode {
    pub fn make_append_entries(&self, peer: NodeId) -> AppendEntries {
        let next_index = *self
            .next_index
            .get(&peer)
            .expect("next_index missing for peer");

        // With the dummy entry at index 0, next_index should
        // never be 0.
        assert!(next_index >= 1);
        assert!(next_index <= self.log.len());

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

    pub fn handle_append_entries(
        &mut self,
        entries: AppendEntries,
        now: Instant,
    ) -> AppendEntriesResponse {
        if entries.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                follower_id: self.id,
                success: false,
                match_index: self.log.len() - 1,
            };
        }

        if entries.term > self.current_term {
            self.step_down(entries.term, now);
        }

        self.role = Role::Follower;
        self.leader_id = Some(entries.leader_id);

        self.reset_election_timeout(now);

        let prev_log_index = entries.prev_log_index;

        if prev_log_index >= self.log.len() {
            return AppendEntriesResponse {
                term: self.current_term,
                follower_id: self.id,
                success: false,
                match_index: self.log.len() - 1,
            };
        }

        if self.log[prev_log_index].term != entries.prev_log_term {
            return AppendEntriesResponse {
                term: self.current_term,
                follower_id: self.id,
                success: false,
                match_index: self.log.len() - 1,
            };
        }

        // Report only the last index covered by this request,
        // not the follower's entire local log length.
        let match_index = prev_log_index + entries.entries.len();

        let mut index = prev_log_index + 1;

        for incoming_entry in entries.entries {
            if index < self.log.len() {
                // Existing entry conflicts with the leader's entry.
                if self.log[index].term != incoming_entry.term {
                    self.truncate_log(index);
                    self.push_log_entry(incoming_entry);
                }

                // Same term: the entry already exists.
            } else {
                // The follower's log is shorter.
                self.push_log_entry(incoming_entry);
            }

            index += 1;
        }

        self.commit_index = std::cmp::min(entries.leader_commit, self.log.len() - 1);

        // Persist the new commit_index so that committed membership changes
        // (AddNode / RemoveNode) survive a crash on this follower.
        self.persist_meta();

        // Apply newly committed entries to the state machine.
        self.apply_committed_entries();

        AppendEntriesResponse {
            term: self.current_term,
            follower_id: self.id,
            success: true,
            match_index,
        }
    }

    pub fn handle_append_entries_response(
        &mut self,
        response: AppendEntriesResponse,
        now: Instant,
    ) {
        if response.term > self.current_term {
            self.step_down(response.term, now);
            return;
        }

        if self.role != Role::Leader {
            return;
        }

        if response.term < self.current_term {
            return;
        }

        let peer = response.follower_id;

        if response.success {
            self.match_index.insert(peer, response.match_index);

            self.next_index.insert(peer, response.match_index + 1);

            // See if this allows us to commit more entries.
            self.update_commit_index();

            return;
        }

        let next_index = self
            .next_index
            .get_mut(&peer)
            .expect("next_index missing for peer");

        if *next_index > 1 {
            *next_index -= 1;
        }

        let retry = self.make_append_entries(peer);

        self.outbox.push((peer, Message::AppendEntries(retry)));
    }

    pub fn update_commit_index(&mut self) {
        let cluster_size = self.peers.len() + 1;
        let majority = cluster_size / 2 + 1;

        for index in (self.commit_index + 1)..self.log.len() {
            if self.log[index].term != self.current_term {
                continue;
            }

            let replicated = self
                .match_index
                .values()
                .filter(|&&match_index| match_index >= index)
                .count()
                + 1;

            if replicated >= majority {
                self.commit_index = index;
            }
        }

        // Persist the advanced commit_index before applying entries so that a
        // crash after this point will replay from the correct position on restart.
        self.persist_meta();

        // Apply everything that became committed.
        self.apply_committed_entries();
    }

    pub fn append_command(&mut self, command: Command) -> usize {
        assert_eq!(
            self.role,
            Role::Leader,
            "Only leader can append client commands"
        );

        let index = self.log.len();

        self.push_log_entry(LogEntry {
            term: self.current_term,
            payload: EntryPayload::Command(LogRecord::new(0, command)),
        });

        if self.peers.is_empty() {
            self.update_commit_index();
        } else {
            for peer in self.peers.clone() {
                let entries = self.make_append_entries(peer);
                self.outbox.push((peer, Message::AppendEntries(entries)));
            }
        }

        index
    }

    pub fn apply_committed_entries(&mut self) {
        while self.last_applied < self.commit_index {
            let index = self.last_applied + 1;
            let term = self.log[index].term;
            let mut result = Ok(());
            let mut peer_change = None;

            match self.log[index].payload.clone() {
                EntryPayload::Command(record) => {
                    result = match record.command {
                        Command::Set { key, value } => self.db.insert(key, value).map(|_| ()),
                        Command::Delete { key } => self.db.delete(key).map(|_| ()),
                    };

                    // Preserve your existing pending-request response handling here.
                }

                EntryPayload::AddNode { id, addr } => {
                    if id != self.id && !self.peers.contains(&id) {
                        self.peers.push(id);
                    }

                    // Only the leader needs replication progress for this peer.
                    if self.role == Role::Leader {
                        self.next_index.entry(id).or_insert(self.log.len());

                        self.match_index.entry(id).or_insert(0);
                    }

                    peer_change = Some(PeerChange::AddPeer { id, addr });
                }

                EntryPayload::RemoveNode { id } => {
                    self.peers.retain(|p| *p != id);

                    self.next_index.remove(&id);
                    self.match_index.remove(&id);

                    peer_change = Some(PeerChange::RemovePeer { id });
                }
            }

            self.last_applied = index;

            // Send peer change to network layer if any
            if let Some(change) = peer_change {
                if let Some(tx) = &self.peer_changes_tx {
                    let _ = tx.send(change);
                }
            }

            if let Some(responder) = self.pending_requests.remove(&(index, term)) {
                let (success, message) = match result {
                    Ok(()) => (
                        true,
                        format!("Write committed and applied at log index {index}"),
                    ),
                    Err(err) => (
                        false,
                        format!("Log entry committed, but database application failed: {err}"),
                    ),
                };

                let leader_id = self.leader_id.or(Some(self.id));
                let id = self.id;
                let role = self.role;

                responder.send(
                    success,
                    None,
                    message,
                    leader_id,
                    term,
                    id,
                    role,
                    self.peers.clone(),
                    &mut self.outbox,
                );
            } else if let Err(err) = result {
                eprintln!(
                    "Node {} failed to apply committed entry {}: {}",
                    self.id.id, index, err
                );
            }
        }
    }
}
