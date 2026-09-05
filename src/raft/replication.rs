use super::{
    message::{AppendEntries, AppendEntriesResponse, Message},
    node::{Command, LogEntry, NodeId, RaftNode, Role},
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

    pub fn handle_append_entries(&mut self, entries: AppendEntries) -> AppendEntriesResponse {
        if entries.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                follower_id: self.id,
                success: false,
                match_index: self.log.len() - 1,
            };
        }

        if entries.term > self.current_term {
            self.step_down(entries.term);
        }

        self.role = Role::Follower;

        self.reset_election_timeout();

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

        let mut index = prev_log_index + 1;

        for incoming_entry in entries.entries {
            if index < self.log.len() {
                // Existing entry conflicts with leader's entry.
                if self.log[index].term != incoming_entry.term {
                    self.log.truncate(index);
                    self.log.push(incoming_entry);
                }

                // Same term means the entry already exists.
            } else {
                // Follower's log is shorter.
                self.log.push(incoming_entry);
            }

            index += 1;
        }

        self.commit_index = std::cmp::min(entries.leader_commit, self.log.len() - 1);

        // Apply newly committed entries to the state machine.
        self.apply_committed_entries();

        AppendEntriesResponse {
            term: self.current_term,
            follower_id: self.id,
            success: true,
            match_index: self.log.len() - 1,
        }
    }

    pub fn handle_append_entries_response(&mut self, response: AppendEntriesResponse) {
        if response.term > self.current_term {
            self.step_down(response.term);
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

        // Apply everything that became committed.
        self.apply_committed_entries();
    }

    pub fn append_command(&mut self, command: Command) {
        assert_eq!(
            self.role,
            Role::Leader,
            "Only leader can append client commands"
        );

        self.log.push(LogEntry {
            term: self.current_term,
            command,
        });

        if self.peers.is_empty() {
            self.update_commit_index();
        } else {
            for peer in self.peers.clone() {
                let entries = self.make_append_entries(peer);

                self.outbox.push((peer, Message::AppendEntries(entries)));
            }
        }
    }

    pub fn apply_committed_entries(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;

            let command = self.log[self.last_applied].command.clone();

            if super::is_log_enabled() {
                match command {
                    Command::Set(key, value) => {
                        println!("Node {} applied SET {} = {}", self.id.id, key, value);
                    }

                    Command::Delete(key) => {
                        println!("Node {} applied DELETE {}", self.id.id, key);
                    }
                }
            }
        }
    }
}
