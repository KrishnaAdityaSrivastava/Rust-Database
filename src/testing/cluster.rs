use std::collections::HashMap;
use tempfile::tempdir;

use crate::database::{Config, Database};
use crate::raft::node::EntryPayload;
use crate::raft::{Role, Simulation};
use crate::wal::log_record::{Command, Value};

/// Minimal cluster fixture combining Simulation with per-node Database instances.
pub struct TestCluster {
    pub sim: Simulation,
    pub databases: HashMap<u64, Database>,
    pub db_applied: HashMap<u64, usize>,
    _temp_dirs: Vec<tempfile::TempDir>,
}

impl TestCluster {
    pub fn new(size: usize) -> Self {
        Self::new_with_config(size, Config::new(3, 200, 4))
    }

    pub fn new_with_config(size: usize, config: Config) -> Self {
        let node_ids: Vec<u64> = (1..=(size as u64)).collect();
        let mut sim = Simulation::new_cluster(&node_ids);
        let mut databases = HashMap::new();
        let mut db_applied = HashMap::new();
        let mut temp_dirs = Vec::new();

        for &id in &node_ids {
            let dir = tempdir().expect("Failed to create tempdir for node db");
            let db = Database::open_in_dir(dir.path(), config.clone()).expect("Failed DB open");
            databases.insert(id, db);
            db_applied.insert(id, 0);
            temp_dirs.push(dir);
        }

        // Elect Node 1 as leader
        sim.start_election(1);
        sim.route_messages();
        assert_eq!(sim[1].role, Role::Leader);

        Self {
            sim,
            databases,
            db_applied,
            _temp_dirs: temp_dirs,
        }
    }

    /// Appends commands to leader, routes messages, and applies committed entries to all node databases.
    pub fn execute_workload(&mut self, num_ops: usize) {
        for i in 0..num_ops {
            let key = format!("k_{:06}", i);
            let val = format!("v_{}", i);

            self.sim[1].append_command(Command::Set {
                key,
                value: Value::String(val),
            });
            self.sim.route_messages();
            self.apply_committed();
        }

        // Heartbeats to advance follower commit indices to leader commit
        for _ in 0..2 {
            self.sim[1].send_heartbeats();
            self.sim.route_messages();
        }
        self.apply_committed();
    }

    /// Drains newly committed log entries into each node's local database.
    pub fn apply_committed(&mut self) {
        let committed_entries: Vec<(u64, Vec<(usize, EntryPayload)>)> = self
            .sim
            .nodes
            .iter()
            .map(|(&node_id, node)| {
                let applied = *self.db_applied.get(&node_id.id).unwrap();

                let entries = ((applied + 1)..=node.commit_index)
                    .filter_map(|index| {
                        node.log
                            .get(index)
                            .map(|entry| (index, entry.payload.clone()))
                    })
                    .collect();

                (node_id.id, entries)
            })
            .collect();

        for (node_id, entries) in committed_entries {
            let db = self.databases.get(&node_id).unwrap();

            for (index, payload) in entries {
                match payload {
                    EntryPayload::Command(record) => match record.command {
                        Command::Set { key, value } => {
                            db.insert(key, value).unwrap();
                        }
                        Command::Delete { key } => {
                            db.delete(key).unwrap();
                        }
                    },

                    EntryPayload::AddNode { .. } => {
                        // Configuration entries do not modify the key-value database.
                    }

                    EntryPayload::RemoveNode { .. } => {
                        // Configuration entries do not modify the key-value database.
                    }
                }

                self.db_applied.insert(node_id, index);
            }
        }
    }

    /// Verifies data correctness across all nodes for the workload.
    pub fn verify_correctness(&self) -> bool {
        let final_commit = self.sim[1].commit_index;
        for i in (0..final_commit).step_by(10) {
            let key = format!("k_{:06}", i);
            let expected_val = Value::String(format!("v_{}", i));

            for (&node_id, db) in &self.databases {
                let actual = db.get(&key).unwrap();
                if actual != Some(expected_val.clone()) {
                    eprintln!(
                        "Correctness mismatch on Node {} for key {}: got {:?}, expected {:?}",
                        node_id, key, actual, expected_val
                    );
                    return false;
                }
            }
        }
        true
    }
}
