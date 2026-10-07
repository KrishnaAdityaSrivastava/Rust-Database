use kv_store::raft::{LogEntry, Message, NodeId, Role, Simulation};
use kv_store::wal::log_record::{Command, LogRecord, Value};

fn string(value: &str) -> Value {
    Value::String(value.to_string())
}

// -----------------------------------------------------------------------------
// 1. Leader Election & Quorum
// -----------------------------------------------------------------------------

#[test]
fn test_leader_election_and_quorum() {
    let mut cluster = Simulation::new_cluster(&[1, 2, 3]);

    cluster.start_election(1);
    cluster.route_messages();

    assert_eq!(cluster.get_leader(), Some(1));
    assert_eq!(cluster[1].current_term, 1);
    assert_eq!(cluster[2].role, Role::Follower);
    assert_eq!(cluster[3].role, Role::Follower);
}

// -----------------------------------------------------------------------------
// 2. Leader Failure & Re-election
// -----------------------------------------------------------------------------

#[test]
fn test_leader_failure_and_reelection() {
    let mut cluster = Simulation::new_cluster(&[1, 2, 3]);

    cluster.start_election(1);
    cluster.route_messages();

    assert_eq!(cluster.get_leader(), Some(1));

    // Fail leader (Node 1)
    cluster.disable_node(1);

    // Node 2 triggers election for term 2
    cluster.start_election(2);
    cluster.route_messages();

    assert_eq!(cluster.get_leader(), Some(2));
    assert_eq!(cluster[2].current_term, 2);
}

// -----------------------------------------------------------------------------
// 3. Follower Failure with Quorum Progress
// -----------------------------------------------------------------------------

#[test]
fn test_follower_failure_quorum_progress() {
    let mut cluster = Simulation::new_cluster(&[1, 2, 3]);

    cluster.start_election(1);
    cluster.route_messages();

    // Disable follower Node 3
    cluster.disable_node(3);

    // Leader receives client command
    cluster[1].append_command(Command::Set {
        key: "k1".into(),
        value: string("v1"),
    });

    cluster.route_messages();

    // Quorum of 2 (Node 1 & Node 2) is enough to commit!
    assert_eq!(cluster[1].commit_index, 1);
    assert_eq!(cluster[2].log.len(), 2);

    // Send heartbeat to inform Node 2 that entry 1 is committed
    cluster[1].send_heartbeats();
    cluster.route_messages();

    assert_eq!(cluster[2].commit_index, 1);
}

// -----------------------------------------------------------------------------
// 4. Leader Restart & Rejoin as Follower
// -----------------------------------------------------------------------------

#[test]
fn test_leader_restart_and_rejoin_as_follower() {
    let mut cluster = Simulation::new_cluster(&[1, 2, 3]);

    cluster.start_election(1);
    cluster.route_messages();

    // Fail Node 1
    cluster.disable_node(1);

    // Node 2 becomes leader in term 2
    cluster.start_election(2);
    cluster.route_messages();

    assert_eq!(cluster.get_leader(), Some(2));

    // Node 1 recovers and comes back online
    cluster.enable_node(1);

    // Leader (Node 2) sends heartbeats
    cluster[2].send_heartbeats();
    cluster.route_messages();

    // Old leader Node 1 must step down to follower and update its term to 2
    assert_eq!(cluster[1].role, Role::Follower);
    assert_eq!(cluster[1].current_term, 2);
}

// -----------------------------------------------------------------------------
// 5. Follower Restart & Log Catchup
// -----------------------------------------------------------------------------

#[test]
fn test_follower_restart_and_log_catchup() {
    let mut cluster = Simulation::new_cluster(&[1, 2, 3]);

    cluster.start_election(1);
    cluster.route_messages();

    // Fail Node 3
    cluster.disable_node(3);

    // Append 2 commands while Node 3 is offline
    cluster[1].append_command(Command::Set {
        key: "a".into(),
        value: string("1"),
    });

    cluster[1].append_command(Command::Set {
        key: "b".into(),
        value: string("2"),
    });

    cluster.route_messages();

    assert_eq!(cluster[3].log.len(), 1); // Only dummy entry

    // Node 3 recovers
    cluster.enable_node(3);

    // Leader sends heartbeats/append_entries to catch Node 3 up
    cluster[1].send_heartbeats();
    cluster.route_messages();

    // Node 3 must be caught up to log length 3 and commit_index 2
    assert_eq!(cluster[3].log.len(), 3);
    assert_eq!(cluster[3].commit_index, 2);
}

// -----------------------------------------------------------------------------
// 6. Log Conflict Resolution
// -----------------------------------------------------------------------------

#[test]
fn test_log_conflict_resolution() {
    let mut cluster = Simulation::new_cluster(&[1, 2, 3]);

    // Node 1 becomes leader in term 1
    cluster.start_election(1);
    cluster.route_messages();

    // Partition Node 1 away before it replicates
    cluster.partition(&[1], &[2, 3]);

    cluster[1].append_command(Command::Set {
        key: "uncommitted".into(),
        value: string("val"),
    });

    // Node 2 becomes leader in term 2
    cluster.start_election(2);
    cluster.route_messages();

    cluster[2].append_command(Command::Set {
        key: "committed".into(),
        value: string("correct"),
    });

    cluster.route_messages();

    // Heal network
    cluster.heal_all_partitions();

    // Leader Node 2 sends heartbeats
    cluster[2].send_heartbeats();
    cluster.route_messages();

    // Node 1's conflicting entry must be overwritten with Node 2's entry
    let node1_entry = &cluster[1].log[1];
    let node2_entry = &cluster[2].log[1];

    assert_eq!(node1_entry, node2_entry);

    assert_eq!(
        node1_entry.record.command(),
        &Command::Set {
            key: "committed".into(),
            value: string("correct"),
        }
    );
}

// -----------------------------------------------------------------------------
// 7. Network Partition & Healing (Split-Brain Prevention)
// -----------------------------------------------------------------------------

#[test]
fn test_network_partition_and_healing() {
    let mut cluster = Simulation::new_cluster(&[1, 2, 3, 4, 5]);

    // Node 1 elected leader in term 1
    cluster.start_election(1);
    cluster.route_messages();

    assert_eq!(cluster.get_leader(), Some(1));

    // Partition: {1, 2} (minority) vs {3, 4, 5} (majority)
    cluster.partition(&[1, 2], &[3, 4, 5]);

    // Command sent to minority leader (Node 1)
    cluster[1].append_command(Command::Set {
        key: "minority".into(),
        value: string("fail"),
    });

    cluster.route_messages();

    // Node 1 cannot commit because minority = 2 nodes (needs 3)
    assert_eq!(cluster[1].commit_index, 0);

    // Majority side elects Node 3 as leader for term 2
    cluster.start_election(3);
    cluster.route_messages();

    assert_eq!(cluster[3].role, Role::Leader);

    // Command sent to majority leader (Node 3)
    cluster[3].append_command(Command::Set {
        key: "majority".into(),
        value: string("pass"),
    });

    cluster.route_messages();

    // Majority commits entry!
    assert_eq!(cluster[3].commit_index, 1);

    // Heal network
    cluster.heal_all_partitions();

    cluster[3].send_heartbeats();
    cluster.route_messages();

    // Node 1 steps down and accepts Node 3's log
    assert_eq!(cluster[1].role, Role::Follower);
    assert_eq!(cluster[1].commit_index, 1);

    assert_eq!(
        cluster[1].log[1].record.command(),
        &Command::Set {
            key: "majority".into(),
            value: string("pass"),
        }
    );
}

// -----------------------------------------------------------------------------
// 8. Message Loss & Retries
// -----------------------------------------------------------------------------

#[test]
fn test_message_loss_and_retries() {
    let mut cluster = Simulation::new_cluster(&[1, 2, 3]);

    cluster.start_election(1);
    cluster.route_messages();

    // Block link from 1 to 2 temporarily (simulating dropped packet)
    cluster.block_link(1, 2);

    cluster[1].append_command(Command::Set {
        key: "k".into(),
        value: string("v"),
    });

    cluster.route_messages();

    // Node 2 missed the append, but Node 3 received it (quorum met)
    assert_eq!(cluster[2].log.len(), 1);

    // Unblock link and retry heartbeat
    cluster.heal_all_partitions();

    cluster[1].send_heartbeats();
    cluster.route_messages();

    // Node 2 receives missing entries on retry
    assert_eq!(cluster[2].log.len(), 2);
}

// -----------------------------------------------------------------------------
// 9. Message Reordering Safety
// -----------------------------------------------------------------------------

#[test]
fn test_message_reordering_safety() {
    let mut cluster = Simulation::new_cluster(&[1, 2, 3]);

    cluster.start_election(1);
    cluster.route_messages();

    // Leader appends cmd1 (index 1) and cmd2 (index 2)
    cluster[1].log.push(LogEntry {
        term: 1,
        record: LogRecord::new(
            1,
            Command::Set {
                key: "cmd1".into(),
                value: string("v1"),
            },
        ),
    });

    // ae1 target index 1 (prev_log_index = 0)
    cluster[1].next_index.insert(NodeId { id: 2 }, 1);
    let ae1 = cluster[1].make_append_entries(NodeId { id: 2 });

    cluster[1].log.push(LogEntry {
        term: 1,
        record: LogRecord::new(
            2,
            Command::Set {
                key: "cmd2".into(),
                value: string("v2"),
            },
        ),
    });

    // ae2 target index 2 (prev_log_index = 1)
    cluster[1].next_index.insert(NodeId { id: 2 }, 2);
    let ae2 = cluster[1].make_append_entries(NodeId { id: 2 });

    // Deliver ae2 (prev_log_index = 1) BEFORE ae1 to Node 2
    // Node 2 must reject because prev_log_index (1) >= log.len() (1)
    let now = cluster.now();
    let resp2 = cluster[2].handle_append_entries(ae2, now);

    assert!(!resp2.success);

    // Deliver ae1 (prev_log_index = 0)
    let resp1 = cluster[2].handle_append_entries(ae1, now);

    assert!(resp1.success);
}

// -----------------------------------------------------------------------------
// 10. Duplicate Message Idempotency
// -----------------------------------------------------------------------------

#[test]
fn test_duplicate_message_idempotency() {
    let mut cluster = Simulation::new_cluster(&[1, 2, 3]);

    cluster.start_election(1);
    cluster.route_messages();

    cluster[1].append_command(Command::Set {
        key: "dup".into(),
        value: string("val"),
    });

    let ae = cluster[1].make_append_entries(NodeId { id: 2 });

    // Process exact same AppendEntries twice
    let now = cluster.now();
    cluster[2].handle_append_entries(ae.clone(), now);
    cluster[2].handle_append_entries(ae, now);

    // Log length must remain 2 (dummy + 1 entry), no duplicate entries appended
    assert_eq!(cluster[2].log.len(), 2);
}

// -----------------------------------------------------------------------------
// 11. Higher Term Discovery Step-Down
// -----------------------------------------------------------------------------

#[test]
fn test_higher_term_discovery_step_down() {
    let mut cluster = Simulation::new_cluster(&[1, 2, 3]);

    cluster.start_election(1);
    cluster.route_messages();

    assert_eq!(cluster[1].role, Role::Leader);

    // Node 1 receives message from a higher term 5
    let msg_higher_term = Message::AppendEntries(kv_store::raft::AppendEntries {
        term: 5,
        leader_id: NodeId { id: 2 },
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
    });

    let now = cluster.now();
    cluster[1].handle_message(msg_higher_term, now);

    // Node 1 must step down to follower and update term to 5
    assert_eq!(cluster[1].role, Role::Follower);
    assert_eq!(cluster[1].current_term, 5);
}

// -----------------------------------------------------------------------------
// 12. Old Leader Returning Isolation
// -----------------------------------------------------------------------------

#[test]
fn test_old_leader_returning_isolation() {
    let mut cluster = Simulation::new_cluster(&[1, 2, 3]);

    cluster.start_election(1);
    cluster.route_messages();

    // Isolate Node 1
    cluster.partition(&[1], &[2, 3]);

    // Node 2 becomes leader in term 2 and appends a command
    cluster.start_election(2);
    cluster.route_messages();

    cluster[2].append_command(Command::Set {
        key: "new".into(),
        value: string("pass"),
    });

    cluster.route_messages();

    // Old leader Node 1 tries to process client command while isolated
    let now = cluster.now();
    cluster[1].handle_message(
        Message::ClientCommand(Command::Set {
            key: "old".into(),
            value: string("fail"),
        }),
        now,
    );

    cluster.route_messages();

    // Heal network
    cluster.heal_all_partitions();

    cluster[2].send_heartbeats();
    cluster.route_messages();

    // Node 1's isolated command was overwritten by new leader Node 2's command
    assert_eq!(cluster[1].role, Role::Follower);

    assert_eq!(
        cluster[1].log[1].record.command(),
        &Command::Set {
            key: "new".into(),
            value: string("pass"),
        }
    );
}

// -----------------------------------------------------------------------------
// 13. Uncommitted vs Committed Entry Visibility
// -----------------------------------------------------------------------------

#[test]
fn test_uncommitted_vs_committed_visibility() {
    let mut cluster = Simulation::new_cluster(&[1, 2, 3]);

    cluster.start_election(1);
    cluster.route_messages();

    let ae_uncommitted = {
        let leader = &mut cluster[1];

        leader.log.push(LogEntry {
            term: 1,
            record: LogRecord::new(
                1,
                Command::Set {
                    key: "visibility".into(),
                    value: string("test"),
                },
            ),
        });

        // Create AppendEntries with leader_commit = 0 (uncommitted)
        let ae = leader.make_append_entries(NodeId { id: 2 });

        assert_eq!(ae.leader_commit, 0);

        ae
    };

    let now = cluster.now();
    cluster[2].handle_append_entries(ae_uncommitted, now);

    // Follower has the entry in log, but last_applied is still 0!
    assert_eq!(cluster[2].log.len(), 2);
    assert_eq!(cluster[2].last_applied, 0);

    // Now leader updates commit_index = 1 and sends heartbeat
    let ae_committed = {
        let leader = &mut cluster[1];

        leader.commit_index = 1;

        leader.make_append_entries(NodeId { id: 2 })
    };

    let now = cluster.now();
    cluster[2].handle_append_entries(ae_committed, now);

    // Now follower updates last_applied to 1!
    assert_eq!(cluster[2].commit_index, 1);
    assert_eq!(cluster[2].last_applied, 1);
}

// -----------------------------------------------------------------------------
// 14. Typed Value Replication
// -----------------------------------------------------------------------------

#[test]
fn test_typed_value_replication() {
    let mut cluster = Simulation::new_cluster(&[1, 2, 3]);

    cluster.start_election(1);
    cluster.route_messages();

    cluster[1].append_command(Command::Set {
        key: "integer".into(),
        value: Value::Int(42),
    });

    cluster.route_messages();

    assert_eq!(
        cluster[2].log[1].record.command(),
        &Command::Set {
            key: "integer".into(),
            value: Value::Int(42),
        }
    );
}

// -----------------------------------------------------------------------------
// 15. Direct Request to Follower Node (Auto Forwarding to Leader)
// -----------------------------------------------------------------------------

#[test]
fn test_command_sent_directly_to_follower_is_forwarded() {
    let mut cluster = Simulation::new_cluster(&[1, 2, 3]);

    // Node 1 becomes leader
    cluster.start_election(1);
    cluster.route_messages();

    assert_eq!(cluster[1].role, Role::Leader);
    assert_eq!(cluster[2].role, Role::Follower);

    // Send client command directly to Node 2 (a follower)
    let now = cluster.now();
    cluster[2].submit_command(
        Command::Set {
            key: "follower_key".into(),
            value: string("follower_val"),
        },
        now,
    );

    // Route messages so Node 2 forwards to Leader (Node 1) and Leader replicates to followers
    cluster.route_messages();

    // Leader sends heartbeat to communicate updated commit_index (1) to followers
    cluster[1].send_heartbeats();
    cluster.route_messages();

    // Verify command was successfully committed and applied on all nodes
    assert_eq!(cluster[1].commit_index, 1);
    assert_eq!(cluster[2].commit_index, 1);
    assert_eq!(cluster[3].commit_index, 1);

    assert_eq!(
        cluster[2].get("follower_key").unwrap(),
        Some(string("follower_val"))
    );
}
