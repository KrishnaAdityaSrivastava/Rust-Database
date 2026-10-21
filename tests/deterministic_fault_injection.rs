use std::time::Duration;

use kv_store::raft::node::EntryPayload;
use kv_store::raft::{
    ChaosConfig, Fault, NodeId, Role, Simulation,
};
use kv_store::wal::log_record::{Command, LogRecord, Value};

fn string(val: &str) -> Value {
    Value::String(val.to_string())
}

// -----------------------------------------------------------------------------
// 1. Scheduled Fault Timeline
// -----------------------------------------------------------------------------

#[test]
fn test_scheduled_fault_timeline() {
    let mut sim = Simulation::new_cluster(&[1, 2, 3]);

    // Schedule an election on Node 1 at t + 10ms
    sim.schedule_fault_after(Duration::from_millis(10), Fault::TriggerElection(NodeId { id: 1 }));

    // Schedule node 1 to crash at t + 50ms
    sim.schedule_fault_after(Duration::from_millis(50), Fault::CrashNode(NodeId { id: 1 }));

    // Schedule node 2 to elect itself at t + 60ms
    sim.schedule_fault_after(Duration::from_millis(60), Fault::TriggerElection(NodeId { id: 2 }));

    // Schedule node 1 to restart at t + 100ms
    sim.schedule_fault_after(Duration::from_millis(100), Fault::RestartNode(NodeId { id: 1 }));

    // Run until all scheduled events and messages settle
    sim.run_until_idle();

    // Node 2 should now be the leader for term 2
    assert_eq!(sim.get_leader(), Some(2));
    assert_eq!(sim[2].current_term, 2);

    // Node 1 restarted and stepped down to Follower
    assert_eq!(sim[1].role, Role::Follower);

    // Verify consensus safety invariants
    sim.check_invariants().expect("Invariants must hold");
}

// -----------------------------------------------------------------------------
// 2. Link Delay & Deterministic Packet Reordering
// -----------------------------------------------------------------------------

#[test]
fn test_link_delay_and_reordering_safety() {
    let mut sim = Simulation::new_cluster(&[1, 2, 3]);

    // Node 1 becomes leader
    sim.start_election(1);
    sim.route_messages();
    assert_eq!(sim.get_leader(), Some(1));

    // Delay link from Node 1 to Node 2 by 50ms
    sim.delay_link(1, 2, Duration::from_millis(50));

    // Append command 1 (index 1)
    sim[1].append_command(Command::Set {
        key: "key1".into(),
        value: string("val1"),
    });
    sim.flush_node_outbox(NodeId { id: 1 });

    // Clear delay so subsequent messages are delivered immediately
    sim.clear_link_delay(1, 2);

    // Append command 2 (index 2)
    sim[1].append_command(Command::Set {
        key: "key2".into(),
        value: string("val2"),
    });
    sim.flush_node_outbox(NodeId { id: 1 });

    // Node 3 receives both in order, commits
    // Node 2 receives command 2 before command 1 due to delay, rejects until command 1 arrives
    sim.run_until_idle();

    // Leader sends heartbeats to ensure followers advance commit_index
    sim[1].send_heartbeats();
    sim.route_messages();

    assert_eq!(sim[1].commit_index, 2);
    assert_eq!(sim[2].commit_index, 2);
    assert_eq!(sim[3].commit_index, 2);

    sim.check_invariants().expect("Invariants must hold");
}

// -----------------------------------------------------------------------------
// 3. Packet Drops and Retries
// -----------------------------------------------------------------------------

#[test]
fn test_packet_drops_and_automatic_recovery() {
    let mut sim = Simulation::new_cluster(&[1, 2, 3]);

    sim.start_election(1);
    sim.route_messages();
    assert_eq!(sim.get_leader(), Some(1));

    // Drop the next 1 packet from Node 1 to Node 2
    sim.drop_packets(1, 2, 1);

    // Node 1 appends command
    sim[1].append_command(Command::Set {
        key: "k_drop".into(),
        value: string("v_drop"),
    });
    sim.route_messages();

    // Node 2 missed the append, but Node 3 received it (quorum met!)
    assert_eq!(sim[2].log.len(), 1);
    assert_eq!(sim[3].log.len(), 2);
    assert_eq!(sim[1].commit_index, 1);

    // Leader sends heartbeat (drop quota expired, packet reaches Node 2)
    sim[1].send_heartbeats();
    sim.route_messages();

    assert_eq!(sim[2].log.len(), 2);
    assert_eq!(sim[2].commit_index, 1);

    sim.check_invariants().expect("Invariants must hold");
}

// -----------------------------------------------------------------------------
// 4. Packet Duplication Safety (Idempotence)
// -----------------------------------------------------------------------------

#[test]
fn test_packet_duplication_safety() {
    let mut sim = Simulation::new_cluster(&[1, 2, 3]);

    sim.start_election(1);
    sim.route_messages();

    // Duplicate next 3 packets from Node 1 to Node 2 with 10ms delay
    sim.duplicate_packets(1, 2, 3, Duration::from_millis(10));

    sim[1].append_command(Command::Set {
        key: "dup_key".into(),
        value: string("dup_val"),
    });

    sim.run_until_idle();

    // Leader sends heartbeats so follower learns the new commit index
    sim[1].send_heartbeats();
    sim.route_messages();

    // Despite duplicates, Node 2's log must have exactly 1 dummy entry + 1 command entry = 2 entries
    assert_eq!(sim[2].log.len(), 2);
    assert_eq!(sim[2].commit_index, 1);

    sim.check_invariants().expect("Invariants must hold");
}

// -----------------------------------------------------------------------------
// 5. Node Pause & Resume (Simulated GC Freeze)
// -----------------------------------------------------------------------------

#[test]
fn test_node_pause_and_resume() {
    let mut sim = Simulation::new_cluster(&[1, 2, 3]);

    sim.start_election(1);
    sim.route_messages();
    assert_eq!(sim.get_leader(), Some(1));

    // Pause follower Node 3
    sim.pause_node(3);

    // Node 1 appends 2 commands while Node 3 is frozen
    sim[1].append_command(Command::Set {
        key: "p1".into(),
        value: string("v1"),
    });
    sim[1].append_command(Command::Set {
        key: "p2".into(),
        value: string("v2"),
    });
    sim.route_messages();

    // Leader sends heartbeat so Node 2 learns new commit index
    sim[1].send_heartbeats();
    sim.route_messages();

    // Quorum of 2 (Node 1 and Node 2) committed both entries
    assert_eq!(sim[1].commit_index, 2);
    assert_eq!(sim[2].commit_index, 2);
    assert_eq!(sim[3].commit_index, 0);

    // Unpause Node 3
    sim.resume_node(3);

    // Send heartbeats and run until idle
    sim[1].send_heartbeats();
    sim.run_until_idle();

    // Node 3 caught up completely
    assert_eq!(sim[3].commit_index, 2);
    assert_eq!(sim[3].log.len(), 3);

    sim.check_invariants().expect("Invariants must hold");
}

// -----------------------------------------------------------------------------
// 6. Node Crash and Recovery with Invariant Verification
// -----------------------------------------------------------------------------

#[test]
fn test_leader_crash_and_recovery_safety() {
    let mut sim = Simulation::new_cluster(&[1, 2, 3]);

    sim.start_election(1);
    sim.route_messages();
    assert_eq!(sim.get_leader(), Some(1));

    sim[1].append_command(Command::Set {
        key: "x".into(),
        value: string("10"),
    });
    sim.route_messages();
    assert_eq!(sim[1].commit_index, 1);

    // Crash leader (Node 1)
    sim.crash_node(1);

    // Node 2 starts election in term 2
    sim.start_election(2);
    sim.route_messages();

    assert_eq!(sim.get_leader(), Some(2));
    assert_eq!(sim[2].current_term, 2);

    // Append new command under Node 2
    sim[2].append_command(Command::Set {
        key: "y".into(),
        value: string("20"),
    });
    sim.route_messages();

    // Restart old leader Node 1
    sim.restart_node(1);

    // Leader sends heartbeats
    sim[2].send_heartbeats();
    sim.route_messages();

    // Node 1 adopts term 2 and synchronizes log
    assert_eq!(sim[1].role, Role::Follower);
    assert_eq!(sim[1].current_term, 2);
    assert_eq!(sim[1].commit_index, 2);

    sim.check_invariants().expect("Invariants must hold");
}

// -----------------------------------------------------------------------------
// 7. Network Partition and Split-Brain Prevention
// -----------------------------------------------------------------------------

#[test]
fn test_complex_partition_and_healing() {
    let mut sim = Simulation::new_cluster(&[1, 2, 3, 4, 5]);

    sim.start_election(1);
    sim.route_messages();
    assert_eq!(sim.get_leader(), Some(1));

    // Partition: {1, 2} (minority) vs {3, 4, 5} (majority)
    sim.partition(&[1, 2], &[3, 4, 5]);

    // Command sent to minority leader cannot commit
    sim[1].append_command(Command::Set {
        key: "isolated".into(),
        value: string("uncommitted"),
    });
    sim.route_messages();
    assert_eq!(sim[1].commit_index, 0);

    // Majority side elects Node 3
    sim.start_election(3);
    sim.route_messages();
    assert_eq!(sim[3].role, Role::Leader);
    assert_eq!(sim[3].current_term, 2);

    // Command sent to majority leader commits
    sim[3].append_command(Command::Set {
        key: "majority".into(),
        value: string("committed"),
    });
    sim.route_messages();
    assert_eq!(sim[3].commit_index, 1);

    // Heal all partitions
    sim.heal_all_partitions();

    // Send heartbeats from true leader
    sim[3].send_heartbeats();
    sim.route_messages();

    // Old leader Node 1 steps down and accepts majority's committed log
    assert_eq!(sim[1].role, Role::Follower);
    assert_eq!(sim[1].commit_index, 1);
    assert_eq!(
        sim[1].log[1].payload,
        EntryPayload::Command(LogRecord::new(0, Command::Set {
            key: "majority".into(),
            value: string("committed"),
        }))
    );

    sim.check_invariants().expect("Invariants must hold");
}

// -----------------------------------------------------------------------------
// 8. Deterministic Chaos Fuzzer (Continuous Randomized Invariant Testing)
// -----------------------------------------------------------------------------

#[test]
fn test_deterministic_chaos_fuzzer() {
    let seeds = [42, 1337, 99999];

    for &seed in &seeds {
        let mut sim = Simulation::new_cluster(&[1, 2, 3]);

        // Start initial election
        sim.start_election(1);
        sim.route_messages();

        let config = ChaosConfig {
            crash_prob: 0.05,
            restart_prob: 0.10,
            partition_prob: 0.08,
            heal_prob: 0.15,
            packet_drop_prob: 0.05,
            packet_delay_prob: 0.10,
            max_packet_delay: Duration::from_millis(30),
        };

        // Run 200 chaos steps with continuous invariant assertions
        let ops = sim
            .run_chaos_test(seed, 200, &config)
            .unwrap_or_else(|err| panic!("Invariant violation on seed {seed}: {err}"));

        println!("Seed {seed}: Chaos test completed successfully ({ops} ops applied).");
        sim.check_invariants().expect("Invariants must hold");
    }
}

// -----------------------------------------------------------------------------
// 9. Exact Deterministic Reproducibility
// -----------------------------------------------------------------------------

#[test]
fn test_deterministic_reproducibility() {
    let seed = 7777;

    let run_sim = || {
        let mut sim = Simulation::new_cluster(&[1, 2, 3]);
        sim.start_election(1);
        sim.route_messages();

        let config = ChaosConfig::default();
        sim.run_chaos_test(seed, 100, &config).unwrap();

        // Capture snapshot state
        let term1 = sim[1].current_term;
        let term2 = sim[2].current_term;
        let term3 = sim[3].current_term;
        let log1_len = sim[1].log.len();
        let log2_len = sim[2].log.len();
        let log3_len = sim[3].log.len();
        let commit1 = sim[1].commit_index;
        let commit2 = sim[2].commit_index;
        let commit3 = sim[3].commit_index;

        (term1, term2, term3, log1_len, log2_len, log3_len, commit1, commit2, commit3)
    };

    let result1 = run_sim();
    let result2 = run_sim();

    // Two independent runs with the same seed must produce the exact identical state!
    assert_eq!(
        result1, result2,
        "Simulations with identical seed must produce bit-for-bit identical state"
    );
}
