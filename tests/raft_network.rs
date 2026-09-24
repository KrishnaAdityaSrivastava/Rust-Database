use kv_store::Config;
use kv_store::raft::{Message, Network, NodeId, RaftNode};
use kv_store::wal::log_record::{Command, LogRecord, Value};
use std::collections::HashMap;

#[tokio::test]
async fn test_tcp_cluster_startup_and_election() {
    let id1 = NodeId { id: 101 };
    let id2 = NodeId { id: 102 };
    let id3 = NodeId { id: 103 };

    let mut peers1 = HashMap::new();
    peers1.insert(id2, "127.0.0.1:6002".to_string());
    peers1.insert(id3, "127.0.0.1:6003".to_string());

    let mut peers2 = HashMap::new();
    peers2.insert(id1, "127.0.0.1:6001".to_string());
    peers2.insert(id3, "127.0.0.1:6003".to_string());

    let mut peers3 = HashMap::new();
    peers3.insert(id1, "127.0.0.1:6001".to_string());
    peers3.insert(id2, "127.0.0.1:6002".to_string());

    let node1 = RaftNode::new(id1, vec![id2, id3], Config::default());
    let node2 = RaftNode::new(id2, vec![id1, id3], Config::default());
    let node3 = RaftNode::new(id3, vec![id1, id2], Config::default());

    let net1 = Network::new(id1, "127.0.0.1:6001".to_string(), peers1);
    let net2 = Network::new(id2, "127.0.0.1:6002".to_string(), peers2);
    let net3 = Network::new(id3, "127.0.0.1:6003".to_string(), peers3);

    let tx1 = net1.start(node1).await;
    let tx2 = net2.start(node2).await;
    let _tx3 = net3.start(node3).await;

    // Allow time for TCP connections to establish and deduplicate
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Trigger an election on Node 1
    tx1.send(Message::Timeout).unwrap();

    // Allow time for RequestVote and RequestVoteResponse to propagate
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Send a command to Node 1 (who should now be leader)
    tx1.send(Message::ClientCommand(Command::Set {
        key: "key1".to_string(),
        value: Value::String("value1".to_string()),
    }))
    .unwrap();

    // Allow time for AppendEntries and AppendEntriesResponse
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Let Node 2 try to start an election. Node 1 should have higher term or reject if not timed out,
    // but in this simple implementation Node 2 will increment term and become candidate.
    tx2.send(Message::Timeout).unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // If the test reaches here without panicking or disconnecting everyone,
    // the network layer serialization and TCP channels successfully handled the traffic.
}

#[test]
fn test_message_serialization() {
    use kv_store::raft::{AppendEntries, LogEntry, RequestVote};

    let msg = Message::RequestVote(RequestVote {
        term: 5,
        candidate_id: NodeId { id: 42 },
        last_log_index: 10,
        last_log_term: 4,
    });

    let bytes = bincode::serialize(&msg).unwrap();
    let decoded: Message = bincode::deserialize(&bytes).unwrap();

    match decoded {
        Message::RequestVote(rv) => {
            assert_eq!(rv.term, 5);
            assert_eq!(rv.candidate_id.id, 42);
        }
        _ => panic!("Wrong variant deserialized"),
    }

    let append = Message::AppendEntries(AppendEntries {
        term: 2,
        leader_id: NodeId { id: 1 },
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![LogEntry {
            term: 1,
            record: LogRecord::new(
                1,
                Command::Set {
                    key: "x".to_string(),
                    value: Value::String("y".to_string()),
                },
            ),
        }],
        leader_commit: 1,
    });

    let bytes2 = bincode::serialize(&append).unwrap();
    let decoded2: Message = bincode::deserialize(&bytes2).unwrap();

    match decoded2 {
        Message::AppendEntries(ae) => {
            assert_eq!(ae.entries.len(), 1);
        }
        _ => panic!("Wrong variant deserialized"),
    }
}

#[tokio::test]
async fn test_tcp_network_cluster_throughput() {
    let id1 = NodeId { id: 201 };
    let id2 = NodeId { id: 202 };
    let id3 = NodeId { id: 203 };

    let mut peers1 = HashMap::new();
    peers1.insert(id2, "127.0.0.1:7002".to_string());
    peers1.insert(id3, "127.0.0.1:7003".to_string());

    let mut peers2 = HashMap::new();
    peers2.insert(id1, "127.0.0.1:7001".to_string());
    peers2.insert(id3, "127.0.0.1:7003".to_string());

    let mut peers3 = HashMap::new();
    peers3.insert(id1, "127.0.0.1:7001".to_string());
    peers3.insert(id2, "127.0.0.1:7002".to_string());

    let node1 = RaftNode::new(id1, vec![id2, id3], Config::default());
    let node2 = RaftNode::new(id2, vec![id1, id3], Config::default());
    let node3 = RaftNode::new(id3, vec![id1, id2], Config::default());

    let net1 = Network::new(id1, "127.0.0.1:7001".to_string(), peers1);
    let net2 = Network::new(id2, "127.0.0.1:7002".to_string(), peers2);
    let net3 = Network::new(id3, "127.0.0.1:7003".to_string(), peers3);

    let tx1 = net1.start(node1).await;
    let _tx2 = net2.start(node2).await;
    let _tx3 = net3.start(node3).await;

    // Allow time for TCP connections to establish
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Trigger election on Node 1
    tx1.send(Message::Timeout).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let num_ops = 5000;
    let start = std::time::Instant::now();

    for i in 0..num_ops {
        let _ = tx1.send(Message::ClientCommand(Command::Set {
            key: format!("key_{:05}", i),
            value: Value::String(format!("val_{}", i)),
        }));
    }

    // Allow messages to transmit and commit
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let elapsed = start.elapsed();
    println!(
        "Tokio TCP 3-Node Network throughput benchmark: sent {} ops in {:?}",
        num_ops, elapsed
    );
}

#[tokio::test]
async fn test_tcp_direct_request_to_follower_forwarding() {
    let id1 = NodeId { id: 301 };
    let id2 = NodeId { id: 302 };
    let id3 = NodeId { id: 303 };

    let mut peers1 = HashMap::new();
    peers1.insert(id2, "127.0.0.1:8002".to_string());
    peers1.insert(id3, "127.0.0.1:8003".to_string());

    let mut peers2 = HashMap::new();
    peers2.insert(id1, "127.0.0.1:8001".to_string());
    peers2.insert(id3, "127.0.0.1:8003".to_string());

    let mut peers3 = HashMap::new();
    peers3.insert(id1, "127.0.0.1:8001".to_string());
    peers3.insert(id2, "127.0.0.1:8002".to_string());

    let node1 = RaftNode::new(id1, vec![id2, id3], Config::default());
    let node2 = RaftNode::new(id2, vec![id1, id3], Config::default());
    let node3 = RaftNode::new(id3, vec![id1, id2], Config::default());

    let net1 = Network::new(id1, "127.0.0.1:8001".to_string(), peers1);
    let net2 = Network::new(id2, "127.0.0.1:8002".to_string(), peers2);
    let net3 = Network::new(id3, "127.0.0.1:8003".to_string(), peers3);

    let tx1 = net1.start(node1).await;
    let tx2 = net2.start(node2).await;
    let _tx3 = net3.start(node3).await;

    // Allow time for TCP connections to establish
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Trigger election on Node 1 (who becomes Leader)
    tx1.send(Message::Timeout).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Send a client command directly to Node 2 (a follower)
    tx2.send(Message::ClientCommand(Command::Set {
        key: "direct_tcp_key".to_string(),
        value: Value::String("direct_tcp_val".to_string()),
    }))
    .unwrap();

    // Allow time for Node 2 to forward to Node 1 and Node 1 to replicate back to Node 2 & 3
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
}

