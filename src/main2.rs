use std::collections::HashMap;

use kv_store::raft::{Message, Network, NodeId, RaftNode};
use kv_store::{Command, wal::log_record::Value};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        eprintln!(
            "Usage: {} <local_id> <local_addr> [<peer_id>:<peer_addr> ...]",
            args[0]
        );

        std::process::exit(1);
    }

    let local_id_val: u64 = args[1]
        .parse()
        .expect("Invalid local_id");

    let local_id = NodeId {
        id: local_id_val,
    };

    let local_addr = args[2].clone();

    let mut peers_map = HashMap::new();
    let mut peer_ids = Vec::new();

    for arg in args.iter().skip(3) {
        let parts: Vec<&str> = arg.splitn(2, ':').collect();

        if parts.len() != 2 {
            continue;
        }

        let pid = NodeId {
            id: parts[0]
                .parse()
                .expect("Invalid peer_id"),
        };

        peers_map.insert(pid, parts[1].to_string());
        peer_ids.push(pid);
    }

    let node = RaftNode::new(local_id, peer_ids);
    let network = Network::new(local_id, local_addr, peers_map);

    let tx = network.start(node).await;

    // Manually trigger an election on Node 1 for testing.
    if local_id.id == 1 {
        let tx_clone = tx.clone();

        tokio::spawn(async move {
            // Allow connections to establish.
            tokio::time::sleep(
                std::time::Duration::from_secs(2)
            )
            .await;

            println!("--- Node 1 initiating election ---");

            let _ = tx_clone.send(Message::Timeout);

            // Allow the election to complete.
            tokio::time::sleep(
                std::time::Duration::from_secs(2)
            )
            .await;

            println!("--- Node 1 sending client command ---");

            let _ = tx_clone.send(
                Message::ClientCommand(
                    Command::Set { key: "tcp_test".to_string(), value: Value::String("success".to_string()) }
                ),
            );
        });
    }

    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");

    println!("Shutting down...");
}