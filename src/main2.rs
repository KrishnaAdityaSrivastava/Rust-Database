pub mod raft;

use std::collections::HashMap;

use raft::{Command, Message, Network, NodeId, RaftNode};

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

    let local_id_val: u64 = args[1].parse().expect("Invalid local_id");
    let local_id = NodeId { id: local_id_val };
    let local_addr = args[2].clone();

    let mut peers_map = HashMap::new();
    let mut peer_ids = Vec::new();

    for arg in args.iter().skip(3) {
        let parts: Vec<&str> = arg.splitn(2, ':').collect();
        if parts.len() == 2 {
            let pid = NodeId {
                id: parts[0].parse().expect("Invalid peer_id"),
            };
            peers_map.insert(pid, parts[1].to_string());
            peer_ids.push(pid);
        }
    }

    let node = RaftNode::new(local_id, peer_ids);
    let network = Network::new(local_id, local_addr, peers_map);

    let tx = network.start(node).await;

    // For educational simplicity, we will manually trigger an election on Node 1
    // after allowing connections to establish, then send a client command.
    if local_id.id == 1 {
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            // Wait for connections
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            println!("--- Node 1 initiating election ---");
            let _ = tx_clone.send(Message::Timeout);

            // Wait for election to complete
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            println!("--- Node 1 sending client command ---");
            let _ = tx_clone.send(Message::ClientCommand(Command::Set(
                "tcp_test".to_string(),
                "success".to_string(),
            )));
        });
    }

    // Keep the main process alive until Ctrl+C
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");
    println!("Shutting down...");
}
