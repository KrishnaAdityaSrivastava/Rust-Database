use std::collections::HashMap;
use std::path::PathBuf;

use kv_store::raft::{Network, NodeId, RaftNode};
use kv_store::Config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    let mut local_id_val: Option<u64> = None;
    let mut local_addr: Option<String> = None;
    let mut peer_args: Vec<String> = Vec::new();
    let mut data_dir: Option<PathBuf> = None;

    let mut verbose = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--log" | "-v" | "--verbose" => {
                verbose = true;
            }
            "--id" => {
                i += 1;
                if i < args.len() {
                    local_id_val = Some(args[i].parse()?);
                }
            }
            "--addr" => {
                i += 1;
                if i < args.len() {
                    local_addr = Some(args[i].clone());
                }
            }
            "--peers" => {
                i += 1;
                if i < args.len() {
                    peer_args.extend(args[i].split(',').map(|s| s.to_string()));
                }
            }
            "--dir" | "--data-dir" => {
                i += 1;
                if i < args.len() {
                    data_dir = Some(PathBuf::from(&args[i]));
                }
            }
            _ => {
                if local_id_val.is_none() {
                    if let Ok(id) = args[i].parse::<u64>() {
                        local_id_val = Some(id);
                        i += 1;
                        continue;
                    }
                }
                if local_addr.is_none() && local_id_val.is_some() {
                    local_addr = Some(args[i].clone());
                    i += 1;
                    continue;
                }
                peer_args.push(args[i].clone());
            }
        }
        i += 1;
    }

    if verbose {
        kv_store::raft::enable_logging(true);
    }

    if local_id_val.is_none() || local_addr.is_none() {
        eprintln!("Usage:");
        eprintln!("  cargo run --bin node -- <id> <addr> [<peer_id>:<peer_addr> ...]");
        eprintln!("  cargo run --bin node -- --id <id> --addr <addr> --peers <id:addr,id:addr> [--dir <path>]");
        eprintln!("\nExample:");
        eprintln!("  cargo run --bin node -- 1 127.0.0.1:6001 2:127.0.0.1:6002 3:127.0.0.1:6003");
        std::process::exit(1);
    }

    let id_num = local_id_val.unwrap();
    let addr = local_addr.unwrap();
    let local_id = NodeId { id: id_num };

    let mut peers_map = HashMap::new();
    let mut peer_ids = Vec::new();

    for peer_str in peer_args {
        let parts: Vec<&str> = peer_str.splitn(2, ':').collect();
        if parts.len() != 2 {
            continue;
        }
        if let Ok(pid_val) = parts[0].parse::<u64>() {
            let pid = NodeId { id: pid_val };
            peers_map.insert(pid, parts[1].to_string());
            peer_ids.push(pid);
        }
    }

    let dir = data_dir.unwrap_or_else(|| {
        std::env::temp_dir().join(format!("kv_store_node_{}", id_num))
    });

    println!("============================================================");
    println!(" Starting Raft Node {}", local_id.id);
    println!(" Address:   {}", addr);
    println!(" Data Dir:  {}", dir.display());
    println!(" Peers:     {:?}", peers_map);
    println!(" Logging:   {}", if verbose || std::env::var("RAFT_LOG").is_ok() { "ENABLED" } else { "DISABLED (use --log or RAFT_LOG=1 to enable)" });
    println!("============================================================");

    let node = RaftNode::new_in_dir(&dir, local_id, peer_ids, Config::default());
    let network = Network::new(local_id, addr, peers_map);

    let _tx = network.start(node).await;

    println!("Node {} is active and listening for peers/clients.", local_id.id);
    println!("Press Ctrl+C to shut down cleanly.");

    tokio::signal::ctrl_c().await?;

    println!("\nNode {} shutting down gracefully.", local_id.id);
    Ok(())
}
