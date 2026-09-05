use std::collections::HashMap;
use std::time::Instant;
use kv_store::raft::{Command, NodeId, RaftNode, Role};

struct BenchmarkCluster {
    nodes: HashMap<u64, RaftNode>,
}

impl BenchmarkCluster {
    fn new(size: usize) -> Self {
        let node_ids: Vec<NodeId> = (1..=(size as u64)).map(|id| NodeId { id }).collect();
        let mut nodes = HashMap::new();

        for &id in &node_ids {
            let peers = node_ids.iter().filter(|n| n.id != id.id).cloned().collect();
            nodes.insert(id.id, RaftNode::new(id, peers));
        }

        let mut cluster = Self { nodes };
        
        // Elect Node 1 as leader
        cluster.nodes.get_mut(&1).unwrap().start_election();
        cluster.route_messages();
        assert_eq!(cluster.nodes[&1].role, Role::Leader);

        cluster
    }

    fn route_messages(&mut self) {
        loop {
            let mut pending_messages = Vec::new();
            for (&from_id, node) in self.nodes.iter_mut() {
                for (to_node, msg) in node.outbox.drain(..) {
                    pending_messages.push((from_id, to_node.id, msg));
                }
            }

            if pending_messages.is_empty() {
                break;
            }

            for (_from_id, to_id, msg) in pending_messages {
                if let Some(target_node) = self.nodes.get_mut(&to_id) {
                    target_node.handle_message(msg);
                }
            }
        }
    }

    fn benchmark_replications(&mut self, num_commands: usize) {
        let initial_commit = self.nodes[&1].commit_index;
        let start = Instant::now();

        for i in 0..num_commands {
            let key = format!("bench_key_{:06}", i);
            let val = format!("val_{}", i);
            let leader = self.nodes.get_mut(&1).unwrap();
            leader.append_command(Command::Set(key, val));
            self.route_messages();
        }

        let duration = start.elapsed();
        let leader_commit = self.nodes[&1].commit_index;
        assert_eq!(leader_commit - initial_commit, num_commands);

        let ops_per_sec = (num_commands as f64) / duration.as_secs_f64();
        let avg_latency = duration / (num_commands as u32);

        println!(
            "Cluster Size: {} nodes | Operations: {} | Throughput: {:.2} ops/sec | Avg Latency: {:?}",
            self.nodes.len(),
            num_commands,
            ops_per_sec,
            avg_latency
        );
    }
}

#[test]
fn run_raft_benchmarks() {
    println!("\n==================================================");
    println!("          RAFT REPLICATION BENCHMARKS             ");
    println!("==================================================");

    // 3-node cluster benchmark
    let mut cluster_3 = BenchmarkCluster::new(3);
    cluster_3.benchmark_replications(1_000);
    cluster_3.benchmark_replications(10_000);

    // 5-node cluster benchmark
    let mut cluster_5 = BenchmarkCluster::new(5);
    cluster_5.benchmark_replications(1_000);
    cluster_5.benchmark_replications(10_000);

    println!("==================================================\n");
}
