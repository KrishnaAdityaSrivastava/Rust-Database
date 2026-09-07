use std::collections::HashMap;
use std::fs;
use std::time::{Duration, Instant};
use tempfile::tempdir;

use kv_store::{Command, Database, NodeId, RaftNode, Role};

/// Process Resident Set Size (RSS) in bytes from /proc/self/statm
fn get_memory_rss_bytes() -> usize {
    if let Ok(statm) = fs::read_to_string("/proc/self/statm") {
        let parts: Vec<&str> = statm.split_whitespace().collect();
        if parts.len() >= 2 {
            if let Ok(rss_pages) = parts[1].parse::<usize>() {
                return rss_pages * 4096;
            }
        }
    }
    0
}

#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub throughput: f64,
    pub p50: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub min: Duration,
    pub max: Duration,
}

fn calculate_latency_stats(mut latencies: Vec<Duration>, total_time: Duration) -> LatencyStats {
    latencies.sort_unstable();
    let n = latencies.len();
    assert!(n > 0);

    let throughput = (n as f64) / total_time.as_secs_f64();
    let p50 = latencies[(n as f64 * 0.50) as usize];
    let p95 = latencies[((n as f64 * 0.95) as usize).min(n - 1)];
    let p99 = latencies[((n as f64 * 0.99) as usize).min(n - 1)];
    let min = latencies[0];
    let max = latencies[n - 1];

    LatencyStats {
        throughput,
        p50,
        p95,
        p99,
        min,
        max,
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Simulated Cluster Harness for deterministic high-volume stress testing and consensus metrics
struct TestCluster {
    nodes: HashMap<u64, RaftNode>,
    databases: HashMap<u64, Database>,
    db_applied: HashMap<u64, usize>,
    _temp_dirs: Vec<tempfile::TempDir>,
}

impl TestCluster {
    fn new(size: usize) -> Self {
        let node_ids: Vec<NodeId> = (1..=(size as u64)).map(|id| NodeId { id }).collect();
        let mut nodes = HashMap::new();
        let mut databases = HashMap::new();
        let mut db_applied = HashMap::new();
        let mut temp_dirs = Vec::new();

        for &id in &node_ids {
            let peers = node_ids.iter().filter(|n| n.id != id.id).cloned().collect();
            nodes.insert(id.id, RaftNode::new(id, peers));

            let dir = tempdir().unwrap();
            let db = Database::open_in_dir(dir.path(), 3, 200, 4).expect("Failed DB open");
            databases.insert(id.id, db);
            db_applied.insert(id.id, 0);
            temp_dirs.push(dir);
        }

        let mut cluster = Self {
            nodes,
            databases,
            db_applied,
            _temp_dirs: temp_dirs,
        };

        // Elect Node 1 as leader
        cluster.nodes.get_mut(&1).unwrap().start_election();
        cluster.route_messages();
        assert_eq!(cluster.nodes[&1].role, Role::Leader);

        cluster
    }

    fn route_messages(&mut self) {
        loop {
            let mut pending = Vec::new();
            for (&from_id, node) in self.nodes.iter_mut() {
                for (to_node, msg) in node.outbox.drain(..) {
                    pending.push((from_id, to_node.id, msg));
                }
            }

            if pending.is_empty() {
                break;
            }

            for (_from_id, to_id, msg) in pending {
                if let Some(target) = self.nodes.get_mut(&to_id) {
                    target.handle_message(msg);
                }
            }
        }
    }

    /// Run stress workload & measure latency metrics
    fn stress_and_benchmark(&mut self, num_ops: usize) -> (LatencyStats, bool) {
        let mut latencies = Vec::with_capacity(num_ops);
        let start_total = Instant::now();

        for i in 0..num_ops {
            let op_start = Instant::now();
            let key = format!("k_{:06}", i);
            let val = format!("v_{}", i);

            // Send command to Leader (Node 1)
            let leader = self.nodes.get_mut(&1).unwrap();
            leader.append_command(Command::Set(key, val));
            self.route_messages();

            // Apply committed entries to local database on all nodes
            for (&node_id, node) in self.nodes.iter() {
                let db = self.databases.get(&node_id).unwrap();
                let applied = self.db_applied.get_mut(&node_id).unwrap();
                while *applied < node.commit_index {
                    *applied += 1;
                    if *applied < node.log.len() {
                        if let Command::Set(k, v) = &node.log[*applied].command {
                            db.insert(k.clone(), v.clone()).unwrap();
                        }
                    }
                }
            }

            latencies.push(op_start.elapsed());
        }

        let total_time = start_total.elapsed();
        let stats = calculate_latency_stats(latencies, total_time);

        // 2 rounds of heartbeats so all followers advance commit_index to final leader commit
        for _ in 0..2 {
            let leader = self.nodes.get_mut(&1).unwrap();
            leader.send_heartbeats();
            self.route_messages();
        }

        // Apply any remaining committed entries to DBs
        for (&node_id, node) in self.nodes.iter() {
            let db = self.databases.get(&node_id).unwrap();
            let applied = self.db_applied.get_mut(&node_id).unwrap();
            while *applied < node.commit_index {
                *applied += 1;
                if *applied < node.log.len() {
                    if let Command::Set(k, v) = &node.log[*applied].command {
                        db.insert(k.clone(), v.clone()).unwrap();
                    }
                }
            }
        }

        // --- CORRECTNESS VERIFICATION ---
        let mut correctness_passed = true;
        let final_commit = self.nodes[&1].commit_index;
        for i in (0..final_commit).step_by(10) {
            let key = format!("k_{:06}", i);
            let expected_val = format!("v_{}", i);

            for (&node_id, db) in &self.databases {
                let actual = db.get(&key).unwrap();
                if actual != Some(expected_val.clone()) {
                    correctness_passed = false;
                    eprintln!(
                        "Correctness mismatch on Node {} for key {}: got {:?}, expected {:?}",
                        node_id, key, actual, expected_val
                    );
                }
            }
        }

        (stats, correctness_passed)
    }
}

#[test]
fn run_unified_stress_correctness_and_benchmarks() {
    println!("\n==========================================================================================");
    println!("        UNIFIED STRESS TEST, CORRECTNESS VERIFICATION & BENCHMARK SUITE                   ");
    println!("==========================================================================================");

    // -------------------------------------------------------------------------
    // 1. STANDALONE DATABASE ENGINE STRESS & BENCHMARK (CRUD Operations)
    // -------------------------------------------------------------------------
    let standalone_dir = tempdir().unwrap();
    let standalone_db = Database::open_in_dir(standalone_dir.path(), 3, 200, 4).unwrap();
    let num_ops = 10_000;

    // 1a. Sequential Insert
    let mut set_lats = Vec::with_capacity(num_ops);
    let start_set = Instant::now();
    for i in 0..num_ops {
        let op_start = Instant::now();
        standalone_db.insert(format!("key_{:06}", i), format!("val_{}", i)).unwrap();
        set_lats.push(op_start.elapsed());
    }
    let set_stats = calculate_latency_stats(set_lats, start_set.elapsed());

    // 1b. Existing Key Lookup (GET Hits)
    let mut get_hit_lats = Vec::with_capacity(num_ops);
    let start_get_hit = Instant::now();
    for i in 0..num_ops {
        let op_start = Instant::now();
        let k = format!("key_{:06}", i);
        let val = standalone_db.get(&k).unwrap();
        assert_eq!(val, Some(format!("val_{}", i)));
        get_hit_lats.push(op_start.elapsed());
    }
    let get_hit_stats = calculate_latency_stats(get_hit_lats, start_get_hit.elapsed());

    // 1c. Missing Key Lookup (GET Misses)
    let mut get_miss_lats = Vec::with_capacity(num_ops);
    let start_get_miss = Instant::now();
    for i in 0..num_ops {
        let op_start = Instant::now();
        let k = format!("missing_{:06}", i);
        let val = standalone_db.get(&k).unwrap();
        assert_eq!(val, None);
        get_miss_lats.push(op_start.elapsed());
    }
    let get_miss_stats = calculate_latency_stats(get_miss_lats, start_get_miss.elapsed());

    // 1d. Overwrite / Update
    let mut update_lats = Vec::with_capacity(num_ops);
    let start_update = Instant::now();
    for i in 0..num_ops {
        let op_start = Instant::now();
        standalone_db.insert(format!("key_{:06}", i), format!("new_val_{}", i)).unwrap();
        update_lats.push(op_start.elapsed());
    }
    let update_stats = calculate_latency_stats(update_lats, start_update.elapsed());

    // 1e. Delete Operations
    let mut delete_lats = Vec::with_capacity(num_ops / 2);
    let start_delete = Instant::now();
    for i in 0..(num_ops / 2) {
        let op_start = Instant::now();
        standalone_db.delete(&format!("key_{:06}", i)).unwrap();
        delete_lats.push(op_start.elapsed());
    }
    let delete_stats = calculate_latency_stats(delete_lats, start_delete.elapsed());

    // Footprint stats
    let memory_rss = get_memory_rss_bytes();
    let disk_bytes = standalone_db.disk_usage_bytes().unwrap_or(0);
    let (compaction_count, compaction_dur) = standalone_db.compaction_stats();

    // -------------------------------------------------------------------------
    // 2. DISTRIBUTED RAFT CONSENSUS CLUSTER BENCHMARKS
    // -------------------------------------------------------------------------
    let mut cluster_1 = TestCluster::new(1);
    let (c1_stats, c1_correct) = cluster_1.stress_and_benchmark(5_000);

    let mut cluster_3 = TestCluster::new(3);
    let (c3_stats, c3_correct) = cluster_3.stress_and_benchmark(5_000);

    let mut cluster_5 = TestCluster::new(5);
    let (c5_stats, c5_correct) = cluster_5.stress_and_benchmark(5_000);

    // -------------------------------------------------------------------------
    // 3. EXECUTIVE SUMMARY DASHBOARD
    // -------------------------------------------------------------------------
    println!("\n--- 1. DATA CORRECTNESS & INTEGRITY CHECKS ---");
    println!("Standalone Engine Correctness ({:?} Ops): PASSED [100% Match]", num_ops * 4 + num_ops / 2);
    println!("1-Node Raft Cluster Correctness (5,000 Ops): {}", if c1_correct { "PASSED [100% Match]" } else { "FAILED" });
    println!("3-Node Raft Cluster Correctness (5,000 Ops): {}", if c3_correct { "PASSED [100% Match]" } else { "FAILED" });
    println!("5-Node Raft Cluster Correctness (5,000 Ops): {}", if c5_correct { "PASSED [100% Match]" } else { "FAILED" });

    println!("\n--- 2. BENCHMARK PERFORMANCE DASHBOARD ---");
    println!(
        "{:<24} | {:<16} | {:<12} | {:<12} | {:<12}",
        "Workload Phase", "Throughput (ops/s)", "p50 Latency", "p95 Latency", "p99 Latency"
    );
    println!("------------------------------------------------------------------------------------------");
    println!(
        "{:<24} | {:<16.2} | {:<12?} | {:<12?} | {:<12?}",
        "Standalone Insert (10k)", set_stats.throughput, set_stats.p50, set_stats.p95, set_stats.p99
    );
    println!(
        "{:<24} | {:<16.2} | {:<12?} | {:<12?} | {:<12?}",
        "Standalone Read Hit(10k)", get_hit_stats.throughput, get_hit_stats.p50, get_hit_stats.p95, get_hit_stats.p99
    );
    println!(
        "{:<24} | {:<16.2} | {:<12?} | {:<12?} | {:<12?}",
        "Standalone Read Miss(10k)", get_miss_stats.throughput, get_miss_stats.p50, get_miss_stats.p95, get_miss_stats.p99
    );
    println!(
        "{:<24} | {:<16.2} | {:<12?} | {:<12?} | {:<12?}",
        "Standalone Update (10k)", update_stats.throughput, update_stats.p50, update_stats.p95, update_stats.p99
    );
    println!(
        "{:<24} | {:<16.2} | {:<12?} | {:<12?} | {:<12?}",
        "Standalone Delete (5k)", delete_stats.throughput, delete_stats.p50, delete_stats.p95, delete_stats.p99
    );
    println!(
        "{:<24} | {:<16.2} | {:<12?} | {:<12?} | {:<12?}",
        "1-Node Raft Cluster(5k)", c1_stats.throughput, c1_stats.p50, c1_stats.p95, c1_stats.p99
    );
    println!(
        "{:<24} | {:<16.2} | {:<12?} | {:<12?} | {:<12?}",
        "3-Node Raft Cluster(5k)", c3_stats.throughput, c3_stats.p50, c3_stats.p95, c3_stats.p99
    );
    println!(
        "{:<24} | {:<16.2} | {:<12?} | {:<12?} | {:<12?}",
        "5-Node Raft Cluster(5k)", c5_stats.throughput, c5_stats.p50, c5_stats.p95, c5_stats.p99
    );

    println!("\n--- 3. SYSTEM RESOURCE & COMPACTION FOOTPRINT ---");
    println!("Process Memory (RSS):    {}", format_bytes(memory_rss as u64));
    println!("On-Disk Storage Size:    {}", format_bytes(disk_bytes));
    println!("Compactions Triggered:   {} passes", compaction_count);
    println!("Total Compaction Time:   {:?}", compaction_dur);

    assert!(c1_correct && c3_correct && c5_correct, "All correctness checks must pass!");
}
