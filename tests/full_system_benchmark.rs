use std::collections::HashMap;
use std::fs;
use std::time::{Duration, Instant};
use tempfile::tempdir;

use kv_store::{Command, Database, NodeId, RaftNode};

/// Reads process Resident Set Size (RSS) in bytes from /proc/self/statm on Linux
fn get_memory_rss_bytes() -> usize {
    if let Ok(statm) = fs::read_to_string("/proc/self/statm") {
        let parts: Vec<&str> = statm.split_whitespace().collect();
        if parts.len() >= 2 {
            if let Ok(rss_pages) = parts[1].parse::<usize>() {
                // Standard Linux page size is 4096 bytes
                return rss_pages * 4096;
            }
        }
    }
    0
}

/// Helper struct for latency percentiles
struct LatencyStats {
    throughput: f64,
    p50: Duration,
    p95: Duration,
    p99: Duration,
    min: Duration,
    max: Duration,
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

// -----------------------------------------------------------------------------
// In-memory Raft Harness for Overhead Comparison
// -----------------------------------------------------------------------------
struct RaftBenchCluster {
    nodes: HashMap<u64, RaftNode>,
}

impl RaftBenchCluster {
    fn new(size: usize) -> Self {
        let node_ids: Vec<NodeId> = (1..=(size as u64)).map(|id| NodeId { id }).collect();
        let mut nodes = HashMap::new();

        for &id in &node_ids {
            let peers = node_ids.iter().filter(|n| n.id != id.id).cloned().collect();
            nodes.insert(id.id, RaftNode::new(id, peers));
        }

        let mut cluster = Self { nodes };
        cluster.nodes.get_mut(&1).unwrap().start_election();
        cluster.route();
        cluster
    }

    fn route(&mut self) {
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

    fn benchmark_replications(&mut self, num_ops: usize) -> LatencyStats {
        let mut latencies = Vec::with_capacity(num_ops);
        let start_total = Instant::now();

        for i in 0..num_ops {
            let op_start = Instant::now();
            let key = format!("k_{:06}", i);
            let val = format!("v_{}", i);

            let leader = self.nodes.get_mut(&1).unwrap();
            leader.append_command(Command::Set(key, val));
            self.route();

            latencies.push(op_start.elapsed());
        }

        let total_time = start_total.elapsed();
        calculate_latency_stats(latencies, total_time)
    }
}

// -----------------------------------------------------------------------------
// MAIN SYSTEM BENCHMARK TEST
// -----------------------------------------------------------------------------
#[test]
fn test_full_system_benchmark() {
    println!("\n==========================================================================");
    println!("             SYSTEM BENCHMARK & METRICS REPORT                            ");
    println!("==========================================================================");

    let dir = tempdir().unwrap();
    let num_ops = 10_000;

    // Database with threshold 200 items per flush, 4 SSTables per level compaction
    let db = Database::open_in_dir(dir.path(), 3, 200, 4).expect("Failed to open DB");

    // -------------------------------------------------------------------------
    // 1. SET WORKLOAD BENCHMARK
    // -------------------------------------------------------------------------
    let mut set_latencies = Vec::with_capacity(num_ops);
    let start_set_total = Instant::now();

    for i in 0..num_ops {
        let op_start = Instant::now();
        let key = format!("user_{:08}", i);
        let val = format!("payload_value_{:08}", i);
        db.insert(key, val).unwrap();
        set_latencies.push(op_start.elapsed());
    }

    let set_total_duration = start_set_total.elapsed();
    let set_stats = calculate_latency_stats(set_latencies, set_total_duration);

    // -------------------------------------------------------------------------
    // 2. GET WORKLOAD BENCHMARK (Existing + Missing Keys)
    // -------------------------------------------------------------------------
    let mut get_latencies = Vec::with_capacity(num_ops);
    let start_get_total = Instant::now();

    for i in 0..num_ops {
        let op_start = Instant::now();
        let key = if i % 2 == 0 {
            format!("user_{:08}", i / 2) // Hit
        } else {
            format!("missing_{:08}", i) // Miss
        };
        let _ = db.get(&key).unwrap();
        get_latencies.push(op_start.elapsed());
    }

    let get_total_duration = start_get_total.elapsed();
    let get_stats = calculate_latency_stats(get_latencies, get_total_duration);

    // -------------------------------------------------------------------------
    // 3. MEMORY & DISK USAGE & COMPACTION METRICS
    // -------------------------------------------------------------------------
    let memory_rss = get_memory_rss_bytes();
    let disk_usage = db.disk_usage_bytes().unwrap_or(0);
    let (compaction_count, compaction_dur) = db.compaction_stats();

    // -------------------------------------------------------------------------
    // 4. RAFT CONSENSUS REPLICATION OVERHEAD
    // -------------------------------------------------------------------------
    let raft_ops = 2_000;
    let mut raft_cluster_1 = RaftBenchCluster::new(1);
    let raft_1_stats = raft_cluster_1.benchmark_replications(raft_ops);

    let mut raft_cluster_3 = RaftBenchCluster::new(3);
    let raft_3_stats = raft_cluster_3.benchmark_replications(raft_ops);

    let mut raft_cluster_5 = RaftBenchCluster::new(5);
    let raft_5_stats = raft_cluster_5.benchmark_replications(raft_ops);

    // -------------------------------------------------------------------------
    // DISPLAY PRINTED REPORT
    // -------------------------------------------------------------------------
    println!("\n--- 1. SET WORKLOAD METRICS ---");
    println!("Total SET Operations:    {}", num_ops);
    println!("SET Throughput:          {:.2} ops/sec", set_stats.throughput);
    println!("SET Latency Min:         {:?}", set_stats.min);
    println!("SET Latency p50 (Median): {:?}", set_stats.p50);
    println!("SET Latency p95:         {:?}", set_stats.p95);
    println!("SET Latency p99:         {:?}", set_stats.p99);
    println!("SET Latency Max:         {:?}", set_stats.max);

    println!("\n--- 2. GET WORKLOAD METRICS ---");
    println!("Total GET Operations:    {}", num_ops);
    println!("GET Throughput:          {:.2} ops/sec", get_stats.throughput);
    println!("GET Latency Min:         {:?}", get_stats.min);
    println!("GET Latency p50 (Median): {:?}", get_stats.p50);
    println!("GET Latency p95:         {:?}", get_stats.p95);
    println!("GET Latency p99:         {:?}", get_stats.p99);
    println!("GET Latency Max:         {:?}", get_stats.max);

    println!("\n--- 3. SYSTEM RESOURCE FOOTPRINT ---");
    println!("Process Memory (RSS):    {}", format_bytes(memory_rss as u64));
    println!("On-Disk Storage Size:    {}", format_bytes(disk_usage));

    println!("\n--- 4. COMPACTION OVERHEAD ---");
    println!("Compactions Triggered:   {}", compaction_count);
    println!("Total Compaction Time:   {:?}", compaction_dur);
    let set_duration_secs = set_total_duration.as_secs_f64();
    let compaction_overhead_pct = if set_duration_secs > 0.0 {
        (compaction_dur.as_secs_f64() / set_duration_secs) * 100.0
    } else {
        0.0
    };
    println!("Compaction Overhead %:   {:.2}% of total SET duration", compaction_overhead_pct);

    println!("\n--- 5. SINGLE-NODE VS MULTI-NODE RAFT CONSENSUS METRICS ---");
    println!("Standalone Engine:       {:.2} ops/sec | p50: {:?}", set_stats.throughput, set_stats.p50);
    println!("1-Node Raft Cluster:     {:.2} ops/sec | p50: {:?}", raft_1_stats.throughput, raft_1_stats.p50);
    println!("3-Node Raft Cluster:     {:.2} ops/sec | p50: {:?}", raft_3_stats.throughput, raft_3_stats.p50);
    println!("5-Node Raft Cluster:     {:.2} ops/sec | p50: {:?}", raft_5_stats.throughput, raft_5_stats.p50);

    let overhead_1node_pct = (1.0 - (raft_1_stats.throughput / set_stats.throughput)) * 100.0;
    let overhead_3node_pct = (1.0 - (raft_3_stats.throughput / set_stats.throughput)) * 100.0;
    let overhead_5node_pct = (1.0 - (raft_5_stats.throughput / set_stats.throughput)) * 100.0;
    println!("1-Node Raft Overhead:    {:.2}% throughput penalty", overhead_1node_pct.max(0.0));
    println!("3-Node Raft Overhead:    {:.2}% throughput penalty", overhead_3node_pct.max(0.0));
    println!("5-Node Raft Overhead:    {:.2}% throughput penalty", overhead_5node_pct.max(0.0));

    println!("\n==========================================================================");
    println!("             END BENCHMARK REPORT                                         ");
    println!("==========================================================================\n");
}
