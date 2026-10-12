use std::fs;
use std::time::{Duration, Instant};
use tempfile::tempdir;

use kv_store::testing::TestCluster;
use kv_store::wal::log_record::{Command, Value};
use kv_store::{Config, Database};

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

fn benchmark_cluster(size: usize, num_ops: usize) -> LatencyStats {
    let mut cluster = TestCluster::new(size);
    let mut latencies = Vec::with_capacity(num_ops);
    let start_total = Instant::now();

    for i in 0..num_ops {
        let op_start = Instant::now();
        let key = format!("k_{:06}", i);
        let val = format!("v_{}", i);

        cluster.sim[1].append_command(Command::Set {
            key,
            value: Value::String(val),
        });
        cluster.sim.route_messages();
        cluster.apply_committed();

        latencies.push(op_start.elapsed());
    }

    // Two heartbeat rounds to propagate commit index
    for _ in 0..2 {
        cluster.sim[1].send_heartbeats();
        cluster.sim.route_messages();
    }
    cluster.apply_committed();

    let total_time = start_total.elapsed();
    calculate_latency_stats(latencies, total_time)
}

fn main() {
    println!("\n==========================================================================================");
    println!("             KV-STORE LOCAL PERFORMANCE EVALUATION & BENCHMARK RUNNER                      ");
    println!("==========================================================================================");

    // -------------------------------------------------------------------------
    // 1. STANDALONE DATABASE ENGINE BENCHMARKS (10,000 Ops each)
    // -------------------------------------------------------------------------
    println!("\n[1/2] Running Standalone LSM Storage Engine Benchmarks...");
    let standalone_dir = tempdir().expect("Failed to create tempdir");
    let standalone_db =
        Database::open_in_dir(standalone_dir.path(), Config::new(3, 10_000, 10_000)).expect("Failed DB open");
    let num_ops = 10_000;

    // 1a. Sequential Inserts
    let mut set_lats = Vec::with_capacity(num_ops);
    let start_set = Instant::now();
    for i in 0..num_ops {
        let op_start = Instant::now();
        standalone_db
            .insert(format!("key_{:06}", i), Value::String(format!("val_{}", i)))
            .unwrap();
        set_lats.push(op_start.elapsed());
    }
    let set_stats = calculate_latency_stats(set_lats, start_set.elapsed());

    // 1b. Existing Key Lookups (GET Hits)
    let mut get_hit_lats = Vec::with_capacity(num_ops);
    let start_get_hit = Instant::now();
    for i in 0..num_ops {
        let op_start = Instant::now();
        let k = format!("key_{:06}", i);
        let _ = standalone_db.get(&k).unwrap();
        get_hit_lats.push(op_start.elapsed());
    }
    let get_hit_stats = calculate_latency_stats(get_hit_lats, start_get_hit.elapsed());

    // 1c. Missing Key Lookups (GET Misses)
    let mut get_miss_lats = Vec::with_capacity(num_ops);
    let start_get_miss = Instant::now();
    for i in 0..num_ops {
        let op_start = Instant::now();
        let k = format!("missing_{:06}", i);
        let _ = standalone_db.get(&k).unwrap();
        get_miss_lats.push(op_start.elapsed());
    }
    let get_miss_stats = calculate_latency_stats(get_miss_lats, start_get_miss.elapsed());

    // 1d. Overwrite / Update
    let mut update_lats = Vec::with_capacity(num_ops);
    let start_update = Instant::now();
    for i in 0..num_ops {
        let op_start = Instant::now();
        let k = format!("key_{:06}", i);
        standalone_db
            .insert(k, Value::String(format!("new_val_{}", i)))
            .unwrap();
        update_lats.push(op_start.elapsed());
    }
    let update_stats = calculate_latency_stats(update_lats, start_update.elapsed());

    // 1e. Delete Operations
    let mut delete_lats = Vec::with_capacity(num_ops / 2);
    let start_delete = Instant::now();
    for i in 0..(num_ops / 2) {
        let op_start = Instant::now();
        standalone_db.delete(format!("key_{:06}", i)).unwrap();
        delete_lats.push(op_start.elapsed());
    }
    let delete_stats = calculate_latency_stats(delete_lats, start_delete.elapsed());

    let memory_rss = get_memory_rss_bytes();
    let disk_bytes = standalone_db.disk_usage_bytes().unwrap_or(0);
    let (compaction_count, compaction_dur) = standalone_db.compaction_stats();

    // -------------------------------------------------------------------------
    // 2. DISTRIBUTED RAFT CONSENSUS CLUSTER BENCHMARKS
    // -------------------------------------------------------------------------
    println!("[2/2] Running Distributed Raft Consensus Replication Benchmarks...");
    let c1_stats = benchmark_cluster(1, 2_000);
    let c3_stats = benchmark_cluster(3, 2_000);
    let c5_stats = benchmark_cluster(5, 2_000);

    // -------------------------------------------------------------------------
    // 3. EXECUTIVE PERFORMANCE DASHBOARD
    // -------------------------------------------------------------------------
    println!("\n==========================================================================================");
    println!("                           PERFORMANCE EVALUATION DASHBOARD                               ");
    println!("==========================================================================================");
    println!(
        "{:<26} | {:<16} | {:<10} | {:<10} | {:<10} | {:<10}",
        "Workload Phase", "Throughput (ops/s)", "p50 Lat", "p95 Lat", "p99 Lat", "Max Lat"
    );
    println!("------------------------------------------------------------------------------------------");
    println!(
        "{:<26} | {:<16.2} | {:<10?} | {:<10?} | {:<10?} | {:<10?}",
        "Standalone Insert (10k)", set_stats.throughput, set_stats.p50, set_stats.p95, set_stats.p99, set_stats.max
    );
    println!(
        "{:<26} | {:<16.2} | {:<10?} | {:<10?} | {:<10?} | {:<10?}",
        "Standalone Read Hit (10k)", get_hit_stats.throughput, get_hit_stats.p50, get_hit_stats.p95, get_hit_stats.p99, get_hit_stats.max
    );
    println!(
        "{:<26} | {:<16.2} | {:<10?} | {:<10?} | {:<10?} | {:<10?}",
        "Standalone Read Miss (10k)", get_miss_stats.throughput, get_miss_stats.p50, get_miss_stats.p95, get_miss_stats.p99, get_miss_stats.max
    );
    println!(
        "{:<26} | {:<16.2} | {:<10?} | {:<10?} | {:<10?} | {:<10?}",
        "Standalone Update (10k)", update_stats.throughput, update_stats.p50, update_stats.p95, update_stats.p99, update_stats.max
    );
    println!(
        "{:<26} | {:<16.2} | {:<10?} | {:<10?} | {:<10?} | {:<10?}",
        "Standalone Delete (5k)", delete_stats.throughput, delete_stats.p50, delete_stats.p95, delete_stats.p99, delete_stats.max
    );
    println!(
        "{:<26} | {:<16.2} | {:<10?} | {:<10?} | {:<10?} | {:<10?}",
        "1-Node Raft Cluster (2k)", c1_stats.throughput, c1_stats.p50, c1_stats.p95, c1_stats.p99, c1_stats.max
    );
    println!(
        "{:<26} | {:<16.2} | {:<10?} | {:<10?} | {:<10?} | {:<10?}",
        "3-Node Raft Cluster (2k)", c3_stats.throughput, c3_stats.p50, c3_stats.p95, c3_stats.p99, c3_stats.max
    );
    println!(
        "{:<26} | {:<16.2} | {:<10?} | {:<10?} | {:<10?} | {:<10?}",
        "5-Node Raft Cluster (2k)", c5_stats.throughput, c5_stats.p50, c5_stats.p95, c5_stats.p99, c5_stats.max
    );

    println!("\n--- SYSTEM RESOURCE & FOOTPRINT ---");
    println!("Process Memory (RSS):    {}", format_bytes(memory_rss as u64));
    println!("On-Disk Storage Size:    {}", format_bytes(disk_bytes));
    println!("Compactions Triggered:   {} passes", compaction_count);
    println!("Total Compaction Time:   {:?}", compaction_dur);
    println!("==========================================================================================\n");
}
