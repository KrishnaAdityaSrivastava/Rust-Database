use std::fs;
use std::time::{Duration, Instant};

use tempfile::tempdir;

use kv_store::testing::TestCluster;
use kv_store::wal::log_record::{Command, Value};
use kv_store::{Config, Database};

// -----------------------------------------------------------------------------
// SYSTEM METRICS
// -----------------------------------------------------------------------------

/// Process Resident Set Size (RSS) in bytes from /proc/self/statm.
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
// LATENCY STATISTICS
// -----------------------------------------------------------------------------

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
    assert!(n > 0, "Cannot calculate statistics for zero operations");

    let throughput = n as f64 / total_time.as_secs_f64();

    let percentile = |p: f64| -> Duration { latencies[((n as f64 * p) as usize).min(n - 1)] };

    LatencyStats {
        throughput,
        p50: percentile(0.50),
        p95: percentile(0.95),
        p99: percentile(0.99),
        min: latencies[0],
        max: latencies[n - 1],
    }
}

// -----------------------------------------------------------------------------
// EXPERIMENT CONFIGURATION
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Experiment {
    name: &'static str,
    config: Config,
}

/// Add or remove experiments here.
///
/// Config::new(sstable_levels, data_threshold, sstable_threshold)
fn experiments() -> Vec<Experiment> {
    vec![
        Experiment {
            name: "baseline_3_levels",
            config: Config::new(3, 100, 100),
        },
        Experiment {
            name: "levels_4",
            config: Config::new(4, 100, 100),
        },
        Experiment {
            name: "levels_5",
            config: Config::new(5, 100, 100),
        },
        Experiment {
            name: "data_threshold_50",
            config: Config::new(4, 50, 100),
        },
        Experiment {
            name: "data_threshold_200",
            config: Config::new(4, 200, 100),
        },
        Experiment {
            name: "sstable_threshold_50",
            config: Config::new(4, 100, 50),
        },
        Experiment {
            name: "sstable_threshold_200",
            config: Config::new(4, 100, 200),
        },
    ]
}

// -----------------------------------------------------------------------------
// EXPERIMENT RESULTS
// -----------------------------------------------------------------------------

struct ExperimentResult {
    name: &'static str,
    config: Config,

    insert: LatencyStats,
    read_hit: LatencyStats,
    read_miss: LatencyStats,
    update: LatencyStats,
    delete: LatencyStats,

    memory_bytes: usize,
    disk_bytes: u64,
    compaction_count: usize,
    compaction_time: Duration,
}

// -----------------------------------------------------------------------------
// STANDALONE LSM BENCHMARK
// -----------------------------------------------------------------------------

fn run_standalone(experiment: &Experiment) -> ExperimentResult {
    // Every experiment gets a clean database.
    let standalone_dir = tempdir().expect("Failed to create tempdir");

    let db = Database::open_in_dir(standalone_dir.path(), experiment.config.clone())
        .expect("Failed DB open");

    let num_ops = 10_000;

    println!("  Running inserts...");
    let mut set_lats = Vec::with_capacity(num_ops);
    let start_set = Instant::now();

    for i in 0..num_ops {
        let op_start = Instant::now();

        db.insert(format!("key_{:06}", i), Value::String(format!("val_{}", i)))
            .unwrap();

        set_lats.push(op_start.elapsed());
    }

    let set_stats = calculate_latency_stats(set_lats, start_set.elapsed());

    println!("  Running read hits...");
    let mut get_hit_lats = Vec::with_capacity(num_ops);
    let start_get_hit = Instant::now();

    for i in 0..num_ops {
        let op_start = Instant::now();
        let key = format!("key_{:06}", i);

        let _ = db.get(&key).unwrap();

        get_hit_lats.push(op_start.elapsed());
    }

    let get_hit_stats = calculate_latency_stats(get_hit_lats, start_get_hit.elapsed());

    println!("  Running read misses...");
    let mut get_miss_lats = Vec::with_capacity(num_ops);
    let start_get_miss = Instant::now();

    for i in 0..num_ops {
        let op_start = Instant::now();
        let key = format!("missing_{:06}", i);

        let _ = db.get(&key).unwrap();

        get_miss_lats.push(op_start.elapsed());
    }

    let get_miss_stats = calculate_latency_stats(get_miss_lats, start_get_miss.elapsed());

    println!("  Running updates...");
    let mut update_lats = Vec::with_capacity(num_ops);
    let start_update = Instant::now();

    for i in 0..num_ops {
        let op_start = Instant::now();
        let key = format!("key_{:06}", i);

        db.insert(key, Value::String(format!("new_val_{}", i)))
            .unwrap();

        update_lats.push(op_start.elapsed());
    }

    let update_stats = calculate_latency_stats(update_lats, start_update.elapsed());

    println!("  Running deletes...");
    let delete_ops = num_ops / 2;
    let mut delete_lats = Vec::with_capacity(delete_ops);
    let start_delete = Instant::now();

    for i in 0..delete_ops {
        let op_start = Instant::now();

        db.delete(format!("key_{:06}", i)).unwrap();

        delete_lats.push(op_start.elapsed());
    }

    let delete_stats = calculate_latency_stats(delete_lats, start_delete.elapsed());

    // Collect system and storage metrics after the workload.
    let memory_bytes = get_memory_rss_bytes();
    let disk_bytes = db.disk_usage_bytes().unwrap_or(0);
    let (compaction_count, compaction_time) = db.compaction_stats();

    ExperimentResult {
        name: experiment.name,
        config: experiment.config.clone(),

        insert: set_stats,
        read_hit: get_hit_stats,
        read_miss: get_miss_stats,
        update: update_stats,
        delete: delete_stats,

        memory_bytes,
        disk_bytes,
        compaction_count: compaction_count.try_into().unwrap(),
        compaction_time,
    }
}

// -----------------------------------------------------------------------------
// RESULT DISPLAY
// -----------------------------------------------------------------------------

fn print_workload(name: &str, stats: &LatencyStats) {
    println!(
        "{:<25} | {:>12.0} | {:>10?} | {:>10?} | {:>10?} | {:>10?}",
        name, stats.throughput, stats.p50, stats.p95, stats.p99, stats.max,
    );
}

fn print_experiment_result(result: &ExperimentResult) {
    println!("\n{}", "=".repeat(100));
    println!("EXPERIMENT: {}", result.name);
    println!("CONFIG: {:?}", result.config);
    println!("{}", "-".repeat(100));

    println!(
        "{:<25} | {:>12} | {:>10} | {:>10} | {:>10} | {:>10}",
        "Workload", "Ops/s", "p50", "p95", "p99", "Max",
    );

    println!("{}", "-".repeat(100));

    print_workload("Insert (10k)", &result.insert);
    print_workload("Read hit (10k)", &result.read_hit);
    print_workload("Read miss (10k)", &result.read_miss);
    print_workload("Update (10k)", &result.update);
    print_workload("Delete (5k)", &result.delete);

    println!("\nSystem and storage metrics:");
    println!(
        "Process RSS:          {}",
        format_bytes(result.memory_bytes as u64)
    );
    println!("On-disk storage:      {}", format_bytes(result.disk_bytes));
    println!("Compaction passes:    {}", result.compaction_count);
    println!("Compaction time:      {:?}", result.compaction_time);
}

// -----------------------------------------------------------------------------
// COMPARISON TABLE
// -----------------------------------------------------------------------------

fn print_comparison(results: &[ExperimentResult]) {
    println!("\n\n{}", "=".repeat(125));
    println!("                    CONFIGURATION COMPARISON");
    println!("{}", "=".repeat(125));

    println!(
        "{:<25} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10}",
        "Experiment", "Insert/s", "Hit/s", "Miss/s", "Update/s", "Delete/s", "Disk KB", "Compacts",
    );

    println!("{}", "-".repeat(125));

    for r in results {
        println!(
            "{:<25} | {:>10.0} | {:>10.0} | {:>10.0} | {:>10.0} | {:>10.0} | {:>10.2} | {:>10}",
            r.name,
            r.insert.throughput,
            r.read_hit.throughput,
            r.read_miss.throughput,
            r.update.throughput,
            r.delete.throughput,
            r.disk_bytes as f64 / 1024.0,
            r.compaction_count,
        );
    }

    println!("{}", "=".repeat(125));

    // Compare each experiment against the first experiment.
    if let Some(baseline) = results.first() {
        println!(
            "\nThroughput change relative to baseline: {}",
            baseline.name
        );

        println!(
            "{:<25} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10}",
            "Experiment", "Insert %", "Hit %", "Miss %", "Update %", "Delete %",
        );

        println!("{}", "-".repeat(90));

        for r in results {
            let change = |current: f64, base: f64| -> f64 {
                if base == 0.0 {
                    0.0
                } else {
                    (current / base - 1.0) * 100.0
                }
            };

            println!(
                "{:<25} | {:>9.2}% | {:>9.2}% | {:>9.2}% | {:>9.2}% | {:>9.2}%",
                r.name,
                change(r.insert.throughput, baseline.insert.throughput),
                change(r.read_hit.throughput, baseline.read_hit.throughput),
                change(r.read_miss.throughput, baseline.read_miss.throughput),
                change(r.update.throughput, baseline.update.throughput),
                change(r.delete.throughput, baseline.delete.throughput),
            );
        }
    }
}

// -----------------------------------------------------------------------------
// RAFT BENCHMARK
// -----------------------------------------------------------------------------

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

    // Two heartbeat rounds to propagate commit index.
    for _ in 0..2 {
        cluster.sim[1].send_heartbeats();
        cluster.sim.route_messages();
    }

    cluster.apply_committed();

    let total_time = start_total.elapsed();
    calculate_latency_stats(latencies, total_time)
}

// -----------------------------------------------------------------------------
// MAIN
// -----------------------------------------------------------------------------

fn main() {
    println!("\n{}", "=".repeat(100));
    println!("             KV-STORE CONFIGURATION EXPERIMENT RUNNER");
    println!("{}", "=".repeat(100));

    let experiments = experiments();
    let mut results = Vec::with_capacity(experiments.len());

    // Run all standalone configurations.
    for (index, experiment) in experiments.iter().enumerate() {
        println!(
            "\n[{}/{}] Running experiment: {}",
            index + 1,
            experiments.len(),
            experiment.name,
        );

        println!("Configuration: {:?}", experiment.config);

        let result = run_standalone(experiment);
        print_experiment_result(&result);
        results.push(result);
    }

    // Compare all standalone runs.
    print_comparison(&results);

    // Raft results are independent of the above configurations unless
    // TestCluster is modified to accept and use Config.
    println!("\n\n{}", "=".repeat(100));
    println!("             RAFT CLUSTER BENCHMARKS (DEFAULT CONFIGURATION)");
    println!("{}", "=".repeat(100));

    for size in [1, 3, 5] {
        println!("\nRunning {}-node Raft cluster...", size);

        let stats = benchmark_cluster(size, 2_000);

        println!(
            "{:>2}-node cluster | Throughput: {:>10.2} ops/s | p50: {:>10?} | p95: {:>10?} | p99: {:>10?} | max: {:>10?}",
            size, stats.throughput, stats.p50, stats.p95, stats.p99, stats.max,
        );
    }

    println!("\nBenchmark suite completed.");
}
