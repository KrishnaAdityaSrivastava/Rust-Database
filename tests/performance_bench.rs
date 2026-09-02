use kv_store::Database;
use tempfile::tempdir;
use std::time::Instant;

fn create_test_db(dir: &std::path::Path, threshold: usize) -> Database {
    // 3 levels, threshold entries before flush, threshold sstables before compaction
    Database::open_in_dir(dir, 3, threshold, 4).expect("Failed to create database")
}

fn benchmark_size(size: usize, threshold: usize) {
    let dir = tempdir().unwrap();
    let db = create_test_db(dir.path(), threshold);

    println!("--------------------------------------------------");
    println!("Benchmarking with Size: {}, Threshold: {}", size, threshold);
    
    // 1. Sequential Insert
    let start = Instant::now();
    for i in 0..size {
        let key = format!("seq_key_{:08}", i);
        let val = format!("val_{}", i);
        db.insert(key, val).unwrap();
    }
    let duration = start.elapsed();
    let ops_per_sec = (size as f64) / duration.as_secs_f64();
    println!("Sequential Insert: {:.2} ops/sec, Latency: {:?}", ops_per_sec, duration / size as u32);

    // 2. Random Read (In-memory and SSTables)
    let start = Instant::now();
    for i in (0..size).step_by(2) {
        let key = format!("seq_key_{:08}", i);
        let _ = db.get(&key).unwrap();
    }
    let read_ops = size / 2;
    let duration = start.elapsed();
    let ops_per_sec = (read_ops as f64) / duration.as_secs_f64();
    println!("Existing Key Read: {:.2} ops/sec, Latency: {:?}", ops_per_sec, duration / read_ops as u32);

    // 3. Missing Key Lookup
    let start = Instant::now();
    for i in 0..read_ops {
        let key = format!("missing_{:08}", i);
        let _ = db.get(&key).unwrap();
    }
    let duration = start.elapsed();
    let ops_per_sec = (read_ops as f64) / duration.as_secs_f64();
    println!("Missing Key Read:  {:.2} ops/sec, Latency: {:?}", ops_per_sec, duration / read_ops as u32);

    // 4. Overwrite/Update
    let start = Instant::now();
    for i in 0..read_ops {
        let key = format!("seq_key_{:08}", i);
        let val = format!("new_val_{}", i);
        db.insert(key, val).unwrap();
    }
    let duration = start.elapsed();
    let ops_per_sec = (read_ops as f64) / duration.as_secs_f64();
    println!("Overwrite/Update:  {:.2} ops/sec, Latency: {:?}", ops_per_sec, duration / read_ops as u32);

    // 5. Delete
    let start = Instant::now();
    for i in 0..read_ops {
        let key = format!("seq_key_{:08}", i);
        db.delete(&key).unwrap();
    }
    let duration = start.elapsed();
    let ops_per_sec = (read_ops as f64) / duration.as_secs_f64();
    println!("Delete Operations: {:.2} ops/sec, Latency: {:?}", ops_per_sec, duration / read_ops as u32);
}

#[test]
fn run_benchmarks() {
    // Run benchmarks with different sizes and threshold settings
    
    // Small dataset
    benchmark_size(1_000, 100);
    
    // Medium dataset
    benchmark_size(10_000, 1_000);
    
    // Large dataset
    benchmark_size(50_000, 5_000);
    
    // If you want even larger, you can add 100_000. It might take a few seconds.
    benchmark_size(100_000, 10_000);
}
