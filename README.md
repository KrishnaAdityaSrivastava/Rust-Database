# Rust-Database

**Rust-Database** is a Rust-based key-value storage engine evolving into a small distributed database.

It is built from scratch to explore the internals of persistent storage and distributed systems, including Write-Ahead Logging, LSM trees, SSTables, compaction, TCP networking, and Raft consensus.

The project focuses on understanding system behavior through implementation, benchmarking, and Linux `perf` profiling rather than relying on existing database frameworks.

---

## Highlights

* Persistent key-value storage using a WAL and immutable SSTables
* LSM-tree storage with indexed reads and leveled compaction
* TCP-based database communication using Tokio
* Raft consensus implemented from scratch
* Leader election, replicated logs, quorum commitment, and follower recovery
* Benchmark-driven optimization using Linux `perf`

---

## Performance at a Glance

The current benchmark evaluates the storage engine across standalone and Raft-backed workloads.

| Workload                   |          Throughput | p50 Latency | p95 Latency | p99 Latency |
| -------------------------- | ------------------: | ----------: | ----------: | ----------: |
| Standalone Insert (10k)    |    **97,082 ops/s** |    7.337 µs |    9.546 µs |   17.434 µs |
| Standalone Read Hit (10k)  |   **265,966 ops/s** |    3.696 µs |    3.785 µs |    3.993 µs |
| Standalone Read Miss (10k) | **1,678,406 ops/s** |      552 ns |      564 ns |      577 ns |
| Standalone Update (10k)    |    **81,853 ops/s** |    7.353 µs |    7.608 µs |   16.001 µs |
| Standalone Delete (5k)     |    **92,509 ops/s** |    6.369 µs |    7.575 µs |   14.653 µs |
| 1-Node Raft (5k)           |   **106,307 ops/s** |    7.770 µs |    7.972 µs |   14.712 µs |
| 3-Node Raft (5k)           |    **33,580 ops/s** |   23.386 µs |   30.318 µs |  103.867 µs |
| 5-Node Raft (5k)           |    **20,864 ops/s** |   38.668 µs |   45.001 µs |  117.940 µs |

The latest benchmark completed in **0.79 seconds** with a process RSS of **6.46 MB**. During the workload, 31 compaction passes were performed with a combined compaction time of **72.027 ms**.

> Benchmarks are intended to track implementation and optimization progress, not to claim production-level performance.

---

## Correctness

The unified test suite validates both the standalone engine and replicated configurations.

| Configuration | Operations | Result              |
| ------------- | ---------: | ------------------- |
| Standalone    |     20,000 | **100% data match** |
| 1-Node Raft   |     10,000 | **100% data match** |
| 3-Node Raft   |     10,000 | **100% data match** |
| 5-Node Raft   |     10,000 | **100% data match** |

```text
test result: ok
1 passed; 0 failed
```

---

# Architecture

```text
                         Client
                           │
                           ▼
                    ┌─────────────┐
                    │     Raft    │
                    │  (optional) │
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │   Database  │
                    └──────┬──────┘
                           │
                 ┌─────────┴─────────┐
                 ▼                   ▼
              MemTable               WAL
                 │
                 ▼
              SSTables
                 │
                 ▼
           Leveled Compaction
```

The storage engine can run independently or underneath the Raft replication layer.

---

# Storage Engine

Rust-Database uses an LSM-style architecture.

Writes are applied to an in-memory structure and recorded in the WAL. Once the configured threshold is reached, the in-memory data is flushed into an immutable SSTable.

```text
Write
  │
  ├──────────────► WAL
  │
  ▼
MemTable
  │
  ▼
SSTable
  │
  ▼
Compaction
```

SSTables contain sorted records and an index used for efficient point lookups. Reads therefore do not require scanning the entire file.

The WAL provides recovery by replaying persisted operations during startup.

---

# SSTables & Compaction

SSTables are immutable once written. Sequential access uses a 256 KiB `BufReader` to reduce small-read and syscall overhead.

Compaction performs a streaming merge of sorted SSTables:

```text
SSTables
   │
   ▼
SequentialReader
   │
   ▼
BinaryHeap
   │
   ▼
StreamingWriter
   │
   ▼
New SSTable
```

The compaction path does not reload existing SSTables. Already-open SSTable objects are used directly as sequential readers.

---

# Raft

The distributed layer implements Raft from scratch over Tokio TCP.

The implementation covers leader election, terms and voting, replicated logs, `RequestVote` and `AppendEntries` RPCs, quorum-based commitment, log conflict resolution, follower catch-up, and persistent consensus state.

The same storage engine can therefore be used as a standalone database or as a replicated Raft node.

The current test suite validates operation across 1, 3, and 5-node configurations, with all tested workloads producing a 100% data match.

---

# Performance Engineering

Performance work has followed a simple loop:

```text
Benchmark → Profile → Identify bottleneck → Optimize → Benchmark again
```

Linux `perf` was used to inspect CPU, syscall, allocation, and filesystem overhead.

Several early bottlenecks have already been removed.

SSTable sequential reads were buffered to reduce repeated kernel reads. Per-record `stream_position()` calls were removed because the SSTable already stores its entry count. SSTables now retain their file handles instead of reopening files for every point lookup. Compaction also avoids reopening a newly-created SSTable just to reload an index that already exists in memory.

These changes shifted the profile away from the original syscall-heavy read path and toward the actual work performed during compaction.

---

# Profiling

Representative profiling commands:

```bash
sudo perf stat \
    -e cycles,instructions,context-switches,cpu-migrations,page-faults,minor-faults,major-faults \
    ./target/release/deps/unified_suite --nocapture
```

For call-graph profiling:

```bash
sudo perf record -g --call-graph dwarf \
    ./target/release/deps/unified_suite --nocapture
```

An earlier profiling run recorded approximately **33.4 billion cycles** and **29.1 billion instructions** over a 13.47-second workload.

Profiling is used to determine whether a slowdown originates from application code, memory allocation, serialization, filesystem operations, or the Linux kernel rather than optimizing based purely on intuition.

---

# Current Optimization

The major I/O inefficiencies identified during earlier profiling have largely been addressed:

```text
Repeated SSTable opens      → fixed
Small sequential reads      → buffered
Per-record stream_position  → removed
Redundant SSTable reopen    → removed
WAL recovery small reads    → buffered
Unnecessary WAL end seek    → removed
```

The current focus is the compaction path, particularly record allocation and copying during `read_record()`, serialization in `write_record()`, and the overhead of maintaining the `BinaryHeap` merge structure.

---

# Installation

## Requirements

Rust stable and Linux are currently required. Linux `perf` is used for performance profiling.

Clone the repository:

```bash
git clone https://github.com/KrishnaAdityaSrivastava/Rust-Database.git
cd Rust-Database
```

Build in release mode:

```bash
cargo build --release
```

Run the test suite:

```bash
cargo test --release
```

Run the unified benchmark:

```bash
cargo test --test unified_suite --release -- --nocapture
```

---

# Project Structure

```text
src/
├── database/
├── wal/
├── sstable/
│   ├── format
│   ├── index
│   ├── reader
│   └── writer
├── compaction/
└── raft/

tests/
└── unified_suite.rs

Cargo.toml
Cargo.lock
```

---

# Design Goal

The project is primarily an exploration of what happens underneath a database API.

It combines storage-engine design, operating-system I/O, concurrency, networking, and distributed consensus in one system:

```text
Key-Value API
      │
      ▼
Storage Engine
      │
      ├── WAL
      ├── MemTable
      ├── SSTables
      └── Compaction
      │
      ▼
Linux I/O

          +

      Raft
       │
       ▼
     TCP
       │
       ▼
     Tokio
```

Rather than treating performance as a single throughput number, the project uses profiling to understand where CPU time, memory, syscalls, and I/O are actually being spent.

---

# Limitations

This is an experimental systems project rather than a production database. Durability semantics, failure handling, compaction, networking, and benchmarking are still being refined.

The current multi-node benchmarks run several Raft nodes locally. They validate the replication and networking architecture, but do not represent the behavior of nodes communicating across a real network.

---

# License

MIT License.
