<div align="center">

# Rust-Database

**A key-value storage engine built from scratch in Rust with LSM-based storage and Raft replication.**

**Rust · LSM Trees · WAL · SSTables · Compaction · Tokio · TCP · Raft · Linux `perf`**

</div>

---

## Overview

Rust-Database is a from-scratch key-value database implemented in Rust, combining a persistent LSM-based storage engine with a Raft-based replication layer.

The project focuses on storage systems, networking, concurrency, distributed consensus, and performance engineering rather than application-level database usage.

### Core Components

* **WAL** — persistent write-ahead logging and crash recovery
* **MemTable** — in-memory write buffer with configurable flush thresholds
* **SSTables** — immutable sorted files with indexed point lookups
* **Compaction** — leveled, streaming SSTable merge
* **TCP Networking** — asynchronous communication using Tokio
* **Raft** — leader election, log replication, quorum commitment, and recovery
* **Performance Profiling** — Linux `perf` for CPU, syscall, I/O, and allocation analysis

---

## Architecture

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
             Compaction
```

The storage engine can run standalone or as the replicated state machine of a Raft node.

---

## Storage Engine

The database follows an LSM-style architecture:

```text
Write
  │
  ├──────────► WAL
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

Writes are recorded in the WAL and applied to the MemTable. When the MemTable reaches its configured threshold, it is flushed into an immutable SSTable.

SSTables contain sorted records and an index for efficient point lookups. Compaction performs a streaming merge of sorted SSTables using sequential readers and a heap-based merge structure.

The storage layer also supports WAL replay for recovery and concurrent database access.

---

## Raft Replication

The database can be extended into a replicated cluster using Raft implemented from scratch over Tokio TCP.

Implemented protocol behavior includes:

* Leader election and term management
* `RequestVote` and `AppendEntries`
* Log replication
* Quorum-based commitment
* Log conflict resolution
* Follower catch-up
* Persistent consensus state
* Leader failure and re-election
* Follower restart and recovery
* Network partitions and healing
* Message loss and retry
* Message reordering
* Duplicate message handling
* Stale leader isolation

```text
             ┌──────────┐
             │  Leader  │
             └────┬─────┘
                  │
          ┌───────┴───────┐
          ▼               ▼
     ┌─────────┐     ┌─────────┐
     │Follower │     │Follower │
     └─────────┘     └─────────┘
```

---

## Testing

The repository includes an automated four-phase test suite:

```bash
./run_all_tests.sh
```

The suite covers:

| Area                 | Coverage                                                       |
| -------------------- | -------------------------------------------------------------- |
| LSM correctness      | CRUD, WAL recovery, flush/compaction, concurrency              |
| Raft fault tolerance | Elections, failures, partitions, conflicts, message faults     |
| TCP networking       | Serialization, cluster startup, election, network behavior     |
| System stress        | Standalone + 1/3/5-node workloads, correctness and performance |

Current test status:

```text
LSM correctness              4/4   PASS
Raft fault tolerance        13/13  PASS
TCP networking                3/3  PASS
Unified system suite          1/1  PASS
──────────────────────────────────────
All automated suites          PASS
```

The unified stress suite additionally verifies end-to-end data consistency across standalone and multi-node Raft configurations.

---

## Performance Engineering

Performance optimization is driven by profiling rather than benchmark numbers alone.

```text
Benchmark → perf → Identify Hotspot → Optimize → Re-test
```

Linux `perf` was used to investigate CPU and I/O hotspots in the storage and compaction paths.

Profiling-driven improvements include:

* Buffered sequential SSTable reads
* Removal of unnecessary file-position queries
* Reuse of open SSTable file handles
* Elimination of redundant SSTable reopening after compaction
* Buffered WAL recovery
* Removal of unnecessary WAL end-of-file seeking
* Streaming compaction without reloading complete SSTables

Example profiling:

```bash
sudo perf record -g --call-graph dwarf \
    ./target/release/deps/unified_suite --nocapture

sudo perf report
```

---

## Project Structure

```text
src/
├── database/
├── wal/
├── lsm/
│   └── sstable/
│       ├── format/
│       ├── index/
│       ├── reader/
│       └── writer/
├── compaction/
└── raft/

tests/
├── correctness.rs
├── raft_fault_tolerance.rs
├── raft_network.rs
└── unified_suite.rs

run_all_tests.sh
Cargo.toml
Cargo.lock
```

---

## Build & Test

Requirements:

* Rust stable
* Linux
* Cargo
* `perf` for profiling

```bash
git clone https://github.com/KrishnaAdityaSrivastava/Rust-Database.git
cd Rust-Database

cargo build --release
./run_all_tests.sh
```

Individual test suites:

```bash
cargo test --test correctness

cargo test --test raft_fault_tolerance

cargo test --test raft_network

cargo test --test unified_suite --release -- --nocapture
```

---

## Limitations

This is an experimental systems project rather than a production database.

* Raft snapshotting and log compaction are not yet implemented.
* Multi-node tests currently run locally rather than across independent machines.
* Real-world distributed network behavior has not yet been benchmarked.
* Durability and storage semantics are still being refined.

---

## License

MIT License.
