use std::cmp::Ordering;
use std::time::{Duration, Instant};

use crate::raft::node::NodeId;
use crate::wal::log_record::Command;

/// Types of deterministic faults that can be injected into a cluster.
#[derive(Debug, Clone, PartialEq)]
pub enum Fault {
    /// Crash a node immediately (stops processing, drops messages, clears outbox).
    CrashNode(NodeId),
    /// Restart a crashed node (reboots as Follower, retaining persisted state, resetting volatile timers).
    RestartNode(NodeId),
    /// Pause a node (simulating GC pause or IO freeze). Node stops ticking, incoming events deferred.
    PauseNode(NodeId),
    /// Resume a paused node.
    ResumeNode(NodeId),
    /// Block a directed link from `from` to `to`.
    BlockLink { from: NodeId, to: NodeId },
    /// Unblock a directed link from `from` to `to`.
    UnblockLink { from: NodeId, to: NodeId },
    /// Bi-directional network partition isolating group A from group B.
    Partition { group_a: Vec<u64>, group_b: Vec<u64> },
    /// Heal all network partitions and unblock all blocked links.
    HealAllPartitions,
    /// Isolate a single node from all other nodes in the cluster.
    IsolateNode(NodeId),
    /// Reconnect an isolated node to all peers.
    ReconnectNode(NodeId),
    /// Drop the next N messages on the directed link from `from` to `to`.
    DropPackets { from: NodeId, to: NodeId, count: usize },
    /// Add network latency/delay to all messages on link `(from, to)`.
    DelayLink { from: NodeId, to: NodeId, delay: Duration },
    /// Remove latency/delay from link `(from, to)`.
    ClearLinkDelay { from: NodeId, to: NodeId },
    /// Duplicate the next N messages on link `(from, to)` with a specified delay.
    DuplicatePackets { from: NodeId, to: NodeId, count: usize, delay: Duration },
    /// Force trigger an election on `node_id`.
    TriggerElection(NodeId),
    /// Submit a client command to a specific node.
    SubmitCommand { node_id: NodeId, command: Command },
}

/// A fault scheduled to execute at a specific simulated instant.
#[derive(Debug, Clone)]
pub struct ScheduledFault {
    pub trigger_at: Instant,
    pub fault: Fault,
    pub sequence: u64,
}

impl PartialEq for ScheduledFault {
    fn eq(&self, other: &Self) -> bool {
        self.trigger_at == other.trigger_at && self.sequence == other.sequence
    }
}

impl Eq for ScheduledFault {}

impl PartialOrd for ScheduledFault {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Min-heap ordering: earlier trigger_at comes first.
impl Ord for ScheduledFault {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .trigger_at
            .cmp(&self.trigger_at)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

/// Directed link-level fault policies (packet drops, delays, duplications).
#[derive(Debug, Clone, Default)]
pub struct LinkFaultPolicy {
    /// Number of upcoming packets to drop.
    pub drop_remaining: usize,
    /// Fixed delay added to packets on this link.
    pub delay: Option<Duration>,
    /// Number of upcoming packets to duplicate.
    pub duplicate_remaining: usize,
    /// Delay before duplicate packet is delivered.
    pub duplicate_delay: Duration,
}

/// Configuration for pseudo-random deterministic chaos testing.
#[derive(Debug, Clone)]
pub struct ChaosConfig {
    pub crash_prob: f64,
    pub restart_prob: f64,
    pub partition_prob: f64,
    pub heal_prob: f64,
    pub packet_drop_prob: f64,
    pub packet_delay_prob: f64,
    pub max_packet_delay: Duration,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            crash_prob: 0.05,
            restart_prob: 0.10,
            partition_prob: 0.08,
            heal_prob: 0.15,
            packet_drop_prob: 0.05,
            packet_delay_prob: 0.10,
            max_packet_delay: Duration::from_millis(50),
        }
    }
}

/// Linear generator PRNG (SplitMix64) for 100% deterministic, dependency-free random fault generation.
#[derive(Debug, Clone)]
pub struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64)
    }

    pub fn gen_range(&mut self, min: u64, max: u64) -> u64 {
        if min >= max {
            return min;
        }
        min + (self.next_u64() % (max - min + 1))
    }

    pub fn choose<'a, T>(&mut self, slice: &'a [T]) -> Option<&'a T> {
        if slice.is_empty() {
            None
        } else {
            let idx = (self.next_u64() as usize) % slice.len();
            Some(&slice[idx])
        }
    }
}

/// Violations of distributed consensus safety invariants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvariantViolation {
    /// Multiple leaders elected in the same term.
    ElectionSafetyViolation {
        term: u64,
        leaders: Vec<u64>,
    },
    /// Log entries at the same index have different terms.
    LogMatchingViolation {
        node_a: u64,
        node_b: u64,
        index: usize,
        term_a: u64,
        term_b: u64,
    },
    /// Two nodes committed different commands at the same index.
    StateSafetyViolation {
        index: usize,
        node_a: u64,
        node_b: u64,
        entry_a: String,
        entry_b: String,
    },
}

impl std::fmt::Display for InvariantViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InvariantViolation::ElectionSafetyViolation { term, leaders } => {
                write!(
                    f,
                    "Election Safety Violated: Multiple leaders in term {term}: {leaders:?}"
                )
            }
            InvariantViolation::LogMatchingViolation {
                node_a,
                node_b,
                index,
                term_a,
                term_b,
            } => {
                write!(
                    f,
                    "Log Matching Violated: At index {index}, Node {node_a} has term {term_a} but Node {node_b} has term {term_b}"
                )
            }
            InvariantViolation::StateSafetyViolation {
                index,
                node_a,
                node_b,
                entry_a,
                entry_b,
            } => {
                write!(
                    f,
                    "State Machine Safety Violated: Conflicting committed entries at index {index}: Node {node_a} ({entry_a}) vs Node {node_b} ({entry_b})"
                )
            }
        }
    }
}

impl std::error::Error for InvariantViolation {}
