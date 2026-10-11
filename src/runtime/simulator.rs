use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::ops::{Index, IndexMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::database::Config;
use crate::raft::message::Message;
use crate::raft::node::{NodeId, RaftNode, Role};
use super::fault::{
    ChaosConfig, DeterministicRng, Fault, InvariantViolation, LinkFaultPolicy, ScheduledFault,
};
use super::runtime::{Clock, Transport};

/// An event in the simulated network.
#[derive(Debug, Clone)]
pub struct Event {
    pub deliver_at: Instant,
    pub sequence: u64,
    pub from: NodeId,
    pub to: NodeId,
    pub message: Message,
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.deliver_at == other.deliver_at && self.sequence == other.sequence
    }
}

impl Eq for Event {}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

// Min-heap ordering: earlier deliver_at first, then lower sequence first.
impl Ord for Event {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .deliver_at
            .cmp(&self.deliver_at)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

pub struct SimulatedTransport {
    events: Arc<Mutex<BinaryHeap<Event>>>,
    clock: Arc<Mutex<Clock>>,
    sequence: Arc<AtomicU64>,
}

impl SimulatedTransport {
    pub fn new(
        events: Arc<Mutex<BinaryHeap<Event>>>,
        clock: Arc<Mutex<Clock>>,
        sequence: Arc<AtomicU64>,
    ) -> Self {
        Self {
            events,
            clock,
            sequence,
        }
    }
}

impl Transport for SimulatedTransport {
    fn send(&self, from: NodeId, to: NodeId, message: Message) {
        let deliver_at = self.clock.lock().unwrap().now();
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);

        self.events.lock().unwrap().push(Event {
            deliver_at,
            sequence: seq,
            from,
            to,
            message,
        });
    }
}

pub struct Simulation {
    pub clock: Arc<Mutex<Clock>>,
    pub events: Arc<Mutex<BinaryHeap<Event>>>,
    pub sequence_counter: Arc<AtomicU64>,
    pub nodes: BTreeMap<NodeId, RaftNode>,

    // Nodes that are completely unavailable (crashed/offline).
    pub disabled_nodes: BTreeSet<NodeId>,

    // Nodes that are paused (e.g. GC pause or IO freeze).
    pub paused_nodes: BTreeSet<NodeId>,

    // Buffered incoming events for paused nodes.
    pub paused_events: BTreeMap<NodeId, Vec<Event>>,

    // Directed network links that are blocked.
    pub blocked_links: BTreeSet<(NodeId, NodeId)>,

    // Link fault policies (drops, delays, duplications).
    pub link_policies: BTreeMap<(NodeId, NodeId), LinkFaultPolicy>,

    // Scheduled deterministic faults timeline.
    pub scheduled_faults: BinaryHeap<ScheduledFault>,
    pub fault_sequence_counter: u64,
}

impl Simulation {
    pub fn new(nodes: impl IntoIterator<Item = (NodeId, RaftNode)>) -> Self {
        let clock = Arc::new(Mutex::new(Clock::new()));
        let events = Arc::new(Mutex::new(BinaryHeap::new()));
        let sequence_counter = Arc::new(AtomicU64::new(0));

        Self {
            clock,
            events,
            sequence_counter,
            nodes: nodes.into_iter().collect(),
            disabled_nodes: BTreeSet::new(),
            paused_nodes: BTreeSet::new(),
            paused_events: BTreeMap::new(),
            blocked_links: BTreeSet::new(),
            link_policies: BTreeMap::new(),
            scheduled_faults: BinaryHeap::new(),
            fault_sequence_counter: 0,
        }
    }

    pub fn new_cluster(ids: &[u64]) -> Self {
        let all_node_ids: Vec<NodeId> = ids.iter().map(|&id| NodeId { id }).collect();

        let mut nodes = BTreeMap::new();

        for &id in ids {
            let peers: Vec<NodeId> = all_node_ids
                .iter()
                .copied()
                .filter(|node| node.id != id)
                .collect();

            let node = RaftNode::new(NodeId { id }, peers, Config::default());
            nodes.insert(NodeId { id }, node);
        }

        let mut sim = Self::new(nodes);
        let now = sim.now();
        for (&id, node) in sim.nodes.iter_mut() {
            let timeout = Duration::from_millis(150 + id.id * 50);
            node.set_election_timeout(timeout, now);
        }

        sim
    }

    pub fn now(&self) -> Instant {
        self.clock.lock().unwrap().now()
    }

    // -------------------------------------------------------------------------
    // Node Management & Faults
    // -------------------------------------------------------------------------

    pub fn disable_node(&mut self, id: impl Into<NodeId>) {
        let node_id = id.into();
        self.disabled_nodes.insert(node_id);

        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.outbox.clear();
        }
    }

    pub fn enable_node(&mut self, id: impl Into<NodeId>) {
        let node_id = id.into();
        self.disabled_nodes.remove(&node_id);
    }

    pub fn crash_node(&mut self, id: impl Into<NodeId>) {
        self.disable_node(id);
    }

    pub fn restart_node(&mut self, id: impl Into<NodeId>) {
        let node_id = id.into();
        self.disabled_nodes.remove(&node_id);
        self.paused_nodes.remove(&node_id);
        self.paused_events.remove(&node_id);

        let now = self.now();
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.restart(now);
        }
    }

    pub fn pause_node(&mut self, id: impl Into<NodeId>) {
        let node_id = id.into();
        self.paused_nodes.insert(node_id);
    }

    pub fn resume_node(&mut self, id: impl Into<NodeId>) {
        let node_id = id.into();
        self.paused_nodes.remove(&node_id);

        if let Some(deferred) = self.paused_events.remove(&node_id) {
            let now = self.now();
            let mut events = self.events.lock().unwrap();
            for mut event in deferred {
                event.deliver_at = now;
                events.push(event);
            }
        }

        self.flush_node_outbox(node_id);
    }

    // -------------------------------------------------------------------------
    // Network Management & Partitions
    // -------------------------------------------------------------------------

    pub fn block_link(&mut self, from: impl Into<NodeId>, to: impl Into<NodeId>) {
        self.blocked_links.insert((from.into(), to.into()));
    }

    pub fn unblock_link(&mut self, from: impl Into<NodeId>, to: impl Into<NodeId>) {
        self.blocked_links.remove(&(from.into(), to.into()));
    }

    pub fn partition(&mut self, group_a: &[u64], group_b: &[u64]) {
        for &a in group_a {
            for &b in group_b {
                self.block_link(a, b);
                self.block_link(b, a);
            }
        }
    }

    pub fn heal_all_partitions(&mut self) {
        self.blocked_links.clear();
    }

    pub fn isolate_node(&mut self, id: impl Into<NodeId>) {
        let node_id = id.into();
        let peers: Vec<NodeId> = self
            .nodes
            .keys()
            .copied()
            .filter(|&p| p != node_id)
            .collect();

        for peer in peers {
            self.block_link(node_id, peer);
            self.block_link(peer, node_id);
        }
    }

    pub fn reconnect_node(&mut self, id: impl Into<NodeId>) {
        let node_id = id.into();
        let peers: Vec<NodeId> = self
            .nodes
            .keys()
            .copied()
            .filter(|&p| p != node_id)
            .collect();

        for peer in peers {
            self.unblock_link(node_id, peer);
            self.unblock_link(peer, node_id);
        }
    }

    // -------------------------------------------------------------------------
    // Packet-Level Fault Policies
    // -------------------------------------------------------------------------

    pub fn drop_packets(
        &mut self,
        from: impl Into<NodeId>,
        to: impl Into<NodeId>,
        count: usize,
    ) {
        let key = (from.into(), to.into());
        self.link_policies.entry(key).or_default().drop_remaining += count;
    }

    pub fn delay_link(
        &mut self,
        from: impl Into<NodeId>,
        to: impl Into<NodeId>,
        delay: Duration,
    ) {
        let key = (from.into(), to.into());
        self.link_policies.entry(key).or_default().delay = Some(delay);
    }

    pub fn clear_link_delay(
        &mut self,
        from: impl Into<NodeId>,
        to: impl Into<NodeId>,
    ) {
        let key = (from.into(), to.into());
        if let Some(policy) = self.link_policies.get_mut(&key) {
            policy.delay = None;
        }
    }

    pub fn duplicate_packets(
        &mut self,
        from: impl Into<NodeId>,
        to: impl Into<NodeId>,
        count: usize,
        delay: Duration,
    ) {
        let key = (from.into(), to.into());
        let policy = self.link_policies.entry(key).or_default();
        policy.duplicate_remaining += count;
        policy.duplicate_delay = delay;
    }

    // -------------------------------------------------------------------------
    // Scheduled Faults
    // -------------------------------------------------------------------------

    pub fn schedule_fault(&mut self, trigger_at: Instant, fault: Fault) {
        self.fault_sequence_counter += 1;
        self.scheduled_faults.push(ScheduledFault {
            trigger_at,
            fault,
            sequence: self.fault_sequence_counter,
        });
    }

    pub fn schedule_fault_after(&mut self, delay: Duration, fault: Fault) {
        let trigger_at = self.now() + delay;
        self.schedule_fault(trigger_at, fault);
    }

    pub fn apply_fault(&mut self, fault: Fault) {
        match fault {
            Fault::CrashNode(id) => self.disable_node(id),
            Fault::RestartNode(id) => self.restart_node(id),
            Fault::PauseNode(id) => self.pause_node(id),
            Fault::ResumeNode(id) => self.resume_node(id),
            Fault::BlockLink { from, to } => self.block_link(from, to),
            Fault::UnblockLink { from, to } => self.unblock_link(from, to),
            Fault::Partition { group_a, group_b } => self.partition(&group_a, &group_b),
            Fault::HealAllPartitions => self.heal_all_partitions(),
            Fault::IsolateNode(id) => self.isolate_node(id),
            Fault::ReconnectNode(id) => self.reconnect_node(id),
            Fault::DropPackets { from, to, count } => self.drop_packets(from, to, count),
            Fault::DelayLink { from, to, delay } => self.delay_link(from, to, delay),
            Fault::ClearLinkDelay { from, to } => self.clear_link_delay(from, to),
            Fault::DuplicatePackets { from, to, count, delay } => {
                self.duplicate_packets(from, to, count, delay)
            }
            Fault::TriggerElection(id) => self.start_election(id),
            Fault::SubmitCommand { node_id, command } => {
                let now = self.now();
                if let Some(node) = self.nodes.get_mut(&node_id) {
                    node.submit_command(command, now);
                }
                self.flush_node_outbox(node_id);
            }
        }
    }

    fn trigger_due_faults(&mut self) {
        let now = self.now();
        while let Some(scheduled) = self.scheduled_faults.peek() {
            if scheduled.trigger_at <= now {
                let fault = self.scheduled_faults.pop().unwrap().fault;
                self.apply_fault(fault);
            } else {
                break;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Consensus & Cluster Helpers
    // -------------------------------------------------------------------------

    pub fn start_election(&mut self, id: impl Into<NodeId>) {
        let node_id = id.into();
        let now = self.now();

        if self.disabled_nodes.contains(&node_id) || self.paused_nodes.contains(&node_id) {
            return;
        }

        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.start_election(now);
        }

        self.flush_node_outbox(node_id);
    }

    pub fn get_leader(&self) -> Option<u64> {
        let mut best: Option<(u64, u64)> = None; // (current_term, node_id)
        for (&node_id, node) in &self.nodes {
            if !self.disabled_nodes.contains(&node_id)
                && !self.paused_nodes.contains(&node_id)
                && node.role == Role::Leader
            {
                match best {
                    Some((best_term, _)) => {
                        if node.current_term > best_term {
                            best = Some((node.current_term, node_id.id));
                        }
                    }
                    None => {
                        best = Some((node.current_term, node_id.id));
                    }
                }
            }
        }

        best.map(|(_, id)| id)
    }

    pub fn leader_count(&self) -> usize {
        self.nodes
            .iter()
            .filter(|(id, node)| {
                !self.disabled_nodes.contains(id)
                    && !self.paused_nodes.contains(id)
                    && node.role == Role::Leader
            })
            .count()
    }

    // -------------------------------------------------------------------------
    // Event Routing & Simulation Execution
    // -------------------------------------------------------------------------

    pub fn deliver_next(&mut self) -> bool {
        self.trigger_due_faults();

        let event = {
            let mut events = self.events.lock().unwrap();
            events.pop()
        };

        let event = match event {
            Some(e) => e,
            None => {
                // If there are future scheduled faults and no messages, advance time to next fault
                if let Some(next_fault) = self.scheduled_faults.peek() {
                    let fault_time = next_fault.trigger_at;
                    if fault_time > self.now() {
                        let duration = fault_time.duration_since(self.now());
                        self.advance_time(duration.as_millis().max(1) as u64);
                        return true;
                    }
                }
                return false;
            }
        };

        // Move simulated time forward to the event's delivery time
        {
            let mut clock = self.clock.lock().unwrap();

            if event.deliver_at > clock.now() {
                let duration = event.deliver_at.duration_since(clock.now());
                clock.advance_duration(duration);
            }
        }

        self.trigger_due_faults();

        let now = self.now();

        // Simulate node failure and network partitions
        if self.disabled_nodes.contains(&event.from)
            || self.disabled_nodes.contains(&event.to)
            || self.blocked_links.contains(&(event.from, event.to))
        {
            return true;
        }

        // Defer delivery if destination node is paused
        if self.paused_nodes.contains(&event.to) {
            self.paused_events.entry(event.to).or_default().push(event);
            return true;
        }

        if let Some(node) = self.nodes.get_mut(&event.to) {
            node.handle_message(event.message, now);
        }

        self.flush_node_outbox(event.to);

        true
    }

    pub fn flush_node_outbox(&mut self, node_id: NodeId) {
        if self.disabled_nodes.contains(&node_id) {
            if let Some(node) = self.nodes.get_mut(&node_id) {
                node.outbox.clear();
            }
            return;
        }

        if self.paused_nodes.contains(&node_id) {
            return;
        }

        let messages = {
            if let Some(node) = self.nodes.get_mut(&node_id) {
                node.outbox.drain(..).collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        };

        let now = self.now();

        let mut events = self.events.lock().unwrap();

        for (to, message) in messages {
            if matches!(message, Message::Timeout) {
                continue;
            }

            if self.blocked_links.contains(&(node_id, to)) || self.disabled_nodes.contains(&to) {
                continue;
            }

            let mut deliver_at = now;
            let mut duplicate = None;

            if let Some(policy) = self.link_policies.get_mut(&(node_id, to)) {
                if policy.drop_remaining > 0 {
                    policy.drop_remaining -= 1;
                    continue;
                }

                if let Some(delay) = policy.delay {
                    deliver_at += delay;
                }

                if policy.duplicate_remaining > 0 {
                    policy.duplicate_remaining -= 1;
                    duplicate = Some(deliver_at + policy.duplicate_delay);
                }
            }

            let seq = self.sequence_counter.fetch_add(1, Ordering::Relaxed);
            events.push(Event {
                deliver_at,
                sequence: seq,
                from: node_id,
                to,
                message: message.clone(),
            });

            if let Some(dup_time) = duplicate {
                let dup_seq = self.sequence_counter.fetch_add(1, Ordering::Relaxed);
                events.push(Event {
                    deliver_at: dup_time,
                    sequence: dup_seq,
                    from: node_id,
                    to,
                    message,
                });
            }
        }
    }

    pub fn flush_all_outboxes(&mut self) {
        let node_ids: Vec<NodeId> = self.nodes.keys().copied().collect();

        for node_id in node_ids {
            self.flush_node_outbox(node_id);
        }
    }

    pub fn run_until_idle(&mut self) {
        self.flush_all_outboxes();

        let mut steps = 0;

        while self.deliver_next() {
            steps += 1;

            if steps > 10000 {
                panic!("simulation exceeded 10000 steps");
            }
        }
    }

    pub fn route_messages(&mut self) {
        self.run_until_idle();
    }

    pub fn step(&mut self) -> bool {
        self.deliver_next()
    }

    pub fn run_steps(&mut self, max_steps: usize) -> usize {
        self.flush_all_outboxes();
        let mut executed = 0;
        while executed < max_steps && self.deliver_next() {
            executed += 1;
        }
        executed
    }

    pub fn advance_time(&mut self, millis: u64) {
        {
            let mut clock = self.clock.lock().unwrap();
            clock.advance(millis);
        }

        self.trigger_due_faults();

        let now = self.now();

        let node_ids: Vec<NodeId> = self.nodes.keys().copied().collect();

        for node_id in node_ids {
            if self.disabled_nodes.contains(&node_id) || self.paused_nodes.contains(&node_id) {
                continue;
            }

            if let Some(node) = self.nodes.get_mut(&node_id) {
                node.tick(now);
            }

            self.flush_node_outbox(node_id);
        }
    }

    // -------------------------------------------------------------------------
    // Safety Invariants & Deterministic Chaos Testing
    // -------------------------------------------------------------------------

    pub fn check_invariants(&self) -> Result<(), InvariantViolation> {
        // 1. Election Safety: At most one leader in any term
        let mut term_leaders: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        for (&node_id, node) in &self.nodes {
            if node.role == Role::Leader {
                term_leaders
                    .entry(node.current_term)
                    .or_default()
                    .push(node_id.id);
            }
        }
        for (term, leaders) in term_leaders {
            if leaders.len() > 1 {
                return Err(InvariantViolation::ElectionSafetyViolation { term, leaders });
            }
        }

        // 2. Log Matching Invariant
        let node_list: Vec<(&NodeId, &RaftNode)> = self.nodes.iter().collect();
        for i in 0..node_list.len() {
            for j in (i + 1)..node_list.len() {
                let (id_a, node_a) = node_list[i];
                let (id_b, node_b) = node_list[j];
                let min_len = node_a.log.len().min(node_b.log.len());
                for idx in 1..min_len {
                    if node_a.log[idx].term == node_b.log[idx].term {
                        for k in 1..=idx {
                            if node_a.log[k].term != node_b.log[k].term {
                                return Err(InvariantViolation::LogMatchingViolation {
                                    node_a: id_a.id,
                                    node_b: id_b.id,
                                    index: k,
                                    term_a: node_a.log[k].term,
                                    term_b: node_b.log[k].term,
                                });
                            }
                        }
                    }
                }
            }
        }

        // 3. State Machine / Commit Safety
        for i in 0..node_list.len() {
            for j in (i + 1)..node_list.len() {
                let (id_a, node_a) = node_list[i];
                let (id_b, node_b) = node_list[j];
                let common_commit = node_a.commit_index.min(node_b.commit_index);
                for idx in 1..=common_commit {
                    if idx < node_a.log.len() && idx < node_b.log.len() {
                        let cmd_a = format!("{:?}", node_a.log[idx].record.command);
                        let cmd_b = format!("{:?}", node_b.log[idx].record.command);
                        if cmd_a != cmd_b {
                            return Err(InvariantViolation::StateSafetyViolation {
                                index: idx,
                                node_a: id_a.id,
                                node_b: id_b.id,
                                entry_a: cmd_a,
                                entry_b: cmd_b,
                            });
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn inject_random_fault(
        &mut self,
        rng: &mut DeterministicRng,
        config: &ChaosConfig,
    ) -> Option<Fault> {
        let node_ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        if node_ids.len() < 2 {
            return None;
        }

        let roll = rng.next_f64();
        let mut cumulative = 0.0;

        // Crash Node
        cumulative += config.crash_prob;
        if roll < cumulative {
            let active_nodes: Vec<NodeId> = node_ids
                .iter()
                .copied()
                .filter(|id| !self.disabled_nodes.contains(id))
                .collect();
            if active_nodes.len() > 1 {
                let target = *rng.choose(&active_nodes).unwrap();
                let fault = Fault::CrashNode(target);
                self.apply_fault(fault.clone());
                return Some(fault);
            }
        }

        // Restart Node
        cumulative += config.restart_prob;
        if roll < cumulative {
            let crashed_nodes: Vec<NodeId> = self.disabled_nodes.iter().copied().collect();
            if !crashed_nodes.is_empty() {
                let target = *rng.choose(&crashed_nodes).unwrap();
                let fault = Fault::RestartNode(target);
                self.apply_fault(fault.clone());
                return Some(fault);
            }
        }

        // Network Partition
        cumulative += config.partition_prob;
        if roll < cumulative {
            let mut shuffled = node_ids.clone();
            for i in (1..shuffled.len()).rev() {
                let j = (rng.next_u64() as usize) % (i + 1);
                shuffled.swap(i, j);
            }
            let split_idx = 1.max(shuffled.len() / 2);
            let group_a: Vec<u64> = shuffled[..split_idx].iter().map(|n| n.id).collect();
            let group_b: Vec<u64> = shuffled[split_idx..].iter().map(|n| n.id).collect();
            let fault = Fault::Partition { group_a, group_b };
            self.apply_fault(fault.clone());
            return Some(fault);
        }

        // Heal All Partitions
        cumulative += config.heal_prob;
        if roll < cumulative {
            if !self.blocked_links.is_empty() {
                let fault = Fault::HealAllPartitions;
                self.apply_fault(fault.clone());
                return Some(fault);
            }
        }

        // Drop Packets
        cumulative += config.packet_drop_prob;
        if roll < cumulative {
            let from = *rng.choose(&node_ids).unwrap();
            let to = *rng.choose(&node_ids).unwrap();
            if from != to {
                let count = rng.gen_range(1, 3) as usize;
                let fault = Fault::DropPackets { from, to, count };
                self.apply_fault(fault.clone());
                return Some(fault);
            }
        }

        // Delay Link
        cumulative += config.packet_delay_prob;
        if roll < cumulative {
            let from = *rng.choose(&node_ids).unwrap();
            let to = *rng.choose(&node_ids).unwrap();
            if from != to {
                let millis = rng.gen_range(5, config.max_packet_delay.as_millis().max(6) as u64);
                let delay = Duration::from_millis(millis);
                let fault = Fault::DelayLink { from, to, delay };
                self.apply_fault(fault.clone());
                return Some(fault);
            }
        }

        None
    }

    pub fn run_chaos_test(
        &mut self,
        seed: u64,
        steps: usize,
        config: &ChaosConfig,
    ) -> Result<usize, InvariantViolation> {
        let mut rng = DeterministicRng::new(seed);
        let node_ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        let mut ops_applied = 0;

        for step in 0..steps {
            if step % 8 == 0 {
                let target = self
                    .get_leader()
                    .map(NodeId::from)
                    .unwrap_or_else(|| *rng.choose(&node_ids).unwrap());

                if !self.disabled_nodes.contains(&target) && !self.paused_nodes.contains(&target) {
                    let cmd = crate::wal::log_record::Command::Set {
                        key: format!("chaos_key_{}", ops_applied),
                        value: crate::wal::log_record::Value::Int(ops_applied as i64),
                    };
                    self.apply_fault(Fault::SubmitCommand {
                        node_id: target,
                        command: cmd,
                    });
                    ops_applied += 1;
                }
            }

            self.inject_random_fault(&mut rng, config);

            if rng.next_f64() < 0.35 {
                let advance_ms = rng.gen_range(10, 50);
                self.advance_time(advance_ms);
            } else {
                self.deliver_next();
            }

            self.check_invariants()?;
        }

        // Converge cluster: heal network and restart crashed/paused nodes
        self.heal_all_partitions();
        for &id in &node_ids {
            if self.disabled_nodes.contains(&id) {
                self.restart_node(id);
            }
            if self.paused_nodes.contains(&id) {
                self.resume_node(id);
            }
        }

        self.run_until_idle();
        self.check_invariants()?;

        Ok(ops_applied)
    }
}

impl Index<u64> for Simulation {
    type Output = RaftNode;

    fn index(&self, id: u64) -> &Self::Output {
        &self.nodes[&NodeId { id }]
    }
}

impl IndexMut<u64> for Simulation {
    fn index_mut(&mut self, id: u64) -> &mut Self::Output {
        self.nodes.get_mut(&NodeId { id }).unwrap()
    }
}

impl Index<NodeId> for Simulation {
    type Output = RaftNode;

    fn index(&self, id: NodeId) -> &Self::Output {
        &self.nodes[&id]
    }
}

impl IndexMut<NodeId> for Simulation {
    fn index_mut(&mut self, id: NodeId) -> &mut Self::Output {
        self.nodes.get_mut(&id).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_node_cluster_elects_one_leader() {
        let mut sim = Simulation::new_cluster(&[1, 2, 3]);

        assert_eq!(sim.get_leader(), None);
        assert_eq!(sim.leader_count(), 0);

        // Node 1 has a 200 ms election timeout.
        sim.advance_time(200);

        // Deliver RequestVote messages and responses.
        sim.run_until_idle();

        assert_eq!(sim.get_leader(), Some(1));
        assert_eq!(sim.leader_count(), 1);
    }
}
