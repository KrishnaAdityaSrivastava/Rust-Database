use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::{Index, IndexMut};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::database::Config;
use crate::raft::message::Message;
use crate::raft::node::{NodeId, RaftNode, Role};

use super::runtime::{Clock, Transport};

#[derive(Debug, Clone)]
pub struct Event {
    pub deliver_at: Instant,
    pub from: NodeId,
    pub to: NodeId,
    pub message: Message,
}

pub struct SimulatedTransport {
    events: Arc<Mutex<VecDeque<Event>>>,
    clock: Arc<Mutex<Clock>>,
}

impl SimulatedTransport {
    pub fn new(events: Arc<Mutex<VecDeque<Event>>>, clock: Arc<Mutex<Clock>>) -> Self {
        Self { events, clock }
    }
}

impl Transport for SimulatedTransport {
    fn send(&self, from: NodeId, to: NodeId, message: Message) {
        let deliver_at = self.clock.lock().unwrap().now();

        self.events.lock().unwrap().push_back(Event {
            deliver_at,
            from,
            to,
            message,
        });
    }
}

pub struct Simulation {
    pub clock: Arc<Mutex<Clock>>,
    pub events: Arc<Mutex<VecDeque<Event>>>,
    pub nodes: HashMap<NodeId, RaftNode>,

    // Nodes that are completely unavailable.
    pub disabled_nodes: HashSet<NodeId>,

    // Directed network links that are blocked.
    pub blocked_links: HashSet<(NodeId, NodeId)>,
}

impl Simulation {
    pub fn new(nodes: HashMap<NodeId, RaftNode>) -> Self {
        let clock = Arc::new(Mutex::new(Clock::new()));
        let events = Arc::new(Mutex::new(VecDeque::new()));

        Self {
            clock,
            events,
            nodes,
            disabled_nodes: HashSet::new(),
            blocked_links: HashSet::new(),
        }
    }

    pub fn new_cluster(ids: &[u64]) -> Self {
        let all_node_ids: Vec<NodeId> = ids.iter().map(|&id| NodeId { id }).collect();

        let mut nodes = HashMap::new();

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

    pub fn start_election(&mut self, id: impl Into<NodeId>) {
        let node_id = id.into();
        let now = self.now();

        if self.disabled_nodes.contains(&node_id) {
            return;
        }

        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.start_election(now);
        }

        self.flush_node_outbox(node_id);
    }

    pub fn get_leader(&self) -> Option<u64> {
        for (&node_id, node) in &self.nodes {
            if !self.disabled_nodes.contains(&node_id) && node.role == Role::Leader {
                return Some(node_id.id);
            }
        }

        None
    }

    pub fn leader_count(&self) -> usize {
        self.nodes
            .iter()
            .filter(|(id, node)| !self.disabled_nodes.contains(id) && node.role == Role::Leader)
            .count()
    }

    pub fn deliver_next(&mut self) -> bool {
        let event = {
            let mut events = self.events.lock().unwrap();

            if events.is_empty() {
                return false;
            }

            events.pop_front().unwrap()
        };

        // Move simulated time forward to the event's
        // delivery time.
        {
            let mut clock = self.clock.lock().unwrap();

            if event.deliver_at > clock.now() {
                let duration = event.deliver_at.duration_since(clock.now());

                clock.advance(duration.as_millis() as u64);
            }
        }

        let now = self.now();

        // Simulate node failure and network partitions.
        if self.disabled_nodes.contains(&event.from)
            || self.disabled_nodes.contains(&event.to)
            || self.blocked_links.contains(&(event.from, event.to))
        {
            return true;
        }

        if let Some(node) = self.nodes.get_mut(&event.to) {
            node.handle_message(event.message, now);
        }

        self.flush_node_outbox(event.to);

        true
    }

    pub fn flush_node_outbox(&mut self, node_id: NodeId) {
        // A disabled node cannot send anything.
        if self.disabled_nodes.contains(&node_id) {
            if let Some(node) = self.nodes.get_mut(&node_id) {
                node.outbox.clear();
            }

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
            // Timeout is handled internally by tick().
            if matches!(message, Message::Timeout) {
                continue;
            }

            events.push_back(Event {
                deliver_at: now,
                from: node_id,
                to,
                message,
            });
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

            // Prevent an accidental infinite message loop
            // from hanging a test.
            if steps > 1000 {
                panic!("simulation exceeded 1000 steps");
            }
        }
    }

    pub fn route_messages(&mut self) {
        self.run_until_idle();
    }

    pub fn advance_time(&mut self, millis: u64) {
        {
            let mut clock = self.clock.lock().unwrap();
            clock.advance(millis);
        }

        let now = self.now();

        let node_ids: Vec<NodeId> = self.nodes.keys().copied().collect();

        for node_id in node_ids {
            if self.disabled_nodes.contains(&node_id) {
                continue;
            }

            if let Some(node) = self.nodes.get_mut(&node_id) {
                node.tick(now);
            }

            self.flush_node_outbox(node_id);
        }
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
