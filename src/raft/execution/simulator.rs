use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::{Index, IndexMut};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::runtime::{Clock, Transport};
use crate::database::Config;
use crate::raft::message::Message;
use crate::raft::node::{NodeId, RaftNode, Role};

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
    pub disabled_nodes: HashSet<NodeId>,
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
                .cloned()
                .filter(|node| node.id != id)
                .collect();

            nodes.insert(
                NodeId { id },
                RaftNode::new(NodeId { id }, peers, Config::default()),
            );
        }

        Self::new(nodes)
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
        self.disabled_nodes.remove(&id.into());
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
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.start_election(now);
            self.flush_node_outbox(node_id);
        }
    }

    pub fn get_leader(&self) -> Option<u64> {
        for (&node_id, node) in &self.nodes {
            if !self.disabled_nodes.contains(&node_id) && node.role == Role::Leader {
                return Some(node_id.id);
            }
        }
        None
    }

    pub fn deliver_next(&mut self) -> bool {
        let event = {
            let mut events = self.events.lock().unwrap();

            if events.is_empty() {
                return false;
            }

            events.pop_front().unwrap()
        };

        {
            let mut clock = self.clock.lock().unwrap();

            if event.deliver_at > clock.now() {
                let duration = event.deliver_at.duration_since(clock.now());

                clock.advance(duration.as_millis() as u64);
            }
        }

        let now = self.clock.lock().unwrap().now();

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

        let clock = self.clock.lock().unwrap();
        let now = clock.now();
        drop(clock);

        let mut events = self.events.lock().unwrap();

        for (to, message) in messages {
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
            if steps > 1000 {
                break;
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

        let now = self.clock.lock().unwrap().now();

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

