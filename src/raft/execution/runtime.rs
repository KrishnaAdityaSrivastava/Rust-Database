use std::time::Instant;

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{self, Duration};

use super::super::message::Message;
use super::super::node::{NodeId, RaftNode};

pub struct Runtime<T: Transport> {
    node: RaftNode,
    transport: T,
}

pub struct Clock {
    now: Instant,
}

impl Clock {
    pub fn new() -> Self {
        Self {
            now: Instant::now(),
        }
    }

    pub fn now(&self) -> Instant {
        self.now
    }

    pub fn advance(&mut self, millis: u64) {
        self.now += Duration::from_millis(millis);
    }
}

pub trait Transport: Send + Sync {
    fn send(&self, from: NodeId, to: NodeId, message: Message);
}

impl<T: Transport + 'static> Runtime<T> {
    pub fn new(node: RaftNode, transport: T) -> Self {
        Self { node, transport }
    }

    pub fn start(mut self, mut rx: UnboundedReceiver<Message>) {
        tokio::spawn(async move {
            let mut tick_interval = time::interval(Duration::from_millis(50));

            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        match msg {
                            Some(msg) => {
                                self.node.handle_message(msg, Instant::now());
                                self.flush_outbox();
                            }

                            None => return,
                        }
                    }

                    _ = tick_interval.tick() => {
                        self.node.tick(Instant::now());
                        self.flush_outbox();
                    }
                }
            }
        });
    }

    fn flush_outbox(&mut self) {
        for (to, message) in self.node.outbox.drain(..) {
            if matches!(message, Message::Timeout) {
                continue;
            }

            self.transport.send(self.node.id, to, message);
        }
    }
}