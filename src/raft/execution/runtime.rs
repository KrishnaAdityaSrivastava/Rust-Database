use std::time::Instant;

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{self, Duration};

use crate::Command;
use crate::raft::service::{ServiceOperation, ServiceRequest, ServiceResponse};
use crate::wal::log_record::Value;

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

    pub fn start(
        mut self,
        mut rx: UnboundedReceiver<Message>,
        mut service_rx: UnboundedReceiver<ServiceRequest>,
    ) {
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

                    request = service_rx.recv() => {
                        if let Some(request) = request {
                            self.handle_service_request(request);
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

    fn submit_service_write(
        &mut self,
        command: Command,
        response_tx: tokio::sync::oneshot::Sender<ServiceResponse>,
    ) {
        use crate::raft::Role;

        if self.node.role != Role::Leader {
            let _ = response_tx.send(ServiceResponse {
                success: false,
                value: None,
                message: "Not leader".to_string(),
                leader_id: self.node.leader_id.map(|id| id.id),
                term: self.node.current_term,
            });
            return;
        }

        let index = self.node.append_command(command);
        let term = self.node.log[index].term;

        // Single-node clusters may already have committed and applied it.
        if self.node.last_applied >= index {
            let _ = response_tx.send(ServiceResponse {
                success: true,
                value: None,
                message: format!("Write committed and applied at log index {index}"),
                leader_id: Some(self.node.id.id),
                term,
            });
        } else {
            self.node
                .pending_service_writes
                .insert((index, term), response_tx);
        }

        self.flush_outbox();
    }

    fn handle_service_request(&mut self, request: ServiceRequest) {
        match request.operation {
            ServiceOperation::Set { key, value } => {
                self.submit_service_write(Command::Set { key, value }, request.response_tx);
            }

            ServiceOperation::Delete { key } => {
                self.submit_service_write(Command::Delete { key }, request.response_tx);
            }

            ServiceOperation::Get { key } => {
                let response = match self.node.get(&key) {
                    Ok(value) => ServiceResponse {
                        success: true,
                        value,
                        message: "Read from local node; may be stale".into(),
                        leader_id: self.node.leader_id.map(|id| id.id),
                        term: self.node.current_term,
                    },
                    Err(err) => ServiceResponse {
                        success: false,
                        value: None,
                        message: err.to_string(),
                        leader_id: self.node.leader_id.map(|id| id.id),
                        term: self.node.current_term,
                    },
                };

                let _ = request.response_tx.send(response);
            }

            ServiceOperation::Status => {
                let _ = request.response_tx.send(ServiceResponse {
                    success: true,
                    value: None,
                    message: format!("Node role: {:?}", self.node.role),
                    leader_id: self.node.leader_id.map(|id| id.id),
                    term: self.node.current_term,
                });
            }
        }
    }
}
