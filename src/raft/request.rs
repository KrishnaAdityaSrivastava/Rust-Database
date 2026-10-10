use tokio::sync::oneshot;

use crate::raft::message::{ClientResponse, Message};
use crate::raft::node::{NodeId, Role};
use crate::runtime::service::ServiceResponse;
use crate::lsm::wal::log_record::{Command, Value};

#[derive(Debug)]
pub enum StoreOperation {
    Set { key: String, value: Value },
    Delete { key: String },
    Get { key: String },
    Status,
}

impl StoreOperation {
    pub fn to_command(&self) -> Option<Command> {
        match self {
            StoreOperation::Set { key, value } => Some(Command::Set {
                key: key.clone(),
                value: value.clone(),
            }),
            StoreOperation::Delete { key } => Some(Command::Delete { key: key.clone() }),
            StoreOperation::Get { .. } | StoreOperation::Status => None,
        }
    }
}

pub enum Responder {
    Oneshot(oneshot::Sender<ServiceResponse>),
    RaftClient { client_id: NodeId, request_id: u64 },
}

impl Responder {
    pub fn send(
        self,
        success: bool,
        value: Option<Value>,
        message: String,
        leader_id: Option<NodeId>,
        term: u64,
        node_id: NodeId,
        role: Role,
        outbox: &mut Vec<(NodeId, Message)>,
    ) {
        match self {
            Responder::Oneshot(tx) => {
                let response = ServiceResponse {
                    success,
                    value,
                    message,
                    leader_id: leader_id.map(|id| id.id),
                    term,
                };
                let _ = tx.send(response);
            }
            Responder::RaftClient {
                client_id,
                request_id,
            } => {
                let response_msg = Message::ClientResponse(ClientResponse {
                    request_id,
                    success,
                    value,
                    message,
                    leader_id,
                    node_id,
                    role: format!("{:?}", role),
                    term,
                });
                outbox.push((client_id, response_msg));
            }
        }
    }
}

pub struct StoreRequest {
    pub operation: StoreOperation,
    pub responder: Responder,
}
