use crate::lsm::wal::log_record::Value;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{mpsc::UnboundedSender, oneshot, Mutex},
};

pub enum ServiceOperation {
    Set { key: String, value: Value },
    Delete { key: String },
    Get { key: String },
    Status,
    AddNode { id: u64, addr: String },
    RemoveNode { id: u64 },
}

/// ClusterManager maintains a dynamic registry of node IDs and their addresses.
/// This is shared between the API server and the Raft runtime so that
/// /health/node/{id} can look up any node's address, even after dynamic
/// membership changes via register_node / remove_node.
#[derive(Clone)]
pub struct ClusterManager {
    // Shared inner state protected by a tokio mutex for async access.
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    // node_id -> "host:port" address
    nodes: HashMap<u64, String>,
}

impl ClusterManager {
    /// Create a new ClusterManager with the given initial nodes.
    pub fn new(initial: Vec<(u64, String)>) -> Self {
        let inner = Inner {
            nodes: initial.into_iter().collect(),
        };
        ClusterManager {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// Look up the address for a node ID.
    pub async fn get_address(&self, node_id: u64) -> Option<String> {
        self.inner.lock().await.nodes.get(&node_id).cloned()
    }

    /// Add a node to the cluster.
    pub async fn add_node(&self, node_id: u64, addr: String) {
        self.inner.lock().await.nodes.insert(node_id, addr);
    }

    /// Remove a node from the cluster.
    pub async fn remove_node(&self, node_id: u64) {
        self.inner.lock().await.nodes.remove(&node_id);
    }
}

impl Default for ClusterManager {
    fn default() -> Self {
        // Start with the default known nodes.
        Self::new(vec![
            (1, "node1:7001".to_string()),
            (2, "node2:7002".to_string()),
            (3, "node3:7003".to_string()),
        ])
    }
}

pub struct ServiceRequest {
    pub operation: ServiceOperation,
    pub response_tx: oneshot::Sender<ServiceResponse>,
}

pub struct ServiceResponse {
    pub success: bool,
    pub value: Option<Value>,
    pub message: String,
    pub leader_id: Option<u64>,
    pub term: u64,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "UPPERCASE")]
enum WireRequest {
    Set {
        key: String,
        value: WireValue,
    },
    Get {
        key: String,
    },
    Delete {
        key: String,
    },
    Status,
    AddNode {
        #[serde(rename = "node_id")]
        id: u64,
        addr: String,
    },
    RemoveNode {
        #[serde(rename = "node_id")]
        id: u64,
    },
}

/// Shared cluster manager instance, passed to the API listener.
pub struct ApiState {
    cluster_manager: ClusterManager,
}

impl ApiState {
    pub fn new(cluster_manager: ClusterManager) -> Self {
        Self { cluster_manager }
    }

    /// Look up a node address by ID.
    pub async fn get_node_address(&self, node_id: u64) -> Option<String> {
        self.cluster_manager.get_address(node_id).await
    }

    /// Add a node to the cluster (called from API handler).
    pub async fn add_node(&self, node_id: u64, addr: String) {
        self.cluster_manager.add_node(node_id, addr).await;
    }

    /// Remove a node from the cluster (called from API handler).
    pub async fn remove_node(&self, node_id: u64) {
        self.cluster_manager.remove_node(node_id).await;
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "lowercase")]
enum WireValue {
    Int(i64),
    Float(f64),
    String(String),
}

impl From<WireValue> for Value {
    fn from(value: WireValue) -> Self {
        match value {
            WireValue::Int(v) => Value::Int(v),
            WireValue::Float(v) => Value::Float(v),
            WireValue::String(v) => Value::String(v),
        }
    }
}

impl From<Value> for WireValue {
    fn from(value: Value) -> Self {
        match value {
            Value::Int(v) => WireValue::Int(v),
            Value::Float(v) => WireValue::Float(v),
            Value::String(v) => WireValue::String(v),
        }
    }
}

#[derive(Serialize)]
struct WireResponse {
    success: bool,
    value: Option<WireValue>,
    message: String,
    leader_id: Option<u64>,
    term: u64,
}

pub async fn run_listener(
    addr: String,
    service_tx: UnboundedSender<ServiceRequest>,
    api_state: Arc<ApiState>,
) -> io::Result<()> {
    let listener = TcpListener::bind(&addr).await?;

    if crate::raft::is_log_enabled() {
        eprintln!("[API] Listening on {addr}");
    }

    loop {
        let (stream, peer) = listener.accept().await?;
        eprintln!("[API] Connection from {peer}");

        let tx = service_tx.clone();
        let api_state_clone = api_state.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, tx, api_state_clone).await {
                eprintln!("[API] Client error: {e}");
            }
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    service_tx: UnboundedSender<ServiceRequest>,
    api_state: Arc<ApiState>,
) -> io::Result<()> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    loop {
        line.clear();

        let bytes_read = reader.read_line(&mut line).await?;

        if bytes_read == 0 {
            break;
        }

        let response = match serde_json::from_str::<WireRequest>(line.trim()) {
            Ok(request) => {
                let operation = match request {
                    WireRequest::Set { key, value } => ServiceOperation::Set {
                        key,
                        value: value.into(),
                    },
                    WireRequest::Get { key } => ServiceOperation::Get { key },
                    WireRequest::Delete { key } => ServiceOperation::Delete { key },
                    WireRequest::Status => ServiceOperation::Status,
                    WireRequest::AddNode { id, addr } => {
                        // Update the cluster manager's dynamic node registry.
                        let api_state_clone = api_state.clone();
                        let addr_clone = addr.clone();
                        tokio::spawn(async move {
                            let _ = api_state_clone.add_node(id, addr_clone).await;
                        });
                        ServiceOperation::AddNode { id, addr: addr.clone() }
                    }
                    WireRequest::RemoveNode { id } => {
                        // Update the cluster manager's dynamic node registry.
                        let api_state_clone = api_state.clone();
                        tokio::spawn(async move {
                            let _ = api_state_clone.remove_node(id).await;
                        });
                        ServiceOperation::RemoveNode { id }
                    }
                };

                let (response_tx, response_rx) = oneshot::channel();

                let request = ServiceRequest {
                    operation,
                    response_tx,
                };

                if service_tx.send(request).is_err() {
                    WireResponse {
                        success: false,
                        value: None,
                        message: "Raft runtime unavailable".into(),
                        leader_id: None,
                        term: 0,
                    }
                } else {
                    match response_rx.await {
                        Ok(response) => WireResponse {
                            success: response.success,
                            value: response.value.map(Into::into),
                            message: response.message,
                            leader_id: response.leader_id,
                            term: response.term,
                        },
                        Err(_) => WireResponse {
                            success: false,
                            value: None,
                            message: "Raft runtime dropped the response".into(),
                            leader_id: None,
                            term: 0,
                        },
                    }
                }
            }
            Err(e) => WireResponse {
                success: false,
                value: None,
                message: format!("Invalid JSON request: {e}"),
                leader_id: None,
                term: 0,
            },
        };

        let mut output = serde_json::to_vec(&response).map_err(io::Error::other)?;

        output.push(b'\n');

        reader.get_mut().write_all(&output).await?;
    }

    Ok(())
}
