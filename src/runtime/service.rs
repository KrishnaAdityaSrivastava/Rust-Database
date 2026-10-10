use crate::lsm::wal::log_record::Value;

use serde::{Deserialize, Serialize};
use std::io;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{mpsc::UnboundedSender, oneshot},
};

pub enum ServiceOperation {
    Set { key: String, value: Value },
    Delete { key: String },
    Get { key: String },
    Status,
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
    Set { key: String, value: WireValue },
    Get { key: String },
    Delete { key: String },
    Status,
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
) -> io::Result<()> {
    let listener = TcpListener::bind(&addr).await?;

    if crate::raft::is_log_enabled() {
        eprintln!("[API] Listening on {addr}");
    }

    loop {
        let (stream, peer) = listener.accept().await?;
        eprintln!("[API] Connection from {peer}");

        let tx = service_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, tx).await {
                eprintln!("[API] Client error: {e}");
            }
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    service_tx: UnboundedSender<ServiceRequest>,
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
