use std::io::{self, Write};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use kv_store::raft::{ClientQuery, ClientRequest, ClientResponse, Message, NodeId, StatusQuery};
use kv_store::wal::log_record::{Command, Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    let default_addr = "127.0.0.1:6001".to_string();
    let target_addr = if args.len() > 1 && !args[1].starts_with('-') {
        args[1].clone()
    } else {
        default_addr
    };

    println!("============================================================");
    println!(" KV-Store Raft Cluster Client");
    println!(" Connecting to node at: {}", target_addr);
    println!(" Type 'help' for available commands or 'exit' to quit.");
    println!("============================================================");

    let client_id_num = rand::random::<u32>() as u64 + 10000;
    let client_id = NodeId { id: client_id_num };

    let mut client = ClusterClient::connect(&target_addr, client_id).await;

    let stdin = io::stdin();
    let mut input = String::new();

    loop {
        print!("kv-store ({})> ", client.current_addr());
        io::stdout().flush()?;

        input.clear();
        if stdin.read_line(&mut input)? == 0 {
            break; // EOF
        }

        let trimmed = input.trim();
        if trimmed.is_empty() {
            continue;
        }

        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        let cmd_name = parts[0].to_lowercase();

        match cmd_name.as_str() {
            "exit" | "quit" => {
                println!("Goodbye!");
                break;
            }
            "help" => {
                print_help();
            }
            "status" => {
                client.send_status().await;
            }
            "connect" => {
                if parts.len() < 2 {
                    println!("Usage: connect <ip:port>");
                } else {
                    let new_addr = parts[1];
                    println!("Connecting to {}...", new_addr);
                    client = ClusterClient::connect(new_addr, client_id).await;
                }
            }
            "get" => {
                if parts.len() < 2 {
                    println!("Usage: get <key>");
                } else {
                    let key = parts[1];
                    client.get_key(key).await;
                }
            }
            "set" => {
                if parts.len() < 3 {
                    println!("Usage: set <key> <value>");
                } else {
                    let key = parts[1];
                    let val_str = parts[2..].join(" ");
                    let val = parse_value(&val_str);
                    client.set_key(key, val).await;
                }
            }
            "delete" | "del" => {
                if parts.len() < 2 {
                    println!("Usage: delete <key>");
                } else {
                    let key = parts[1];
                    client.delete_key(key).await;
                }
            }
            _ => {
                println!("Unknown command: '{}'. Type 'help' for usage.", parts[0]);
            }
        }
    }

    Ok(())
}

fn print_help() {
    println!("\nAvailable Commands:");
    println!("  set <key> <value>   - Insert or update a key-value pair");
    println!("  get <key>           - Retrieve value associated with key");
    println!("  delete <key>        - Delete a key");
    println!("  status              - Query status of connected Raft node");
    println!("  connect <ip:port>   - Switch connection to a different node");
    println!("  help                - Show this help menu");
    println!("  exit / quit         - Disconnect and exit client\n");
}

fn parse_value(s: &str) -> Value {
    if let Ok(i) = s.parse::<i64>() {
        Value::Int(i)
    } else if let Ok(f) = s.parse::<f64>() {
        Value::Float(f)
    } else {
        Value::String(s.to_string())
    }
}

struct ClusterClient {
    addr: String,
    stream: Option<TcpStream>,
    client_id: NodeId,
    request_counter: u64,
}

impl ClusterClient {
    async fn connect(addr: &str, client_id: NodeId) -> Self {
        let stream = match TcpStream::connect(addr).await {
            Ok(mut s) => {
                // Send 8-byte client handshake ID
                if let Err(e) = s.write_all(&client_id.id.to_be_bytes()).await {
                    eprintln!("Handshake failed with {}: {}", addr, e);
                    None
                } else {
                    println!("Successfully connected to node at {}", addr);
                    Some(s)
                }
            }
            Err(e) => {
                eprintln!("Failed to connect to node at {}: {}", addr, e);
                None
            }
        };

        Self {
            addr: addr.to_string(),
            stream,
            client_id,
            request_counter: 1,
        }
    }

    fn current_addr(&self) -> &str {
        &self.addr
    }

    async fn send_message(&mut self, msg: Message) -> Option<ClientResponse> {
        let stream = match self.stream.as_mut() {
            Some(s) => s,
            None => {
                eprintln!("Not connected to node at {}. Try 'connect <ip:port>'.", self.addr);
                return None;
            }
        };

        let mut buf = Vec::with_capacity(256);
        if let Err(e) = bincode::serialize_into(&mut buf, &msg) {
            eprintln!("Serialization error: {}", e);
            return None;
        }

        let len = buf.len() as u32;
        if stream.write_all(&len.to_be_bytes()).await.is_err() || stream.write_all(&buf).await.is_err() {
            eprintln!("Lost connection to node at {}.", self.addr);
            self.stream = None;
            return None;
        }

        // Read response header length
        let mut len_buf = [0u8; 4];
        if stream.read_exact(&mut len_buf).await.is_err() {
            eprintln!("Failed to read response from node.");
            self.stream = None;
            return None;
        }

        let resp_len = u32::from_be_bytes(len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        if stream.read_exact(&mut resp_buf).await.is_err() {
            eprintln!("Failed to read response payload.");
            self.stream = None;
            return None;
        }

        match bincode::deserialize::<Message>(&resp_buf) {
            Ok(Message::ClientResponse(resp)) => Some(resp),
            Ok(other) => {
                eprintln!("Unexpected response variant: {:?}", other);
                None
            }
            Err(e) => {
                eprintln!("Deserialization error: {}", e);
                None
            }
        }
    }

    async fn send_status(&mut self) {
        let req_id = self.request_counter;
        self.request_counter += 1;

        let msg = Message::StatusQuery(StatusQuery {
            request_id: req_id,
            client_id: self.client_id,
        });

        if let Some(resp) = self.send_message(msg).await {
            println!("------------------------------------------------------------");
            println!(" Node Status Report");
            println!(" Connected Node ID: {}", resp.node_id.id);
            println!(" Role:              {}", resp.role);
            println!(" Current Term:      {}", resp.term);
            if let Some(l) = resp.leader_id {
                println!(" Known Leader ID:   {}", l.id);
            } else {
                println!(" Known Leader ID:   <None>");
            }
            println!(" Details:           {}", resp.message);
            println!("------------------------------------------------------------");
        }
    }

    async fn get_key(&mut self, key: &str) {
        let req_id = self.request_counter;
        self.request_counter += 1;

        let msg = Message::ClientQuery(ClientQuery {
            request_id: req_id,
            client_id: self.client_id,
            key: key.to_string(),
        });

        if let Some(resp) = self.send_message(msg).await {
            match resp.value {
                Some(val) => println!("{}: {:?}", key, val),
                None => println!("{}: <not found>", key),
            }
        }
    }

    async fn set_key(&mut self, key: &str, value: Value) {
        let req_id = self.request_counter;
        self.request_counter += 1;

        let msg = Message::ClientRequest(ClientRequest {
            request_id: req_id,
            client_id: self.client_id,
            command: Command::Set {
                key: key.to_string(),
                value,
            },
        });

        if let Some(resp) = self.send_message(msg).await {
            if resp.success {
                println!("OK: {}", resp.message);
            } else {
                println!("ERROR: {}", resp.message);
            }
        }
    }

    async fn delete_key(&mut self, key: &str) {
        let req_id = self.request_counter;
        self.request_counter += 1;

        let msg = Message::ClientRequest(ClientRequest {
            request_id: req_id,
            client_id: self.client_id,
            command: Command::Delete {
                key: key.to_string(),
            },
        });

        if let Some(resp) = self.send_message(msg).await {
            if resp.success {
                println!("OK: {}", resp.message);
            } else {
                println!("ERROR: {}", resp.message);
            }
        }
    }
}
