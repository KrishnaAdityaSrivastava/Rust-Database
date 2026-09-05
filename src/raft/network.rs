use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::time::Duration;

use super::message::Message;
use super::node::{NodeId, RaftNode};

pub struct Network {
    local_id: NodeId,
    local_addr: String,
    peers: HashMap<NodeId, String>,

    // Peer NodeId -> channel used by the writer task.
    active_senders: Arc<RwLock<HashMap<NodeId, UnboundedSender<Message>>>>,
}

impl Network {
    pub fn new(local_id: NodeId, local_addr: String, peers: HashMap<NodeId, String>) -> Self {
        Self {
            local_id,
            local_addr,
            peers,
            active_senders: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start(self, mut node: RaftNode) -> UnboundedSender<Message> {
        let (node_tx, mut node_rx) = mpsc::unbounded_channel::<Message>();

        let local_id = self.local_id;
        let senders_ref = self.active_senders.clone();

        // ------------------------------------------------------------
        // 1. NODE PROCESSING TASK (with tick loop)
        // ------------------------------------------------------------

        tokio::spawn(async move {
            let mut tick_interval = tokio::time::interval(Duration::from_millis(50));

            loop {
                tokio::select! {
                    msg = node_rx.recv() => {
                        match msg {
                            Some(msg) => {
                                node.handle_message(msg);

                                // Send everything produced by Raft directly to peer channels.
                                if !node.outbox.is_empty() {
                                    let guard = senders_ref.read().unwrap();
                                    for (to, out_msg) in node.outbox.drain(..) {
                                        match out_msg {
                                            Message::ClientCommand(_) | Message::Timeout => continue,
                                            _ => {}
                                        }
                                        if let Some(sender) = guard.get(&to) {
                                            let _ = sender.send(out_msg);
                                        }
                                    }
                                }
                            }
                            None => return,
                        }
                    }

                    _ = tick_interval.tick() => {
                        node.tick();

                        // Send everything produced by tick directly to peer channels.
                        if !node.outbox.is_empty() {
                            let guard = senders_ref.read().unwrap();
                            for (to, out_msg) in node.outbox.drain(..) {
                                match out_msg {
                                    Message::ClientCommand(_) | Message::Timeout => continue,
                                    _ => {}
                                }
                                if let Some(sender) = guard.get(&to) {
                                    let _ = sender.send(out_msg);
                                }
                            }
                        }
                    }
                }
            }
        });

        // ------------------------------------------------------------
        // 2. TCP LISTENER TASK
        // ------------------------------------------------------------

        let acceptor_addr = self.local_addr.clone();
        let acceptor_senders = self.active_senders.clone();
        let acceptor_node_tx = node_tx.clone();

        tokio::spawn(async move {
            let listener = match TcpListener::bind(&acceptor_addr).await {
                Ok(listener) => listener,

                Err(err) => {
                    eprintln!(
                        "Node {} failed to bind {}: {}",
                        local_id.id, acceptor_addr, err
                    );

                    return;
                }
            };

            if super::is_log_enabled() {
                println!("Node {} listening on {}", local_id.id, acceptor_addr);
            }

            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        Self::handle_incoming_connection(
                            local_id,
                            stream,
                            acceptor_senders.clone(),
                            acceptor_node_tx.clone(),
                        )
                        .await;
                    }

                    Err(err) => {
                        eprintln!("Node {} failed to accept connection: {}", local_id.id, err);
                    }
                }
            }
        });

        // ------------------------------------------------------------
        // 3. RECONNECT TASK
        // ------------------------------------------------------------

        let reconnect_senders = self.active_senders.clone();
        let reconnect_peers = self.peers.clone();
        let reconnect_node_tx = node_tx.clone();

        tokio::spawn(async move {
            loop {
                for (peer_id, peer_addr) in &reconnect_peers {
                    /*
                     * Only the node with the HIGHER ID initiates
                     * the TCP connection.
                     *
                     * Example:
                     *
                     * Node 2 <---- Node 3
                     *
                     * Node 3 initiates.
                     * Node 2 accepts.
                     *
                     * The TCP connection is still bidirectional.
                     */
                    if local_id.id <= peer_id.id {
                        continue;
                    }

                    let is_connected = { reconnect_senders.read().unwrap().contains_key(peer_id) };

                    if is_connected {
                        continue;
                    }

                    match TcpStream::connect(peer_addr).await {
                        Ok(stream) => {
                            Self::handle_outgoing_connection(
                                local_id,
                                *peer_id,
                                stream,
                                reconnect_senders.clone(),
                                reconnect_node_tx.clone(),
                            )
                            .await;
                        }

                        Err(_) => {
                            // Peer may simply be offline.
                            // Retry later.
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });

        node_tx
    }

    // ================================================================
    // INCOMING CONNECTION
    // ================================================================

    async fn handle_incoming_connection(
        local_id: NodeId,
        mut stream: TcpStream,
        active_senders: Arc<RwLock<HashMap<NodeId, UnboundedSender<Message>>>>,
        node_tx: UnboundedSender<Message>,
    ) {
        let mut id_buf = [0u8; 8];

        if let Err(err) = stream.read_exact(&mut id_buf).await {
            eprintln!("Node {} failed to read handshake: {}", local_id.id, err);

            return;
        }

        let peer_id = NodeId {
            id: u64::from_be_bytes(id_buf),
        };

        if peer_id.id <= local_id.id {
            eprintln!(
                "Node {} rejected unexpected connection from Node {}",
                local_id.id, peer_id.id
            );

            return;
        }

        if super::is_log_enabled() {
            println!(
                "Node {} accepted connection from Node {}",
                local_id.id, peer_id.id
            );
        }

        Self::spawn_connection_tasks(peer_id, stream, active_senders, node_tx);
    }

    // ================================================================
    // OUTGOING CONNECTION
    // ================================================================

    async fn handle_outgoing_connection(
        local_id: NodeId,
        peer_id: NodeId,
        mut stream: TcpStream,
        active_senders: Arc<RwLock<HashMap<NodeId, UnboundedSender<Message>>>>,
        node_tx: UnboundedSender<Message>,
    ) {
        /*
         * Send our NodeId as the handshake.
         */
        if let Err(err) = stream.write_all(&local_id.id.to_be_bytes()).await {
            eprintln!(
                "Node {} failed handshake with Node {}: {}",
                local_id.id, peer_id.id, err
            );

            return;
        }

        /*
         * Because only the higher NodeId initiates,
         * there should normally never be an existing connection.
         *
         * Still check to protect against races/reconnects.
         */
        {
            let senders = active_senders.read().unwrap();

            if senders.contains_key(&peer_id) {
                return;
            }
        }

        if super::is_log_enabled() {
            println!(
                "Node {} successfully connected to Node {}",
                local_id.id, peer_id.id
            );
        }

        Self::spawn_connection_tasks(peer_id, stream, active_senders, node_tx);
    }

    // ================================================================
    // CONNECTION TASKS
    // ================================================================

    fn spawn_connection_tasks(
        peer_id: NodeId,
        stream: TcpStream,
        active_senders: Arc<RwLock<HashMap<NodeId, UnboundedSender<Message>>>>,
        node_tx: UnboundedSender<Message>,
    ) {
        let (writer_tx, mut writer_rx) = mpsc::unbounded_channel::<Message>();

        /*
         * Register the writer channel.
         */
        {
            let mut senders = active_senders.write().unwrap();

            /*
             * If another connection already exists, replace it.
             *
             * Dropping the old Sender causes its writer task
             * to eventually terminate.
             */
            senders.insert(peer_id, writer_tx);
        }

        /*
         * Split the TCP stream into owned read and write halves.
         *
         * One half is used for reading.
         * One is used for writing.
         */
        let (mut reader, mut writer) = stream.into_split();

        // ------------------------------------------------------------
        // WRITER TASK
        // ------------------------------------------------------------

        tokio::spawn(async move {
            let mut buf = Vec::with_capacity(1024);
            while let Some(msg) = writer_rx.recv().await {
                buf.clear();
                if let Err(err) = bincode::serialize_into(&mut buf, &msg) {
                    eprintln!(
                        "Failed to serialize message for Node {}: {}",
                        peer_id.id, err
                    );
                    continue;
                }

                let len = buf.len() as u32;

                if let Err(err) = writer.write_all(&len.to_be_bytes()).await {
                    eprintln!(
                        "Connection to Node {} lost while writing: {}",
                        peer_id.id, err
                    );

                    break;
                }

                if let Err(err) = writer.write_all(&buf).await {
                    eprintln!(
                        "Connection to Node {} lost while writing: {}",
                        peer_id.id, err
                    );

                    break;
                }
            }
        });

        // ------------------------------------------------------------
        // READER TASK
        // ------------------------------------------------------------

        let reader_senders = active_senders.clone();

        tokio::spawn(async move {
            loop {
                let mut len_buf = [0u8; 4];

                if reader.read_exact(&mut len_buf).await.is_err() {
                    break;
                }

                let len = u32::from_be_bytes(len_buf) as usize;

                const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

                if len > MAX_MESSAGE_SIZE {
                    eprintln!(
                        "Message from Node {} is too large: {} bytes",
                        peer_id.id, len
                    );

                    break;
                }

                let mut msg_buf = vec![0u8; len];

                if reader.read_exact(&mut msg_buf).await.is_err() {
                    break;
                }

                match bincode::deserialize::<Message>(&msg_buf) {
                    Ok(msg) => {
                        if node_tx.send(msg).is_err() {
                            break;
                        }
                    }

                    Err(err) => {
                        eprintln!(
                            "Failed to deserialize message from Node {}: {}",
                            peer_id.id, err
                        );

                        break;
                    }
                }
            }

            // Clean up the sender for this peer on disconnect.
            reader_senders.write().unwrap().remove(&peer_id);

            if super::is_log_enabled() {
                println!("Connection to Node {} lost", peer_id.id);
            }
        });
    }
}

