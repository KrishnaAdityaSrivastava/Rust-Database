pub mod election;
pub mod message;
pub mod network;
pub mod node;
pub mod replication;

pub use node::{LogEntry, NodeId, RaftNode, Role};

pub use message::{
    AppendEntries, AppendEntriesResponse, ClientQuery, ClientRequest, ClientResponse, Message,
    RequestVote, RequestVoteResponse, StatusQuery,
};

pub use network::Network;

use std::sync::atomic::{AtomicBool, Ordering};

static LOG_ENABLED: AtomicBool = AtomicBool::new(false);

pub fn enable_logging(enabled: bool) {
    LOG_ENABLED.store(enabled, Ordering::Relaxed);
}

pub(crate) fn is_log_enabled() -> bool {
    LOG_ENABLED.load(Ordering::Relaxed)
        || std::env::var("RAFT_LOG").is_ok()
        || std::env::var("RUST_LOG").is_ok()
}
