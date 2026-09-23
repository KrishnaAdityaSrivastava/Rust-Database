pub mod election;
pub mod message;
pub mod network;
pub mod node;
pub mod replication;

pub use node::{LogEntry, NodeId, RaftNode, Role};

pub use message::{
    AppendEntries, AppendEntriesResponse, Message, RequestVote, RequestVoteResponse,
};

pub use network::Network;

pub(crate) fn is_log_enabled() -> bool {
    std::env::var("RAFT_LOG").is_ok()
}
