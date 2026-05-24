//! Configuration types for [`crate::RaftDRefContext`].

use std::time::Duration;

/// Address of a node in the cluster. `id` is a stable identifier (typically
/// short string like "node-1") and `address` is the host:port reachable via
/// gRPC (e.g. `127.0.0.1:50051`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeEndpoint {
    pub id: String,
    pub address: String,
}

impl NodeEndpoint {
    pub fn new<S: Into<String>>(id: S, address: S) -> Self {
        Self {
            id: id.into(),
            address: address.into(),
        }
    }
}

/// Runtime configuration for a Raft-backed DRef node.
///
/// Mirrors `RaftConfig` in the Scala source: a port to listen on, an
/// optional default TTL, and timers controlling leader-discovery polling.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// gRPC port this node should bind to. The address advertised to peers
    /// is `127.0.0.1:<port>` unless `bind_address` is set.
    pub port: u16,
    /// Optional explicit bind/advertise address. Defaults to
    /// `127.0.0.1:<port>` (useful for tests).
    pub bind_address: Option<String>,
    /// Stable node id. If unset, a random id is generated at construction.
    pub node_id: Option<String>,
    /// Default TTL for elements written without an explicit one.
    pub ttl: Option<Duration>,
    /// How often each node polls peers for liveness and membership.
    pub address_poll_interval: Duration,
    /// Per-RPC connection timeout.
    pub connection_timeout: Duration,
    /// Election timeout — how long without a heartbeat a follower waits
    /// before starting an election.
    pub election_timeout: Duration,
    /// Heartbeat interval — how often the leader pings followers.
    pub heartbeat_interval: Duration,
    /// Initial cluster membership. The first node should be reachable so
    /// joining nodes can discover the leader.
    pub initial_endpoints: Vec<NodeEndpoint>,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            port: 0,
            bind_address: None,
            node_id: None,
            ttl: Some(Duration::from_secs(10)),
            address_poll_interval: Duration::from_secs(3),
            connection_timeout: Duration::from_secs(5),
            election_timeout: Duration::from_millis(1500),
            heartbeat_interval: Duration::from_millis(300),
            initial_endpoints: Vec::new(),
        }
    }
}
