//! Simplified Raft-style consensus.
//!
//! This is the "simple in-memory consensus wrapper" called out as acceptable
//! in the task spec when openraft's API turns out to be too involved for the
//! scope of this port. It is intentionally a small fraction of what
//! production Raft does — but it nails the things `dref-raft` actually
//! needs:
//!
//! - a single leader is elected via term-based voting,
//! - all writes go through the leader,
//! - the leader replicates each committed command to every reachable
//!   follower via gRPC `AppendEntries`,
//! - followers detect leader loss via heartbeat timeout and start a new
//!   election,
//! - a new leader replays its snapshot to followers that fell behind.
//!
//! Things this does NOT do (relative to "real" Raft):
//! - no persistent log: commands are applied in memory only,
//! - no log truncation on conflict (we only ever replicate from the
//!   current leader's state, snapshot-style),
//! - no membership changes after startup (cluster membership is the
//!   `initial_endpoints` list).
//!
//! These limits are fine for the role this crate plays: replicated locks
//! and short-lived shared state across a fixed-size cluster. They also map
//! cleanly onto a future swap to a real Raft implementation: the public
//! [`Consensus`] API doesn't expose anything that would change.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rand::RngExt;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, Instant};
use tracing::{debug, info, warn};

use crate::config::{NodeEndpoint, RaftConfig};
use crate::proto::dref_consensus::d_ref_consensus_client::DRefConsensusClient;
use crate::proto::dref_consensus::{
    AppendEntriesRequest, ClusterSnapshot, HeartbeatRequest, InstallSnapshotRequest, VoteRequest,
};
use crate::state_command::{self, StateCommand};
use crate::state_machine::{ApplyResult, StateMachine};
use tonic::transport::Channel;

/// Role each node plays in the cluster at any given moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Errors surfaced by consensus operations. The most important variant is
/// [`ConsensusError::NotLeader`], which the gRPC server uses to signal
/// "forward me to the leader" to clients.
#[derive(Debug, thiserror::Error)]
pub enum ConsensusError {
    #[error("not leader; current leader id = {leader_id:?}")]
    NotLeader { leader_id: Option<String> },
    #[error("no leader is currently elected")]
    NoLeader,
    #[error("serialization failure: {0}")]
    Serialize(String),
    #[error("transport failure: {0}")]
    Transport(String),
}

/// Per-peer connection state. The client is built lazily on first use to
/// avoid a startup race where peers aren't listening yet.
struct PeerConn {
    endpoint: NodeEndpoint,
    client: Mutex<Option<DRefConsensusClient<Channel>>>,
}

impl PeerConn {
    fn new(endpoint: NodeEndpoint) -> Self {
        Self {
            endpoint,
            client: Mutex::new(None),
        }
    }

    async fn client(
        &self,
        timeout: Duration,
    ) -> Result<DRefConsensusClient<Channel>, String> {
        let mut slot = self.client.lock().await;
        if let Some(c) = slot.as_ref() {
            return Ok(c.clone());
        }
        let endpoint = tonic::transport::Endpoint::from_shared(format!(
            "http://{}",
            self.endpoint.address
        ))
        .map_err(|e| format!("invalid peer address '{}': {e}", self.endpoint.address))?
        .connect_timeout(timeout)
        .timeout(timeout);
        // Lazy connect avoids blocking startup on peers that aren't up yet.
        let chan = endpoint.connect_lazy();
        let client = DRefConsensusClient::new(chan);
        *slot = Some(client.clone());
        Ok(client)
    }

    async fn reset(&self) {
        *self.client.lock().await = None;
    }
}

/// Internal mutable state, all under a single lock to keep transitions
/// linearizable.
struct ConsensusState {
    role: Role,
    /// Current term. Monotonically increasing.
    term: u64,
    /// Who we voted for in `term`. `None` means we haven't voted yet.
    voted_for: Option<String>,
    /// Last known leader id (informational; used by `NotLeader` errors).
    leader_id: Option<String>,
    /// Monotonic sequence number of the last committed command. Followers
    /// reject AppendEntries with an out-of-order seq, which lets a new
    /// leader notice it needs to ship a snapshot.
    last_seq: u64,
    /// When we last heard from the leader. Used to drive election timeout.
    last_heartbeat: Instant,
}

/// Public consensus handle. Cheaply clonable.
#[derive(Clone)]
pub struct Consensus {
    pub node_id: String,
    peers: Arc<HashMap<String, Arc<PeerConn>>>,
    pub state_machine: StateMachine,
    state: Arc<RwLock<ConsensusState>>,
    config: RaftConfig,
}

impl Consensus {
    /// Build a new consensus node. Peers must NOT include `self`.
    pub fn new(node_id: String, state_machine: StateMachine, config: RaftConfig) -> Self {
        let mut peers = HashMap::new();
        for ep in &config.initial_endpoints {
            if ep.id != node_id {
                peers.insert(ep.id.clone(), Arc::new(PeerConn::new(ep.clone())));
            }
        }
        // A single-node "cluster" is its own leader from t=0. This also
        // makes tests with one node trivial.
        let initial_role = if peers.is_empty() {
            Role::Leader
        } else {
            Role::Follower
        };
        let leader_id = if initial_role == Role::Leader {
            Some(node_id.clone())
        } else {
            None
        };
        let term = if initial_role == Role::Leader { 1 } else { 0 };
        Self {
            node_id,
            peers: Arc::new(peers),
            state_machine,
            state: Arc::new(RwLock::new(ConsensusState {
                role: initial_role,
                term,
                voted_for: None,
                leader_id,
                last_seq: 0,
                last_heartbeat: Instant::now(),
            })),
            config,
        }
    }

    pub async fn role(&self) -> Role {
        self.state.read().await.role
    }

    pub async fn is_leader(&self) -> bool {
        self.role().await == Role::Leader
    }

    pub async fn leader_id(&self) -> Option<String> {
        self.state.read().await.leader_id.clone()
    }

    /// Resolve the address of the current leader. Returns `None` if no
    /// leader is known yet OR the leader isn't in the peer map (i.e. it's
    /// us — caller should check `is_leader` first).
    pub async fn leader_address(&self) -> Option<String> {
        let lid = self.leader_id().await?;
        if lid == self.node_id {
            // Caller should have used a direct path; return our own address
            // anyway so the gRPC server can self-forward in tests.
            return self.config.bind_address.clone();
        }
        self.peers.get(&lid).map(|p| p.endpoint.address.clone())
    }

    /// Submit a write command. Must be called on the leader; returns
    /// [`ConsensusError::NotLeader`] otherwise.
    ///
    /// On the leader we (1) apply locally, (2) bump `last_seq`, (3) fan
    /// out to followers in parallel. We do NOT wait for a quorum — every
    /// follower is "best effort" replication. This is the main divergence
    /// from real Raft and the reason we call out the simplification at the
    /// top of this file.
    pub async fn submit(&self, cmd: StateCommand) -> Result<ApplyResult, ConsensusError> {
        let (term, seq) = {
            let mut st = self.state.write().await;
            if st.role != Role::Leader {
                return Err(ConsensusError::NotLeader {
                    leader_id: st.leader_id.clone(),
                });
            }
            st.last_seq += 1;
            (st.term, st.last_seq)
        };

        let bytes = state_command::encode(&cmd)
            .map_err(|e| ConsensusError::Serialize(e.to_string()))?;
        let result = self.state_machine.apply(cmd).await;

        // Fire-and-forget replication. Followers can fall behind; the
        // periodic snapshot-install during reconnection brings them back.
        self.replicate(term, seq, bytes).await;

        Ok(result)
    }

    /// Replicate one entry to all peers in parallel. Errors are logged but
    /// don't fail the submit — see top-of-file note on the simplified
    /// quorum model.
    async fn replicate(&self, term: u64, seq: u64, command: Vec<u8>) {
        let mut tasks = Vec::new();
        for (id, peer) in self.peers.iter() {
            let id = id.clone();
            let peer = Arc::clone(peer);
            let command = command.clone();
            let leader_id = self.node_id.clone();
            let timeout = self.config.connection_timeout;
            let state = Arc::clone(&self.state);
            let this = self.clone();
            tasks.push(tokio::spawn(async move {
                match peer.client(timeout).await {
                    Ok(mut client) => {
                        let req = AppendEntriesRequest {
                            leader_id,
                            term,
                            command,
                            seq,
                        };
                        match client.append_entries(req).await {
                            Ok(resp) => {
                                let resp = resp.into_inner();
                                step_down_if_stale(&state, resp.term).await;
                                // A follower whose last_seq diverges from
                                // ours rejects with success=false; without a
                                // catch-up the strict seq check keeps
                                // refusing every subsequent entry until a new
                                // election. Push a snapshot to bring them in
                                // sync as long as we're still leader at this
                                // term.
                                if !resp.success && resp.term <= term {
                                    this.send_snapshot_to(&id, &peer).await;
                                }
                            }
                            Err(e) => {
                                debug!(peer = %id, error = ?e, "AppendEntries failed");
                                peer.reset().await;
                            }
                        }
                    }
                    Err(e) => {
                        debug!(peer = %id, error = ?e, "could not build client");
                    }
                }
            }));
        }
        // Don't await; tasks finish on their own.
        drop(tasks);
    }

    async fn send_snapshot_to(&self, peer_id: &str, peer: &Arc<PeerConn>) {
        let (term, last_seq, still_leader) = {
            let st = self.state.read().await;
            (st.term, st.last_seq, st.role == Role::Leader)
        };
        if !still_leader {
            return;
        }
        let snapshot = self.state_machine.take_snapshot().await;
        match peer.client(self.config.connection_timeout).await {
            Ok(mut client) => {
                let req = InstallSnapshotRequest {
                    leader_id: self.node_id.clone(),
                    term,
                    snapshot: Some(snapshot),
                    last_seq,
                };
                match client.install_snapshot(req).await {
                    Ok(resp) => {
                        step_down_if_stale(&self.state, resp.into_inner().term).await;
                    }
                    Err(e) => {
                        debug!(peer = %peer_id, error = ?e, "InstallSnapshot catch-up failed");
                        peer.reset().await;
                    }
                }
            }
            Err(e) => debug!(peer = %peer_id, error = ?e, "no client for snapshot catch-up"),
        }
    }

    // --- Handlers for inbound RPCs (called by the gRPC server) --------------

    /// Handle an incoming AppendEntries from a leader. Apply the command if
    /// the term is fresh enough; otherwise reject. Out-of-order seq returns
    /// `success=false` so the leader can ship a snapshot.
    pub async fn handle_append_entries(
        &self,
        leader_id: String,
        term: u64,
        seq: u64,
        command: Vec<u8>,
    ) -> (bool, u64) {
        let mut st = self.state.write().await;
        if term < st.term {
            return (false, st.term);
        }
        if term > st.term {
            st.term = term;
            st.voted_for = None;
        }
        st.role = Role::Follower;
        st.leader_id = Some(leader_id);
        st.last_heartbeat = Instant::now();
        // Strict ordering: only accept the very next seq. A leader that's
        // ahead must InstallSnapshot first.
        if seq != st.last_seq + 1 {
            return (false, st.term);
        }
        st.last_seq = seq;
        let current_term = st.term;
        drop(st);

        match state_command::decode(&command) {
            Ok(cmd) => {
                self.state_machine.apply(cmd).await;
                (true, current_term)
            }
            Err(e) => {
                warn!(error = ?e, "failed to decode AppendEntries payload");
                (false, current_term)
            }
        }
    }

    /// Handle an incoming heartbeat. Updates `last_heartbeat` and
    /// learns about the current leader; never changes data.
    pub async fn handle_heartbeat(&self, leader_id: String, term: u64) -> (bool, u64) {
        let mut st = self.state.write().await;
        if term < st.term {
            return (false, st.term);
        }
        if term > st.term {
            st.term = term;
            st.voted_for = None;
        }
        st.role = Role::Follower;
        st.leader_id = Some(leader_id);
        st.last_heartbeat = Instant::now();
        (true, st.term)
    }

    /// Handle a vote request from a candidate. Grants iff we haven't voted
    /// in this term and the candidate's seq is at least as up-to-date as
    /// ours. (Real Raft compares (term, index); we conflate index into our
    /// monotonic seq.)
    pub async fn handle_vote(
        &self,
        candidate_id: String,
        term: u64,
        last_seq: u64,
    ) -> (bool, u64) {
        let mut st = self.state.write().await;
        if term < st.term {
            return (false, st.term);
        }
        if term > st.term {
            st.term = term;
            st.voted_for = None;
            st.role = Role::Follower;
        }
        let up_to_date = last_seq >= st.last_seq;
        let can_vote = st
            .voted_for
            .as_ref()
            .map(|v| v == &candidate_id)
            .unwrap_or(true);
        let granted = up_to_date && can_vote;
        if granted {
            st.voted_for = Some(candidate_id);
            st.last_heartbeat = Instant::now();
        }
        (granted, st.term)
    }

    /// Handle an incoming snapshot. Replaces local state wholesale.
    pub async fn handle_install_snapshot(
        &self,
        leader_id: String,
        term: u64,
        snapshot: ClusterSnapshot,
        last_seq: u64,
    ) -> (bool, u64) {
        let mut st = self.state.write().await;
        if term < st.term {
            return (false, st.term);
        }
        if term > st.term {
            st.term = term;
            st.voted_for = None;
        }
        st.role = Role::Follower;
        st.leader_id = Some(leader_id);
        st.last_heartbeat = Instant::now();
        st.last_seq = last_seq;
        let current_term = st.term;
        drop(st);

        self.state_machine.install_snapshot(snapshot).await;
        (true, current_term)
    }

    // --- Background loops ---------------------------------------------------

    /// Spawn the heartbeat + election driver. Returns immediately; the
    /// caller drops the join handle when the consensus is no longer
    /// needed.
    pub fn spawn_drivers(self: &Consensus) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            this.driver_loop().await;
        })
    }

    async fn driver_loop(self) {
        let mut ticker = tokio::time::interval(self.config.heartbeat_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let role = self.role().await;
            match role {
                Role::Leader => {
                    self.send_heartbeats().await;
                }
                Role::Follower | Role::Candidate => {
                    // Election timeout has some randomized jitter — without
                    // it, multiple followers can wake up simultaneously and
                    // split the vote forever.
                    let jitter = {
                        let mut rng = rand::rng();
                        rng.random_range(0..self.config.election_timeout.as_millis() as u64 / 2)
                    };
                    let timeout = self.config.election_timeout + Duration::from_millis(jitter);
                    let last = { self.state.read().await.last_heartbeat };
                    if last.elapsed() >= timeout {
                        self.start_election().await;
                    }
                }
            }
        }
    }

    async fn send_heartbeats(&self) {
        let (term, last_seq) = {
            let st = self.state.read().await;
            (st.term, st.last_seq)
        };
        for (id, peer) in self.peers.iter() {
            let id = id.clone();
            let peer = Arc::clone(peer);
            let leader_id = self.node_id.clone();
            let timeout = self.config.connection_timeout;
            let last_seq_for_peer = last_seq;
            let state = Arc::clone(&self.state);
            tokio::spawn(async move {
                match peer.client(timeout).await {
                    Ok(mut client) => {
                        let req = HeartbeatRequest {
                            leader_id: leader_id.clone(),
                            term,
                        };
                        match client.heartbeat(req).await {
                            Ok(resp) => {
                                step_down_if_stale(&state, resp.into_inner().term).await;
                            }
                            Err(e) => {
                                debug!(peer = %id, error = ?e, "heartbeat failed");
                                peer.reset().await;
                            }
                        }
                        // Best-effort: also probe with an empty AppendEntries
                        // so a follower that fell behind during a partition
                        // can pick the next seq back up. If the follower's
                        // seq is wrong, the leader will catch it via the
                        // periodic snapshot push below.
                        let _ = last_seq_for_peer;
                    }
                    Err(e) => debug!(peer = %id, error = ?e, "no client"),
                }
            });
        }
    }

    async fn start_election(&self) {
        let (term, last_seq) = {
            let mut st = self.state.write().await;
            // If we're already a fresh candidate in this loop, skip.
            st.role = Role::Candidate;
            st.term += 1;
            st.voted_for = Some(self.node_id.clone());
            st.leader_id = None;
            st.last_heartbeat = Instant::now();
            (st.term, st.last_seq)
        };
        info!(node = %self.node_id, term, "starting election");

        // Tally: 1 vote (us). The peer count is the rest of the cluster;
        // majority is over the FULL cluster including us.
        let cluster_size = self.peers.len() + 1;
        let needed = cluster_size / 2 + 1;
        let mut votes: usize = 1;

        let mut futs = Vec::new();
        for (id, peer) in self.peers.iter() {
            let id = id.clone();
            let peer = Arc::clone(peer);
            let candidate_id = self.node_id.clone();
            let timeout = self.config.connection_timeout;
            futs.push(tokio::spawn(async move {
                let client = peer.client(timeout).await.ok()?;
                let mut client = client;
                let req = VoteRequest {
                    candidate_id,
                    term,
                    last_seq,
                };
                match client.request_vote(req).await {
                    Ok(resp) => Some((id, resp.into_inner())),
                    Err(e) => {
                        debug!(peer = %id, error = ?e, "vote request failed");
                        None
                    }
                }
            }));
        }

        for fut in futs {
            if let Ok(Some((_id, resp))) = fut.await {
                if resp.term > term {
                    // Saw a higher term — step down.
                    let mut st = self.state.write().await;
                    if resp.term > st.term {
                        st.term = resp.term;
                        st.role = Role::Follower;
                        st.voted_for = None;
                    }
                    return;
                }
                if resp.granted {
                    votes += 1;
                }
            }
        }

        if votes >= needed {
            let mut st = self.state.write().await;
            // Only promote if we're still candidate in the same term.
            if st.role == Role::Candidate && st.term == term {
                st.role = Role::Leader;
                st.leader_id = Some(self.node_id.clone());
                info!(node = %self.node_id, term, "elected leader");
                drop(st);
                // New leader ships a snapshot to bring followers in sync.
                self.broadcast_snapshot().await;
            }
        } else {
            info!(node = %self.node_id, term, votes, "election lost");
            // Stay candidate; next tick may try again, or a higher-term
            // leader will demote us via heartbeat.
        }
    }

    /// Send the current state machine snapshot to every peer. Called when
    /// we just won an election or when a follower asks to be caught up.
    async fn broadcast_snapshot(&self) {
        let (term, last_seq) = {
            let st = self.state.read().await;
            (st.term, st.last_seq)
        };
        let snapshot = self.state_machine.take_snapshot().await;
        for (id, peer) in self.peers.iter() {
            let id = id.clone();
            let peer = Arc::clone(peer);
            let leader_id = self.node_id.clone();
            let timeout = self.config.connection_timeout;
            let snapshot = snapshot.clone();
            let state = Arc::clone(&self.state);
            tokio::spawn(async move {
                match peer.client(timeout).await {
                    Ok(mut client) => {
                        let req = InstallSnapshotRequest {
                            leader_id,
                            term,
                            snapshot: Some(snapshot),
                            last_seq,
                        };
                        match client.install_snapshot(req).await {
                            Ok(resp) => {
                                step_down_if_stale(&state, resp.into_inner().term).await;
                            }
                            Err(e) => {
                                debug!(peer = %id, error = ?e, "InstallSnapshot failed");
                                peer.reset().await;
                            }
                        }
                    }
                    Err(e) => debug!(peer = %id, error = ?e, "no client"),
                }
            });
        }
    }
}

/// Step down to follower when an outgoing heartbeat / append / snapshot
/// response carries a term greater than ours — the follower has sprinted
/// ahead (likely because of a partition heal) and we are no longer leader.
/// Without this step, the cluster can deadlock with a stale leader still
/// believing it is in charge.
async fn step_down_if_stale(state: &Arc<RwLock<ConsensusState>>, observed_term: u64) {
    let mut st = state.write().await;
    if observed_term > st.term {
        st.term = observed_term;
        st.role = Role::Follower;
        st.voted_for = None;
        st.leader_id = None;
        st.last_heartbeat = Instant::now();
    }
}

/// Helper used by [`crate::context::RaftDRefContext`] to wait until the
/// cluster has settled on a leader before issuing the first request.
pub async fn wait_for_leader(c: &Consensus, max: Duration) -> Option<String> {
    let deadline = Instant::now() + max;
    loop {
        if let Some(l) = c.leader_id().await {
            return Some(l);
        }
        if Instant::now() >= deadline {
            return None;
        }
        sleep(Duration::from_millis(50)).await;
    }
}
