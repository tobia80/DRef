//! Raft-style consensus with quorum commit and linearizable reads.
//!
//! This is a pragmatic in-memory Raft implementation for replicated locks
//! and short-lived shared state. It provides:
//!
//! - term-based leader election with PreVote,
//! - quorum-committed writes (ack only after majority replication),
//! - ReadIndex-style linearizable reads on the leader,
//! - snapshot catch-up for lagging followers.
//!
//! Relative to full Raft (Ongaro §5–§7):
//! - append-only command log (`command-log`, magic DRFL) fsync'd before
//!   replication acks; truncated when snapshots catch up,
//! - no log truncation on conflict (strict monotonic `seq` + snapshot
//!   install for divergence),
//! - membership refreshed via [`Consensus::sync_peers`] when an
//!   [`IpProvider`] is configured.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use rand::RngExt;
use std::sync::Arc as StdArc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, Instant};
use tracing::{debug, info, warn};

use crate::command_log_store::{
    CommandLogStore, FileCommandLogStore, NoopCommandLogStore,
};
use crate::config::{NodeEndpoint, RaftConfig};
use crate::proto::dref_consensus::d_ref_consensus_client::DRefConsensusClient;
use crate::proto::dref_consensus::{
    AppendEntriesRequest, ClusterSnapshot, HeartbeatRequest, InstallSnapshotRequest,
    PreVoteRequest, ReadIndexRequest, VoteRequest,
};
use crate::state_command::{self, StateCommand};
use crate::state_machine::{ApplyResult, StateMachine};
use crate::state_machine_snapshot_store::{
    FileStateMachineSnapshotStore, NoopStateMachineSnapshotStore, StateMachineSnapshotStore,
};
use crate::voter_state_store::{
    FileVoterStateStore, NoopVoterStateStore, VoterState, VoterStateStore,
};
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
    #[error("write could not be replicated to a quorum")]
    QuorumLost,
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

    async fn client(&self, timeout: Duration) -> Result<DRefConsensusClient<Channel>, String> {
        let mut slot = self.client.lock().await;
        if let Some(c) = slot.as_ref() {
            return Ok(c.clone());
        }
        let endpoint =
            tonic::transport::Endpoint::from_shared(format!("http://{}", self.endpoint.address))
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
    /// Monotonic sequence number of the last replicated command.
    last_seq: u64,
    /// Highest sequence known committed on a majority.
    commit_seq: u64,
    /// Highest sequence applied to the state machine.
    last_applied: u64,
    /// When we last heard from the leader. Used to drive election timeout.
    last_heartbeat: Instant,
}

/// Public consensus handle. Cheaply clonable.
#[derive(Clone)]
pub struct Consensus {
    pub node_id: String,
    peers: Arc<RwLock<HashMap<String, Arc<PeerConn>>>>,
    pub state_machine: StateMachine,
    state: Arc<RwLock<ConsensusState>>,
    pending: Arc<RwLock<BTreeMap<u64, Vec<u8>>>>,
    command_log_store: StdArc<dyn CommandLogStore>,
    config: RaftConfig,
    voter_store: StdArc<dyn VoterStateStore>,
    persist_mutex: Arc<Mutex<()>>,
    log_mutex: Arc<Mutex<()>>,
    snapshot_store: StdArc<dyn StateMachineSnapshotStore>,
    snapshot_mutex: Arc<Mutex<()>>,
    applies_since_snapshot: Arc<std::sync::atomic::AtomicU64>,
}

impl Consensus {
    /// Build a new consensus node. Peers must NOT include `self`.
    ///
    /// Async because the state machine is rehydrated from any on-disk snapshot before peers come
    /// online — we don't want to serve empty reads or accept appends with a stale `last_seq`
    /// baseline while we wait for the leader to ship a fresh snapshot.
    pub async fn new(node_id: String, state_machine: StateMachine, config: RaftConfig) -> Self {
        let voter_store: StdArc<dyn VoterStateStore> = match &config.storage_dir {
            Some(dir) => StdArc::new(
                FileVoterStateStore::open(dir).expect("create voter-state storage directory"),
            ),
            None => StdArc::new(NoopVoterStateStore),
        };
        let snapshot_store: StdArc<dyn StateMachineSnapshotStore> = match &config.storage_dir {
            Some(dir) => StdArc::new(
                FileStateMachineSnapshotStore::open(dir)
                    .expect("create state-machine snapshot storage directory"),
            ),
            None => StdArc::new(NoopStateMachineSnapshotStore),
        };
        let command_log_store: StdArc<dyn CommandLogStore> = match &config.storage_dir {
            Some(dir) => {
                StdArc::new(FileCommandLogStore::open(dir).expect("create command-log storage directory"))
            }
            None => StdArc::new(NoopCommandLogStore),
        };
        let persisted = snapshot_store
            .load()
            .expect("load persisted state-machine snapshot");
        if let Some(snapshot) = persisted.clone() {
            state_machine.install_snapshot(snapshot).await;
        }
        let snapshot_last_seq = persisted.as_ref().map(|s| s.last_seq).unwrap_or(0);
        let log_state = command_log_store
            .load()
            .expect("load persisted command log");
        let max_log_seq = log_state.entries.keys().max().copied().unwrap_or(0);
        let recovered_commit = log_state.commit_seq.min(max_log_seq);
        let initial_commit_seq = snapshot_last_seq.max(recovered_commit);
        let initial_last_seq = snapshot_last_seq.max(max_log_seq);
        for seq in (snapshot_last_seq + 1)..=initial_commit_seq {
            if let Some(bytes) = log_state.entries.get(&seq) {
                if let Ok(cmd) = state_command::decode(bytes) {
                    state_machine.apply(cmd).await;
                }
            }
        }
        let initial_pending: BTreeMap<u64, Vec<u8>> = log_state
            .entries
            .into_iter()
            .filter(|(seq, _)| *seq > initial_commit_seq)
            .collect();
        let loaded = voter_store.load().expect("load persisted voter state");
        let mut peers = HashMap::new();
        for ep in &config.initial_endpoints {
            if ep.id != node_id {
                peers.insert(ep.id.clone(), Arc::new(PeerConn::new(ep.clone())));
            }
        }
        let has_persisted = loaded.term > 0 || loaded.voted_for.is_some();
        // A single-node cluster bootstraps as leader only when there is no
        // prior on-disk state — otherwise run the election path.
        let initial_role = if peers.is_empty() && !has_persisted {
            Role::Leader
        } else {
            Role::Follower
        };
        let leader_id = if initial_role == Role::Leader {
            Some(node_id.clone())
        } else {
            None
        };
        let initial_term = if initial_role == Role::Leader {
            1
        } else {
            loaded.term
        };
        let initial_voted_for = if initial_role == Role::Leader {
            None
        } else {
            loaded.voted_for.clone()
        };
        if initial_term != loaded.term || initial_voted_for != loaded.voted_for {
            voter_store
                .save(&VoterState {
                    term: initial_term,
                    voted_for: initial_voted_for.clone(),
                })
                .expect("persist bootstrap voter state");
        }
        Self {
            node_id,
            peers: Arc::new(RwLock::new(peers)),
            state_machine,
            state: Arc::new(RwLock::new(ConsensusState {
                role: initial_role,
                term: initial_term,
                voted_for: initial_voted_for,
                leader_id,
                last_seq: initial_last_seq,
                commit_seq: initial_commit_seq,
                last_applied: initial_commit_seq,
                last_heartbeat: Instant::now(),
            })),
            pending: Arc::new(RwLock::new(initial_pending)),
            command_log_store,
            config,
            voter_store,
            persist_mutex: Arc::new(Mutex::new(())),
            log_mutex: Arc::new(Mutex::new(())),
            snapshot_store,
            snapshot_mutex: Arc::new(Mutex::new(())),
            applies_since_snapshot: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    async fn with_command_log<F, R>(&self, op: F) -> R
    where
        F: FnOnce(&dyn CommandLogStore) -> R,
    {
        let _guard = self.log_mutex.lock().await;
        op(self.command_log_store.as_ref())
    }

    /// Align the consensus peer map with the current discovery snapshot.
    /// Endpoints must not include this node (callers filter by node id / bind).
    pub async fn sync_peers(&self, endpoints: &[NodeEndpoint]) {
        let desired: HashSet<String> = endpoints.iter().map(|e| e.id.clone()).collect();
        let mut peers = self.peers.write().await;
        peers.retain(|id, _| desired.contains(id));
        for ep in endpoints {
            peers
                .entry(ep.id.clone())
                .or_insert_with(|| Arc::new(PeerConn::new(ep.clone())));
        }
    }

    /// Mutate consensus state and fsync `(term, votedFor)` when either changes.
    async fn update_and_persist<A, F>(&self, f: F) -> A
    where
        F: FnOnce(ConsensusState) -> (A, ConsensusState),
    {
        let _guard = self.persist_mutex.lock().await;
        let mut st = self.state.write().await;
        let before_term = st.term;
        let before_vote = st.voted_for.clone();
        let current = ConsensusState {
            role: st.role,
            term: st.term,
            voted_for: st.voted_for.clone(),
            leader_id: st.leader_id.clone(),
            last_seq: st.last_seq,
            commit_seq: st.commit_seq,
            last_applied: st.last_applied,
            last_heartbeat: st.last_heartbeat,
        };
        let (result, next) = f(current);
        *st = next;
        let after_term = st.term;
        let after_vote = st.voted_for.clone();
        drop(st);
        if after_term != before_term || after_vote != before_vote {
            self.voter_store
                .save(&VoterState {
                    term: after_term,
                    voted_for: after_vote,
                })
                .expect("persist voter state");
        }
        result
    }

    /// Record that a command was successfully applied. When the running tally crosses the
    /// configured `snapshot_every` threshold, spawn a background task that fsyncs a snapshot to
    /// disk. The write is spawned rather than awaited: snapshotting is a *durability*
    /// optimisation, so slow disk must not stretch replication latency.
    async fn note_applied(&self) {
        if self.config.snapshot_every == 0 {
            return;
        }
        let threshold = self.config.snapshot_every as u64;
        let next = self
            .applies_since_snapshot
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        if next >= threshold {
            // Reset by subtracting `next` so concurrent increments are accounted for.
            self.applies_since_snapshot
                .fetch_sub(next, std::sync::atomic::Ordering::Relaxed);
            let this = self.clone();
            tokio::spawn(async move {
                if let Err(e) = this.persist_snapshot_now().await {
                    warn!(error = ?e, "state-machine snapshot save failed");
                }
            });
        }
    }

    /// Take a snapshot of the state machine and fsync it to disk. Serialised so concurrent
    /// triggers don't both write — the second waits, then takes a fresh snapshot itself.
    async fn persist_snapshot_now(&self) -> Result<(), String> {
        let _guard = self.snapshot_mutex.lock().await;
        let commit_seq = self.state.read().await.commit_seq;
        let last_seq = self.state.read().await.last_seq;
        let mut snapshot = self.state_machine.take_snapshot().await;
        snapshot.last_seq = last_seq;
        self.snapshot_store
            .save(&snapshot)
            .map_err(|e| e.to_string())?;
        self.with_command_log(|log| {
            log.truncate_through(commit_seq)
                .map_err(|e| e.to_string())
        })
        .await
    }

    /// Force a snapshot persist now. Exposed for tests and graceful shutdown.
    pub async fn take_and_persist_snapshot(&self) -> Result<(), String> {
        self.persist_snapshot_now().await
    }

    /// Exposed for integration tests that assert stale-leader demotion.
    #[doc(hidden)]
    pub async fn test_step_down_if_stale(&self, observed_term: u64) {
        self.step_down_if_stale(observed_term).await
    }

    async fn step_down_if_stale(&self, observed_term: u64) {
        self.update_and_persist(|mut st| {
            if observed_term > st.term {
                st.term = observed_term;
                st.role = Role::Follower;
                st.voted_for = None;
                st.leader_id = None;
                st.last_heartbeat = Instant::now();
            }
            ((), st)
        })
        .await;
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

    /// Current Raft term as this node sees it. Exposed for tests that need
    /// to assert the term does not spike across cluster events (e.g.
    /// follower restart with PreVote enabled).
    pub async fn current_term(&self) -> u64 {
        self.state.read().await.term
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
        self.peers
            .read()
            .await
            .get(&lid)
            .map(|p| p.endpoint.address.clone())
    }

    fn quorum_needed(cluster_size: usize) -> usize {
        cluster_size / 2 + 1
    }

    async fn apply_committed(&self, commit_seq: u64) {
        let start = {
            let st = self.state.read().await;
            if commit_seq <= st.commit_seq {
                return;
            }
            st.last_applied + 1
        };
        for seq in start..=commit_seq {
            let bytes = {
                let pending = self.pending.read().await;
                pending.get(&seq).cloned()
            };
            let Some(bytes) = bytes else {
                warn!(seq, commit_seq, "missing pending entry while applying commit");
                return;
            };
            match state_command::decode(&bytes) {
                Ok(cmd) => {
                    self.state_machine.apply(cmd).await;
                    self.note_applied().await;
                    {
                        let mut st = self.state.write().await;
                        st.last_applied = seq;
                    }
                    self.pending.write().await.remove(&seq);
                }
                Err(e) => {
                    warn!(error = ?e, seq, "failed to decode pending entry");
                    return;
                }
            }
        }
        {
            let mut st = self.state.write().await;
            if commit_seq > st.commit_seq {
                st.commit_seq = commit_seq;
            }
        }
        let _ = self
            .with_command_log(|log| log.set_commit_seq(commit_seq))
            .await;
    }

    async fn propagate_commit_seq(&self, commit_seq: u64, term: u64) {
        let peers: Vec<(String, Arc<PeerConn>)> = self
            .peers
            .read()
            .await
            .iter()
            .map(|(id, p)| (id.clone(), Arc::clone(p)))
            .collect();
        let mut futs = Vec::new();
        for (id, peer) in peers {
            let leader_id = self.node_id.clone();
            let timeout = self.config.connection_timeout;
            let this = self.clone();
            futs.push(tokio::spawn(async move {
                match peer.client(timeout).await {
                    Ok(mut client) => {
                        let req = HeartbeatRequest {
                            leader_id,
                            term,
                            commit_seq,
                        };
                        match client.heartbeat(req).await {
                            Ok(resp) => {
                                this.step_down_if_stale(resp.into_inner().term).await;
                            }
                            Err(e) => {
                                debug!(peer = %id, error = ?e, "commit heartbeat failed");
                                peer.reset().await;
                            }
                        }
                    }
                    Err(e) => debug!(peer = %id, error = ?e, "no client for commit heartbeat"),
                }
            }));
        }
        for fut in futs {
            let _ = fut.await;
        }
    }

    /// Submit a write command. Must be called on the leader; returns
    /// [`ConsensusError::NotLeader`] otherwise. The write is acknowledged
    /// only after a quorum of nodes has stored the entry.
    pub async fn submit(&self, cmd: StateCommand) -> Result<ApplyResult, ConsensusError> {
        let (term, seq, commit_seq_before) = {
            let mut st = self.state.write().await;
            if st.role != Role::Leader {
                return Err(ConsensusError::NotLeader {
                    leader_id: st.leader_id.clone(),
                });
            }
            st.last_seq += 1;
            (st.term, st.last_seq, st.commit_seq)
        };

        let bytes =
            state_command::encode(&cmd).map_err(|e| ConsensusError::Serialize(e.to_string()))?;
        self.with_command_log(|log| {
            log.append(seq, &bytes)
                .map_err(|e| ConsensusError::Serialize(e.to_string()))
        })
        .await?;
        let peers: Vec<(String, Arc<PeerConn>)> = self
            .peers
            .read()
            .await
            .iter()
            .map(|(id, p)| (id.clone(), Arc::clone(p)))
            .collect();
        let needed = Self::quorum_needed(peers.len() + 1);
        let follower_acks = self
            .replicate_for_quorum(term, seq, bytes, commit_seq_before, peers)
            .await;
        let total_acks = 1 + follower_acks;
        if total_acks < needed {
            let _ = self
                .with_command_log(|log| log.truncate_through(seq - 1))
                .await;
            let mut st = self.state.write().await;
            st.last_seq = seq - 1;
            return Err(ConsensusError::QuorumLost);
        }
        if !self.is_leader().await {
            return Err(ConsensusError::NotLeader {
                leader_id: self.leader_id().await,
            });
        }

        let result = self.state_machine.apply(cmd).await;
        self.note_applied().await;
        {
            let mut st = self.state.write().await;
            st.commit_seq = seq;
            st.last_applied = seq;
        }
        let _ = self
            .with_command_log(|log| log.set_commit_seq(seq))
            .await;
        self.propagate_commit_seq(seq, term).await;
        Ok(result)
    }

    /// Confirm leadership with a quorum before serving a linearizable read.
    pub async fn read_index(&self) -> Result<(), ConsensusError> {
        let (term, leader_id, commit_seq, last_applied) = {
            let st = self.state.read().await;
            if st.role != Role::Leader {
                return Err(ConsensusError::NotLeader {
                    leader_id: st.leader_id.clone(),
                });
            }
            (
                st.term,
                st.leader_id.clone(),
                st.commit_seq,
                st.last_applied,
            )
        };

        let peers: Vec<(String, Arc<PeerConn>)> = self
            .peers
            .read()
            .await
            .iter()
            .map(|(id, p)| (id.clone(), Arc::clone(p)))
            .collect();
        let needed = Self::quorum_needed(peers.len() + 1);
        let mut grants: usize = 1;
        let mut futs = Vec::new();
        for (id, peer) in peers {
            let leader_id = self.node_id.clone();
            let timeout = self.config.connection_timeout;
            futs.push(tokio::spawn(async move {
                let mut client = peer.client(timeout).await.ok()?;
                let req = ReadIndexRequest {
                    leader_id,
                    term,
                };
                match client.read_index(req).await {
                    Ok(resp) => Some((id, resp.into_inner())),
                    Err(e) => {
                        debug!(peer = %id, error = ?e, "read-index request failed");
                        None
                    }
                }
            }));
        }
        for fut in futs {
            if let Ok(Some((_id, resp))) = fut.await {
                if resp.term > term {
                    self.step_down_if_stale(resp.term).await;
                    return Err(ConsensusError::NotLeader { leader_id: None });
                }
                if resp.granted {
                    grants += 1;
                }
            }
        }
        if grants < needed {
            return Err(ConsensusError::NotLeader { leader_id });
        }
        if last_applied < commit_seq {
            return Err(ConsensusError::NotLeader { leader_id: None });
        }
        Ok(())
    }

    async fn replicate_for_quorum(
        &self,
        term: u64,
        seq: u64,
        command: Vec<u8>,
        commit_seq: u64,
        peers: Vec<(String, Arc<PeerConn>)>,
    ) -> usize {
        let mut futs = Vec::new();
        for (id, peer) in peers {
            let command = command.clone();
            let leader_id = self.node_id.clone();
            let timeout = self.config.connection_timeout;
            let this = self.clone();
            futs.push(tokio::spawn(async move {
                match peer.client(timeout).await {
                    Ok(mut client) => {
                        let req = AppendEntriesRequest {
                            leader_id,
                            term,
                            command,
                            seq,
                            commit_seq,
                        };
                        match client.append_entries(req).await {
                            Ok(resp) => {
                                let resp = resp.into_inner();
                                this.step_down_if_stale(resp.term).await;
                                if !resp.success && resp.term <= term {
                                    this.send_snapshot_to(&id, &peer).await;
                                }
                                resp.success
                            }
                            Err(e) => {
                                debug!(peer = %id, error = ?e, "AppendEntries failed");
                                peer.reset().await;
                                false
                            }
                        }
                    }
                    Err(e) => {
                        debug!(peer = %id, error = ?e, "could not build client");
                        false
                    }
                }
            }));
        }
        let mut acks = 0usize;
        for fut in futs {
            if let Ok(true) = fut.await {
                acks += 1;
            }
        }
        acks
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
                        self.step_down_if_stale(resp.into_inner().term).await;
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

    /// Handle an incoming AppendEntries from a leader. Stores the command
    /// in the pending buffer and applies it once the leader's commit index
    /// covers this sequence.
    pub async fn handle_append_entries(
        &self,
        leader_id: String,
        term: u64,
        seq: u64,
        command: Vec<u8>,
        commit_seq: u64,
    ) -> (bool, u64) {
        let (accepted, current_term) = self
            .update_and_persist(|mut st| {
                if term < st.term {
                    return ((false, st.term), st);
                }
                if term > st.term {
                    st.term = term;
                    st.voted_for = None;
                }
                st.role = Role::Follower;
                st.leader_id = Some(leader_id.clone());
                st.last_heartbeat = Instant::now();
                if seq != st.last_seq + 1 {
                    return ((false, st.term), st);
                }
                st.last_seq = seq;
                ((true, st.term), st)
            })
            .await;

        if !accepted {
            return (false, current_term);
        }

        if self
            .with_command_log(|log| log.append(seq, &command))
            .await
            .is_err()
        {
            return (false, current_term);
        }
        self.pending.write().await.insert(seq, command);
        self.apply_committed(commit_seq).await;
        (true, current_term)
    }

    /// Handle an incoming heartbeat. Updates `last_heartbeat`, learns about
    /// the current leader, and applies any newly committed entries.
    pub async fn handle_heartbeat(
        &self,
        leader_id: String,
        term: u64,
        commit_seq: u64,
    ) -> (bool, u64) {
        let (acknowledged, current_term) = self
            .update_and_persist(|mut st| {
                if term < st.term {
                    return ((false, st.term), st);
                }
                if term > st.term {
                    st.term = term;
                    st.voted_for = None;
                }
                st.role = Role::Follower;
                st.leader_id = Some(leader_id);
                st.last_heartbeat = Instant::now();
                ((true, st.term), st)
            })
            .await;

        if acknowledged {
            self.apply_committed(commit_seq).await;
        }
        (acknowledged, current_term)
    }

    /// Follower ack for a ReadIndex probe — confirms the requester is still
    /// the leader we know.
    pub async fn handle_read_index(&self, leader_id: String, term: u64) -> (bool, u64) {
        let st = self.state.read().await;
        if term < st.term {
            return (false, st.term);
        }
        if st.leader_id.as_deref() == Some(leader_id.as_str()) && term == st.term {
            return (true, st.term);
        }
        (false, st.term)
    }

    /// Handle a PreVote request. PreVote (Ongaro thesis §9.6) is a
    /// "would-you-vote-for-me" query that runs BEFORE the candidate bumps
    /// its term. The voter:
    ///
    /// 1. does NOT change its own term or `voted_for` — granting a PreVote
    ///    is just a hypothetical answer, so there is nothing to persist;
    /// 2. refuses if it has heard from a leader within the election
    ///    timeout — that's the whole point: a node that lost contact with
    ///    the cluster and keeps incrementing its term in the background
    ///    must not be able to force a real election that disrupts the
    ///    current leader once it rejoins;
    /// 3. otherwise grants iff the candidate's `last_seq` is at least as
    ///    up-to-date as ours AND the candidate's proposed term (the term
    ///    it would enter) is strictly greater than our current term.
    ///
    /// The returned `term` is always our current term — the voter never
    /// adopts the candidate's hypothetical term from a PreVote.
    pub async fn handle_pre_vote(
        &self,
        _candidate_id: String,
        term: u64,
        last_seq: u64,
    ) -> (bool, u64) {
        let st = self.state.read().await;
        // Stale candidate: its proposed term doesn't even beat ours.
        if term <= st.term {
            return (false, st.term);
        }
        // Leader-stickiness: a node that still believes it is leading the
        // cluster must refuse pre-votes outright — granting one would
        // amount to volunteering its own demotion. A leader that has gone
        // stale will only learn so when a real AppendEntries/Heartbeat
        // response comes back with a higher term; until then it trusts
        // its own role. For followers, the recency check on the last
        // heartbeat plays the same role — if we've heard from a leader
        // within the election timeout, the cluster is healthy and we
        // shouldn't help an isolated candidate disrupt it.
        let leader_recent = st.last_heartbeat.elapsed() < self.config.election_timeout;
        let is_active_leader = st.role == Role::Leader;
        if is_active_leader || (leader_recent && st.leader_id.is_some()) {
            return (false, st.term);
        }
        let up_to_date = last_seq >= st.last_seq;
        (up_to_date, st.term)
    }

    /// Handle a vote request from a candidate. Grants iff we haven't voted
    /// in this term and the candidate's seq is at least as up-to-date as
    /// ours. (Real Raft compares (term, index); we conflate index into our
    /// monotonic seq.)
    pub async fn handle_vote(&self, candidate_id: String, term: u64, last_seq: u64) -> (bool, u64) {
        self.update_and_persist(|mut st| {
            if term < st.term {
                return ((false, st.term), st);
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
            ((granted, st.term), st)
        })
        .await
    }

    /// Handle an incoming snapshot. Replaces local state wholesale.
    pub async fn handle_install_snapshot(
        &self,
        leader_id: String,
        term: u64,
        snapshot: ClusterSnapshot,
        last_seq: u64,
    ) -> (bool, u64) {
        let (accepted, current_term) = self
            .update_and_persist(|mut st| {
                if term < st.term {
                    return ((false, st.term), st);
                }
                if term > st.term {
                    st.term = term;
                    st.voted_for = None;
                }
                st.role = Role::Follower;
                st.leader_id = Some(leader_id);
                st.last_heartbeat = Instant::now();
                st.last_seq = last_seq;
                ((true, st.term), st)
            })
            .await;

        if accepted {
            // Installing a snapshot replaces the whole state machine. Reset the applies-counter
            // and durably re-write the snapshot so a restart picks up the freshly-received state
            // rather than the leader's stale `last_seq` gap.
            let mut durable_snapshot = snapshot;
            durable_snapshot.last_seq = last_seq;
            let _ = self
                .with_command_log(|log| log.truncate_through(last_seq))
                .await;
            self.pending.write().await.clear();
            self.state_machine
                .install_snapshot(durable_snapshot.clone())
                .await;
            {
                let mut st = self.state.write().await;
                st.commit_seq = last_seq;
                st.last_applied = last_seq;
            }
            self.applies_since_snapshot
                .store(0, std::sync::atomic::Ordering::Relaxed);
            let this = self.clone();
            tokio::spawn(async move {
                if let Err(e) = this.persist_snapshot_now().await {
                    warn!(error = ?e, "state-machine snapshot save (post-install) failed");
                }
            });
        }
        (accepted, current_term)
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
        let (term, commit_seq) = {
            let st = self.state.read().await;
            (st.term, st.commit_seq)
        };
        let peers: Vec<(String, Arc<PeerConn>)> = self
            .peers
            .read()
            .await
            .iter()
            .map(|(id, p)| (id.clone(), Arc::clone(p)))
            .collect();
        for (id, peer) in peers {
            let leader_id = self.node_id.clone();
            let timeout = self.config.connection_timeout;
            let this = self.clone();
            tokio::spawn(async move {
                match peer.client(timeout).await {
                    Ok(mut client) => {
                        let req = HeartbeatRequest {
                            leader_id: leader_id.clone(),
                            term,
                            commit_seq,
                        };
                        match client.heartbeat(req).await {
                            Ok(resp) => {
                                this.step_down_if_stale(resp.into_inner().term).await;
                            }
                            Err(e) => {
                                debug!(peer = %id, error = ?e, "heartbeat failed");
                                peer.reset().await;
                            }
                        }
                    }
                    Err(e) => debug!(peer = %id, error = ?e, "no client"),
                }
            });
        }
    }

    /// Run a PreVote round before bumping our term. If we can't win a
    /// majority of pre-votes, we stay follower and avoid disturbing the
    /// current leader's term. Returns `true` iff we should proceed to a
    /// real election.
    ///
    /// PreVote uses our CURRENT term + 1 as the hypothetical term, but
    /// does NOT mutate our state. A single-node cluster trivially wins.
    async fn run_pre_vote(&self) -> bool {
        let (current_term, last_seq) = {
            let st = self.state.read().await;
            (st.term, st.last_seq)
        };
        let proposed_term = current_term + 1;
        let peers: Vec<(String, Arc<PeerConn>)> = self
            .peers
            .read()
            .await
            .iter()
            .map(|(id, p)| (id.clone(), Arc::clone(p)))
            .collect();
        let cluster_size = peers.len() + 1;
        let needed = cluster_size / 2 + 1;
        // We always pre-vote for ourselves.
        let mut grants: usize = 1;
        if grants >= needed {
            return true;
        }

        let mut futs = Vec::new();
        for (id, peer) in peers {
            let candidate_id = self.node_id.clone();
            let timeout = self.config.connection_timeout;
            futs.push(tokio::spawn(async move {
                let mut client = peer.client(timeout).await.ok()?;
                let req = PreVoteRequest {
                    candidate_id,
                    term: proposed_term,
                    last_seq,
                };
                match client.request_pre_vote(req).await {
                    Ok(resp) => Some((id, resp.into_inner())),
                    Err(e) => {
                        debug!(peer = %id, error = ?e, "pre-vote request failed");
                        None
                    }
                }
            }));
        }

        for fut in futs {
            if let Ok(Some((_id, resp))) = fut.await {
                // PreVote responses can carry a strictly-greater term if a
                // peer has already advanced past our current_term. Treat
                // that as a step-down signal — a real election would just
                // lose to the same higher-term holder.
                if resp.term > current_term {
                    self.step_down_if_stale(resp.term).await;
                    return false;
                }
                if resp.granted {
                    grants += 1;
                }
            }
        }
        grants >= needed
    }

    async fn start_election(&self) {
        // PreVote gate: only proceed if a quorum says they'd vote for us
        // right now. This prevents a partitioned node that keeps timing
        // out from incrementing its term forever and disrupting the
        // cluster the moment its network heals.
        if !self.run_pre_vote().await {
            debug!(node = %self.node_id, "pre-vote did not pass; staying follower");
            // Refresh the heartbeat clock so we don't immediately spin
            // into another pre-vote attempt on the next tick.
            let mut st = self.state.write().await;
            st.last_heartbeat = Instant::now();
            return;
        }

        let (term, last_seq) = self
            .update_and_persist(|mut st| {
                st.role = Role::Candidate;
                st.term += 1;
                st.voted_for = Some(self.node_id.clone());
                st.leader_id = None;
                st.last_heartbeat = Instant::now();
                ((st.term, st.last_seq), st)
            })
            .await;
        info!(node = %self.node_id, term, "starting election");

        // Tally: 1 vote (us). The peer count is the rest of the cluster;
        // majority is over the FULL cluster including us.
        let peers: Vec<(String, Arc<PeerConn>)> = self
            .peers
            .read()
            .await
            .iter()
            .map(|(id, p)| (id.clone(), Arc::clone(p)))
            .collect();
        let cluster_size = peers.len() + 1;
        let needed = cluster_size / 2 + 1;
        let mut votes: usize = 1;

        let mut futs = Vec::new();
        for (id, peer) in peers {
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
                    self.step_down_if_stale(resp.term).await;
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
        let peers: Vec<(String, Arc<PeerConn>)> = self
            .peers
            .read()
            .await
            .iter()
            .map(|(id, p)| (id.clone(), Arc::clone(p)))
            .collect();
        for (id, peer) in peers {
            let leader_id = self.node_id.clone();
            let timeout = self.config.connection_timeout;
            let snapshot = snapshot.clone();
            let this = self.clone();
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
                                this.step_down_if_stale(resp.into_inner().term).await;
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
