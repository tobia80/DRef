//! Public entry point: [`RaftDRefContext`] — a [`DRefContext`]
//! implementation backed by the Raft-replicated state machine in this
//! crate.
//!
//! Constructing one spawns:
//! - a gRPC server (DRefRaft + RaftInternal services) bound to the
//!   configured port,
//! - the consensus driver loop (heartbeats, elections),
//! - a TTL reaper on the leader that periodically runs
//!   `DeleteIfExpired` for every expired key.
//!
//! Writes are routed to the current leader via gRPC. If the local node
//! IS the leader we still go through the gRPC server (loopback) — this
//! keeps a single code path and matches how the Scala impl works.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use rand::RngExt;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{BroadcastStream, IntervalStream};
use tracing::{debug, warn};

use dref_core::{ChangeEvent, DRefContext, DRefError, StolenElement};

/// Boxed stream alias matching the one in `dref_core::context::BoxStream`
/// (which is module-private). Re-defined here so the trait signatures
/// match without depending on a re-export.
type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;

use crate::config::{NodeEndpoint, RaftConfig};
use crate::consensus::{wait_for_leader, Consensus};
use crate::grpc_client::{ClientError, GrpcClient};
use crate::grpc_server::{DRefConsensusService, DRefRaftService};
use crate::ip_provider;
use crate::proto::dref::d_ref_raft_server::DRefRaftServer;
use crate::proto::dref_consensus::d_ref_consensus_server::DRefConsensusServer;
use crate::state_command::StateCommand;
use crate::state_machine::{unix_millis, StateMachine};

/// Background tasks owned by a single context, joined on drop.
struct Tasks {
    server: JoinHandle<()>,
    consensus_driver: JoinHandle<()>,
    reaper: JoinHandle<()>,
    address_poll: Option<JoinHandle<()>>,
    _shutdown_tx: oneshot::Sender<()>,
}

impl Drop for Tasks {
    fn drop(&mut self) {
        self.server.abort();
        self.consensus_driver.abort();
        self.reaper.abort();
        if let Some(h) = self.address_poll.take() {
            h.abort();
        }
    }
}

/// Cheaply clonable handle to a running Raft node.
///
/// Construction is async (gRPC bind has to happen) so use [`Self::start`]
/// rather than `new`. The returned context is ready to serve reads/writes
/// once a leader has been elected — [`Self::start`] waits for that.
#[derive(Clone)]
pub struct RaftDRefContext {
    inner: Arc<Inner>,
}

struct Inner {
    consensus: Arc<Consensus>,
    client: GrpcClient,
    node_id: String,
    default_ttl: Duration,
    /// All known peer ids including ourselves. Used to round-robin /
    /// re-resolve the leader when forwarding.
    member_ids: Arc<tokio::sync::RwLock<Vec<String>>>,
    /// Kept alive for the lifetime of the context; drop aborts tasks.
    _tasks: Tasks,
}

impl RaftDRefContext {
    /// Start a Raft node and wait for the cluster to elect a leader.
    /// Returns once the gRPC server is bound and the node knows the
    /// current leader id.
    ///
    /// `leader_wait` bounds how long we wait for leader election before
    /// returning anyway (the context is usable, but the first write will
    /// retry until a leader is up).
    pub async fn start(config: RaftConfig, leader_wait: Duration) -> Result<Self, DRefError> {
        let ip_provider = ip_provider::from_env()
            .await
            .map_err(|e| DRefError::Backend(e.to_string()))?;
        let grpc_port = if config.port == 0 {
            ip_provider::port_from_env(8082)
        } else {
            config.port
        };

        // Derive a node id if the caller didn't provide one. Match Scala's
        // "nextLongBetween(0, 99999)" — short, easy to recognize in logs.
        let node_id = config.node_id.clone().unwrap_or_else(|| {
            let mut rng = rand::rng();
            rng.random_range(0u64..99_999).to_string()
        });

        let mut config = config;
        config.port = grpc_port;

        // When no peers are configured, honour DREF_* env vars (k8s, DNS, static).
        if config.initial_endpoints.is_empty() {
            if let Some(ref provider) = ip_provider {
                let ips = provider
                    .find_node_addresses()
                    .await
                    .map_err(|e| DRefError::Backend(e.to_string()))?;
                config.initial_endpoints = ip_provider::node_endpoints_from_ips(&ips, grpc_port);
                if config.bind_address.is_none() {
                    let my_ip = provider
                        .find_my_address()
                        .await
                        .map_err(|e| DRefError::Backend(e.to_string()))?;
                    config.bind_address = Some(format!("{my_ip}:{grpc_port}"));
                }
            }
        }

        // bind_address default mirrors the Scala impl's "localhost:<port>".
        let bind = config
            .bind_address
            .clone()
            .unwrap_or_else(|| format!("127.0.0.1:{}", grpc_port));
        let addr: SocketAddr = bind
            .parse()
            .map_err(|e| DRefError::Backend(format!("invalid bind_address '{bind}': {e}")))?;

        // Re-write the config so the consensus / peer-lookup code can see
        // the resolved bind_address and node id.
        config.bind_address = Some(bind.clone());
        config.node_id = Some(node_id.clone());

        // IP-based discovery uses the IP itself as a placeholder endpoint id,
        // so our own address would otherwise survive into the consensus peer
        // list (whose self-skip matches on node_id) and inflate the quorum.
        // Drop any endpoint pointing at our bind, then add ourselves once
        // under the real node_id.
        config.initial_endpoints.retain(|e| e.address != bind);
        config
            .initial_endpoints
            .push(NodeEndpoint::new(node_id.clone(), bind.clone()));

        let state_machine = StateMachine::new();
        let consensus =
            Arc::new(Consensus::new(node_id.clone(), state_machine.clone(), config.clone()).await);

        // gRPC services. Both share one server.
        let endpoint_ids: Vec<String> = config
            .initial_endpoints
            .iter()
            .map(|e| e.id.clone())
            .collect();
        let dref_service = DRefRaftService::new(consensus.clone(), endpoint_ids.clone());
        let consensus_service = DRefConsensusService::new(consensus.clone());

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            let svc = tonic::transport::Server::builder()
                .add_service(DRefRaftServer::new(dref_service))
                .add_service(DRefConsensusServer::new(consensus_service))
                .serve_with_shutdown(addr, async move {
                    let _ = shutdown_rx.await;
                });
            if let Err(e) = svc.await {
                warn!(error = ?e, "gRPC server exited");
            }
        });

        // Give the server a moment to bind before peers try to connect.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Start the consensus driver (heartbeats + election).
        let consensus_driver = consensus.spawn_drivers();

        // The TTL reaper runs only on the leader. We poll every 200ms,
        // matching the LocalDRefContext reaper cadence; the Scala impl
        // does the same thing every 100ms via `removingExpiringElementsStream`.
        let reaper = {
            let consensus_for_reaper = consensus.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(200));
                loop {
                    tick.tick().await;
                    if !consensus_for_reaper.is_leader().await {
                        continue;
                    }
                    let now = unix_millis();
                    let table = consensus_for_reaper.state_machine.expiration_table().await;
                    for (name, expire_at) in table {
                        if expire_at <= now {
                            let cmd = StateCommand::delete_if_expired(name.clone(), now);
                            if let Err(e) = consensus_for_reaper.submit(cmd).await {
                                debug!(error = ?e, key = %name, "reaper submit failed");
                            }
                        }
                    }
                }
            })
        };

        // gRPC client targets every peer + ourselves; forwarding to "self"
        // is just loopback through the gRPC server.
        let client = GrpcClient::new(config.initial_endpoints.clone(), config.connection_timeout);

        // Discover each peer's real nodeId so consensus-layer ids (set via
        // heartbeat/vote) resolve to a routable address. DNS gave us IP-keyed
        // placeholders; the `GetEndpoints` RPC returns each peer's memberIds
        // ending in its own nodeId, so we alias that id onto the same channel.
        discover_peer_node_aliases(&client, &node_id).await;

        // Wait for a leader (best-effort; we don't fail if the cluster is
        // still electing — first request will retry).
        let _ = wait_for_leader(&consensus, leader_wait).await;

        let member_ids = Arc::new(tokio::sync::RwLock::new(endpoint_ids));
        let address_poll = ip_provider.as_ref().map(|provider| {
            let provider = Arc::clone(provider);
            let client = client.clone();
            let consensus = consensus.clone();
            let member_ids = Arc::clone(&member_ids);
            let node_id = node_id.clone();
            let bind = bind.clone();
            let interval = config.address_poll_interval;
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                loop {
                    tick.tick().await;
                    match provider.find_node_addresses().await {
                        Ok(ips) => {
                            let mut endpoints =
                                ip_provider::node_endpoints_from_ips(&ips, grpc_port);
                            endpoints.retain(|e| e.address != bind);
                            let peer_endpoints = endpoints.clone();
                            endpoints.push(NodeEndpoint::new(node_id.clone(), bind.clone()));
                            client.sync_endpoints(endpoints.clone()).await;
                            consensus.sync_peers(&peer_endpoints).await;
                            *member_ids.write().await =
                                endpoints.into_iter().map(|e| e.id).collect();
                            // Refresh nodeId aliases so rolling cluster changes
                            // (new pods, recycled IPs) stay routable.
                            discover_peer_node_aliases(&client, &node_id).await;
                        }
                        Err(e) => warn!(error = %e, "address poll failed"),
                    }
                }
            })
        });

        let inner = Inner {
            consensus,
            client,
            node_id,
            default_ttl: config.ttl.unwrap_or(Duration::from_secs(20)),
            member_ids,
            _tasks: Tasks {
                server: server_task,
                consensus_driver,
                reaper,
                address_poll,
                _shutdown_tx: shutdown_tx,
            },
        };

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Best-effort discovery of the current leader's id. Used by the
    /// retry helpers.
    async fn current_leader(&self) -> Result<String, DRefError> {
        // First, ask the local consensus.
        if let Some(id) = self.inner.consensus.leader_id().await {
            return Ok(id);
        }
        // Otherwise probe peers in random order: any of them might know.
        let mut ids = self.inner.member_ids.read().await.clone();
        {
            let mut rng = rand::rng();
            for i in (1..ids.len()).rev() {
                let j = rng.random_range(0..=i);
                ids.swap(i, j);
            }
        }
        for id in ids {
            // We don't have a "who is leader" RPC, so we ping for endpoints
            // — any reachable peer would have a more current view.
            if (self.inner.client.get_endpoints(&id).await).is_ok() {
                // If the local consensus learned it during the call,
                // surface it.
                if let Some(l) = self.inner.consensus.leader_id().await {
                    return Ok(l);
                }
            }
        }
        // Still nothing — wait briefly.
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if let Some(l) = self.inner.consensus.leader_id().await {
                return Ok(l);
            }
        }
        Err(DRefError::Backend("no leader elected".to_string()))
    }

    /// Helper: run `op` against the current leader; if it returns
    /// `NotLeader`, re-resolve and retry. `Transport` errors are also
    /// retried a few times (the leader may have just restarted).
    async fn with_leader<F, Fut, T>(&self, mut op: F) -> Result<T, DRefError>
    where
        F: FnMut(GrpcClient, String) -> Fut,
        Fut: std::future::Future<Output = Result<T, ClientError>>,
    {
        // Match Scala's `Schedule.forever.whileInput(LeaderException)` —
        // retry forever on leader changes, but with a small cap for
        // transport errors so a totally offline cluster fails fast.
        let mut transport_attempts: u32 = 0;
        let mut unknown_attempts: u32 = 0;
        loop {
            let leader = self.current_leader().await?;
            let client = self.inner.client.clone();
            match op(client, leader.clone()).await {
                Ok(v) => return Ok(v),
                Err(ClientError::NotLeader(hint)) => {
                    debug!(prev = %leader, hint = ?hint, "leader moved; retrying");
                    // Brief pause so we don't tight-loop during an election.
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    continue;
                }
                Err(ClientError::Transport(msg)) => {
                    transport_attempts += 1;
                    if transport_attempts > 20 {
                        return Err(DRefError::Backend(format!(
                            "transport error talking to leader: {msg}"
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                Err(ClientError::UnknownNode(target)) => {
                    // Followers learn the leader's random nodeId via
                    // heartbeats before the alias-refresh task maps it to a
                    // gRPC channel. Wait briefly so refresh can catch up,
                    // then retry rather than failing the caller with a
                    // transient routing miss.
                    unknown_attempts += 1;
                    if unknown_attempts > 20 {
                        return Err(DRefError::Backend(format!("unknown node id {target}")));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(ClientError::Other(msg)) => {
                    return Err(DRefError::Backend(msg));
                }
            }
        }
    }

    /// Read-side helper: same retry semantics as [`Self::with_leader`].
    async fn get_via_leader(&self, name: &str) -> Result<Option<Vec<u8>>, DRefError> {
        let name = name.to_string();
        self.with_leader(|client, leader| {
            let name = name.clone();
            async move { client.get_element(&leader, &name).await }
        })
        .await
    }
}

fn ttl_to_expire_at(ttl: Option<Duration>) -> Option<u64> {
    ttl.map(|d| unix_millis() + d.as_millis() as u64)
}

/// For every IP-keyed peer in the client map, ask `GetEndpoints` and learn
/// the peer's real nodeId (the last element of the response, by convention
/// shared with the Scala side). Aliases that nodeId onto the same address so
/// consensus-layer ids (leader_id from heartbeats, candidate_id from votes)
/// route to the right peer. Best-effort: peers that aren't yet serving are
/// silently skipped and picked up on the next refresh.
async fn discover_peer_node_aliases(client: &GrpcClient, self_node_id: &str) {
    let entries = client.entries_snapshot().await;
    for (id, address) in entries {
        if id == self_node_id {
            continue;
        }
        match client.get_endpoints(&id).await {
            Ok(ids) if !ids.is_empty() => {
                let real_id = ids.last().unwrap().clone();
                if real_id != id && real_id != self_node_id {
                    client
                        .upsert_endpoint(NodeEndpoint::new(real_id, address))
                        .await;
                }
            }
            _ => {}
        }
    }
}

#[async_trait]
impl DRefContext for RaftDRefContext {
    fn default_ttl(&self) -> Duration {
        self.inner.default_ttl
    }

    async fn set_element(
        &self,
        name: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), DRefError> {
        let name = name.to_string();
        let expire_at = ttl_to_expire_at(ttl);
        self.with_leader(|client, leader| {
            let name = name.clone();
            let value = value.clone();
            async move { client.set_element(&leader, &name, value, expire_at).await }
        })
        .await
    }

    async fn set_element_if_not_exist(
        &self,
        name: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<bool, DRefError> {
        let name = name.to_string();
        let expire_at = ttl_to_expire_at(ttl);
        self.with_leader(|client, leader| {
            let name = name.clone();
            let value = value.clone();
            async move {
                client
                    .set_element_if_not_exist(&leader, &name, value, expire_at)
                    .await
            }
        })
        .await
    }

    async fn get_element(&self, name: &str) -> Result<Option<Vec<u8>>, DRefError> {
        self.get_via_leader(name).await
    }

    async fn delete_element(&self, name: &str) -> Result<(), DRefError> {
        let name = name.to_string();
        self.with_leader(|client, leader| {
            let name = name.clone();
            async move { client.delete_element(&leader, &name).await }
        })
        .await
    }

    fn on_change_stream(&self, name: &str) -> BoxStream<'static, Result<ChangeEvent, DRefError>> {
        // Subscribe to the LOCAL state machine. Followers apply commands
        // too, so a subscriber on any node sees every committed change.
        let rx = self.inner.consensus.state_machine.subscribe();
        let want = name.to_string();
        let s = BroadcastStream::new(rx).filter_map(move |item| {
            let want = want.clone();
            async move {
                match item {
                    Ok(ev) if ev.name() == want => Some(Ok(ev)),
                    Ok(_) => None,
                    Err(e) => Some(Err(DRefError::Backend(format!("change stream lag: {e}")))),
                }
            }
        });
        Box::pin(s)
    }

    fn keep_alive_stream(
        &self,
        name: &str,
        ttl: Duration,
    ) -> BoxStream<'static, Result<(), DRefError>> {
        // Match Scala's `ttl / 1.25` — refresh well before expiry so a slow
        // round-trip doesn't drop the key.
        let period_nanos = (ttl.as_nanos() * 4) / 5;
        let period = Duration::from_nanos(period_nanos.min(u64::MAX as u128) as u64);
        let this = self.clone();
        let name = name.to_string();
        let ticker = tokio::time::interval(period);
        let s = IntervalStream::new(ticker).then(move |_| {
            let this = this.clone();
            let name = name.clone();
            async move {
                let expire_at = unix_millis() + ttl.as_millis() as u64;
                this.with_leader(|client, leader| {
                    let name = name.clone();
                    async move { client.expire_element(&leader, &name, expire_at).await }
                })
                .await
            }
        });
        Box::pin(s)
    }

    fn detect_deletion_from_underlying_stream(
        &self,
        name: &str,
    ) -> BoxStream<'static, Result<ChangeEvent, DRefError>> {
        // Poll every 500ms — same cadence as the Scala impl. Emits a
        // synthetic DeleteElement when the key disappears.
        let this = self.clone();
        let name = name.to_string();
        let ticker = tokio::time::interval(Duration::from_millis(500));
        let s = IntervalStream::new(ticker).filter_map(move |_| {
            let this = this.clone();
            let name = name.clone();
            async move {
                match this.get_via_leader(&name).await {
                    Ok(None) => Some(Ok(ChangeEvent::DeleteElement { name: name.clone() })),
                    Ok(Some(_)) => None,
                    Err(e) => Some(Err(e)),
                }
            }
        });
        Box::pin(s)
    }

    fn detect_stolen_element(
        &self,
        name: &str,
        value: Vec<u8>,
    ) -> BoxStream<'static, Result<StolenElement, DRefError>> {
        let this = self.clone();
        let name = name.to_string();
        let ticker = tokio::time::interval(Duration::from_millis(500));
        let s = IntervalStream::new(ticker).filter_map(move |_| {
            let this = this.clone();
            let name = name.clone();
            let value = value.clone();
            async move {
                match this.get_via_leader(&name).await {
                    // Empty OR mismatched value -> stolen.
                    Ok(None) => Some(Ok(StolenElement { name: name.clone() })),
                    Ok(Some(current)) if current != value => {
                        Some(Ok(StolenElement { name: name.clone() }))
                    }
                    Ok(_) => None,
                    Err(e) => Some(Err(e)),
                }
            }
        });
        Box::pin(s)
    }
}

impl RaftDRefContext {
    /// Stable id of this node, useful for tests and logs.
    pub fn node_id(&self) -> &str {
        &self.inner.node_id
    }

    /// Whether this node currently believes it is the leader.
    pub async fn is_leader(&self) -> bool {
        self.inner.consensus.is_leader().await
    }

    /// Current leader id according to this node.
    pub async fn leader_id(&self) -> Option<String> {
        self.inner.consensus.leader_id().await
    }

    /// Current Raft term according to this node. Exposed so tests can
    /// assert the term remains stable across cluster events.
    pub async fn current_term(&self) -> u64 {
        self.inner.consensus.current_term().await
    }
}
