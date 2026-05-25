//! Client wrapper around the DRefRaft gRPC service.
//!
//! Ported from `GrpcClient.scala`. Two responsibilities:
//!
//! - look up the right peer connection for a given target endpoint,
//! - on a `NotLeader` response, parse out the new leader id from the
//!   status description, swap targets, and retry. The Scala side uses a
//!   typed `LeaderException`; we use a status-description convention
//!   instead because `tonic::Status` doesn't carry custom payloads
//!   easily.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::debug;

use crate::config::NodeEndpoint;
use crate::proto::dref::d_ref_raft_client::DRefRaftClient;
use crate::proto::dref::{
    DeleteElementRequest, ExpireElementRequest, GetElementRequest, GetEndpointsRequest,
    SetElementIfNotExistRequest, SetElementRequest,
};

/// gRPC client pool keyed by node id. Cheaply cloneable.
#[derive(Clone)]
pub struct GrpcClient {
    /// Map from node id -> (endpoint, lazily-built client).
    inner: Arc<Mutex<HashMap<String, ClientEntry>>>,
    timeout: Duration,
}

#[derive(Clone)]
struct ClientEntry {
    endpoint: NodeEndpoint,
    client: Option<DRefRaftClient<Channel>>,
}

/// Status returned by the leader-forwarding helpers. `Ok` carries the
/// successful payload; `Err` describes the failure mode the caller should
/// react to (retry with a new leader, or surface to the user).
#[derive(Debug)]
pub enum ClientError {
    /// The peer told us it isn't the leader. Carries the leader id the
    /// peer suggested, if any. The caller should re-resolve.
    NotLeader(Option<String>),
    /// Transport-level failure. Likely transient.
    Transport(String),
    /// We don't yet have a gRPC channel for this node id. Followers learn
    /// the leader's random nodeId via heartbeats before the alias-refresh
    /// task maps it to a channel; the caller should back off briefly and
    /// retry while discovery catches up.
    UnknownNode(String),
    /// Other gRPC error (e.g. server-side bug).
    Other(String),
}

impl ClientError {
    fn from_status(s: tonic::Status) -> Self {
        let desc = s.message();
        if let Some(rest) = desc.strip_prefix("not-leader:") {
            let id = if rest.is_empty() || rest == "unknown" {
                None
            } else {
                Some(rest.to_string())
            };
            return ClientError::NotLeader(id);
        }
        if let Some(rest) = desc.strip_prefix("no-leader:") {
            let _ = rest;
            return ClientError::NotLeader(None);
        }
        match s.code() {
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                ClientError::Transport(desc.to_string())
            }
            _ => ClientError::Other(desc.to_string()),
        }
    }
}

impl GrpcClient {
    pub fn new(endpoints: Vec<NodeEndpoint>, timeout: Duration) -> Self {
        let mut map = HashMap::new();
        for ep in endpoints {
            map.insert(
                ep.id.clone(),
                ClientEntry {
                    endpoint: ep,
                    client: None,
                },
            );
        }
        Self {
            inner: Arc::new(Mutex::new(map)),
            timeout,
        }
    }

    /// Build (or fetch cached) gRPC client for a given node id.
    async fn client_for(
        &self,
        target_id: &str,
    ) -> Result<DRefRaftClient<Channel>, ClientError> {
        let mut guard = self.inner.lock().await;
        let entry = guard
            .get_mut(target_id)
            .ok_or_else(|| ClientError::UnknownNode(target_id.to_string()))?;
        if let Some(c) = entry.client.as_ref() {
            return Ok(c.clone());
        }
        let endpoint = tonic::transport::Endpoint::from_shared(format!(
            "http://{}",
            entry.endpoint.address
        ))
        .map_err(|e| ClientError::Transport(format!("bad address: {e}")))?
        .connect_timeout(self.timeout)
        .timeout(self.timeout);
        let chan = endpoint.connect_lazy();
        let c = DRefRaftClient::new(chan);
        entry.client = Some(c.clone());
        Ok(c)
    }

    /// Drop the cached connection for `target_id` after an error so the
    /// next call rebuilds it.
    async fn invalidate(&self, target_id: &str) {
        if let Some(entry) = self.inner.lock().await.get_mut(target_id) {
            entry.client = None;
        }
    }

    /// Add (or update) a node endpoint after discovery.
    pub async fn upsert_endpoint(&self, endpoint: NodeEndpoint) {
        let mut guard = self.inner.lock().await;
        guard.insert(
            endpoint.id.clone(),
            ClientEntry {
                endpoint,
                client: None,
            },
        );
    }

    /// All node ids we currently know about.
    pub async fn known_ids(&self) -> Vec<String> {
        self.inner.lock().await.keys().cloned().collect()
    }

    /// Snapshot of (id, address) for every known entry. Used by the peer
    /// node-id discovery step: we walk the IP-keyed entries built from DNS,
    /// ask each one `GetEndpoints`, and alias the real nodeId returned at
    /// the tail of the response onto the same address.
    pub async fn entries_snapshot(&self) -> Vec<(String, String)> {
        self.inner
            .lock()
            .await
            .iter()
            .map(|(id, entry)| (id.clone(), entry.endpoint.address.clone()))
            .collect()
    }

    pub async fn set_element(
        &self,
        target: &str,
        name: &str,
        value: Vec<u8>,
        expire_at: Option<u64>,
    ) -> Result<(), ClientError> {
        let mut client = self.client_for(target).await?;
        let req = SetElementRequest {
            id: target.to_string(),
            name: name.to_string(),
            value,
            expire_at,
        };
        match client.set_element(req).await {
            Ok(_) => Ok(()),
            Err(s) => {
                let err = ClientError::from_status(s);
                if matches!(err, ClientError::Transport(_)) {
                    self.invalidate(target).await;
                }
                Err(err)
            }
        }
    }

    pub async fn set_element_if_not_exist(
        &self,
        target: &str,
        name: &str,
        value: Vec<u8>,
        expire_at: Option<u64>,
    ) -> Result<bool, ClientError> {
        let mut client = self.client_for(target).await?;
        let req = SetElementIfNotExistRequest {
            id: target.to_string(),
            name: name.to_string(),
            value,
            expire_at,
        };
        match client.set_element_if_not_exist(req).await {
            Ok(r) => Ok(r.into_inner().created),
            Err(s) => {
                let err = ClientError::from_status(s);
                if matches!(err, ClientError::Transport(_)) {
                    self.invalidate(target).await;
                }
                Err(err)
            }
        }
    }

    pub async fn get_element(
        &self,
        target: &str,
        name: &str,
    ) -> Result<Option<Vec<u8>>, ClientError> {
        let mut client = self.client_for(target).await?;
        let req = GetElementRequest {
            id: target.to_string(),
            name: name.to_string(),
        };
        match client.get_element(req).await {
            Ok(r) => Ok(r.into_inner().value),
            Err(s) => {
                let err = ClientError::from_status(s);
                if matches!(err, ClientError::Transport(_)) {
                    self.invalidate(target).await;
                }
                Err(err)
            }
        }
    }

    pub async fn delete_element(&self, target: &str, name: &str) -> Result<(), ClientError> {
        let mut client = self.client_for(target).await?;
        let req = DeleteElementRequest {
            id: target.to_string(),
            name: name.to_string(),
        };
        match client.delete_element(req).await {
            Ok(_) => Ok(()),
            Err(s) => {
                let err = ClientError::from_status(s);
                if matches!(err, ClientError::Transport(_)) {
                    self.invalidate(target).await;
                }
                Err(err)
            }
        }
    }

    pub async fn expire_element(
        &self,
        target: &str,
        name: &str,
        expire_at: u64,
    ) -> Result<(), ClientError> {
        let mut client = self.client_for(target).await?;
        let req = ExpireElementRequest {
            id: target.to_string(),
            name: name.to_string(),
            expire_at,
        };
        match client.expire_element(req).await {
            Ok(_) => Ok(()),
            Err(s) => {
                let err = ClientError::from_status(s);
                if matches!(err, ClientError::Transport(_)) {
                    self.invalidate(target).await;
                }
                Err(err)
            }
        }
    }

    pub async fn get_endpoints(&self, target: &str) -> Result<Vec<String>, ClientError> {
        let mut client = self.client_for(target).await?;
        let req = GetEndpointsRequest {};
        match client.get_endpoints(req).await {
            Ok(r) => Ok(r.into_inner().ids),
            Err(s) => {
                let err = ClientError::from_status(s);
                if matches!(err, ClientError::Transport(_)) {
                    self.invalidate(target).await;
                }
                Err(err)
            }
        }
    }

    #[allow(dead_code)]
    fn _quiet_unused_debug() {
        debug!("noop");
    }
}
