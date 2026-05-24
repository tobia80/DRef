//! Replicated state machine: an in-memory expiring key/value store.
//!
//! Applies protobuf [`StateCommand`] entries replicated through
//! [`DRefConsensus`](crate::proto::dref_consensus).

use std::collections::HashMap;
use std::sync::Arc;

use dref_core::ChangeEvent;
use tokio::sync::{broadcast, RwLock};

use crate::proto::dref_consensus::{ClusterSnapshot, KvEntry};
use crate::state_command::{
    state_command, DeleteElementCommand, DeleteIfExpiredCommand, ExpireElementCommand,
    SetElementCommand, SetElementIfNotExistCommand, StateCommand,
};

/// In-memory value with an absolute expiry deadline (unix millis). `None`
/// means "no TTL".
#[derive(Debug, Clone)]
pub struct ExpiringValue {
    pub value: Vec<u8>,
    pub expire_at: Option<u64>,
}

/// Result of applying a single command.
#[derive(Debug, Clone)]
pub enum ApplyResult {
    Unit,
    Created(bool),
    Value(Option<Vec<u8>>),
}

/// Cheaply clonable handle to the replicated state.
#[derive(Clone)]
pub struct StateMachine {
    inner: Arc<RwLock<HashMap<String, ExpiringValue>>>,
    changes_tx: broadcast::Sender<ChangeEvent>,
}

impl Default for StateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl StateMachine {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(256);
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            changes_tx: tx,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ChangeEvent> {
        self.changes_tx.subscribe()
    }

    pub async fn apply(&self, cmd: StateCommand) -> ApplyResult {
        match cmd.op {
            Some(state_command::Op::SetElement(SetElementCommand {
                name,
                value,
                expire_at,
            })) => {
                let mut map = self.inner.write().await;
                map.insert(
                    name.clone(),
                    ExpiringValue {
                        value: value.clone(),
                        expire_at,
                    },
                );
                drop(map);
                let _ = self
                    .changes_tx
                    .send(ChangeEvent::SetElement { name, value });
                ApplyResult::Unit
            }
            Some(state_command::Op::SetElementIfNotExist(SetElementIfNotExistCommand {
                name,
                value,
                expire_at,
            })) => {
                let mut map = self.inner.write().await;
                if map.contains_key(&name) {
                    ApplyResult::Created(false)
                } else {
                    map.insert(
                        name.clone(),
                        ExpiringValue {
                            value: value.clone(),
                            expire_at,
                        },
                    );
                    drop(map);
                    let _ = self
                        .changes_tx
                        .send(ChangeEvent::SetElement { name, value });
                    ApplyResult::Created(true)
                }
            }
            Some(state_command::Op::DeleteElement(DeleteElementCommand { name })) => {
                let mut map = self.inner.write().await;
                map.remove(&name);
                drop(map);
                let _ = self
                    .changes_tx
                    .send(ChangeEvent::DeleteElement { name });
                ApplyResult::Unit
            }
            Some(state_command::Op::ExpireElement(ExpireElementCommand { name, expire_at })) => {
                let mut map = self.inner.write().await;
                if let Some(v) = map.get_mut(&name) {
                    v.expire_at = Some(expire_at);
                }
                ApplyResult::Unit
            }
            Some(state_command::Op::DeleteIfExpired(DeleteIfExpiredCommand { name, now })) => {
                let mut map = self.inner.write().await;
                let should_delete = map
                    .get(&name)
                    .and_then(|v| v.expire_at)
                    .map(|t| t <= now)
                    .unwrap_or(false);
                if should_delete {
                    map.remove(&name);
                    drop(map);
                    let _ = self
                        .changes_tx
                        .send(ChangeEvent::DeleteElement { name });
                }
                ApplyResult::Unit
            }
            Some(state_command::Op::StartNewTerm(_)) | None => ApplyResult::Unit,
        }
    }

    pub async fn get(&self, name: &str) -> Option<Vec<u8>> {
        let map = self.inner.read().await;
        let now = unix_millis();
        map.get(name).and_then(|v| {
            if v.expire_at.map(|t| t <= now).unwrap_or(false) {
                None
            } else {
                Some(v.value.clone())
            }
        })
    }

    pub async fn expiration_table(&self) -> HashMap<String, u64> {
        let map = self.inner.read().await;
        map.iter()
            .filter_map(|(k, v)| v.expire_at.map(|t| (k.clone(), t)))
            .collect()
    }

    pub async fn take_snapshot(&self) -> ClusterSnapshot {
        let map = self.inner.read().await;
        let entries = map
            .iter()
            .map(|(k, v)| KvEntry {
                key: k.clone(),
                value: v.value.clone(),
                expire_at: v.expire_at,
            })
            .collect();
        ClusterSnapshot { entries }
    }

    pub async fn install_snapshot(&self, snapshot: ClusterSnapshot) {
        let mut map = self.inner.write().await;
        map.clear();
        for entry in snapshot.entries {
            map.insert(
                entry.key,
                ExpiringValue {
                    value: entry.value,
                    expire_at: entry.expire_at,
                },
            );
        }
    }
}

pub fn unix_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
