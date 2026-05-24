//! Replicated state machine: an in-memory expiring key/value store.
//!
//! Ported from `DRefStateMachine.scala` in the Scala source. The Scala
//! version implements MicroRaft's `StateMachine` trait; here we expose a
//! simple `apply_command` entry point that the consensus layer calls when
//! a log entry is committed. Like the Scala state machine we:
//!
//! - keep a `HashMap<String, ExpiringValue>`
//! - emit `ChangeEvent`s on a broadcast channel for `on_change_stream`
//!   subscribers
//! - support snapshot take/install via msgpack
//!
//! The Scala `KVEntry` proto is replicated here as a serde-friendly
//! struct so snapshots can move between nodes as plain msgpack bytes.

use std::collections::HashMap;
use std::sync::Arc;

use dref_core::ChangeEvent;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, RwLock};

use crate::command::DRefCommand;

/// In-memory value with an absolute expiry deadline (unix millis). `None`
/// means "no TTL".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpiringValue {
    pub value: Vec<u8>,
    pub expire_at: Option<u64>,
}

/// A single entry in a snapshot. Wire-compatible (msgpack with named
/// fields) with what the Scala `KVEntry` proto carries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KVEntry {
    pub key: String,
    pub value: Vec<u8>,
    pub expire_at: Option<u64>,
}

/// Result of applying a single command. Most commands return `Unit`; only
/// `SetElementIfNotExist` and `GetElement` carry data back to the caller.
#[derive(Debug, Clone)]
pub enum ApplyResult {
    Unit,
    Created(bool),
    Value(Option<Vec<u8>>),
}

/// Cheaply clonable handle to the replicated state. Reads take a read
/// lock; writes take a write lock and broadcast a change event.
#[derive(Clone)]
pub struct StateMachine {
    inner: Arc<RwLock<HashMap<String, ExpiringValue>>>,
    /// Broadcast channel for change events. We use the same `ChangeEvent`
    /// enum as `dref-core` so subscribers to `on_change_stream` see the
    /// same shape regardless of which backend they use.
    changes_tx: broadcast::Sender<ChangeEvent>,
}

impl Default for StateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl StateMachine {
    pub fn new() -> Self {
        // 256 mirrors the capacity used by LocalDRefContext — generous, but
        // change-stream subscribers filter by name so most traffic is
        // discarded by the consumer anyway.
        let (tx, _) = broadcast::channel(256);
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            changes_tx: tx,
        }
    }

    /// Subscribe to change events. Each subscriber gets its own receiver
    /// and filters by name.
    pub fn subscribe(&self) -> broadcast::Receiver<ChangeEvent> {
        self.changes_tx.subscribe()
    }

    /// Apply a committed command. This is the only mutation entry point.
    /// Broadcasts a `ChangeEvent` when the command actually changes the
    /// observable state (matches Scala behaviour: `expireElement` and
    /// `setElementIfNotExist`-with-existing don't emit).
    pub async fn apply(&self, cmd: DRefCommand) -> ApplyResult {
        match cmd {
            DRefCommand::SetElement {
                name,
                value,
                expire_at,
            } => {
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
            DRefCommand::SetElementIfNotExist {
                name,
                value,
                expire_at,
            } => {
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
            DRefCommand::DeleteElement { name } => {
                let mut map = self.inner.write().await;
                map.remove(&name);
                drop(map);
                let _ = self
                    .changes_tx
                    .send(ChangeEvent::DeleteElement { name });
                ApplyResult::Unit
            }
            DRefCommand::ExpireElement { name, expire_at } => {
                let mut map = self.inner.write().await;
                if let Some(v) = map.get_mut(&name) {
                    v.expire_at = Some(expire_at);
                }
                // No change-event broadcast: TTL refresh isn't an
                // observable mutation for `on_change_stream` consumers.
                ApplyResult::Unit
            }
            DRefCommand::DeleteIfExpired { name, now } => {
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
            DRefCommand::StartNewTerm => ApplyResult::Unit,
        }
    }

    /// Read the value at `name`. Returns `None` for missing OR expired
    /// entries — the reaper will eventually remove the expired entry.
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

    /// Return a snapshot of all expiring entries (name -> expire_at). Used
    /// by the leader's TTL reaper.
    pub async fn expiration_table(&self) -> HashMap<String, u64> {
        let map = self.inner.read().await;
        map.iter()
            .filter_map(|(k, v)| v.expire_at.map(|t| (k.clone(), t)))
            .collect()
    }

    /// Encode the entire store as msgpack-encoded `Vec<KVEntry>` for
    /// snapshot transfer. Mirrors `takeSnapshot` in `DRefStateMachine.scala`,
    /// except we ship one blob instead of chunking — the consumers we
    /// support (tests, small clusters) don't need chunking.
    pub async fn take_snapshot(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        let map = self.inner.read().await;
        let entries: Vec<KVEntry> = map
            .iter()
            .map(|(k, v)| KVEntry {
                key: k.clone(),
                value: v.value.clone(),
                expire_at: v.expire_at,
            })
            .collect();
        rmp_serde::to_vec_named(&entries)
    }

    /// Replace the entire store from a snapshot. Mirrors `installSnapshot`.
    pub async fn install_snapshot(
        &self,
        bytes: &[u8],
    ) -> Result<(), rmp_serde::decode::Error> {
        let entries: Vec<KVEntry> = rmp_serde::from_slice(bytes)?;
        let mut map = self.inner.write().await;
        map.clear();
        for entry in entries {
            map.insert(
                entry.key,
                ExpiringValue {
                    value: entry.value,
                    expire_at: entry.expire_at,
                },
            );
        }
        Ok(())
    }
}

/// Returns the current wall-clock time in unix milliseconds. Used in TTL
/// arithmetic. We use `SystemTime::now` rather than `Instant` because TTLs
/// are absolute deadlines replicated across nodes.
pub fn unix_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
