//! In-process, in-memory [`DRefContext`] implementation. Useful for tests and
//! for single-process deployments that just want the API surface.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::{BroadcastStream, IntervalStream};

use crate::context::{BoxStream, ChangeEvent, DRefContext, StolenElement};
use crate::error::DRefError;

#[derive(Debug, Clone)]
struct ExpiringValue {
    value: Vec<u8>,
    /// Wall-clock instant at which this entry expires. `None` means never.
    expire_at: Option<Instant>,
}

#[derive(Debug)]
struct Inner {
    map: Mutex<HashMap<String, ExpiringValue>>,
    /// Broadcasts every change event (we filter per-subscriber by name).
    changes_tx: broadcast::Sender<ChangeEvent>,
}

/// Reference-counted handle to an in-memory store. Cheap to clone.
///
/// Spawns a background task at construction time that sweeps expired entries
/// every 200ms; that task lives until the last `LocalDRefContext` clone is
/// dropped.
#[derive(Clone)]
pub struct LocalDRefContext {
    inner: Arc<Inner>,
}

impl LocalDRefContext {
    /// Create a new empty store with the default TTL reaper running.
    pub fn new() -> Self {
        // Capacity is generous — the receivers filter by name and we don't
        // want spurious lags in normal use.
        let (tx, _) = broadcast::channel(256);
        let inner = Arc::new(Inner {
            map: Mutex::new(HashMap::new()),
            changes_tx: tx,
        });
        spawn_reaper(Arc::clone(&inner));
        Self { inner }
    }

    fn broadcast(&self, event: ChangeEvent) {
        // It's fine if there are no subscribers — broadcast returns an error
        // but we just ignore it.
        let _ = self.inner.changes_tx.send(event);
    }
}

impl Default for LocalDRefContext {
    fn default() -> Self {
        Self::new()
    }
}

fn spawn_reaper(inner: Arc<Inner>) {
    // The reaper holds a weak ref so it doesn't keep the store alive forever.
    let weak = Arc::downgrade(&inner);
    // Drop the strong ref we were handed — caller already has one.
    drop(inner);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(200));
        // First tick fires immediately; skip it so we don't reap before the
        // first writes land.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let Some(inner) = weak.upgrade() else {
                return; // store dropped; exit
            };
            let now = Instant::now();
            let mut map = inner.map.lock().await;
            map.retain(|_k, v| v.expire_at.map(|t| t > now).unwrap_or(true));
        }
    });
}

#[async_trait]
impl DRefContext for LocalDRefContext {
    fn default_ttl(&self) -> Duration {
        Duration::from_secs(5)
    }

    async fn set_element(
        &self,
        name: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), DRefError> {
        let expire_at = ttl.map(|d| Instant::now() + d);
        {
            let mut map = self.inner.map.lock().await;
            map.insert(
                name.to_string(),
                ExpiringValue {
                    value: value.clone(),
                    expire_at,
                },
            );
        }
        self.broadcast(ChangeEvent::SetElement {
            name: name.to_string(),
            value,
        });
        Ok(())
    }

    async fn set_element_if_not_exist(
        &self,
        name: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<bool, DRefError> {
        let expire_at = ttl.map(|d| Instant::now() + d);
        let wrote = {
            let mut map = self.inner.map.lock().await;
            if map.contains_key(name) {
                false
            } else {
                map.insert(
                    name.to_string(),
                    ExpiringValue {
                        value: value.clone(),
                        expire_at,
                    },
                );
                true
            }
        };
        if wrote {
            self.broadcast(ChangeEvent::SetElement {
                name: name.to_string(),
                value,
            });
        }
        Ok(wrote)
    }

    async fn get_element(&self, name: &str) -> Result<Option<Vec<u8>>, DRefError> {
        let map = self.inner.map.lock().await;
        Ok(map.get(name).map(|v| v.value.clone()))
    }

    async fn delete_element(&self, name: &str) -> Result<(), DRefError> {
        {
            let mut map = self.inner.map.lock().await;
            map.remove(name);
        }
        self.broadcast(ChangeEvent::DeleteElement {
            name: name.to_string(),
        });
        Ok(())
    }

    fn on_change_stream(&self, name: &str) -> BoxStream<'static, Result<ChangeEvent, DRefError>> {
        let rx = self.inner.changes_tx.subscribe();
        let want = name.to_string();
        let s = BroadcastStream::new(rx).filter_map(move |item| {
            let want = want.clone();
            async move {
                match item {
                    Ok(ev) if ev.name() == want => Some(Ok(ev)),
                    Ok(_) => None,
                    // BroadcastStream returns Lagged when a slow subscriber
                    // missed events — surface it but don't terminate; the
                    // consumer can decide.
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
        // Refresh more often than the TTL so we don't trip even with jitter.
        // Match Scala's `ttl / 1.25` factor.
        let period_nanos = (ttl.as_nanos() * 4) / 5;
        let period = Duration::from_nanos(period_nanos.min(u64::MAX as u128) as u64);
        let inner = Arc::clone(&self.inner);
        let name = name.to_string();
        let ticker = tokio::time::interval(period);
        let s = IntervalStream::new(ticker).then(move |_| {
            let inner = Arc::clone(&inner);
            let name = name.clone();
            async move {
                let mut map = inner.map.lock().await;
                if let Some(existing) = map.get_mut(&name) {
                    existing.expire_at = Some(Instant::now() + ttl);
                }
                Ok(())
            }
        });
        Box::pin(s)
    }

    fn detect_deletion_from_underlying_stream(
        &self,
        _name: &str,
    ) -> BoxStream<'static, Result<ChangeEvent, DRefError>> {
        // In-memory has no underlying store to observe — never emits, like
        // the Scala version.
        Box::pin(futures::stream::pending())
    }

    fn detect_stolen_element(
        &self,
        _name: &str,
        _value: Vec<u8>,
    ) -> BoxStream<'static, Result<StolenElement, DRefError>> {
        Box::pin(futures::stream::pending())
    }
}

// Allow `Stream` import to compile-test the type alias.
#[allow(dead_code)]
fn _assert_stream<T: Stream>(_: T) {}
