//! Redis-backed [`DRefContext`] implementation.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dref_core::{ChangeEvent, DRefContext, DRefError, StolenElement};
use futures::stream::{Stream, StreamExt};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client, ExistenceCheck, SetExpiry, SetOptions};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::{BroadcastStream, IntervalStream};

/// The Redis pub/sub channel used to broadcast change events. Must match the
/// constant `channel = "dref-change"` in the Scala source.
const CHANGE_CHANNEL: &str = "dref-change";

/// Capacity of the in-process broadcast that fans Redis messages out to
/// per-name subscribers. Matches the Scala `Hub.bounded(64)`.
const BROADCAST_CAPACITY: usize = 64;

/// Local alias matching `dref_core::context::BoxStream` (which isn't
/// re-exported from the crate root).
type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;

/// Configuration for connecting to a Redis backend. Mirrors the Scala
/// `RedisConfig` case class, but only the fields the Rust port actually needs.
#[derive(Debug, Clone)]
pub struct RedisConfig {
    pub host: String,
    pub port: u16,
    pub database: u32,
    pub username: Option<String>,
    pub password: Option<String>,
    /// Path to a CA certificate PEM file. If set, the connection is upgraded
    /// to TLS (`rediss://`). The certificate itself is supplied to the system
    /// trust store at deploy time — we just flip the URL scheme.
    pub ca_cert: Option<String>,
    /// Default TTL for elements written through this context. Falls back to
    /// 20s when unset (same as Scala).
    pub ttl: Option<Duration>,
}

impl RedisConfig {
    fn to_url(&self) -> String {
        let scheme = if self.ca_cert.is_some() { "rediss" } else { "redis" };
        let auth = match (self.username.as_deref(), self.password.as_deref()) {
            (Some(u), Some(p)) => format!("{u}:{p}@"),
            (None, Some(p)) => format!(":{p}@"),
            _ => String::new(),
        };
        format!(
            "{scheme}://{auth}{host}:{port}/{db}",
            host = self.host,
            port = self.port,
            db = self.database,
        )
    }
}

/// Pub/sub payload exchanged with the Scala impl. Field layout must stay
/// binary-compatible with `ChangePayload(name: Chunk[Byte], value: Chunk[Byte],
/// delete: Boolean)` from `RedisDRefContext.scala`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChangePayload {
    name: Vec<u8>,
    value: Vec<u8>,
    delete: bool,
}

impl ChangePayload {
    fn set(name: &str, value: Vec<u8>) -> Self {
        Self { name: name.as_bytes().to_vec(), value, delete: false }
    }
    fn delete(name: &str) -> Self {
        Self { name: name.as_bytes().to_vec(), value: Vec::new(), delete: true }
    }
    fn name_string(&self) -> Option<String> {
        String::from_utf8(self.name.clone()).ok()
    }
    fn encode(&self) -> Result<Vec<u8>, DRefError> {
        // Named-field encoding to match Scala's zio-schema-msg-pack struct maps.
        rmp_serde::to_vec_named(self).map_err(|e| DRefError::Serialize(e.to_string()))
    }
    fn decode(bytes: &[u8]) -> Result<Self, DRefError> {
        rmp_serde::from_slice(bytes).map_err(|e| DRefError::Deserialize(e.to_string()))
    }
}

/// Redis-backed implementation of [`DRefContext`]. Cheap to clone (it's
/// internally an `Arc`).
#[derive(Clone)]
pub struct RedisDRefContext {
    inner: Arc<Inner>,
}

struct Inner {
    config: RedisConfig,
    /// The main command connection. Wrapped in a `Mutex` because
    /// `ConnectionManager` is `Clone` + cheap but we still need exclusive
    /// mut access to call `AsyncCommands`. (Cloning would be fine too; we
    /// keep one shared clone here.)
    conn: Mutex<ConnectionManager>,
    /// Fan-out of decoded `ChangePayload` events from the pub/sub listener.
    changes_tx: broadcast::Sender<ChangePayload>,
    /// Listener task handle — aborted when the last context clone drops.
    listener: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.listener.try_lock() {
            if let Some(h) = guard.take() {
                h.abort();
            }
        }
    }
}

impl RedisDRefContext {
    /// Open a connection to Redis with the given config and start the pub/sub
    /// listener. Returns once the listener is subscribed to `dref-change`.
    pub async fn new(config: RedisConfig) -> Result<Self, DRefError> {
        let url = config.to_url();
        let client = Client::open(url.clone()).map_err(redis_err)?;
        let conn = ConnectionManager::new(client.clone()).await.map_err(redis_err)?;
        let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);
        let tx_for_task = tx.clone();
        let listener = tokio::spawn(async move {
            run_listener(client, tx_for_task).await;
        });
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                conn: Mutex::new(conn),
                changes_tx: tx,
                listener: Mutex::new(Some(listener)),
            }),
        })
    }

    /// Borrow the underlying command connection.
    async fn conn(&self) -> tokio::sync::MutexGuard<'_, ConnectionManager> {
        self.inner.conn.lock().await
    }

    async fn publish(&self, payload: &ChangePayload) -> Result<(), DRefError> {
        let bytes = payload.encode()?;
        let mut conn = self.conn().await;
        let _: i64 = conn
            .publish(CHANGE_CHANNEL, bytes)
            .await
            .map_err(redis_err)?;
        Ok(())
    }
}

#[async_trait]
impl DRefContext for RedisDRefContext {
    fn default_ttl(&self) -> Duration {
        self.inner
            .config
            .ttl
            .unwrap_or_else(|| Duration::from_secs(20))
    }

    async fn set_element(
        &self,
        name: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), DRefError> {
        {
            let mut conn = self.conn().await;
            match ttl {
                Some(d) => {
                    let secs = d.as_secs().max(1);
                    let _: () = conn
                        .set_ex(name, value.clone(), secs)
                        .await
                        .map_err(redis_err)?;
                }
                None => {
                    let _: () = conn
                        .set(name, value.clone())
                        .await
                        .map_err(redis_err)?;
                }
            }
        }
        self.publish(&ChangePayload::set(name, value)).await
    }

    async fn set_element_if_not_exist(
        &self,
        name: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<bool, DRefError> {
        let wrote = {
            let mut conn = self.conn().await;
            let mut opts = SetOptions::default().conditional_set(ExistenceCheck::NX);
            if let Some(d) = ttl {
                opts = opts.with_expiration(SetExpiry::EX(d.as_secs().max(1)));
            }
            let res: Option<String> = conn
                .set_options(name, value.clone(), opts)
                .await
                .map_err(redis_err)?;
            res.is_some()
        };
        if wrote {
            self.publish(&ChangePayload::set(name, value)).await?;
        }
        Ok(wrote)
    }

    async fn get_element(&self, name: &str) -> Result<Option<Vec<u8>>, DRefError> {
        let mut conn = self.conn().await;
        let v: Option<Vec<u8>> = conn.get(name).await.map_err(redis_err)?;
        Ok(v)
    }

    async fn delete_element(&self, name: &str) -> Result<(), DRefError> {
        {
            let mut conn = self.conn().await;
            let _: i64 = conn.del(name).await.map_err(redis_err)?;
        }
        self.publish(&ChangePayload::delete(name)).await
    }

    fn on_change_stream(
        &self,
        name: &str,
    ) -> BoxStream<'static, Result<ChangeEvent, DRefError>> {
        let rx = self.inner.changes_tx.subscribe();
        let want = name.to_string();
        let s = BroadcastStream::new(rx).filter_map(move |item| {
            let want = want.clone();
            async move {
                match item {
                    Ok(p) => match p.name_string() {
                        Some(n) if n == want => Some(Ok(if p.delete {
                            ChangeEvent::DeleteElement { name: n }
                        } else {
                            ChangeEvent::SetElement { name: n, value: p.value }
                        })),
                        _ => None,
                    },
                    Err(e) => {
                        Some(Err(DRefError::Backend(format!("change stream lag: {e}"))))
                    }
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
        // Re-touch the TTL at ttl/1.25 (matches Scala) so we never trip even
        // with scheduler jitter.
        let period_nanos = (ttl.as_nanos() * 4) / 5;
        let period = Duration::from_nanos(period_nanos.min(u64::MAX as u128) as u64);
        let ttl_secs = ttl.as_secs().max(1);
        let inner = Arc::clone(&self.inner);
        let name = name.to_string();
        let ticker = tokio::time::interval(period);
        let s = IntervalStream::new(ticker).then(move |_| {
            let inner = Arc::clone(&inner);
            let name = name.clone();
            async move {
                let mut conn = inner.conn.lock().await;
                let _: bool = conn
                    .expire(&name, ttl_secs as i64)
                    .await
                    .map_err(redis_err)?;
                Ok(())
            }
        });
        Box::pin(s)
    }

    fn detect_deletion_from_underlying_stream(
        &self,
        name: &str,
    ) -> BoxStream<'static, Result<ChangeEvent, DRefError>> {
        let inner = Arc::clone(&self.inner);
        let name = name.to_string();
        // 1s poll; matches `delay(1.second)` in the Scala source.
        let ticker = tokio::time::interval(Duration::from_secs(1));
        let s = IntervalStream::new(ticker).filter_map(move |_| {
            let inner = Arc::clone(&inner);
            let name = name.clone();
            async move {
                let mut conn = inner.conn.lock().await;
                let v: Option<Vec<u8>> = match conn.get(&name).await {
                    Ok(v) => v,
                    Err(e) => return Some(Err(redis_err(e))),
                };
                if v.is_none() {
                    Some(Ok(ChangeEvent::DeleteElement { name: name.clone() }))
                } else {
                    None
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
        let inner = Arc::clone(&self.inner);
        let name = name.to_string();
        let ticker = tokio::time::interval(Duration::from_secs(1));
        let s = IntervalStream::new(ticker).filter_map(move |_| {
            let inner = Arc::clone(&inner);
            let name = name.clone();
            let value = value.clone();
            async move {
                let mut conn = inner.conn.lock().await;
                let v: Option<Vec<u8>> = match conn.get(&name).await {
                    Ok(v) => v,
                    Err(e) => return Some(Err(redis_err(e))),
                };
                // Scala: `result.forall(el => !equals(el, value))` — emit when
                // the stored bytes differ from `value`. A missing key counts
                // as "not stolen" (matches Scala's `forall` on `Option`).
                match v {
                    Some(stored) if stored != value => {
                        Some(Ok(StolenElement { name: name.clone() }))
                    }
                    _ => None,
                }
            }
        });
        Box::pin(s)
    }
}

/// Background task: subscribe to the `dref-change` Redis channel and fan
/// decoded payloads into the in-process broadcast.
///
/// On error the loop logs and exits — the task is short-lived per session and
/// dropped with the context. (We don't auto-reconnect here; matches Scala's
/// `forkDaemon` semantics, where the supervising scope tears it down.)
async fn run_listener(client: Client, tx: broadcast::Sender<ChangePayload>) {
    let mut pubsub = match client.get_async_pubsub().await {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(error = %e, "redis pubsub connect failed");
            return;
        }
    };
    if let Err(e) = pubsub.subscribe(CHANGE_CHANNEL).await {
        tracing::error!(error = %e, "redis pubsub subscribe failed");
        return;
    }
    let mut stream = pubsub.on_message();
    while let Some(msg) = stream.next().await {
        let bytes: Vec<u8> = match msg.get_payload() {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "redis pubsub payload decode failed");
                continue;
            }
        };
        match ChangePayload::decode(&bytes) {
            Ok(p) => {
                tracing::debug!(name = ?p.name, delete = p.delete, "publishing event to broadcast");
                let _ = tx.send(p);
            }
            Err(e) => {
                tracing::warn!(error = %e, "cannot decode redis notification");
            }
        }
    }
}

fn redis_err(e: redis::RedisError) -> DRefError {
    DRefError::Backend(format!("redis: {e}"))
}
