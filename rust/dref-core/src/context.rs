//! `DRefContext`: the pluggable backend abstraction. A backend stores
//! opaque byte values keyed by string name, supports TTLs, change
//! notifications, keep-alives, and lock-stealing/deletion detection.
//!
//! In the Scala source this is a single trait with many `ZStream`-returning
//! methods. In Rust we pin the streams as `BoxStream` so the trait stays
//! object-safe.

use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;
use futures::Stream;

use crate::error::DRefError;

/// A change observed on the underlying store. Mirrors the Scala
/// `ChangeEvent` enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeEvent {
    /// A value was written (either fresh or updated).
    SetElement { name: String, value: Vec<u8> },
    /// A value was deleted.
    DeleteElement { name: String },
}

impl ChangeEvent {
    pub fn name(&self) -> &str {
        match self {
            ChangeEvent::SetElement { name, .. } | ChangeEvent::DeleteElement { name } => name,
        }
    }
}

/// Marker emitted by [`DRefContext::detect_stolen_element`] when the value at
/// `name` no longer matches the value we wrote (i.e. somebody else owns the
/// lock now).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StolenElement {
    pub name: String,
}

/// Boxed stream type used across the API.
pub type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;

#[async_trait]
pub trait DRefContext: Send + Sync {
    /// Default lock/element TTL the backend recommends.
    fn default_ttl(&self) -> Duration;

    async fn set_element(
        &self,
        name: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), DRefError>;

    /// Atomically write `value` at `name` iff no entry exists for `name`.
    /// Returns whether we wrote.
    async fn set_element_if_not_exist(
        &self,
        name: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<bool, DRefError>;

    async fn get_element(&self, name: &str) -> Result<Option<Vec<u8>>, DRefError>;

    async fn delete_element(&self, name: &str) -> Result<(), DRefError>;

    /// Stream of all `ChangeEvent`s observed for the given `name`.
    fn on_change_stream(&self, name: &str) -> BoxStream<'static, Result<ChangeEvent, DRefError>>;

    /// Re-touches the TTL of `name` on a schedule (yielding `()` per tick).
    fn keep_alive_stream(
        &self,
        name: &str,
        ttl: Duration,
    ) -> BoxStream<'static, Result<(), DRefError>>;

    /// Backend-specific: emit a delete event when the entry disappears from
    /// the underlying store (e.g. Redis keyspace notification, Raft state
    /// change). The default in-memory backend never emits.
    fn detect_deletion_from_underlying_stream(
        &self,
        name: &str,
    ) -> BoxStream<'static, Result<ChangeEvent, DRefError>>;

    /// Backend-specific: emit when the value at `name` no longer matches the
    /// one we wrote. The default in-memory backend never emits.
    fn detect_stolen_element(
        &self,
        name: &str,
        value: Vec<u8>,
    ) -> BoxStream<'static, Result<StolenElement, DRefError>>;
}
