//! The user-facing [`DRef`] type and its supporting [`IdProvider`].

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use futures::Stream;
use futures::StreamExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::codec::{DRefCodec, MsgPackCodec};
use crate::context::{ChangeEvent, DRefContext};
use crate::error::DRefError;
use crate::lock::lock_with_context;

/// How the name of a [`DRef`] is derived.
#[derive(Debug, Clone)]
pub enum IdProvider {
    /// Derive name from caller location (file + line, SHA-256 hashed).
    AutoId,
    /// Use the supplied string verbatim.
    ManualId(String),
}

/// Compute the auto-name for a caller location. SHA-256 over
/// `"<file>:<line>"`, lowercase hex. Matches the Scala impl's intent (which
/// hashed a richer "trace+stack" string — we only have file/line in stable
/// Rust without a debugger).
pub(crate) fn auto_name_for(loc: &std::panic::Location<'_>) -> String {
    let key = format!("{}:{}", loc.file(), loc.line());
    let digest = Sha256::digest(key.as_bytes());
    format!("{}:{} ({})", loc.file(), loc.line(), hex::encode(digest))
}

/// A distributed reference. Backed by a [`DRefContext`] (which provides the
/// underlying key/value store) and a [`DRefCodec`] (which converts `T` to/from
/// bytes).
///
/// Cheap to clone — internally it's a small `Arc`.
pub struct DRef<T, C = crate::LocalDRefContext>
where
    T: Send + Sync + 'static,
    C: DRefContext + Clone + Send + Sync + 'static,
{
    inner: Arc<DRefInner<T, C>>,
}

struct DRefInner<T, C>
where
    T: Send + Sync + 'static,
    C: DRefContext + Clone + Send + Sync + 'static,
{
    context: C,
    name: String,
    codec: Arc<dyn DRefCodec<T>>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T, C> Clone for DRef<T, C>
where
    T: Send + Sync + 'static,
    C: DRefContext + Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T, C> DRef<T, C>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
    C: DRefContext + Clone + Send + Sync + 'static,
{
    /// Create a new distributed reference with the default MsgPack codec. The
    /// name is auto-derived from the caller's `file:line`.
    ///
    /// If a value already exists under this name in the backend, it is
    /// preserved; otherwise the closure `init` is called and its result is
    /// stored as the seed value.
    #[track_caller]
    pub fn make<F>(
        context: &C,
        init: F,
    ) -> impl std::future::Future<Output = Result<Self, DRefError>> + Send
    where
        F: FnOnce() -> T + Send + 'static,
        C: 'static,
    {
        // Capture caller location synchronously — `#[track_caller]` is a no-op
        // through `async fn`, so we wrap in a sync fn and forward the
        // already-resolved name into an inner async block.
        let loc = std::panic::Location::caller();
        let name = auto_name_for(loc);
        let context = context.clone();
        async move { Self::make_with_resolved_name(&context, name, init).await }
    }

    /// Like [`Self::make`] but with an explicit name (use this if you want a
    /// stable name across builds or call sites).
    pub async fn make_with_name<F>(
        context: &C,
        name: impl Into<String>,
        init: F,
    ) -> Result<Self, DRefError>
    where
        F: FnOnce() -> T,
    {
        Self::make_with_resolved_name(context, name.into(), init).await
    }

    async fn make_with_resolved_name<F>(
        context: &C,
        name: String,
        init: F,
    ) -> Result<Self, DRefError>
    where
        F: FnOnce() -> T,
    {
        let codec: Arc<dyn DRefCodec<T>> = Arc::new(MsgPackCodec::<T>::new());
        let bytes = codec.serialize(&init())?;
        // Seed the value only if it doesn't already exist (matches Scala).
        context
            .set_element_if_not_exist(&name, bytes, None)
            .await?;
        Ok(Self {
            inner: Arc::new(DRefInner {
                context: context.clone(),
                name,
                codec,
                _phantom: PhantomData,
            }),
        })
    }
}

impl<T, C> DRef<T, C>
where
    T: Send + Sync + 'static,
    C: DRefContext + Clone + Send + Sync + 'static,
{
    /// The fully-qualified name of this ref in the backend.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub async fn get(&self) -> Result<T, DRefError> {
        match self.inner.context.get_element(&self.inner.name).await? {
            Some(bytes) => self.inner.codec.deserialize(&bytes),
            None => Err(DRefError::NotFound(self.inner.name.clone())),
        }
    }

    pub async fn set(&self, value: T) -> Result<(), DRefError> {
        let bytes = self.inner.codec.serialize(&value)?;
        self.inner
            .context
            .set_element(&self.inner.name, bytes, None)
            .await
    }

    pub async fn set_if_not_exist(&self, value: T) -> Result<bool, DRefError> {
        let bytes = self.inner.codec.serialize(&value)?;
        self.inner
            .context
            .set_element_if_not_exist(&self.inner.name, bytes, None)
            .await
    }

    /// Stream of decoded values, one per `SetElement` event for our name.
    /// Delete events are skipped (we don't have a value to decode).
    pub fn change_stream(&self) -> impl Stream<Item = Result<T, DRefError>> + Send + 'static
    where
        T: 'static,
    {
        let raw = self.inner.context.on_change_stream(&self.inner.name);
        let codec = Arc::clone(&self.inner.codec);
        raw.filter_map(move |item| {
            let codec = Arc::clone(&codec);
            async move {
                match item {
                    Ok(ChangeEvent::SetElement { value, .. }) => Some(codec.deserialize(&value)),
                    Ok(ChangeEvent::DeleteElement { .. }) => None,
                    Err(e) => Some(Err(e)),
                }
            }
        })
    }

    /// Spawn a task that invokes `f` for every change event. Returns a
    /// `JoinHandle` so the caller can abort it.
    pub fn on_change<F, Fut>(&self, mut f: F) -> tokio::task::JoinHandle<()>
    where
        F: FnMut(T) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
        T: 'static,
    {
        let mut s = Box::pin(self.change_stream());
        tokio::spawn(async move {
            while let Some(item) = s.next().await {
                if let Ok(v) = item {
                    f(v).await;
                }
            }
        })
    }

    /// Atomically read, transform, write. Returns the `B` chosen by `f`.
    /// Serialised via a distributed lock keyed by `lock:<name>`.
    pub async fn modify<B, F>(&self, f: F) -> Result<B, DRefError>
    where
        F: FnOnce(T) -> (B, T) + Send + 'static,
        B: Send + 'static,
        T: Send + 'static,
    {
        let lock_name = format!("lock:{}", self.inner.name);
        let this = self.clone();
        lock_with_context(
            &self.inner.context,
            IdProvider::ManualId(lock_name),
            move || async move {
                let current = this.get().await?;
                let (b, new_value) = f(current);
                this.set(new_value).await?;
                Ok::<B, DRefError>(b)
            },
        )
        .await
    }

    /// Async version of [`modify`].
    pub async fn modify_async<B, F, Fut>(&self, f: F) -> Result<B, DRefError>
    where
        F: FnOnce(T) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(B, T), DRefError>> + Send + 'static,
        B: Send + 'static,
        T: Send + 'static,
    {
        let lock_name = format!("lock:{}", self.inner.name);
        let this = self.clone();
        lock_with_context(
            &self.inner.context,
            IdProvider::ManualId(lock_name),
            move || async move {
                let current = this.get().await?;
                let (b, new_value) = f(current).await?;
                this.set(new_value).await?;
                Ok::<B, DRefError>(b)
            },
        )
        .await
    }

    pub async fn get_and_update<F>(&self, f: F) -> Result<T, DRefError>
    where
        F: FnOnce(T) -> T + Send + 'static,
        T: Clone + Send + 'static,
    {
        self.modify(move |v: T| {
            let old = v.clone();
            (old, f(v))
        })
        .await
    }

    pub async fn update<F>(&self, f: F) -> Result<(), DRefError>
    where
        F: FnOnce(T) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.modify(move |v: T| ((), f(v))).await
    }

    pub async fn update_and_get<F>(&self, f: F) -> Result<T, DRefError>
    where
        F: FnOnce(T) -> T + Send + 'static,
        T: Clone + Send + 'static,
    {
        self.modify(move |v: T| {
            let result = f(v);
            (result.clone(), result)
        })
        .await
    }

    pub async fn get_and_update_async<F, Fut>(&self, f: F) -> Result<T, DRefError>
    where
        F: FnOnce(T) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, DRefError>> + Send + 'static,
        T: Clone + Send + 'static,
    {
        self.modify_async(move |v: T| {
            let old = v.clone();
            async move {
                let new = f(v).await?;
                Ok((old, new))
            }
        })
        .await
    }

    pub async fn update_async<F, Fut>(&self, f: F) -> Result<(), DRefError>
    where
        F: FnOnce(T) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, DRefError>> + Send + 'static,
        T: Send + 'static,
    {
        self.modify_async(move |v: T| async move {
            let new = f(v).await?;
            Ok(((), new))
        })
        .await
    }

    pub async fn update_and_get_async<F, Fut>(&self, f: F) -> Result<T, DRefError>
    where
        F: FnOnce(T) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, DRefError>> + Send + 'static,
        T: Clone + Send + 'static,
    {
        self.modify_async(move |v: T| async move {
            let new = f(v).await?;
            Ok((new.clone(), new))
        })
        .await
    }
}
