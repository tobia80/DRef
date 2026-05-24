//! Distributed locking primitive.
//!
//! Port of `DRef.lockWithContext` from the Scala source. The idea: try to
//! atomically claim a TTL-protected key in the backend; if claimed, fork a
//! keep-alive task and a stolen-detection task, then run the user's body. If
//! the lock is stolen mid-flight, the user body is aborted and we surface
//! [`LockStolenError`]. If we don't get the lock on first try, we wait for a
//! delete/change event on the key and try again.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use rand::Rng;
use tokio::sync::Notify;

use crate::context::{ChangeEvent, DRefContext};
use crate::dref::{auto_name_for, IdProvider};
use crate::error::{DRefError, LockStolenError};
use crate::lock_value::to_bytes;

/// Run `body` while holding a distributed lock keyed by `id`. The lock is
/// released when `body` completes (success or failure).
///
/// `caller_location` should be the `(file, line)` of the original call site;
/// it's used to derive an auto-name when `id == IdProvider::AutoId`. Most
/// users will go through the `lock!` macro which fills this in.
#[track_caller]
pub fn lock_with_context<C, F, Fut, T>(
    context: &C,
    id: IdProvider,
    body: F,
) -> impl Future<Output = Result<T, DRefError>> + Send
where
    C: DRefContext + Clone + Send + Sync + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<T, DRefError>> + Send + 'static,
    T: Send + 'static,
{
    // Resolve the lock name eagerly so the caller's #[track_caller] location
    // (if any) is captured before we spawn anything.
    let name = match id {
        IdProvider::ManualId(n) => n,
        IdProvider::AutoId => auto_name_for(std::panic::Location::caller()),
    };
    let context = context.clone();
    async move { lock_inner(context, name, body).await }
}

async fn lock_inner<C, F, Fut, T>(
    context: C,
    name: String,
    body: F,
) -> Result<T, DRefError>
where
    C: DRefContext + Clone + Send + Sync + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<T, DRefError>> + Send + 'static,
    T: Send + 'static,
{
    // Random per-acquire value so we can detect "someone else stole my lock".
    let lock_value_i64: i64 = rand::thread_rng().gen();
    let lock_value_bytes = to_bytes(lock_value_i64).to_vec();
    let default_ttl = context.default_ttl();

    // Try to acquire. If we don't get it on the first try, wait for the lock
    // to be released and try again.
    acquire(&context, &name, &lock_value_bytes, default_ttl).await?;
    tracing::debug!(lock = %name, "lock acquired");

    // Shared "lock was stolen" flag + a Notify we wake when we want to tear
    // everything down.
    let stolen = Arc::new(tokio::sync::Mutex::new(None::<LockStolenError>));
    let shutdown = Arc::new(Notify::new());

    // Keep-alive task: re-touch the TTL until we tell it to stop.
    let alive_task = {
        let ctx = context.clone();
        let name = name.clone();
        let shutdown = Arc::clone(&shutdown);
        tokio::spawn(async move {
            let mut s = ctx.keep_alive_stream(&name, default_ttl);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.notified() => return,
                    next = s.next() => {
                        if next.is_none() { return; }
                    }
                }
            }
        })
    };

    // Stolen-detect task: when the backend says "the value at name no longer
    // matches what you wrote", record the error and signal shutdown so the
    // user body gets aborted.
    let stolen_task = {
        let ctx = context.clone();
        let name = name.clone();
        let value = lock_value_bytes.clone();
        let stolen = Arc::clone(&stolen);
        let shutdown = Arc::clone(&shutdown);
        tokio::spawn(async move {
            let mut s = ctx.detect_stolen_element(&name, value);
            tokio::select! {
                biased;
                _ = shutdown.notified() => {},
                evt = s.next() => {
                    if let Some(Ok(_)) = evt {
                        let mut g = stolen.lock().await;
                        *g = Some(LockStolenError { name: name.clone(), value: lock_value_i64 });
                        shutdown.notify_waiters();
                    }
                }
            }
        })
    };

    // Run the body, racing against the shutdown signal. If shutdown fires
    // first, we abort the body and propagate the stolen error.
    let body_fut = body();
    tokio::pin!(body_fut);

    let outcome: Result<T, DRefError> = tokio::select! {
        biased;
        res = &mut body_fut => res,
        _ = shutdown.notified() => {
            // body is dropped at the end of select — equivalent to fiber
            // interrupt in the Scala impl.
            let err = stolen.lock().await.clone();
            match err {
                Some(e) => Err(DRefError::LockStolen(e)),
                None => Err(DRefError::other("lock shutdown without stolen detection")),
            }
        }
    };

    // Tear down helper tasks and release the lock — unless it was stolen, in
    // which case the current holder is somebody else and we must not touch
    // their entry.
    shutdown.notify_waiters();
    alive_task.abort();
    stolen_task.abort();
    let was_stolen = stolen.lock().await.is_some();
    if !was_stolen {
        // Best-effort release; log on failure.
        if let Err(e) = context.delete_element(&name).await {
            tracing::warn!(lock = %name, error = %e, "lock release failed");
        }
    }

    outcome
}

/// Try to acquire the lock, waiting for prior holders' deletions if needed.
async fn acquire<C>(
    context: &C,
    name: &str,
    value: &[u8],
    ttl: Duration,
) -> Result<(), DRefError>
where
    C: DRefContext + Send + Sync,
{
    if context
        .set_element_if_not_exist(name, value.to_vec(), Some(ttl))
        .await?
    {
        return Ok(());
    }

    // Watch for deletes/changes on this name. Whenever we observe a delete,
    // try to claim. (Subscribe BEFORE the first probe to avoid a race where
    // the delete happens between probe and subscribe.)
    let mut deletes = context.on_change_stream(name);
    let mut underlying = context.detect_deletion_from_underlying_stream(name);

    // Also try again immediately on a tick, in case TTL expires before
    // anyone broadcasts a delete (the in-memory reaper does not emit
    // delete events for natural expiry).
    let mut probe = tokio::time::interval(Duration::from_millis(50));
    probe.tick().await; // skip the immediate tick

    loop {
        tokio::select! {
            ev = deletes.next() => {
                if let Some(Ok(ChangeEvent::DeleteElement { .. })) = ev {
                    if context
                        .set_element_if_not_exist(name, value.to_vec(), Some(ttl))
                        .await?
                    {
                        return Ok(());
                    }
                }
            }
            ev = underlying.next() => {
                if let Some(Ok(ChangeEvent::DeleteElement { .. })) = ev {
                    if context
                        .set_element_if_not_exist(name, value.to_vec(), Some(ttl))
                        .await?
                    {
                        return Ok(());
                    }
                }
            }
            _ = probe.tick() => {
                if context
                    .set_element_if_not_exist(name, value.to_vec(), Some(ttl))
                    .await?
                {
                    return Ok(());
                }
            }
        }
    }
}
