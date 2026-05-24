//! Integration tests for the Redis backend. Mirrors `RedisDRefSpec.scala`.
//!
//! These tests require a Redis instance reachable at `localhost:6379` (the
//! same address used by the Scala spec). They are gated behind the
//! `test-redis` feature so `cargo test -p dref-redis` is a no-op when Redis
//! isn't available; run with `cargo test -p dref-redis --features test-redis`.

#![cfg(feature = "test-redis")]

use std::sync::Arc;
use std::time::Duration;

use dref_core::{DRef, DRefContext, DRefError, IdProvider};
use dref_redis::{RedisConfig, RedisDRefContext};
use futures::StreamExt;
use tokio::sync::Mutex;
use tokio::time::sleep;

fn test_config() -> RedisConfig {
    RedisConfig {
        host: "localhost".to_string(),
        port: 6379,
        database: 0,
        username: None,
        password: None,
        ca_cert: None,
        ttl: Some(Duration::from_secs(5)),
    }
}

async fn fresh_context() -> RedisDRefContext {
    RedisDRefContext::new(test_config())
        .await
        .expect("redis must be running on localhost:6379 for these tests")
}

#[tokio::test]
async fn should_create_and_read_via_redis() -> Result<(), DRefError> {
    let ctx = fresh_context().await;
    // Use a manual name so the test is independent of file:line.
    let aref =
        DRef::<String, _>::make_with_name(&ctx, "dref-redis-test:create-and-read", || {
            "hi".to_string()
        })
        .await?;
    aref.set("hello".to_string()).await?;
    let value = aref.get().await?;
    assert_eq!(value, "hello");
    ctx.delete_element("dref-redis-test:create-and-read").await?;
    Ok(())
}

#[tokio::test]
async fn should_listen_for_changes_via_redis() -> Result<(), DRefError> {
    let ctx = fresh_context().await;
    // Clean any leftover from a previous run.
    let key = "dref-redis-test:listen-for-changes";
    ctx.delete_element(key).await?;

    let aref =
        DRef::<String, _>::make_with_name(&ctx, key, || "hi".to_string()).await?;

    // The Scala spec sleeps 50ms after `make` so the initial publish
    // (triggered by `set_element_if_not_exist`) drains before we attach the
    // subscriber. Redis pub/sub is asynchronous round-trip, so on a busy
    // shared Redis instance we want a slightly longer guard.
    sleep(Duration::from_millis(200)).await;

    // Subscribe AFTER the initial publish has drained so we only see writes
    // explicitly made by this test.
    let stream = aref.change_stream();

    let collector = tokio::spawn(async move {
        let mut out: Vec<String> = Vec::new();
        let collect = async {
            let mut s = Box::pin(stream);
            while let Some(item) = s.next().await {
                if let Ok(v) = item {
                    out.push(v);
                }
            }
        };
        let _ = tokio::time::timeout(Duration::from_secs(2), collect).await;
        out
    });

    // Give the subscriber a tick to attach.
    sleep(Duration::from_millis(200)).await;
    aref.set("hello".to_string()).await?;
    aref.set("changed again".to_string()).await?;

    let mutations = collector.await.expect("collector task");
    assert_eq!(
        mutations,
        vec!["hello".to_string(), "changed again".to_string()]
    );
    ctx.delete_element(key).await?;
    Ok(())
}

#[tokio::test]
async fn distributed_locks_should_work() -> Result<(), DRefError> {
    let ctx = fresh_context().await;
    let lock_key = "dref-redis-test:lock-shared";
    ctx.delete_element(lock_key).await?;

    let list: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
    let lock_id = IdProvider::ManualId(lock_key.to_string());

    let ctx_a = ctx.clone();
    let ctx_b = ctx.clone();
    let list_a = list.clone();
    let list_b = list.clone();
    let id_a = lock_id.clone();
    let id_b = lock_id.clone();

    let runner = tokio::spawn(async move {
        let t100 = {
            let ctx = ctx_a.clone();
            let list = list_a.clone();
            let id = id_a.clone();
            tokio::spawn(async move {
                sleep(Duration::from_millis(100)).await;
                dref_core::lock_with_context(&ctx, id, move || async move {
                    list.lock().await.push(100);
                    sleep(Duration::from_secs(1)).await;
                    Ok::<(), DRefError>(())
                })
                .await
            })
        };
        let t200 = {
            let ctx = ctx_b.clone();
            let list = list_b.clone();
            let id = id_b.clone();
            tokio::spawn(async move {
                sleep(Duration::from_millis(200)).await;
                dref_core::lock_with_context(&ctx, id, move || async move {
                    list.lock().await.push(200);
                    sleep(Duration::from_secs(1)).await;
                    Ok::<(), DRefError>(())
                })
                .await
            })
        };
        let _ = t100.await;
        let _ = t200.await;
    });

    sleep(Duration::from_millis(500)).await;
    let value_with_one_lock = list.lock().await.clone();
    runner.await.expect("runner");
    let value_with_two_locks = list.lock().await.clone();

    assert_eq!(value_with_one_lock, vec![100]);
    assert_eq!(value_with_two_locks, vec![100, 200]);
    ctx.delete_element(lock_key).await?;
    Ok(())
}

#[tokio::test]
async fn stolen_lock_should_fail_when_value_overwritten() -> Result<(), DRefError> {
    let ctx = fresh_context().await;
    let lock_key = "dref-redis-test:stolen-lock";
    ctx.delete_element(lock_key).await?;

    let ctx_for_lock = ctx.clone();
    let lock_id = IdProvider::ManualId(lock_key.to_string());

    let lock_task = tokio::spawn(async move {
        dref_core::lock_with_context(&ctx_for_lock, lock_id, move || async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            Ok::<&str, DRefError>("original-lock-completed")
        })
        .await
    });

    sleep(Duration::from_millis(500)).await;
    ctx.set_element(lock_key, b"stolen-value".to_vec(), None)
        .await?;

    let result = lock_task.await.expect("lock task");
    assert!(
        matches!(result, Err(DRefError::LockStolen(_))),
        "expected LockStolen, got {result:?}"
    );

    ctx.delete_element(lock_key).await?;
    Ok(())
}
