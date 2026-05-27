//! Integration tests for the `dref-core` crate. Mirrors `DRefSpec.scala`.

use std::sync::Arc;
use std::time::Duration;

use dref_core::{DRef, DRefError, LocalDRefContext};
use futures::StreamExt;
use tokio::sync::Mutex;
use tokio::time::sleep;

#[tokio::test]
async fn should_create_and_read_dref() -> Result<(), DRefError> {
    let ctx = LocalDRefContext::new();
    let aref = DRef::make(&ctx, || "hi".to_string()).await?;
    aref.set("hello".to_string()).await?;
    let value = aref.get().await?;
    assert_eq!(value, "hello");
    Ok(())
}

#[tokio::test]
async fn should_listen_for_changes() -> Result<(), DRefError> {
    let ctx = LocalDRefContext::new();
    let aref = DRef::make(&ctx, || "hi".to_string()).await?;

    // Subscribe BEFORE the writes so we see them.
    let stream = aref.change_stream();

    // Spawn a collector that runs for ~2 seconds, then yields whatever it saw.
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
    sleep(Duration::from_millis(50)).await;
    aref.set("hello".to_string()).await?;
    aref.set("changed again".to_string()).await?;

    let mutations = collector.await.expect("collector task");
    assert_eq!(
        mutations,
        vec!["hello".to_string(), "changed again".to_string()]
    );
    Ok(())
}

#[tokio::test]
async fn locks_should_work() -> Result<(), DRefError> {
    let ctx = LocalDRefContext::new();
    let list: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

    // Two concurrent tasks try to grab the same lock. The first to start sleeps
    // 1s while holding the lock, so the second has to wait. We observe the
    // list at the 500 ms mark (only the first id is in) and after both
    // finish (both ids in, in order of acquisition).
    let lock_id = dref_core::IdProvider::ManualId("lock-test-shared-lock".to_string());

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
    Ok(())
}
