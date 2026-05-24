//! `dref-examples` binary: runs all bundled examples in sequence so
//! `cargo run -p dref-examples` exercises every snippet shown in
//! `rust/README.md`. For an individual example, prefer
//! `cargo run --example quickstart` (or `leader_election`, `distributed_lock`).

use std::time::{SystemTime, UNIX_EPOCH};

use dref_core::{lock_with_context, DRef, DRefError, IdProvider, LocalDRefContext};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaderState {
    leader: Option<String>,
}

#[tokio::main]
async fn main() {
    println!("=== Example 1: quickstart ===");
    quickstart().await;

    println!();
    println!("=== Example 2: leader election ===");
    leader_election().await;

    println!();
    println!("=== Example 3: distributed lock ===");
    distributed_lock().await;
}

async fn quickstart() {
    let ctx = LocalDRefContext::new();
    let ref_value = DRef::make(&ctx, || 0i32).await.unwrap();
    ref_value.update(|v| v + 1).await.unwrap();
    let current = ref_value.get().await.unwrap();
    println!("Current value: {current}");
}

async fn leader_election() {
    let ctx = LocalDRefContext::new();
    let node_id = format!("node-{}", rand_suffix());
    let leadership = DRef::make(&ctx, || LeaderState { leader: None })
        .await
        .unwrap();

    let me = node_id.clone();
    let previous = leadership
        .get_and_update(move |state| match state.leader {
            None => LeaderState {
                leader: Some(me.clone()),
            },
            Some(_) => state,
        })
        .await
        .unwrap();

    match previous.leader {
        None => println!("{node_id} claimed leadership"),
        Some(other) => println!("{node_id} sees leader already active: {other}"),
    }

    let after = leadership.get().await.unwrap();
    println!("Current leader: {:?}", after.leader);
}

async fn distributed_lock() {
    let ctx = LocalDRefContext::new();
    let result = lock_with_context(
        &ctx,
        IdProvider::ManualId("daily-report".to_string()),
        || async {
            println!("Generating report...");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok::<&'static str, DRefError>("report-ok")
        },
    )
    .await
    .unwrap();
    println!("Critical section finished: {result}");
}

fn rand_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("{nanos:08x}")
}
