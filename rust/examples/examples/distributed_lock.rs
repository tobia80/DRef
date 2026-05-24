//! Distributed lock example: use `lock_with_context` to ensure only one node
//! runs a critical section at a time. Modelled after the Scala README's
//! `ThrottledJob` recipe.

use dref_core::{lock_with_context, IdProvider, LocalDRefContext};

#[tokio::main]
async fn main() {
    let ctx = LocalDRefContext::new();
    let result = lock_with_context(
        &ctx,
        IdProvider::ManualId("daily-report".to_string()),
        || async {
            println!("Generating report...");
            // Pretend to do some work.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok::<&'static str, dref_core::DRefError>("report-ok")
        },
    )
    .await
    .unwrap();

    println!("Critical section finished: {result}");
}
