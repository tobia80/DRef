//! Quick-start example: create a distributed `Ref<i32>`, increment it, read
//! it back. Mirrors the snippet in `rust/README.md` and the Scala quickstart.

use dref_core::{DRef, LocalDRefContext};

#[tokio::main]
async fn main() {
    let ctx = LocalDRefContext::new();
    let ref_value = DRef::make(&ctx, || 0i32).await.unwrap();
    ref_value.update(|v| v + 1).await.unwrap();
    let current = ref_value.get().await.unwrap();
    println!("Current value: {}", current);
}
