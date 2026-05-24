//! Leader-election example: store a `LeaderState` in a `DRef`, then use
//! `get_and_update` to atomically claim leadership iff nobody is leader yet.
//! Modelled after the Scala README's `LeaderElection` recipe.

use dref_core::{DRef, LocalDRefContext};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaderState {
    leader: Option<String>,
}

#[tokio::main]
async fn main() {
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

fn rand_suffix() -> String {
    // Cheap, no extra dep: a few bits of system time.
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("{nanos:08x}")
}
