//! Rust Raft node for the cross-language DRef interop demo.
//!
//! Joins the same cluster as the Scala `InteropMain` nodes
//! (`interop-example/src/main/scala/io/tobia80/dref/InteropMain.scala`):
//!
//!   - the shared protobuf services in `proto/dref_consensus.proto` and
//!     `proto/dref.proto` make Raft replication wire-compatible;
//!   - the MsgPack `DRefMessage` struct below has the same field names and
//!     order as the Scala case class, so rmp-serde and
//!     zio-schema-msg-pack produce identical bytes;
//!   - the manual id `interop-chat-message` matches the Scala side so both
//!     languages target the same Raft log entry.
//!
//! Driven by the same `DREF_*` env vars as the Scala example
//! (see `docker-compose.interop.yml`).
use std::env;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use dref_core::DRef;
use dref_raft::{RaftConfig, RaftDRefContext};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};

const SHARED_KEY: &str = "interop-chat-message";

/// Field names + order must stay aligned with the Scala `DRefMessage` case class.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DRefMessage {
    name: String,
    message: String,
}

fn node_label() -> String {
    let role = env::var("DREF_NODE_LABEL").unwrap_or_else(|_| "rust".to_string());
    let host = env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string());
    format!("{role}/{host}")
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("warn,dref_raft=info,interop_node=info")
            }),
        )
        .init();

    let label = node_label();
    let port: u16 = env::var("DREF_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8082);

    // `RaftDRefContext::start` honours the standard `DREF_NODE_SERVICES`,
    // `DREF_NODE_ADDRESSES`, and `DREF_K8S_*` env vars (see
    // `dref_raft::ip_provider::from_env`) when `initial_endpoints` is empty.
    let config = RaftConfig {
        port,
        ..Default::default()
    };

    println!("[{label}] joining Raft cluster on :{port} ...");
    let ctx = RaftDRefContext::start(config, Duration::from_secs(60)).await?;
    println!(
        "[{label}] joined as node_id={} (leader so far: {:?})",
        ctx.node_id(),
        ctx.leader_id().await
    );

    let dref = Arc::new(
        DRef::make_with_name(&ctx, SHARED_KEY, || DRefMessage {
            name: String::new(),
            message: String::new(),
        })
        .await?,
    );

    let mut stdin = BufReader::new(tokio::io::stdin());

    print!("\x1b[33m[{label}] enter your display name: \x1b[0m");
    std::io::stdout().flush().ok();
    let mut buf = String::new();
    if stdin.read_line(&mut buf).await? == 0 {
        return Ok(());
    }
    let display_name = match buf.trim() {
        "" => label.clone(),
        s => s.to_string(),
    };

    println!(
        "\x1b[32m[{label}] joined as '{display_name}'. Type messages, 'exit' to quit.\x1b[0m"
    );

    // Listener task: log changes from other nodes.
    let listener = {
        let me = display_name.clone();
        let dref = Arc::clone(&dref);
        let label = label.clone();
        tokio::spawn(async move {
            let mut stream = Box::pin(dref.change_stream());
            while let Some(item) = stream.next().await {
                match item {
                    Ok(msg) if !msg.name.is_empty() && msg.name != me => {
                        println!(
                            "\x1b[36m<<< ({}) {} [seen by {}]\x1b[0m",
                            msg.name, msg.message, label
                        );
                    }
                    Ok(_) => {}
                    Err(e) => eprintln!("change stream error: {e}"),
                }
            }
        })
    };

    loop {
        print!("\x1b[34m[{display_name}] message ('exit' to quit): \x1b[0m");
        std::io::stdout().flush().ok();
        let mut line = String::new();
        let n = stdin.read_line(&mut line).await?;
        if n == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.eq_ignore_ascii_case("exit") {
            break;
        }
        if trimmed.is_empty() {
            continue;
        }
        if let Err(e) = dref
            .set(DRefMessage {
                name: display_name.clone(),
                message: trimmed.to_string(),
            })
            .await
        {
            eprintln!("failed to send message: {e}");
        }
    }

    listener.abort();
    println!("[{label}] exiting.");
    Ok(())
}
