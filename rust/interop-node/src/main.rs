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

/// djb2 — same formula as Scala `InteropMain` for stagger offsets.
fn identity_hash(key: &str) -> u64 {
    key.bytes().fold(5381u64, |hash, b| hash.wrapping_mul(33).wrapping_add(u64::from(b)))
}

/// Per-replica broadcast timing: initial delay + fixed interval.
/// Stagger spreads first sends across the interval window so replicas
/// started together do not publish in lockstep. Override with
/// `DREF_AUTO_INTERVAL_SECS` (default 3).
fn auto_demo_timing() -> (Duration, Duration) {
    let interval_secs: u64 = env::var("DREF_AUTO_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(3);
    let identity = env::var("HOSTNAME")
        .or_else(|_| env::var("DREF_NODE_LABEL"))
        .unwrap_or_else(|_| "node".to_string());
    let window_ms = interval_secs.saturating_mul(1000);
    let stagger_ms = identity_hash(&identity) % window_ms;
    (
        Duration::from_millis(stagger_ms),
        Duration::from_secs(interval_secs),
    )
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

    // Non-interactive demo path: when DREF_AUTO_NAME is set, broadcast a
    // timestamped message every few seconds and log everything observed.
    // This is what the default compose setup uses so `docker compose logs -f`
    // shows the cluster working without anyone having to `docker attach`.
    if let Some(auto_name) = env::var("DREF_AUTO_NAME").ok().filter(|s| !s.trim().is_empty()) {
        // Suffix the auto-name with a short hostname so each replica is
        // distinguishable in the chat log — without this, two rust-node
        // replicas both publish as "rust-auto" and the receiver filter
        // hides every Rust-to-Rust message.
        let base = auto_name.trim().to_string();
        let display_name = match env::var("HOSTNAME").ok().filter(|s| !s.is_empty()) {
            Some(h) => format!("{base}-{}", h.chars().take(6).collect::<String>()),
            None => base,
        };
        return run_auto_demo(dref, label, display_name).await;
    }

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

async fn run_auto_demo<C>(
    dref: Arc<DRef<DRefMessage, C>>,
    label: String,
    display_name: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    C: dref_core::DRefContext + Clone + Send + Sync + 'static,
{
    let (stagger, interval) = auto_demo_timing();
    println!(
        "\x1b[32m[{label}] auto-demo joined as '{display_name}' — \
         first message in {stagger:?}, then every {interval:?}.\x1b[0m"
    );

    // Listener task.
    {
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
        });
    }

    let start = tokio::time::Instant::now() + stagger;
    let mut tick = tokio::time::interval_at(start, interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let message = format!("hello @t={now}");
        println!("\x1b[34m[{display_name}] >>> {message}\x1b[0m");
        if let Err(e) = dref
            .set(DRefMessage {
                name: display_name.clone(),
                message,
            })
            .await
        {
            eprintln!("failed to send message: {e}");
        }
    }
}
