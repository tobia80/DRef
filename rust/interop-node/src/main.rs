//! Rust side of the cross-language Raft demo (pairs with Scala `InteropMain`).

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ChatMsg {
    name: String,
    message: String,
}

fn label() -> String {
    let role = env::var("DREF_NODE_LABEL").unwrap_or_else(|_| "rust".into());
    let host = env::var("HOSTNAME").unwrap_or_else(|_| "local".into());
    format!("{role}/{host}")
}

fn display_name(label: &str) -> String {
    let auto = env::var("DREF_AUTO_NAME").ok().filter(|s| !s.trim().is_empty());
    match auto {
        Some(base) => env::var("HOSTNAME")
            .ok()
            .filter(|h| !h.is_empty())
            .map(|h| format!("{}-{}", base.trim(), h.chars().take(6).collect::<String>()))
            .unwrap_or_else(|| base.trim().to_string()),
        None => label.to_string(),
    }
}

fn broadcast_schedule() -> (Duration, Duration) {
    let secs: u64 = env::var("DREF_AUTO_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(3);
    let id = env::var("HOSTNAME")
        .or_else(|_| env::var("DREF_NODE_LABEL"))
        .unwrap_or_else(|_| "node".into());
    let hash = id.bytes().fold(5381u64, |h, b| h.wrapping_mul(33).wrapping_add(u64::from(b)));
    (
        Duration::from_millis(hash % secs.saturating_mul(1000)),
        Duration::from_secs(secs),
    )
}

fn spawn_listener<C>(dref: Arc<DRef<ChatMsg, C>>, me: String, label: String)
where
    C: dref_core::DRefContext + Clone + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let mut stream = Box::pin(dref.change_stream());
        while let Some(item) = stream.next().await {
            match item {
                Ok(msg) if !msg.name.is_empty() && msg.name != me => {
                    println!(
                        "\x1b[36m<<< ({}) {} [{}]\x1b[0m",
                        msg.name, msg.message, label
                    );
                }
                Ok(_) => {}
                Err(e) => eprintln!("change stream error: {e}"),
            }
        }
    });
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

    let label = label();
    let port: u16 = env::var("DREF_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8082);

    println!("\x1b[32m[{label}] joining on :{port} ...\x1b[0m");
    let ctx = RaftDRefContext::start(RaftConfig { port, ..Default::default() }, Duration::from_secs(60)).await?;
    println!("\x1b[32m[{label}] Raft ready (node_id={})\x1b[0m", ctx.node_id());

    let dref = Arc::new(
        DRef::make_with_name(&ctx, SHARED_KEY, || ChatMsg {
            name: String::new(),
            message: String::new(),
        })
        .await?,
    );

    let auto = env::var("DREF_AUTO_NAME").ok().is_some_and(|s| !s.trim().is_empty());
    let name = if auto {
        display_name(&label)
    } else {
        print!("\x1b[33m[{label}] display name: \x1b[0m");
        std::io::stdout().flush().ok();
        let mut buf = String::new();
        BufReader::new(tokio::io::stdin()).read_line(&mut buf).await?;
        let trimmed = buf.trim();
        if trimmed.is_empty() { label.clone() } else { trimmed.to_string() }
    };

    println!(
        "\x1b[32m[{label}] {} as '{name}'\x1b[0m",
        if auto { "auto-demo" } else { "chat (type exit to quit)" }
    );

    spawn_listener(Arc::clone(&dref), name.clone(), label.clone());

    if auto {
        let (stagger, every) = broadcast_schedule();
        let start = tokio::time::Instant::now() + stagger;
        let mut tick = tokio::time::interval_at(start, every);
        loop {
            tick.tick().await;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let message = format!("hello @t={now}");
            println!("\x1b[34m[{name}] >>> {message}\x1b[0m");
            dref.set(ChatMsg {
                name: name.clone(),
                message,
            })
            .await?;
        }
    } else {
        let mut stdin = BufReader::new(tokio::io::stdin());
        loop {
            print!("\x1b[34m[{name}] > \x1b[0m");
            std::io::stdout().flush().ok();
            let mut line = String::new();
            if stdin.read_line(&mut line).await? == 0 {
                break;
            }
            let text = line.trim();
            if text.eq_ignore_ascii_case("exit") {
                break;
            }
            if !text.is_empty() {
                dref.set(ChatMsg {
                    name: name.clone(),
                    message: text.to_string(),
                })
                .await?;
            }
        }
    }

    Ok(())
}
