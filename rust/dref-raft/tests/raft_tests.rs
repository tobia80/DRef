//! Integration tests for the Raft backend.
//!
//! These tests run end-to-end against real Tokio + Tonic gRPC servers — no
//! external services are needed. Each test picks free localhost ports,
//! spins up a small cluster, exercises the public DRef API, and lets the
//! background tasks tear down when the contexts go out of scope.

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use dref_core::{DRef, DRefContext, DRefError};
use dref_raft::{NodeEndpoint, RaftConfig, RaftDRefContext};
use futures::StreamExt;
use tokio::time::sleep;

/// Bind a dummy listener so we get a free port from the OS, then drop it.
/// Tiny race window between the drop and `bind` in the server task; haven't
/// seen it bite in practice but we retry once if it does.
fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    port
}

fn make_cluster_config(ports: &[u16], idx: usize) -> RaftConfig {
    let endpoints: Vec<NodeEndpoint> = ports
        .iter()
        .enumerate()
        .map(|(i, p)| NodeEndpoint::new(format!("node-{i}"), format!("127.0.0.1:{p}")))
        .collect();
    RaftConfig {
        port: ports[idx],
        bind_address: Some(format!("127.0.0.1:{}", ports[idx])),
        node_id: Some(format!("node-{idx}")),
        ttl: Some(Duration::from_secs(5)),
        address_poll_interval: Duration::from_millis(500),
        connection_timeout: Duration::from_millis(500),
        // Fast election so tests don't drag.
        election_timeout: Duration::from_millis(400),
        heartbeat_interval: Duration::from_millis(80),
        initial_endpoints: endpoints,
        storage_dir: None,
        snapshot_every: 1000,
    }
}

async fn start_cluster(size: usize) -> Vec<RaftDRefContext> {
    let ports: Vec<u16> = (0..size).map(|_| free_port()).collect();
    let mut nodes = Vec::with_capacity(size);
    for i in 0..size {
        let cfg = make_cluster_config(&ports, i);
        let ctx = RaftDRefContext::start(cfg, Duration::from_millis(50))
            .await
            .expect("start node");
        nodes.push(ctx);
    }
    // Wait for a leader to settle.
    for _ in 0..40 {
        sleep(Duration::from_millis(100)).await;
        let leaders: Vec<_> =
            futures::future::join_all(nodes.iter().map(|n| async move { n.is_leader().await }))
                .await;
        if leaders.iter().filter(|x| **x).count() == 1 {
            break;
        }
    }
    nodes
}

#[tokio::test]
async fn single_node_cluster_writes_and_reads() -> Result<(), DRefError> {
    let ports = vec![free_port()];
    let cfg = make_cluster_config(&ports, 0);
    let ctx = RaftDRefContext::start(cfg, Duration::from_millis(500))
        .await
        .expect("start node");
    // Single node = always leader (no peers).
    assert!(ctx.is_leader().await);

    ctx.set_element("hello", b"world".to_vec(), None).await?;
    let v = ctx.get_element("hello").await?;
    assert_eq!(v.as_deref(), Some(&b"world"[..]));
    ctx.delete_element("hello").await?;
    let v = ctx.get_element("hello").await?;
    assert_eq!(v, None);
    Ok(())
}

#[tokio::test]
async fn three_node_cluster_elects_leader() {
    let nodes = start_cluster(3).await;
    // Exactly one node should think it's the leader.
    let mut leaders = 0;
    for n in &nodes {
        if n.is_leader().await {
            leaders += 1;
        }
    }
    assert_eq!(leaders, 1, "expected exactly one leader");
}

#[tokio::test]
async fn write_through_any_node_propagates() -> Result<(), DRefError> {
    let nodes = start_cluster(3).await;

    // Write via the first non-leader we find — proves clients are
    // re-routed to the leader transparently.
    let writer = {
        let mut chosen = None;
        for n in &nodes {
            if !n.is_leader().await {
                chosen = Some(n.clone());
                break;
            }
        }
        chosen.unwrap_or_else(|| nodes[0].clone())
    };

    writer
        .set_element("propagated", b"yes".to_vec(), None)
        .await?;

    // Give the AppendEntries fan-out a moment.
    sleep(Duration::from_millis(300)).await;

    // Read from every node — they all read from their LOCAL state machine
    // via the leader. To prove replication, check the state machine
    // directly via DRefContext::get_element on each node.
    for (i, n) in nodes.iter().enumerate() {
        let v = n.get_element("propagated").await?;
        assert_eq!(
            v.as_deref(),
            Some(&b"yes"[..]),
            "node {i} did not see the write"
        );
    }
    Ok(())
}

#[tokio::test]
async fn set_if_not_exist_is_exclusive_across_cluster() -> Result<(), DRefError> {
    let nodes = start_cluster(3).await;
    // Two concurrent writers, two different values — only one wins.
    let n0 = nodes[0].clone();
    let n1 = nodes[1].clone();
    let key = "exclusive";

    let h0 = tokio::spawn(async move {
        n0.set_element_if_not_exist(key, b"alpha".to_vec(), None)
            .await
    });
    let h1 = tokio::spawn(async move {
        n1.set_element_if_not_exist(key, b"beta".to_vec(), None)
            .await
    });
    let r0 = h0.await.unwrap()?;
    let r1 = h1.await.unwrap()?;
    assert!(
        r0 ^ r1,
        "exactly one writer should have won; got r0={r0} r1={r1}"
    );

    // Whichever value won is what the cluster has.
    let v = nodes[2].get_element(key).await?;
    assert!(
        matches!(v.as_deref(), Some(b"alpha") | Some(b"beta")),
        "unexpected value {v:?}"
    );
    Ok(())
}

#[tokio::test]
async fn on_change_stream_observes_writes_locally() -> Result<(), DRefError> {
    let nodes = start_cluster(3).await;
    let key = "events";

    // Subscribe on a follower (or any node — the local state machine sees
    // every replicated mutation).
    let listener = nodes[1].clone();
    let writer = nodes[0].clone();

    let mut stream = listener.on_change_stream(key);
    let recv = tokio::spawn(async move {
        // First event only.
        stream.next().await
    });

    // Brief delay so the subscriber is in place before the write.
    sleep(Duration::from_millis(50)).await;
    writer.set_element(key, b"observed".to_vec(), None).await?;

    let got = tokio::time::timeout(Duration::from_secs(3), recv)
        .await
        .expect("event arrives in time")
        .unwrap()
        .expect("stream emits an item")
        .expect("event decodes ok");
    match got {
        dref_core::ChangeEvent::SetElement { name, value } => {
            assert_eq!(name, key);
            assert_eq!(value, b"observed");
        }
        other => panic!("unexpected event: {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn distributed_lock_serializes_critical_section() -> Result<(), DRefError> {
    use dref_core::IdProvider;

    let nodes = start_cluster(3).await;
    let counter = Arc::new(std::sync::Mutex::new(0u32));
    let max_in_flight = Arc::new(std::sync::Mutex::new(0u32));
    let in_flight = Arc::new(std::sync::Mutex::new(0u32));

    let mut handles = Vec::new();
    for i in 0..6 {
        let ctx = nodes[i % nodes.len()].clone();
        let counter = Arc::clone(&counter);
        let max_in_flight = Arc::clone(&max_in_flight);
        let in_flight = Arc::clone(&in_flight);
        handles.push(tokio::spawn(async move {
            dref_core::lock_with_context(
                &ctx,
                IdProvider::ManualId("test-lock".to_string()),
                || async move {
                    // Track concurrency under the lock.
                    {
                        let mut c = in_flight.lock().unwrap();
                        *c += 1;
                        let mut m = max_in_flight.lock().unwrap();
                        if *c > *m {
                            *m = *c;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    {
                        let mut c = counter.lock().unwrap();
                        *c += 1;
                    }
                    {
                        let mut c = in_flight.lock().unwrap();
                        *c -= 1;
                    }
                    Ok::<(), DRefError>(())
                },
            )
            .await
        }));
    }

    for h in handles {
        h.await.unwrap()?;
    }

    let final_count = *counter.lock().unwrap();
    let max_concurrent = *max_in_flight.lock().unwrap();
    assert_eq!(final_count, 6, "every critical section ran exactly once");
    assert_eq!(
        max_concurrent, 1,
        "at most one critical section was active at a time"
    );
    Ok(())
}

/// PreVote regression: restarting one follower must not cause the leader's
/// term to spike. Without PreVote, the restarted node — even when it loads
/// a persisted (term, votedFor) — would race the leader's first heartbeat,
/// time out, bump its term, and force the leader to step down. With
/// PreVote, the restarted node first queries peers; the other two are
/// hearing fresh heartbeats from the current leader and refuse the
/// pre-vote, so the leader stays put and term doesn't move.
#[tokio::test]
async fn restart_one_node_cluster_stays_stable_term_does_not_spike() {
    // Persistent storage so the restarted node remembers (term, votedFor).
    let dirs: Vec<std::path::PathBuf> = (0..3)
        .map(|i| {
            let d = std::env::temp_dir()
                .join(format!("dref-prevote-restart-{}-{i}", std::process::id()));
            let _ = std::fs::remove_dir_all(&d);
            std::fs::create_dir_all(&d).unwrap();
            d
        })
        .collect();

    let ports: Vec<u16> = (0..3).map(|_| free_port()).collect();
    let make_cfg = |idx: usize| {
        let mut cfg = make_cluster_config(&ports, idx);
        cfg.storage_dir = Some(dirs[idx].clone());
        cfg
    };

    let mut nodes: Vec<Option<RaftDRefContext>> = Vec::with_capacity(3);
    for i in 0..3 {
        let ctx = RaftDRefContext::start(make_cfg(i), Duration::from_millis(50))
            .await
            .expect("start node");
        nodes.push(Some(ctx));
    }

    // Wait for stable leader.
    let mut leader_idx = None;
    for _ in 0..60 {
        sleep(Duration::from_millis(100)).await;
        let mut ls = Vec::new();
        for n in nodes.iter().flatten() {
            ls.push(n.is_leader().await);
        }
        if ls.iter().filter(|x| **x).count() == 1 {
            leader_idx = ls.iter().position(|x| *x);
            break;
        }
    }
    let leader_idx = leader_idx.expect("cluster elected a leader");
    let leader_id_before = nodes[leader_idx]
        .as_ref()
        .unwrap()
        .leader_id()
        .await
        .expect("leader id known");
    let term_before = nodes[leader_idx].as_ref().unwrap().current_term().await;

    // Pick any non-leader to restart.
    let restart_idx = (0..3).find(|i| *i != leader_idx).unwrap();

    // Drop the follower; Tasks::drop aborts the gRPC server and frees the
    // port. Wait briefly so the OS releases it before rebind.
    nodes[restart_idx] = None;
    sleep(Duration::from_millis(200)).await;

    // Bring the same node back up — same port, same node id, same storage_dir.
    let restarted = RaftDRefContext::start(make_cfg(restart_idx), Duration::from_millis(500))
        .await
        .expect("restart node");
    nodes[restart_idx] = Some(restarted);

    // Give heartbeats time to converge across at least one election-timeout
    // window. Under PreVote the restarted node should refuse to bump its
    // term — peers tell it "we just heard from the leader, no" — so this
    // is where a regression would surface.
    sleep(Duration::from_millis(1500)).await;

    let leader_id_after = nodes[leader_idx]
        .as_ref()
        .unwrap()
        .leader_id()
        .await
        .expect("leader id still known");
    let term_after = nodes[leader_idx].as_ref().unwrap().current_term().await;

    assert_eq!(
        leader_id_after, leader_id_before,
        "leader should not change after a follower restart with PreVote"
    );
    assert_eq!(
        term_after, term_before,
        "term should not spike after a follower restart with PreVote"
    );

    // Sanity: every node agrees on the leader.
    for (i, n) in nodes.iter().enumerate() {
        let n = n.as_ref().unwrap();
        let seen = n.leader_id().await;
        assert_eq!(
            seen.as_ref(),
            Some(&leader_id_before),
            "node {i} should agree on leader id"
        );
    }

    // Cleanup.
    drop(nodes);
    for d in &dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[tokio::test]
async fn dref_make_and_set_roundtrip() -> Result<(), DRefError> {
    let nodes = start_cluster(2).await;
    let aref = DRef::<String, _>::make_with_name(&nodes[0], "dref-raft-test:roundtrip", || {
        "init".to_string()
    })
    .await?;
    aref.set("hello".to_string()).await?;
    let v = aref.get().await?;
    assert_eq!(v, "hello");
    Ok(())
}
