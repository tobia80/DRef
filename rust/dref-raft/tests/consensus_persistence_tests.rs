//! Mirrors Scala `ProtoConsensusPersistenceSpec` — voter-state durability
//! wired through the in-process consensus engine (no gRPC server).

use std::path::PathBuf;
use std::time::Duration;

use dref_raft::config::{NodeEndpoint, RaftConfig};
use dref_raft::consensus::{Consensus, Role};
use dref_raft::state_machine::StateMachine;
use dref_raft::voter_state_store::{FileVoterStateStore, VoterState, VoterStateStore};

fn temp_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("dref-consensus-persist-{name}-{}", std::process::id()))
}

fn rm_dir(path: &PathBuf) {
    let _ = std::fs::remove_dir_all(path);
}

fn single_node_config(storage: Option<PathBuf>) -> RaftConfig {
    RaftConfig {
        port: 0,
        bind_address: Some("127.0.0.1:0".to_string()),
        node_id: Some("node-under-test".to_string()),
        ttl: Some(Duration::from_secs(5)),
        connection_timeout: Duration::from_millis(500),
        election_timeout: Duration::from_secs(5),
        heartbeat_interval: Duration::from_secs(1),
        initial_endpoints: vec![NodeEndpoint::new(
            "node-under-test",
            "127.0.0.1:0",
        )],
        storage_dir: storage,
        ..RaftConfig::default()
    }
}

fn make_engine(storage: Option<PathBuf>) -> Consensus {
    let sm = StateMachine::new();
    Consensus::new(
        "node-under-test".to_string(),
        sm,
        single_node_config(storage),
    )
}

#[tokio::test]
async fn without_storage_vote_grant_does_not_touch_disk() {
    let engine = make_engine(None);
    assert_eq!(engine.role().await, Role::Leader);
    let (granted, term) = engine
        .handle_vote("other-node".to_string(), 5, 0)
        .await;
    assert!(granted);
    assert_eq!(term, 5);
    assert_eq!(engine.role().await, Role::Follower);
}

#[tokio::test]
async fn with_storage_vote_is_persisted() {
    let dir = temp_dir("vote");
    rm_dir(&dir);
    let engine = make_engine(Some(dir.clone()));
    engine
        .handle_vote("candidate-x".to_string(), 9, 0)
        .await;
    let store = FileVoterStateStore::open(&dir).unwrap();
    assert_eq!(
        store.load().unwrap(),
        VoterState {
            term: 9,
            voted_for: Some("candidate-x".to_string())
        }
    );
    rm_dir(&dir);
}

#[tokio::test]
async fn with_storage_higher_term_via_append_entries_persists() {
    let dir = temp_dir("append");
    rm_dir(&dir);
    let engine = make_engine(Some(dir.clone()));
    engine
        .handle_append_entries(
            "leader-x".to_string(),
            12,
            0,
            vec![],
        )
        .await;
    let store = FileVoterStateStore::open(&dir).unwrap();
    let persisted = store.load().unwrap();
    assert_eq!(persisted.term, 12);
    assert!(persisted.voted_for.is_none());
    rm_dir(&dir);
}

#[tokio::test]
async fn second_engine_loads_persisted_vote() {
    let dir = temp_dir("reload");
    rm_dir(&dir);
    {
        let engine = make_engine(Some(dir.clone()));
        engine
            .handle_vote("candidate-y".to_string(), 21, 0)
            .await;
    }
    let engine2 = make_engine(Some(dir.clone()));
    assert_eq!(engine2.role().await, Role::Follower);
    assert!(engine2.leader_id().await.is_none());
    let (granted, term) = engine2
        .handle_vote("other-candidate".to_string(), 21, 0)
        .await;
    assert!(!granted);
    assert_eq!(term, 21);
    rm_dir(&dir);
}

#[tokio::test]
async fn revote_same_candidate_is_idempotent() {
    let dir = temp_dir("revote");
    rm_dir(&dir);
    let engine = make_engine(Some(dir.clone()));
    let first = engine
        .handle_vote("candidate-z".to_string(), 3, 0)
        .await;
    let second = engine
        .handle_vote("candidate-z".to_string(), 3, 0)
        .await;
    assert_eq!(first, (true, 3));
    assert_eq!(second, (true, 3));
    let store = FileVoterStateStore::open(&dir).unwrap();
    assert_eq!(
        store.load().unwrap(),
        VoterState {
            term: 3,
            voted_for: Some("candidate-z".to_string())
        }
    );
    rm_dir(&dir);
}

#[tokio::test]
async fn single_node_bootstrap_persists_term_one() {
    let dir = temp_dir("bootstrap");
    rm_dir(&dir);
    let _engine = make_engine(Some(dir.clone()));
    let store = FileVoterStateStore::open(&dir).unwrap();
    let persisted = store.load().unwrap();
    assert_eq!(persisted.term, 1);
    assert!(persisted.voted_for.is_none());
    rm_dir(&dir);
}

#[tokio::test]
async fn step_down_on_higher_observed_term() {
    let engine = make_engine(None);
    assert_eq!(engine.role().await, Role::Leader);
    engine.test_step_down_if_stale(10).await;
    assert_eq!(engine.role().await, Role::Follower);
    let (_, term) = engine
        .handle_vote("peer".to_string(), 9, 0)
        .await;
    assert_eq!(term, 10);
    assert!(engine.leader_id().await.is_none());
}
