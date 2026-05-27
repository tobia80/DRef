//! Mirrors Scala `ProtoConsensusPersistenceSpec` — voter-state durability
//! wired through the in-process consensus engine (no gRPC server).

use std::path::PathBuf;
use std::time::Duration;

use dref_raft::config::{NodeEndpoint, RaftConfig};
use dref_raft::consensus::{Consensus, Role};
use dref_raft::state_command::{self as sc, SetElementCommand, StateCommand};
use dref_raft::state_machine::StateMachine;
use dref_raft::state_machine_snapshot_store::{
    FileStateMachineSnapshotStore, StateMachineSnapshotStore,
};
use dref_raft::voter_state_store::{FileVoterStateStore, VoterState, VoterStateStore};

fn temp_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "dref-consensus-persist-{name}-{}",
        std::process::id()
    ))
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
        initial_endpoints: vec![NodeEndpoint::new("node-under-test", "127.0.0.1:0")],
        storage_dir: storage,
        ..RaftConfig::default()
    }
}

async fn make_engine(storage: Option<PathBuf>) -> Consensus {
    let sm = StateMachine::new();
    Consensus::new(
        "node-under-test".to_string(),
        sm,
        single_node_config(storage),
    )
    .await
}

async fn make_engine_with_machine(
    storage: Option<PathBuf>,
    snapshot_every: u32,
) -> (StateMachine, Consensus) {
    let sm = StateMachine::new();
    let mut config = single_node_config(storage);
    config.snapshot_every = snapshot_every;
    let consensus = Consensus::new("node-under-test".to_string(), sm.clone(), config).await;
    (sm, consensus)
}

#[tokio::test]
async fn without_storage_vote_grant_does_not_touch_disk() {
    let engine = make_engine(None).await;
    assert_eq!(engine.role().await, Role::Leader);
    let (granted, term) = engine.handle_vote("other-node".to_string(), 5, 0).await;
    assert!(granted);
    assert_eq!(term, 5);
    assert_eq!(engine.role().await, Role::Follower);
}

#[tokio::test]
async fn with_storage_vote_is_persisted() {
    let dir = temp_dir("vote");
    rm_dir(&dir);
    let engine = make_engine(Some(dir.clone())).await;
    engine.handle_vote("candidate-x".to_string(), 9, 0).await;
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
    let engine = make_engine(Some(dir.clone())).await;
    engine
        .handle_append_entries("leader-x".to_string(), 12, 0, vec![])
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
        let engine = make_engine(Some(dir.clone())).await;
        engine.handle_vote("candidate-y".to_string(), 21, 0).await;
    }
    let engine2 = make_engine(Some(dir.clone())).await;
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
    let engine = make_engine(Some(dir.clone())).await;
    let first = engine.handle_vote("candidate-z".to_string(), 3, 0).await;
    let second = engine.handle_vote("candidate-z".to_string(), 3, 0).await;
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
    let _engine = make_engine(Some(dir.clone())).await;
    let store = FileVoterStateStore::open(&dir).unwrap();
    let persisted = store.load().unwrap();
    assert_eq!(persisted.term, 1);
    assert!(persisted.voted_for.is_none());
    rm_dir(&dir);
}

#[tokio::test]
async fn pre_vote_does_not_persist_or_mutate_state() {
    // PreVote is purely a "would you vote for me?" probe. Granting one must
    // not bump term, set voted_for, or touch the durable store.
    let dir = temp_dir("prevote-no-persist");
    rm_dir(&dir);
    let engine = make_engine(Some(dir.clone())).await;
    // Single-node bootstrap leaves us as Leader at term=1, with no recent
    // external leader heartbeat. Step down so we're a follower with no
    // leader_id — the leader-stickiness guard should NOT kick in here.
    engine.test_step_down_if_stale(2).await;

    let (granted, term) = engine
        .handle_pre_vote("candidate-x".to_string(), 99, 0)
        .await;
    assert!(
        granted,
        "fresh follower with no recent leader should grant pre-vote"
    );
    assert_eq!(
        term, 2,
        "voter must return its CURRENT term, not the candidate's"
    );

    // Disk must reflect what was there before the pre-vote call.
    let store = FileVoterStateStore::open(&dir).unwrap();
    let persisted = store.load().unwrap();
    assert_eq!(persisted.term, 2);
    assert!(
        persisted.voted_for.is_none(),
        "pre-vote must not record a vote"
    );
    rm_dir(&dir);
}

#[tokio::test]
async fn pre_vote_refused_when_recent_leader_heartbeat() {
    // The whole point of PreVote: a candidate cannot disrupt a leader the
    // voter has just heard from. Simulate "I just got a heartbeat" by
    // calling handle_heartbeat, then assert pre-vote refuses.
    let engine = make_engine(None).await;
    // Step down from single-node leader so we're a follower observing an
    // external leader.
    engine.test_step_down_if_stale(1).await;
    engine.handle_heartbeat("leader-x".to_string(), 1).await;

    let (granted, term) = engine.handle_pre_vote("disruptor".to_string(), 50, 0).await;
    assert!(!granted, "must refuse pre-vote while a leader is fresh");
    assert_eq!(term, 1, "must not adopt the candidate's hypothetical term");
}

#[tokio::test]
async fn pre_vote_refused_when_candidate_term_not_strictly_greater() {
    let engine = make_engine(None).await;
    engine.test_step_down_if_stale(5).await;
    // Equal term: not strictly greater, must refuse.
    let (granted, _) = engine.handle_pre_vote("candidate".to_string(), 5, 0).await;
    assert!(!granted);
    // Lower term: must refuse.
    let (granted, _) = engine.handle_pre_vote("candidate".to_string(), 4, 0).await;
    assert!(!granted);
}

#[tokio::test]
async fn step_down_on_higher_observed_term() {
    let engine = make_engine(None).await;
    assert_eq!(engine.role().await, Role::Leader);
    engine.test_step_down_if_stale(10).await;
    assert_eq!(engine.role().await, Role::Follower);
    let (_, term) = engine.handle_vote("peer".to_string(), 9, 0).await;
    assert_eq!(term, 10);
    assert!(engine.leader_id().await.is_none());
}

fn set_element(name: &str, value: Vec<u8>) -> StateCommand {
    StateCommand {
        op: Some(sc::state_command::Op::SetElement(SetElementCommand {
            name: name.to_string(),
            value,
            expire_at: None,
        })),
    }
}

#[tokio::test]
async fn explicit_snapshot_persists_state_machine_contents() {
    let dir = temp_dir("snap-explicit");
    rm_dir(&dir);
    let (_sm, engine) = make_engine_with_machine(Some(dir.clone()), 1000).await;
    let _ = engine.submit(set_element("alpha", vec![1, 2, 3])).await;
    let _ = engine.submit(set_element("beta", vec![42])).await;
    engine.take_and_persist_snapshot().await.unwrap();
    let verifier = FileStateMachineSnapshotStore::open(&dir).unwrap();
    let loaded = verifier.load().unwrap().expect("snapshot saved");
    assert_eq!(loaded.last_seq, 2);
    assert_eq!(loaded.entries.len(), 2);
    let alpha = loaded
        .entries
        .iter()
        .find(|e| e.key == "alpha")
        .expect("alpha entry");
    assert_eq!(alpha.value, vec![1, 2, 3]);
    let beta = loaded
        .entries
        .iter()
        .find(|e| e.key == "beta")
        .expect("beta entry");
    assert_eq!(beta.value, vec![42]);
    rm_dir(&dir);
}

#[tokio::test]
async fn restarted_engine_hydrates_state_machine_from_disk() {
    let dir = temp_dir("snap-restart");
    rm_dir(&dir);
    // First engine submits some writes and explicitly snapshots, then is dropped.
    {
        let (_sm, engine) = make_engine_with_machine(Some(dir.clone()), 1000).await;
        let _ = engine.submit(set_element("a", vec![1])).await;
        let _ = engine.submit(set_element("b", vec![2])).await;
        engine.take_and_persist_snapshot().await.unwrap();
    }
    // Second engine should pick up the snapshot during Consensus::new, BEFORE
    // peers come online or any reads are served.
    let (sm2, _engine2) = make_engine_with_machine(Some(dir.clone()), 1000).await;
    assert_eq!(sm2.get("a").await.as_deref(), Some(&[1u8][..]));
    assert_eq!(sm2.get("b").await.as_deref(), Some(&[2u8][..]));
    rm_dir(&dir);
}

#[tokio::test]
async fn restarted_engine_restores_snapshot_last_seq_for_vote_freshness() {
    let dir = temp_dir("snap-restart-last-seq");
    rm_dir(&dir);
    {
        let (_sm, engine) = make_engine_with_machine(Some(dir.clone()), 1000).await;
        let _ = engine.submit(set_element("a", vec![1])).await;
        let _ = engine.submit(set_element("b", vec![2])).await;
        engine.take_and_persist_snapshot().await.unwrap();
    }
    let (_sm2, engine2) = make_engine_with_machine(Some(dir.clone()), 1000).await;
    let (granted, term) = engine2
        .handle_vote("behind-candidate".to_string(), 2, 1)
        .await;
    assert!(
        !granted,
        "candidate with last_seq=1 must lose against restored last_seq=2"
    );
    assert_eq!(term, 2);
    rm_dir(&dir);
}

#[tokio::test]
async fn snapshot_every_one_triggers_auto_snapshot() {
    let dir = temp_dir("snap-auto");
    rm_dir(&dir);
    let (_sm, engine) = make_engine_with_machine(Some(dir.clone()), 1).await;
    let _ = engine.submit(set_element("auto", vec![7])).await;
    let verifier = FileStateMachineSnapshotStore::open(&dir).unwrap();
    // Background fiber: wait up to 2s for the snapshot file to materialise.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut loaded = None;
    while std::time::Instant::now() < deadline {
        if let Ok(Some(snap)) = verifier.load() {
            if !snap.entries.is_empty() {
                loaded = Some(snap);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let loaded = loaded.expect("automatic snapshot should land on disk");
    assert!(loaded.entries.iter().any(|e| e.key == "auto"));
    rm_dir(&dir);
}

#[tokio::test]
async fn snapshot_every_zero_disables_auto_snapshot() {
    let dir = temp_dir("snap-disabled");
    rm_dir(&dir);
    let (_sm, engine) = make_engine_with_machine(Some(dir.clone()), 0).await;
    for i in 0..50u8 {
        let _ = engine.submit(set_element(&format!("k{i}"), vec![i])).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let verifier = FileStateMachineSnapshotStore::open(&dir).unwrap();
    assert!(verifier.load().unwrap().is_none());
    rm_dir(&dir);
}
