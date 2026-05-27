//! Verifies the Rust [`FileStateMachineSnapshotStore`] produces byte-identical
//! files to the Scala implementation. Vectors live in
//! [`compat/snapshot_vectors.json`](../../../../compat/snapshot_vectors.json).
//!
//! Compatibility matters because Scala and Rust nodes in a mixed cluster may
//! share a storage volume (e.g. when migrating a node from one runtime to the
//! other), and they must agree on the on-disk byte layout.

use std::fs;
use std::path::PathBuf;

use dref_raft::proto::dref_consensus::{ClusterSnapshot, KvEntry};
use dref_raft::state_machine_snapshot_store::{
    encode_for_test, FileStateMachineSnapshotStore, StateMachineSnapshotStore,
};

fn temp_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "dref-snapshot-compat-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn empty_snapshot_matches_golden_bytes() {
    let snap = ClusterSnapshot::default();
    let bytes = encode_for_test(&snap).unwrap();
    assert_eq!(hex(&bytes), "445246530100000000");
}

#[test]
fn single_entry_matches_golden_bytes() {
    // KVEntry { key:"a", value:[0x01], expire_at:None }
    // ClusterSnapshot { entries:[that] }
    // See compat/snapshot_vectors.json.
    let snap = ClusterSnapshot {
        entries: vec![KvEntry {
            key: "a".to_string(),
            value: vec![0x01],
            expire_at: None,
        }],
        last_seq: 0,
    };
    let bytes = encode_for_test(&snap).unwrap();
    assert_eq!(hex(&bytes), "4452465301000000080a060a0161120101");
}

#[test]
fn last_seq_matches_golden_bytes() {
    // Same single entry as above, plus ClusterSnapshot.last_seq = 42.
    let snap = ClusterSnapshot {
        entries: vec![KvEntry {
            key: "a".to_string(),
            value: vec![0x01],
            expire_at: None,
        }],
        last_seq: 42,
    };
    let bytes = encode_for_test(&snap).unwrap();
    assert_eq!(hex(&bytes), "44524653010000000a0a060a0161120101102a");
}

#[test]
fn file_store_round_trip_matches_in_memory_encoding() {
    let dir = temp_dir("round-trip");
    let _ = fs::remove_dir_all(&dir);
    let snap = ClusterSnapshot {
        entries: vec![KvEntry {
            key: "round".to_string(),
            value: vec![1, 2, 3, 4],
            expire_at: Some(1_700_000_000_000),
        }],
        last_seq: 9,
    };
    let store = FileStateMachineSnapshotStore::open(&dir).unwrap();
    store.save(&snap).unwrap();
    let on_disk = fs::read(store.target()).unwrap();
    let encoded = encode_for_test(&snap).unwrap();
    assert_eq!(hex(&on_disk), hex(&encoded));
    let _ = fs::remove_dir_all(&dir);
}
