//! Golden command-log wire-format tests shared with Scala `CommandLogStoreSpec`.

use dref_raft::command_log_store::{encode_for_test, CommandLogEntry, CommandLogState, CommandLogStore};
use dref_raft::state_command::{self, StateCommand};
use dref_raft::FileCommandLogStore;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn read_golden_hex(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../compat/command_log_vectors.json");
    let text = std::fs::read_to_string(path).expect("read command_log_vectors.json");
    let needle = format!("\"name\": \"{name}\"");
    let start = text.find(&needle).expect("vector name");
    let tail = &text[start..];
    let hex_key = tail.find("\"hex\": \"").expect("hex field") + 8;
    let rest = &tail[hex_key..];
    let end = rest.find('"').expect("hex end");
    rest[..end].to_string()
}

#[test]
fn single_set_element_record_matches_golden_bytes() {
    let cmd = StateCommand::set_element("my-key", vec![1, 2, 3], Some(1_700_000_000));
    let command = state_command::encode(&cmd).expect("encode");
    let encoded = encode_for_test(
        &CommandLogEntry {
            seq: 1,
            command,
        },
        0,
    )
    .expect("encode log");
    assert_eq!(
        hex(&encoded),
        read_golden_hex("single_set_element_record")
    );
}

#[test]
fn file_store_round_trip() {
    let dir = std::env::temp_dir().join(format!(
        "dref-crosslang-cmdlog-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let store = FileCommandLogStore::open(&dir).expect("open");
    store.append(3, &[9, 9]).expect("append");
    store.set_commit_seq(3).expect("commit");
    let loaded = store.load().expect("load");
    assert_eq!(loaded, CommandLogState {
        commit_seq: 3,
        entries: [(3, vec![9, 9])].into_iter().collect(),
    });
    let _ = std::fs::remove_dir_all(&dir);
}
