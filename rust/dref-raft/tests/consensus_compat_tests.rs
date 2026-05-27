//! Golden protobuf wire-format tests shared with Scala `CrossLangConsensusCompatSpec`.

use dref_raft::proto::dref_consensus::ClusterSnapshot;
use dref_raft::proto::dref_consensus::KvEntry;
use dref_raft::state_command::{self, StateCommand};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn state_command_set_element_matches_golden_bytes() {
    let cmd = StateCommand::set_element("my-key", vec![1, 2, 3], Some(1_700_000_000));
    let encoded = state_command::encode(&cmd).expect("encode");
    assert_eq!(hex(&encoded), "0a130a066d792d6b657912030102031880e2cfaa06");
}

#[test]
fn state_command_delete_element_matches_golden_bytes() {
    let cmd = StateCommand::delete_element("my-key");
    let encoded = state_command::encode(&cmd).expect("encode");
    assert_eq!(hex(&encoded), "1a080a066d792d6b6579");
}

#[test]
fn cluster_snapshot_single_entry_matches_golden_bytes() {
    let snapshot = ClusterSnapshot {
        entries: vec![KvEntry {
            key: "shared-key".to_string(),
            value: vec![0x68, 0x69],
            expire_at: Some(1_700_000_000),
        }],
        last_seq: 0,
    };
    let encoded = prost::Message::encode_to_vec(&snapshot);
    assert_eq!(
        hex(&encoded),
        "0a160a0a7368617265642d6b6579120268691880e2cfaa06"
    );
}

#[test]
fn append_entries_carries_protobuf_state_command() {
    use dref_raft::proto::dref_consensus::AppendEntriesRequest;
    use prost::Message;

    let cmd = StateCommand::set_element("shared-key", vec![0x68, 0x69], Some(1_700_000_000));
    let command = state_command::encode(&cmd).expect("encode");
    let req = AppendEntriesRequest {
        leader_id: "node-0".to_string(),
        term: 7,
        command,
        seq: 99,
    };
    let encoded = req.encode_to_vec();
    let decoded = AppendEntriesRequest::decode(encoded.as_slice()).expect("decode");
    assert_eq!(decoded.leader_id, "node-0");
    assert_eq!(decoded.term, 7);
    assert_eq!(decoded.seq, 99);
    let roundtrip = state_command::decode(&decoded.command).expect("decode cmd");
    assert!(matches!(
        roundtrip.op,
        Some(state_command::state_command::Op::SetElement(_))
    ));
}
