//! Golden voter-state file bytes shared with Scala `CrossLangVoterStateSpec`.

use dref_raft::voter_state_store::{encode_for_test, VoterState};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn term_7_no_vote_matches_golden_bytes() {
    let encoded = encode_for_test(&VoterState {
        term: 7,
        voted_for: None,
    })
    .expect("encode");
    assert_eq!(hex(&encoded), "44524654010000000000000007ffffffff");
}

#[test]
fn term_42_voted_node_7_matches_golden_bytes() {
    let encoded = encode_for_test(&VoterState {
        term: 42,
        voted_for: Some("node-7".to_string()),
    })
    .expect("encode");
    assert_eq!(
        hex(&encoded),
        "4452465401000000000000002a000000066e6f64652d37"
    );
}
