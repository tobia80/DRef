//! Wire-format tests shared with the Scala port via golden bytes in
//! `CrossLangCompatSpec.scala`. Both sides must agree for multi-language
//! Redis/Raft clients to interoperate.

use dref_core::{lock_value_from_bytes, lock_value_to_bytes, DRefCodec, MsgPackCodec};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChangePayload {
    name: Vec<u8>,
    value: Vec<u8>,
    delete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Wrapper {
    value: String,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn lock_value_uses_eight_byte_big_endian_i64() {
    let bytes = lock_value_to_bytes(42);
    assert_eq!(bytes.len(), 8);
    assert_eq!(hex(&bytes), "000000000000002a");
    assert_eq!(lock_value_from_bytes(&bytes), Some(42));
}

#[test]
fn change_payload_set_matches_golden_bytes() {
    let codec = MsgPackCodec::<ChangePayload>::new();
    let payload = ChangePayload {
        name: b"my-key".to_vec(),
        value: vec![1, 2, 3],
        delete: false,
    };
    let encoded = codec.serialize(&payload).expect("encode");
    assert_eq!(
        hex(&encoded),
        "83a46e616d65966d792d6b6579a576616c756593010203a664656c657465c2"
    );
}

#[test]
fn change_payload_delete_matches_golden_bytes() {
    let codec = MsgPackCodec::<ChangePayload>::new();
    let payload = ChangePayload {
        name: b"my-key".to_vec(),
        value: Vec::new(),
        delete: true,
    };
    let encoded = codec.serialize(&payload).expect("encode");
    assert_eq!(
        hex(&encoded),
        "83a46e616d65966d792d6b6579a576616c756590a664656c657465c3"
    );
}

#[test]
fn msgpack_string_matches_golden_bytes() {
    let codec = MsgPackCodec::<String>::new();
    let encoded = codec.serialize(&"hello".to_string()).expect("encode");
    assert_eq!(hex(&encoded), "a568656c6c6f");
}

#[test]
fn msgpack_wrapper_struct_matches_golden_bytes() {
    let codec = MsgPackCodec::<Wrapper>::new();
    let encoded = codec
        .serialize(&Wrapper {
            value: "hello".to_string(),
        })
        .expect("encode");
    assert_eq!(hex(&encoded), "81a576616c7565a568656c6c6f");
}
