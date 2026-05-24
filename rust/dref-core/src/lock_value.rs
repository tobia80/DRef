//! Wire format for distributed lock tokens. Both Scala and Rust clients store
//! lock ownership as an 8-byte big-endian i64 so mixed-language clusters can
//! detect stolen locks consistently.

/// Encode a lock token to its on-the-wire representation.
pub fn to_bytes(value: i64) -> [u8; 8] {
    value.to_be_bytes()
}

/// Decode a lock token from its on-the-wire representation.
pub fn from_bytes(bytes: &[u8]) -> Option<i64> {
    let arr: [u8; 8] = bytes.try_into().ok()?;
    Some(i64::from_be_bytes(arr))
}
