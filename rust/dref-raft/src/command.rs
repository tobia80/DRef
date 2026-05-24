//! Commands written into the replicated log. The leader applies each
//! command to its state machine and broadcasts it to followers via gRPC.
//!
//! All commands are msgpack-encoded for the wire — this matches the
//! Scala/MicroRaft side, where state-machine operations are also opaque
//! blobs from the consensus engine's point of view.

use serde::{Deserialize, Serialize};

/// Mutations applied to the replicated key/value store. Every write goes
/// through the leader, which appends a command, applies it locally, then
/// fans it out to followers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DRefCommand {
    /// Unconditional write of `value` to `name`. Overwrites any existing
    /// value. `expire_at` is a unix-millis absolute deadline.
    SetElement {
        name: String,
        value: Vec<u8>,
        expire_at: Option<u64>,
    },
    /// Conditional write: only writes if `name` is absent. The leader's
    /// response indicates whether a write happened.
    SetElementIfNotExist {
        name: String,
        value: Vec<u8>,
        expire_at: Option<u64>,
    },
    /// Remove `name`.
    DeleteElement { name: String },
    /// Refresh the TTL on `name`. No-op if `name` is absent.
    ExpireElement { name: String, expire_at: u64 },
    /// Delete `name` iff its `expire_at` is on or before `now`. Used by the
    /// background TTL reaper running on the leader.
    DeleteIfExpired { name: String, now: u64 },
    /// Sentinel applied on every new leader term. Lets a state machine know
    /// when a new leader is elected — matches MicroRaft's
    /// `StartNewTermOpProto` (currently a no-op for our state machine).
    StartNewTerm,
}

impl DRefCommand {
    /// Serialize this command to msgpack for inclusion in an AppendEntries
    /// RPC.
    pub fn to_bytes(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec_named(self)
    }

    /// Deserialize a command from its msgpack bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(bytes)
    }
}
