//! Error types returned by [`crate::DRef`] and [`crate::DRefContext`] APIs.

use std::io;

use thiserror::Error;

/// Returned when a lock previously held by this process is observed to be held
/// by a different owner (e.g. a different node won an election after our TTL
/// expired). Mirrors `LockStolenException` in the Scala impl.
#[derive(Debug, Clone, Error)]
#[error("Lock {name} with value {value} has been stolen")]
pub struct LockStolenError {
    pub name: String,
    pub value: i64,
}

/// Top-level error type for the `dref-core` crate.
#[derive(Debug, Error)]
pub enum DRefError {
    #[error("Element {0} not found")]
    NotFound(String),

    #[error("Serialization failed: {0}")]
    Serialize(String),

    #[error("Deserialization failed: {0}")]
    Deserialize(String),

    #[error(transparent)]
    LockStolen(#[from] LockStolenError),

    #[error("Backend error: {0}")]
    Backend(String),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("{0}")]
    Other(String),
}

impl DRefError {
    pub fn other<S: Into<String>>(msg: S) -> Self {
        DRefError::Other(msg.into())
    }
}
