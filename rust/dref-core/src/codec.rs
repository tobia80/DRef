//! Codec abstraction. Default implementation is MessagePack via `rmp-serde`,
//! chosen for binary compatibility with the Scala impl's
//! `zio-schema-msg-pack` codec.

use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::DRefError;

/// A codec converts values of type `T` to/from a byte buffer.
///
/// The trait is intentionally object-safe-free and synchronous: serialization
/// in our case is fast and CPU-bound, no need to drag `async` through it.
pub trait DRefCodec<T>: Send + Sync + 'static {
    fn serialize(&self, value: &T) -> Result<Vec<u8>, DRefError>;
    fn deserialize(&self, bytes: &[u8]) -> Result<T, DRefError>;
}

/// Default MessagePack codec. Uses `rmp-serde` under the hood.
pub struct MsgPackCodec<T> {
    _phantom: PhantomData<fn() -> T>,
}

impl<T> MsgPackCodec<T> {
    pub const fn new() -> Self {
        Self { _phantom: PhantomData }
    }
}

impl<T> Default for MsgPackCodec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> DRefCodec<T> for MsgPackCodec<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn serialize(&self, value: &T) -> Result<Vec<u8>, DRefError> {
        rmp_serde::to_vec(value).map_err(|e| DRefError::Serialize(e.to_string()))
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<T, DRefError> {
        rmp_serde::from_slice(bytes).map_err(|e| DRefError::Deserialize(e.to_string()))
    }
}
