//! Redis-backed [`DRefContext`] implementation.
//!
//! Port of the Scala `dref-redis` module. Stores opaque byte values in Redis
//! (`SET`/`GET`/`DEL`), refreshes TTLs with `EXPIRE`, and broadcasts change
//! events through the `dref-change` Redis pub/sub channel.
//!
//! The on-the-wire payload format is binary-compatible with the Scala impl:
//! a MessagePack-encoded `ChangePayload { name, value, delete }` struct
//! produced by `rmp-serde` / `zio-schema-msg-pack`.
//!
//! [`DRefContext`]: dref_core::DRefContext

mod context;

pub use context::{RedisConfig, RedisDRefContext};
