//! Rust port of `dref-core`: distributed references built on a pluggable
//! key/value backend (`DRefContext`).
//!
//! See the [`DRef`] type for the main entry point and [`LocalDRefContext`] for
//! the bundled in-memory backend (used in tests and single-process apps).

mod codec;
mod context;
mod dref;
mod error;
mod local;
mod lock;
mod lock_value;

pub use codec::{DRefCodec, MsgPackCodec};
pub use context::{ChangeEvent, DRefContext, StolenElement};
pub use dref::{DRef, IdProvider};
pub use error::{DRefError, LockStolenError};
pub use local::LocalDRefContext;
pub use lock::lock_with_context;
pub use lock_value::{from_bytes as lock_value_from_bytes, to_bytes as lock_value_to_bytes};
