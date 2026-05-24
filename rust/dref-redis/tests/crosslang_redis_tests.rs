//! Cross-language Redis tests: reads values written by `CrossLangRedisSpec`
//! (Scala) and verifies MsgPack + lock-token wire compatibility.
//!
//! Run via `./scripts/crosslang-redis-compat.sh` (Scala writer first, then
//! these tests). Gated behind `test-redis` like the other Redis integration
//! tests.

#![cfg(feature = "test-redis")]

use std::time::Duration;

use dref_core::{lock_value_from_bytes, DRef, DRefContext, DRefError};
use dref_redis::{RedisConfig, RedisDRefContext};
use serde::{Deserialize, Serialize};

const CROSS_LANG_KEY: &str = "dref:compat:crosslang-test";
const CROSS_LANG_LOCK_KEY: &str = "dref:compat:crosslang-lock";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Wrapper {
    value: String,
}

fn test_config() -> RedisConfig {
    RedisConfig {
        host: "localhost".to_string(),
        port: 6379,
        database: 0,
        username: None,
        password: None,
        ca_cert: None,
        ttl: Some(Duration::from_secs(5)),
    }
}

#[tokio::test]
#[ignore = "run via scripts/crosslang-redis-compat.sh after Scala CrossLangRedisSpec"]
async fn reads_scala_written_msgpack_value() -> Result<(), DRefError> {
    let ctx = RedisDRefContext::new(test_config())
        .await
        .expect("redis must be running on localhost:6379");

    // Do not call make() — that would seed the key. Scala writer must have run first.
    let raw = ctx.get_element(CROSS_LANG_KEY).await?;
    assert!(
        raw.is_some(),
        "expected Scala CrossLangRedisSpec to have written {CROSS_LANG_KEY}; run ./scripts/crosslang-redis-compat.sh"
    );

    // Decode with the same MsgPack codec Rust uses for all DRefs.
    let aref = DRef::<Wrapper, _>::make_with_name(&ctx, CROSS_LANG_KEY, || {
        Wrapper {
            value: "unused".to_string(),
        }
    })
    .await?;
    let value = aref.get().await?;
    assert_eq!(value, Wrapper {
        value: "scala-updated".to_string()
    });

    // Rust can write back; Scala could read in a follow-up run.
    aref
        .set(Wrapper {
            value: "rust-updated".to_string(),
        })
        .await?;
    let roundtrip = aref.get().await?;
    assert_eq!(
        roundtrip,
        Wrapper {
            value: "rust-updated".to_string()
        }
    );

    ctx.delete_element(CROSS_LANG_KEY).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "run via scripts/crosslang-redis-compat.sh after Scala CrossLangRedisSpec"]
async fn reads_scala_written_lock_token() -> Result<(), DRefError> {
    let ctx = RedisDRefContext::new(test_config())
        .await
        .expect("redis must be running on localhost:6379");

    let raw = ctx.get_element(CROSS_LANG_LOCK_KEY).await?;
    let bytes = raw.expect("expected Scala to have written lock token");
    assert_eq!(bytes.len(), 8);
    assert_eq!(
        lock_value_from_bytes(&bytes),
        Some(0x1234567890abcdef_i64)
    );

    ctx.delete_element(CROSS_LANG_LOCK_KEY).await?;
    Ok(())
}
