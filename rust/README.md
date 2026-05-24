# DRef (Rust)

Distributed Ref (DRef) is a library for synchronising state and coordination
primitives across distributed nodes. It lets you treat distributed state the
same way you would work with an ordinary in-memory ref, while also giving you
cluster-wide locks and change streams when you need stronger coordination.

This workspace is the Rust port of the Scala
[`dref-core`](../README.md) project. It exposes the same conceptual API
(`DRef::make`, `update`, `get`, `modify`, `lock_with_context`, change streams)
backed by either an in-memory store, Redis, or a Raft cluster.

## Crates

- `dref-core` — core traits, the user-facing `DRef<T>`, the in-memory
  `LocalDRefContext`, the MsgPack codec, and the distributed-locking
  primitive.
- `dref-redis` — Redis-backed `RedisDRefContext` (uses Redis keyspace
  notifications for change streams and TTLs for lock keep-alive).
- `dref-raft` — Raft consensus-backed `RaftDRefContext` for strongly
  consistent multi-node deployments.
- `examples` — runnable demos (`dref-examples` binary plus per-recipe
  `cargo --example` targets).

## Quick start

Add the dependency:

```toml
[dependencies]
dref-core = { git = "https://github.com/tobia80/DRef" }
tokio = { version = "1", features = ["full"] }
```

Create a distributed reference, update it, read it back:

```rust
use dref_core::{DRef, LocalDRefContext};

#[tokio::main]
async fn main() {
    let ctx = LocalDRefContext::new();
    let ref_value = DRef::make(&ctx, || 0).await.unwrap();
    ref_value.update(|v| v + 1).await.unwrap();
    let current = ref_value.get().await.unwrap();
    println!("Current value: {}", current);
}
```

`LocalDRefContext` is an in-memory implementation, perfect for tests or single
process apps. Swap it for `RedisDRefContext` or `RaftDRefContext` when you are
ready to go multi-node — every `DRef<T>` method works the same regardless of
backend.

## Running the examples

The `examples` crate ships three recipes:

```bash
# Run all three examples in sequence:
cd rust && cargo run -p dref-examples

# Or run a specific example on its own:
cd rust && cargo run --example quickstart
cd rust && cargo run --example leader_election
cd rust && cargo run --example distributed_lock
```

- `quickstart` — the snippet above.
- `leader_election` — uses `get_and_update` on a `LeaderState` to atomically
  claim leadership iff nobody is leader yet.
- `distributed_lock` — wraps a critical section with `lock_with_context`
  keyed by `"daily-report"` so only one node runs it at a time.

## Running the tests

```bash
# Core (in-memory, fast, no external deps)
cargo test -p dref-core

# Raft backend (in-process cluster; no external services required)
cargo test -p dref-raft

# Redis backend — requires Redis on localhost:6379. The integration tests
# are gated behind the `test-redis` feature so they don't run by default:
cargo test -p dref-redis --features test-redis
```

## Switching backends

Every example above uses `LocalDRefContext` for brevity. To switch backends,
construct the corresponding context type and pass it to `DRef::make` instead —
the rest of your code is unchanged:

```rust,ignore
// Redis backend
use dref_redis::{RedisConfig, RedisDRefContext};
let ctx = RedisDRefContext::connect(RedisConfig::default()).await?;

// Raft backend
use dref_raft::{NodeEndpoint, RaftConfig, RaftDRefContext};
let ctx = RaftDRefContext::start(RaftConfig {
    node_id: 1,
    listen_addr: "0.0.0.0:8082".parse()?,
    peers: vec![/* NodeEndpoint { id, addr } ... */],
    ..Default::default()
}).await?;

let ref_value = dref_core::DRef::make(&ctx, || 0).await?;
```

Configuration notes:

- **`dref-redis`** — set `RedisConfig::host` / `RedisConfig::port` (default
  `127.0.0.1:6379`). Make sure Redis is started with keyspace notifications
  enabled (`notify-keyspace-events Kg$x` or wider) so change-stream and
  lock-stolen detection work.
- **`dref-raft`** — every node needs its own `node_id`, a local `listen_addr`,
  and the `peers` list of `NodeEndpoint`s for the rest of the cluster.

## Architecture

`DRef<T>` is the user-facing handle. It is a thin wrapper around three things:

1. A `DRefContext` — the backend trait. It stores opaque `Vec<u8>` values
   keyed by `&str`, supports TTLs, exposes a change stream, and offers two
   backend-specific hooks (`detect_deletion_from_underlying_stream` and
   `detect_stolen_element`) that the locking primitive uses to detect lock
   loss without polling.
2. A `DRefCodec<T>` — converts `T` to/from bytes. The default is `MsgPackCodec`
   (via `rmp-serde`); you can plug your own if you need a different wire
   format.
3. A name — derived automatically from the caller's `file:line` so each call
   site gets a stable, unique key. Use `DRef::make_with_name` to override.

`lock_with_context` builds a TTL-protected lock on top of any backend by:

1. Atomically writing a random per-acquire value at the lock key (via
   `set_element_if_not_exist`).
2. Spawning a keep-alive task that refreshes the TTL.
3. Spawning a stolen-detection task that watches for the value at the key
   changing — if it does, the user's body is aborted with `LockStolen`.
4. Releasing the lock when the body finishes (unless it was stolen, in which
   case the new owner's entry is left untouched).

Because all of the backend specifics live behind `DRefContext`, application
code only ever depends on `dref-core`. You pick a backend at the edge of your
program, hand the context to `DRef::make`, and never touch the trait again.

## License

Licensed under the Apache License, Version 2.0 (see [`LICENSE`](../LICENSE)).
