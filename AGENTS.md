# AGENTS.md

## Cursor Cloud specific instructions

### Project overview

DRef (Distributed Ref) is a library for synchronising state across distributed nodes. It has two implementations:

- **Scala/JVM** (primary, in repo root): built with ZIO, Scala 3.8.3, sbt 1.12.0
- **Rust port** (in `rust/`): built with Tokio, async-trait, tonic (gRPC)

Both expose the same conceptual API with three backends: in-memory, Redis, and Raft.

The Raft backend on both sides is a hand-rolled implementation that speaks the
shared protobuf services in [`proto/`](proto/) — Scala and Rust nodes can join
the same Raft cluster. There is no longer a separate Microraft-based engine on
the Scala side; the single proto engine (`io.github.tobia80.dref.raft.proto`)
is the only consensus implementation.

### Required system dependencies

- **JDK 21** (pre-installed on Ubuntu)
- **sbt 1.12.0** — installed to `/usr/local/share/sbt`; binary linked at `/usr/local/bin/sbt`
- **Rust stable (1.85+)** — the workspace Cargo.lock pulls crates that require `edition2024`; run `rustup default stable` if the default is outdated
- **Redis 7+** — required for `dref-redis` integration tests (both Scala and Rust)

### Starting Redis

```bash
sudo redis-server --daemonize yes
redis-cli ping  # should return PONG
```

### Running tests

| Scope | Command | External deps |
|-------|---------|---------------|
| Scala – all | `sbt test` | Redis on localhost:6379; PostgreSQL on localhost:5432 (`dref_test` DB) for Raft postgres storage tests |
| Scala – core + raft only | `sbt dref-core/test dref-raft/test` | None |
| Rust – core + raft | `cd rust && cargo test -p dref-core -p dref-raft` | None |
| Rust – redis | `cd rust && cargo test -p dref-redis --features test-redis` | Redis on localhost:6379 |

### Lint / format checks

- **Scala**: `sbt scalafmtCheck` (format check); `sbt scalafmt` (auto-fix)
- **Rust**: `cargo clippy --all-targets --all-features` (in `rust/`)

### Running examples

- **Rust**: `cd rust && cargo run -p dref-examples` (runs quickstart, leader election, distributed lock)
- **Scala/Docker cluster**: `docker compose up --build --scale dref-example=3` (optional, requires Docker)

### Gotchas

- The default Rust toolchain in the base image (1.83) is too old. The update script runs `rustup default stable` to ensure `edition2024` crate support.
- `sbt scalafixAll --check` fails with "NoRulesError" because no scalafix rules are configured — this is expected and not a real lint failure.
- `scalafmtCheck` may report pre-existing formatting drift; use `sbt scalafmt` to auto-fix if needed before committing changes.
- The first `sbt` invocation downloads Scala compiler artifacts and project dependencies; expect ~30s cold-start.
