# DRef

[![CI](https://github.com/tobia80/DRef/actions/workflows/scala.yml/badge.svg)](https://github.com/tobia80/DRef/actions/workflows/scala.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.tobia80/dref-core_3.svg?logo=sonatype)](https://central.sonatype.com/artifact/io.github.tobia80/dref-core_3)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Scala Version](https://img.shields.io/badge/Scala-3.7.3-DC322F?logo=scala)](https://www.scala-lang.org/)
[![Rust](https://img.shields.io/badge/Rust-stable-orange?logo=rust)](rust/README.md)
[![Cross-language](https://img.shields.io/badge/binary%20compatible-Scala%20%2B%20Rust-blue)](#cross-language-compatibility)

Distributed Ref (DRef) is a library for synchronising state and coordination
primitives across distributed nodes. The primary implementation is **Scala/ZIO**
(in this repository root); a **Rust/Tokio** port lives in [`rust/`](rust/README.md).
Both expose the same conceptual API and share on-the-wire formats so mixed
clusters can interoperate.

It lets you treat distributed state the same way you would work with an
ordinary `Ref`, while also giving you cluster-wide locks and change streams
when you need stronger coordination.

DRef makes it easy to build services that stay in sync without wiring together
custom replication logic, retry loops, or bespoke consensus code.

## Highlights
- **Drop-in `Ref` semantics.** Work with a distributed value using familiar
  transactional combinators such as `get`, `set`, `update`, and `modifyZIO`.
- **Strong consistency where it matters.** State is changed in a strong consistent way, using proper backend primitives and guarantees
- **Locking & notifications built-in.** Use distributed locks for mutual
  exclusion or subscribe to change streams to broadcast domain events.
- **Backend flexibility.** Switch between Raft, Redis, or in-memory
  implementations to match the deployment environment.
- **Cross-language binary compatibility.** Scala and Rust nodes can share the
  same Redis keys, Raft state-command payloads, and gRPC services when using the default
  MsgPack codec and shared protobuf definitions (see
  [Cross-language compatibility](#cross-language-compatibility)).
- **Codec agnostic.** Bring your own codecs (e.g. Desert or MsgPack) on the
  Scala side; the default MsgPack wire format is verified against the Rust port.

## When to use DRef
DRef shines whenever you need low-latency coordination across distributed
services — whether they are all on the JVM, all in Rust, or a mix of both. A
few high-impact scenarios include:

- **Leader election & failover.** Run one active worker, fail over in
  seconds, and log who took over.
- **Dynamic configuration flags.** Push feature toggles or rollout percentages
  to every node without redeploying.
- **Cross-service task scheduling.** Coordinate which node processes a batch or
  ensure only one node runs a cron job at a time.
- **Collaborative counters and stats.** Maintain cluster-wide metrics (e.g.
  connected users, processed jobs, scheduled alerts) that update atomically from any node.
- **Event notifications.** Fan-out domain events through change streams while
  sharing a strongly consistent reference to the latest state.

## Quick start
Add DRef to your `build.sbt`:

```scala
// Core (in-memory backend, good for tests and single-node usage)
libraryDependencies += "io.github.tobia80" %% "dref-core" % "<latest-version>"

// Raft consensus backend (for multi-node clusters)
libraryDependencies += "io.github.tobia80" %% "dref-raft" % "<latest-version>"

// Redis backend
libraryDependencies += "io.github.tobia80" %% "dref-redis" % "<latest-version>"
```

Create a distributed reference, update it, and observe the changes:

```scala
import io.github.tobia80.dref.{DRef, DRefContext}
import zio.*

object QuickStart extends ZIOAppDefault:
  override def run =
    (for
      ref <- DRef.make[Int](0)
      _   <- ref.update(_ + 1)
      v   <- ref.get
      _   <- ZIO.logInfo(s"Current value: $v")
    yield ()).provide(DRefContext.local, Scope.default)
```

`DRefContext.local` gives you an in-memory implementation, perfect for tests or
trying the API in a single process. Swap in the Raft or Redis layer when you are
ready to go multi-node.

## Use-case playbook
The following short recipes show how DRef helps with common distributed tasks.

### 1. Coordinated background workers
Keep only one active worker per partition while others stay hot and ready to
fail over:

```scala
import io.github.tobia80.dref.{DRef, DRefContext, ManualId}
import zio.*

final case class WorkerState(leader: Option[String])

object LeaderElection extends ZIOAppDefault:
  private val nodeId = java.util.UUID.randomUUID().toString

  override def run =
    (for
      leadership <- DRef.make(WorkerState(None))
      _          <- leadership.getAndUpdateZIO { state =>
                      state.leader.fold(
                        ZIO.succeed(state.copy(leader = Some(nodeId))) <*
                          ZIO.logInfo("Claimed leadership")
                      )(leader =>
                        ZIO.logInfo(s"Leader already active: $leader").as(state)
                      )
                    }
    yield ()).provide(DRefContext.local, Scope.default)
```

This snippet elects a leader while ensuring every node sees the same decision.
If the leader dies, the next node calling `getAndUpdateZIO` takes over.

### 2. Cluster-wide feature flags
Propagate feature toggles instantly and atomically by listening to change
streams:

```scala
import io.github.tobia80.dref.{DRef, DRefContext, ManualId}
import zio.*

final case class Flags(betaFeature: Boolean, rollout: Int)

val program =
  (for
    flags <- DRef.make(Flags(betaFeature = false, rollout = 0))
    _     <- flags.onChange(flag => ZIO.logInfo(s"Flags changed to $flag"))
    _     <- flags.set(Flags(betaFeature = true, rollout = 5))
  yield ()).provide(DRefContext.local, Scope.default)
```

Every subscriber receives updates as soon as they happen, while reads from the
reference remain strongly consistent.

### 3. Distributed throttling with locks
Ensure that a job runs only once across the cluster using distributed locks:

```scala
import io.github.tobia80.dref.{DRef, DRefContext, ManualId}
import zio.*

object ThrottledJob extends ZIOAppDefault:
  override def run =
    DRef.lock(ManualId("daily-report")) {
      ZIO.logInfo("Generating report...") *>
        generateReport
    }.provide(DRefContext.local, Scope.default)
```

If another node attempts to run `ThrottledJob` at the same time, it will block
until the lock is released (or fail fast if you wrap it with a timeout).
`LockStolenException` is thrown if, for any reason due to backend specifics and long running locks, lock is lost.
Retry can be applied to recover from LockStolenException.

### 4. Live dashboards powered by DRef
Maintain shared counters that drive real-time dashboards:

```scala
import io.github.tobia80.dref.{DRef, DRefContext, ManualId}
import zio.*

object Metrics extends ZIOAppDefault:
  override def run =
    (for
      activeUsers <- DRef.make[Int](0, ManualId("active-users"))
      _           <- activeUsers.update(_ + 1)
      current     <- activeUsers.get
      _           <- ZIO.logInfo(s"Active users: $current")
    yield ()).repeat(Schedule.spaced(5.seconds))
      .provide(DRefContext.local, Scope.default)
```

Because updates are diffed and replicated automatically, every node can display
identical, up-to-date metrics without central bottlenecks.

## Architecture at a glance
- **Core API (`dref-core`).** Defines the distributed reference abstraction,
  codecs, locking helpers, and change streams.
- **Backends.**
  - `dref-raft`: consensus-backed storage for production clusters. Speaks the
    protobuf services in [`proto/`](proto/), which is the same wire format used
    by the Rust port — so a Scala Raft cluster can include Rust nodes (and vice
    versa) with no translation layer.
  - `dref-redis`: integrate with existing Redis deployments.
  - In-memory (`DRefContext.local`): ideal for tests or local development.
- **Rust port (`rust/`).** A Tokio-based implementation of the same API with
  `dref-core`, `dref-redis`, and `dref-raft` crates. See [`rust/README.md`](rust/README.md)
  for Rust-specific setup and examples.
- **Examples.** The `example` module contains ready-to-run demos that show how
  to wire everything together with ZIO layers. The `interop-example` module
  showcases a mixed Scala + Rust Raft cluster.

## Raft safety & durability

DRef's Raft backend persists `(currentTerm, votedFor)` to disk before
acknowledging any vote, append, or heartbeat — so a restarted node cannot vote
twice in the same term. It also snapshots the state machine to the same storage
directory and restores both the key/value contents and the last applied
consensus sequence on restart.

DRef is restart-safe when each node has durable storage. You can use either a
local directory (`storageDir`) or PostgreSQL (`postgres`).

```scala
ProtoRaftConfig(
  port = 8082,
  // Option A — local disk (typical with a per-pod PersistentVolumeClaim).
  storageDir = Some(java.nio.file.Paths.get("/var/lib/dref/node-1")),
  // Option B — shared PostgreSQL (works with ephemeral pods / Deployments).
  // postgres = Some(RaftPostgresConfig(
  //   jdbcUrl = "jdbc:postgresql://postgres:5432/dref",
  //   user = Some("dref"),
  //   password = Some("secret"),
  // )),
  // Persist a state-machine snapshot after every N applied commands.
  // Set to 0 to disable automatic snapshots.
  snapshotEvery = 1000,
  // ...
)
```

When `postgres` is set it takes precedence over `storageDir`. Each node writes
its voter state and snapshots under its stable `nodeId`, so a multi-node cluster
can run as a Kubernetes `Deployment` (no StatefulSet or per-pod volume) as long
as every replica has a distinct `nodeId` and points at the same database.

If both `storageDir` and `postgres` are unset (the default), the node keeps
voter state and snapshots in memory only. That is fine for single-process tests
but unsafe for multi-node clusters: a node that restarts without persistent
`votedFor` can grant a second vote in the same term, and a node that restarts
without a snapshot must be reseeded by the leader.

Snapshots are full state-machine images, not durable Raft log segments. The
current consensus engine uses monotonic sequence numbers and `InstallSnapshot`
catch-up instead of maintaining a persistent command log, so there is no log to
truncate. This keeps restart and lagging-follower recovery bounded in the
current simplified model, but it is not a full Raft §7 log-compaction
implementation.

The on-disk voter-state format is shared with the Rust port (magic `DRFT`,
big-endian fields, atomic rename + `fsync` on every write); see
[`compat/voter_state_vectors.json`](compat/voter_state_vectors.json). The
state-machine snapshot format is shared as well (magic `DRFS`, protobuf
`ClusterSnapshot`, atomic rename + `fsync`); see
[`compat/snapshot_vectors.json`](compat/snapshot_vectors.json).

## Cross-language compatibility

DRef is implemented in both **Scala (ZIO)** and **Rust (Tokio)**. Mixed clusters
are supported when both sides use the shared wire formats below — no translation
layer or sidecar is required.

| Layer | Shared contract | Verification |
| --- | --- | --- |
| **Core values & locks** | MsgPack codec (`rmp-serde` / `zio-schema-msg-pack`), 8-byte big-endian lock tokens | [`compat/vectors.json`](compat/vectors.json), `CrossLangCompatSpec` (Scala), `compat_tests` (Rust) |
| **Raft state commands** | [`proto/state_command.proto`](proto/state_command.proto) | [`compat/consensus_vectors.json`](compat/consensus_vectors.json), `CrossLangConsensusCompatSpec` (Scala), `consensus_compat_tests` (Rust) |
| **Raft snapshots** | `ClusterSnapshot` in [`proto/dref_consensus.proto`](proto/dref_consensus.proto), plus the shared on-disk wrapper | [`compat/snapshot_vectors.json`](compat/snapshot_vectors.json), `StateMachineSnapshotStoreSpec` (Scala), `crosslang_snapshot_tests` (Rust) |
| **Inter-node RPC** | [`proto/dref.proto`](proto/dref.proto), [`proto/dref_consensus.proto`](proto/dref_consensus.proto) | `ProtoRaftDRefSpec` (Scala), `raft_tests` (Rust) |
| **Redis backend** | Same key layout, TTL semantics, and keyspace-notification payloads | `CrossLangRedisSpec` (Scala), `crosslang_redis_tests` (Rust) |

Run the cross-language checks from the repository root:

```bash
# Core MsgPack + lock wire format
sbt "dref-core/testOnly io.github.tobia80.dref.CrossLangCompatSpec"
(cd rust && cargo test -p dref-core --test compat_tests)

# Raft protobuf golden bytes + cluster behaviour
./scripts/crosslang-raft-compat.sh

# Redis: Scala writes fixtures, Rust reads them (requires Redis on localhost:6379)
./scripts/crosslang-redis-compat.sh
```

Golden byte vectors live under [`compat/`](compat/) so either language can
regenerate or verify payloads without a running cluster.

## Testing
Run the full test suite with:

```bash
sbt test
```

> **Note:** The `dref-redis` tests require a running Redis instance
> (`localhost:6379`). Redis tests will fail with connection errors if Redis
> is not available. The `dref-raft` and `dref-core` tests run independently and
> do not require external services.

For the Rust workspace:

```bash
cd rust
cargo test -p dref-core
cargo test -p dref-raft
cargo test -p dref-redis --features test-redis
```

See [Cross-language compatibility](#cross-language-compatibility) for scripts
that verify Scala and Rust nodes agree on wire formats.

## Run the example cluster with Docker

You can experiment with the Raft backend locally by running three instances of
the chat example in Docker. The repository includes a compose file that builds a
container image for the `example` module and uses Docker DNS to make every node
discoverable through a single service name.

1. Build the image and start three replicas of the service:

   ```bash
   docker compose up --build --scale dref-example=3
   ```

   Compose provisions the `dref-example` service and scales it to three
   containers. Each replica exposes the gRPC port `8082` to the internal
   network, and the nodes discover one another through the shared `dref-example`
   hostname.

2. Attach to the containers from separate terminals to interact with the
   running example:

   ```bash
   docker compose attach dref-example-1
   docker compose attach dref-example-2
   docker compose attach dref-example-3
   ```

   Every terminal prompts for a user name and message. When one node sends a
   message it is broadcast to the other replicas through consensus replication.

3. Press <kbd>Ctrl</kbd>+<kbd>p</kbd> followed by <kbd>Ctrl</kbd>+<kbd>q</kbd> to
   detach from a container without stopping it. When you are done testing, stop
   the cluster with:

   ```bash
   docker compose down
   ```

The example honours the following environment variables, which the compose file
sets automatically:

- `DREF_NODE_SERVICES` — comma separated list of DNS names to discover other
  Raft nodes. By default every node resolves `dref-example` to the full replica
  set.
- `DREF_NODE_ADDRESSES` — optional override that accepts a comma separated list
  of IP addresses instead of DNS names.
- `DREF_PORT` — the port used by the gRPC server (defaults to `8082`).

## Run a mixed Scala + Rust cluster locally

For a hands-on demo that Scala and Rust nodes really do participate in the
same Raft cluster, use the interop example:

```bash
scripts/interop-cluster.sh up              # build images + start 2 Scala + 1 Rust nodes
scripts/interop-cluster.sh list            # show replicas (scala[1], rust[1], …)
scripts/interop-cluster.sh add scala       # add another Scala node at runtime
scripts/interop-cluster.sh remove rust 1     # remove rust replica #1
scripts/interop-cluster.sh interactive       # REPL to add/remove nodes while cluster runs
scripts/interop-cluster.sh attach scala      # attach to a Scala node (chat)
scripts/interop-cluster.sh down              # stop and clean up
```

Each container reads a display name from stdin and broadcasts chat messages
through a single `DRef` named `interop-chat-message`. Because both languages
share the protobuf schemas in [`proto/`](proto/), the MsgPack codec for the
`DRefMessage(name, message)` record, and a manual id for the key, every node
sees every message regardless of which language sent it.

The sources of the demo are:

- Scala node: [`interop-example/src/main/scala/io/tobia80/dref/InteropMain.scala`](interop-example/src/main/scala/io/tobia80/dref/InteropMain.scala)
- Rust node: [`rust/interop-node/src/main.rs`](rust/interop-node/src/main.rs)
- Compose file: [`docker-compose.interop.yml`](docker-compose.interop.yml)
- Driver script: [`scripts/interop-cluster.sh`](scripts/interop-cluster.sh)

Override the initial replica counts with `SCALA_REPLICAS=3 RUST_REPLICAS=2 scripts/interop-cluster.sh up`.
While the cluster is running, add or remove nodes with `add` / `remove` (or `interactive`);
existing nodes refresh peer lists from the shared `dref-interop` DNS alias every few seconds.

## License

This project is licensed under the terms of the LICENSE file in this repository.
