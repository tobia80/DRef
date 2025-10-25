# DRef

[![Scala CI](https://github.com/tobia80/DRef/actions/workflows/scala.yml/badge.svg)](https://github.com/tobia80/DRef/actions/workflows/scala.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.tobia80/dref-core_3.svg?logo=sonatype)](https://central.sonatype.com/artifact/io.github.tobia80/dref-core_3)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Scala Version](https://img.shields.io/badge/Scala-3.7.3-DC322F?logo=scala)](https://www.scala-lang.org/)

Distributed Ref (DRef) is a toolkit for synchronising state and coordination
primitives across clusters of ZIO applications. It lets you treat distributed
state the same way you would work with an ordinary `Ref`, while also giving you
cluster-wide locks and change streams when you need stronger coordination.

DRef makes it easy to build services that stay in sync without wiring together
custom replication logic, retry loops, or bespoke consensus code.

## Highlights
- **Drop-in `Ref` semantics.** Work with a distributed value using familiar
  transactional combinators such as `get`, `set`, `update`, and `modifyZIO`.
- **Strong consistency where it matters.** Raft-backed storage keeps state in
  sync across nodes while minimising bandwidth by shipping deltas.
- **Locking & notifications built-in.** Use distributed locks for mutual
  exclusion or subscribe to change streams to broadcast domain events.
- **Backend flexibility.** Switch between Raft, Redis, or in-memory
  implementations to match the deployment environment.
- **Codec agnostic.** Bring your own codecs (e.g. Desert or MsgPack) to talk to
  services written in other languages.

## When to use DRef
DRef shines whenever you need low-latency coordination across JVM services. A
few high-impact scenarios include:

- **Leader election & failover.** Run one active worker per shard, fail over in
  seconds, and log who took over.
- **Dynamic configuration flags.** Push feature toggles or rollout percentages
  to every node without redeploying.
- **Cross-service task scheduling.** Coordinate which node processes a batch or
  ensure only one node runs a cron job at a time.
- **Collaborative counters and stats.** Maintain cluster-wide metrics (e.g.
  connected users, processed jobs) that update atomically from any node.
- **Event notifications.** Fan-out domain events through change streams while
  sharing a strongly consistent reference to the latest state.

## Quick start
Add DRef to your `build.sbt`:

```scala
libraryDependencies += "io.github.tobia80" %% "dref" % "<latest-version>"
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
    yield ()).provideLayer(DRefContext.local)
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
      leadership <- DRef.make(WorkerState(None), id = ManualId("worker-leadership"))
      _          <- leadership.getAndUpdateZIO { state =>
                      state.leader.fold(
                        ZIO.succeed(state.copy(leader = Some(nodeId))) <*
                          ZIO.logInfo("Claimed leadership")
                      )(leader =>
                        ZIO.logInfo(s"Leader already active: $leader").as(state)
                      )
                    }
    yield ()).provideLayer(DRefContext.local)
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
    flags <- DRef.make(Flags(betaFeature = false, rollout = 0), ManualId("feature-flags"))
    _     <- flags.onChange(flag => ZIO.logInfo(s"Flags changed to $flag"))
    _     <- flags.set(Flags(betaFeature = true, rollout = 5))
  yield ()).provideLayer(DRefContext.local)
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
    }.provideLayer(DRefContext.local)
```

If another node attempts to run `ThrottledJob` at the same time, it will block
until the lock is released (or fail fast if you wrap it with a timeout).

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
      .provideLayer(DRefContext.local)
```

Because updates are diffed and replicated automatically, every node can display
identical, up-to-date metrics without central bottlenecks.

## Architecture at a glance
- **Core API (`dref-core`).** Defines the distributed reference abstraction,
  codecs, locking helpers, and change streams.
- **Backends.**
  - `dref-raft`: consensus-backed storage for production clusters.
  - `dref-redis`: integrate with existing Redis deployments.
  - `dref-inmemory`: ideal for tests or local development.
- **Examples.** The `example` module contains ready-to-run demos that show how
  to wire everything together with ZIO layers.

## Testing
Run the full test suite with:

```bash
sbt test
```

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
   message it is broadcast to the other replicas through the Raft log.

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

## License

This project is licensed under the terms of the LICENSE file in this repository.
