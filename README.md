# DRef

Distributed Ref (DRef) is a distributed variable implementation designed to synchronize state across multiple nodes in one or more distributed systems. It provides a simple and robust abstraction for managing shared, mutable state in distributed systems. In addition to distributed references, DRef also implements distributed locks and pub/sub patterns for advanced coordination and messaging.
Memory implementation is backed by Ref and it is useful for testing or single-node applications.

## Use cases
It can be used to implement leader election, shard management, change notifications, locks and everything that needs coordination across nodes.

## Features
- Built on top of ZIO for asynchronous and concurrent programming
- Distributed, strongly-consistent reference (Ref) abstraction
- Distributed locks for mutual exclusion across nodes
- Pub/Sub pattern for event-driven communication
- Transparent serialization and deserialization of data
- Automatic optimisation of data (only diff are sent across the wire)
- Highly configurable and extensible
- Incredibly simple API similar to Ref
- Synchronizes backend across cluster nodes
- Raft, Redis and In Memory implementations
- Pluggable backends
- Testable in ms

## Getting Started

Add DRef to your project dependencies

## Usage Example

Below is a typical usage pattern for DRef. Replace with actual code from your main or test files if available.

```scala
import dref.DRef
import zio._

object Example extends ZIOAppDefault {
  override def run = for {
    dref <- DRef.make[Int](0) // Create a distributed ref with initial value 0
    _    <- dref.update(_ + 1) // Atomically increment the value
    v    <- dref.get           // Read the current value
    _    <- ZIO.logInfo(s"Current value: $v")
  } yield ()
}
```
DRef takes an id to uniquely identify the reference across the nodes. The default provider is Auto that generates automatically an id based on the source code location. If you see weird behaviours, it could be safer to use ManualId setting a unique reference across nodes.

## Leader election example
DRef can be used to implement leader election in a distributed system. Here is an example:

```scala

import dref.DRef
import dref.DRef.*
import zio._

object Example extends ZIOAppDefault {
  override def run = for {
    leadershipInfo <- DRef.make[LeadershipInfo](LeadershipInfo(info = None))
    _    <- leadershipInfo.getAndUpdateZIO{ info =>
      if (!info.leaderElected) setAsLeader(info) <* ZIO.logInfo("I am the leader")
      else ZIO.logInfo(s"Leader already elected: ${info.info}") *> ZIO.succeed(info)
    }
  } yield ()
}
```
## Distributed Locks Example

DRef provides distributed locks to ensure mutual exclusion across fibers or nodes. Here is an example inspired by the test suite:

```scala
import dref.DRef
import zio._

object LockExample extends ZIOAppDefault {
  override def run = for {
    list  <- Ref.make(List.empty[Int])
    fiber <- ZIO.foreachParDiscard(List(100, 200)) { id =>
      (ZIO.logInfo(s"Starting $id") *>
        DRef.lock() {
          ZIO.logInfo(s"Executing $id") *>
          list.update(_ :+ id) *>
          ZIO.sleep(1.second)
        }).delay(id.millis)
    }.fork
    _     <- ZIO.sleep(500.millis)
    valueWithOneLock <- list.get
    _     <- fiber.join
    valueWithTwoLocks <- list.get
    _     <- ZIO.logInfo(s"After 500ms: $valueWithOneLock, after all: $valueWithTwoLocks")
  } yield ()
}
```

This example shows two concurrent tasks attempting to acquire the same lock. The lock ensures that only one task executes its critical section at a time, even in a distributed environment.

## Testing

DRef includes a comprehensive test suite. To run the tests:
```sh
sbt test
```

## License

This project is licensed under the terms of the LICENSE file in this repository.
