# CRef

Cluster Ref (CRef) is a distributed variable implementation designed to synchronize state across multiple nodes in a cluster. It provides a simple and robust abstraction for managing shared, mutable state in distributed systems. In addition to distributed references, CRef also implements distributed locks and pub/sub patterns for advanced coordination and messaging.

## Use cases
It can be used to implement simple leader election, shard management, locks and everything that needs coordination across nodes.

## Features
- Distributed, strongly-consistent reference (Ref) abstraction
- Distributed locks for mutual exclusion across nodes
- Pub/Sub pattern for event-driven communication
- Synchronizes state across cluster nodes
- Fault-tolerant and resilient to node failures
- Simple API for updating and reading shared state
- Pluggable backend (e.g., Redis, in-memory, Raft (soon), etc.)
- Integration with modern Scala concurrency libraries (e.g., ZIO)

## Getting Started

Add CRef to your project dependencies

## Usage Example

Below is a typical usage pattern for CRef. Replace with actual code from your main or test files if available.

```scala
import cref.CRef
import zio._

object Example extends ZIOAppDefault {
  override def run = for {
    cref <- CRef.make[Int](0) // Create a distributed ref with initial value 0
    _    <- cref.update(_ + 1) // Atomically increment the value
    v    <- cref.get           // Read the current value
    _    <- Console.printLine(s"Current value: $v")
  } yield ()
}
```
CRef takes an id to uniquely identify the reference across the cluster. The default provider is Auto that generates automatically an id based on the source code location. If you see weird behaviours, it could be safer to use ManualId setting a unique reference across nodes.

## Distributed Locks Example

CRef provides distributed locks to ensure mutual exclusion across fibers or nodes. Here is an example inspired by the test suite:

```scala
import cref.CRef
import zio._

object LockExample extends ZIOAppDefault {
  override def run = for {
    list  <- Ref.make(List.empty[Int])
    fiber <- ZIO.foreachParDiscard(List(100, 200)) { id =>
      (Console.printLine(s"Starting $id") *>
        CRef.lock() {
          Console.printLine(s"Executing $id") *>
          list.update(_ :+ id) *>
          ZIO.sleep(1.second)
        }).delay(id.millis)
    }.fork
    _     <- ZIO.sleep(500.millis)
    valueWithOneLock <- list.get
    _     <- fiber.join
    valueWithTwoLocks <- list.get
    _     <- Console.printLine(s"After 500ms: $valueWithOneLock, after all: $valueWithTwoLocks")
  } yield ()
}
```

This example shows two concurrent tasks attempting to acquire the same lock. The lock ensures that only one task executes its critical section at a time, even in a distributed environment.

## Testing

CRef includes a comprehensive test suite. To run the tests:
```sh
sbt test
```

## License

This project is licensed under the terms of the LICENSE file in this repository.
