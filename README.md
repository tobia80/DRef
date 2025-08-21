# CRef

Cluster Ref (CRef) is a distributed variable implementation designed to synchronize state across multiple nodes in a cluster. It provides a simple and robust abstraction for managing shared, mutable state in distributed systems.

## Features
- Distributed, strongly-consistent reference (Ref) abstraction
- Synchronizes state across cluster nodes
- Fault-tolerant and resilient to node failures
- Simple API for updating and reading shared state
- Pluggable backend (e.g., Redis, in-memory, Raft (soon) etc.)
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

## Testing

CRef includes a comprehensive test suite. To run the tests:
```sh
sbt test
```

## License

This project is licensed under the terms of the LICENSE file in this repository.
