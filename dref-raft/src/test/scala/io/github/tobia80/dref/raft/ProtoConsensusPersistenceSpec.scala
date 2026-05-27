package io.github.tobia80.dref.raft

import com.google.protobuf.ByteString
import io.github.tobia80.dref.raft.proto.*
import io.github.tobia80.state_command.{SetElementCommand, StateCommand}
import zio.*
import zio.test.*

import java.nio.file.{Files, Path}

object ProtoConsensusPersistenceSpec extends ZIOSpecDefault {

  private val tempDir: ZIO[Scope, Throwable, Path] =
    ZIO.acquireRelease(
      ZIO.attemptBlocking(Files.createTempDirectory("proto-consensus-persist"))
    )(dir => ZIO.attemptBlocking(deleteRecursive(dir)).orDie)

  private def deleteRecursive(path: Path): Unit =
    if Files.exists(path) then {
      if Files.isDirectory(path) then {
        val it = Files.newDirectoryStream(path)
        try it.forEach(deleteRecursive)
        finally it.close()
      }
      Files.deleteIfExists(path)
      ()
    }

  private def singleNodeConfig(
    storage: Option[Path],
    snapshotEvery: Int = 1000
  ): ProtoRaftConfig =
    ProtoRaftConfig(
      port = 0,
      bindAddress = Some("127.0.0.1:0"),
      nodeId = Some("node-under-test"),
      ttl = Some(5.seconds),
      connectionTimeout = 500.millis,
      electionTimeout = 5.seconds,   // long enough that no election fires during a test step
      heartbeatInterval = 1.second,
      initialEndpoints = List(NodeEndpoint("node-under-test", "127.0.0.1:0")),
      storageDir = storage,
      snapshotEvery = snapshotEvery
    )

  private def setElement(name: String, value: Array[Byte]): StateCommand =
    StateCommand(
      StateCommand.Op.SetElement(
        SetElementCommand(name = name, value = ByteString.copyFrom(value), expireAt = None)
      )
    )

  /** Build a consensus engine without spinning up its gRPC server. With a
    * single self-endpoint the peers map is empty, so no gRPC channels are
    * needed — we get to exercise the state machine logic directly.
    */
  private def makeEngine(storage: Option[Path]): ZIO[Scope, Throwable, ProtoConsensusEngine] =
    makeEngineWithMachine(storage).map(_._2)

  private def makeEngineWithMachine(
    storage: Option[Path],
    snapshotEvery: Int = 1000
  ): ZIO[Scope, Throwable, (ProtoStateMachine, ProtoConsensusEngine)] =
    for {
      stateMachine <- ProtoStateMachine.make
      engine       <- ProtoConsensusEngine.make(
                        "node-under-test",
                        stateMachine,
                        singleNodeConfig(storage, snapshotEvery)
                      )
    } yield (stateMachine, engine)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("ProtoConsensusEngine persistence")(
    test("with no storageDir, granting a vote does not touch disk and behaves as before") {
      for {
        engine <- makeEngine(None)
        // single-node engine starts as Leader at term 1 via bootstrap shortcut
        startRole <- engine.role
        granted   <- engine.handleVote("other-node", term = 5L, lastSeq = 0L)
        roleAfter <- engine.role
      } yield assertTrue(
        startRole == Role.Leader,
        granted._1,            // vote granted at higher term
        granted._2 == 5L,
        roleAfter == Role.Follower
      )
    },
    test("with storageDir, granting a vote persists (term, votedFor) before returning") {
      for {
        dir       <- tempDir
        engine    <- makeEngine(Some(dir))
        _         <- engine.handleVote("candidate-x", term = 9L, lastSeq = 0L)
        // Read the file with a fresh store instance to prove durability —
        // the engine's own in-memory state is irrelevant here.
        verifier  <- VoterStateStore.file(dir)
        persisted <- verifier.load
      } yield assertTrue(persisted == VoterState(9L, Some("candidate-x")))
    },
    test("with storageDir, observing a higher term via AppendEntries persists the new term") {
      for {
        dir       <- tempDir
        engine    <- makeEngine(Some(dir))
        _         <- engine.handleAppendEntries(leaderId = "leader-x", term = 12L, seq = 0L, command = Array.emptyByteArray)
        verifier  <- VoterStateStore.file(dir)
        persisted <- verifier.load
      } yield assertTrue(
        persisted.term == 12L,
        persisted.votedFor.isEmpty
      )
    },
    test("a second engine over the same storageDir loads the persisted state") {
      for {
        dir       <- tempDir
        // first engine grants a vote, then we throw it away
        _         <- ZIO.scoped {
                       makeEngine(Some(dir)).flatMap { engine =>
                         engine.handleVote("candidate-y", term = 21L, lastSeq = 0L).unit
                       }
                     }
        // second engine should pick up the persisted (21, Some("candidate-y"))
        // and refuse to short-circuit to leader even though peers is empty
        engine2     <- makeEngine(Some(dir))
        role        <- engine2.role
        leader      <- engine2.leaderId
        // No public reader for term, but a stale vote attempt at the same
        // term from a *different* candidate should be denied — that proves
        // term=21 was loaded *and* votedFor=Some("candidate-y") was loaded.
        sameTerm    <- engine2.handleVote("other-candidate", term = 21L, lastSeq = 0L)
      } yield assertTrue(
        role == Role.Follower,
        leader.isEmpty,
        !sameTerm._1,          // denied — already voted for candidate-y in term 21
        sameTerm._2 == 21L
      )
    },
    test("re-voting for the same candidate in the same term is idempotent and stays granted") {
      for {
        dir       <- tempDir
        engine    <- makeEngine(Some(dir))
        first     <- engine.handleVote("candidate-z", term = 3L, lastSeq = 0L)
        second    <- engine.handleVote("candidate-z", term = 3L, lastSeq = 0L)
        verifier  <- VoterStateStore.file(dir)
        persisted <- verifier.load
      } yield assertTrue(
        first == (true, 3L),
        second == (true, 3L),
        persisted == VoterState(3L, Some("candidate-z"))
      )
    },
    test("with storageDir, denies a second vote to a different candidate in the same term") {
      for {
        dir     <- tempDir
        engine  <- makeEngine(Some(dir))
        _       <- engine.handleVote("candidate-y", term = 21L, lastSeq = 0L)
        denied  <- engine.handleVote("other-candidate", term = 21L, lastSeq = 0L)
        verifier <- VoterStateStore.file(dir)
        persisted <- verifier.load
      } yield assertTrue(
        !denied._1,
        denied._2 == 21L,
        persisted == VoterState(21L, Some("candidate-y"))
      )
    },
    test("single-node bootstrap with storageDir persists term=1 immediately") {
      for {
        dir       <- tempDir
        _         <- makeEngine(Some(dir))
        verifier  <- VoterStateStore.file(dir)
        persisted <- verifier.load
      } yield assertTrue(
        persisted.term == 1L,
        persisted.votedFor.isEmpty
      )
    },
    test("handlePreVote does not persist or mutate (term, votedFor)") {
      // PreVote is a hypothetical probe. Whether granted or refused, it
      // must never bump term, set votedFor, or touch the durable store.
      for {
        dir       <- tempDir
        engine    <- makeEngine(Some(dir))
        // Move into Follower at term=2 via AppendEntries.
        _         <- engine.handleAppendEntries(leaderId = "x", term = 2L, seq = 0L, command = Array.emptyByteArray)
        result    <- engine.handlePreVote(candidateId = "candidate-x", term = 99L, lastSeq = 0L)
        verifier  <- VoterStateStore.file(dir)
        persisted <- verifier.load
      } yield assertTrue(
        result._2 == 2L,       // current term unchanged (returns CURRENT, never the candidate's hypothetical)
        persisted.term == 2L,  // disk shows the term from AppendEntries, NOT 99
        persisted.votedFor.isEmpty
      )
    },
    test("handlePreVote refuses when a leader heartbeat is recent") {
      for {
        engine <- makeEngine(None)
        _      <- engine.handleHeartbeat("leader-x", term = 1L)
        result <- engine.handlePreVote(candidateId = "disruptor", term = 50L, lastSeq = 0L)
      } yield assertTrue(
        !result._1,             // refused: leader was fresh
        result._2 == 1L         // current term unchanged
      )
    },
    test("handlePreVote refuses when proposed term is not strictly greater") {
      for {
        engine <- makeEngine(None)
        _      <- engine.handleAppendEntries(leaderId = "x", term = 5L, seq = 0L, command = Array.emptyByteArray)
        equal  <- engine.handlePreVote(candidateId = "c", term = 5L, lastSeq = 0L)
        lower  <- engine.handlePreVote(candidateId = "c", term = 4L, lastSeq = 0L)
      } yield assertTrue(!equal._1, !lower._1)
    },
    test("handlePreVote refuses when this node is still the leader") {
      // Single-node bootstrap leaves us as Leader at term=1. An active
      // leader must refuse pre-votes — granting one would amount to
      // volunteering its own demotion before any real signal told it to
      // step down.
      for {
        engine <- makeEngine(None)
        role0  <- engine.role
        result <- engine.handlePreVote(candidateId = "challenger", term = 99L, lastSeq = 0L)
      } yield assertTrue(
        role0 == Role.Leader,
        !result._1,
        result._2 == 1L         // leader still believes it is term=1
      )
    },
    test("explicit snapshot persists state machine contents to disk") {
      for {
        dir              <- tempDir
        first            <- makeEngineWithMachine(Some(dir))
        (sm1, engine)     = first
        _                <- engine.submit(setElement("alpha", Array[Byte](1, 2, 3))).either
        _                <- engine.submit(setElement("beta", Array[Byte](42))).either
        _                <- engine.takeAndPersistSnapshot
        // verify the bytes really hit disk by re-opening with a fresh store
        verifier         <- StateMachineSnapshotStore.file(dir)
        loaded           <- verifier.load
      } yield assertTrue(
        loaded.exists(_.lastSeq == 2L),
        loaded.exists(_.entries.size == 2),
        loaded.exists(_.entries.exists(e => e.key == "alpha" && e.value.toByteArray.toSeq == Seq[Byte](1, 2, 3))),
        loaded.exists(_.entries.exists(e => e.key == "beta"  && e.value.toByteArray.toSeq == Seq[Byte](42)))
      )
    },
    test("a restarted engine hydrates the state machine from the on-disk snapshot") {
      for {
        dir   <- tempDir
        // First engine writes some state and explicitly snapshots, then exits.
        _     <- ZIO.scoped {
                   makeEngineWithMachine(Some(dir)).flatMap { case (_, engine) =>
                     engine.submit(setElement("a", Array[Byte](1))).either *>
                       engine.submit(setElement("b", Array[Byte](2))).either *>
                       engine.takeAndPersistSnapshot
                   }
                 }
        // Second engine should pick up the snapshot during make, BEFORE it
        // serves any reads.
        pair  <- makeEngineWithMachine(Some(dir))
        (sm2, _) = pair
        a     <- sm2.get("a")
        b     <- sm2.get("b")
      } yield assertTrue(
        a.exists(_.toSeq == Seq[Byte](1)),
        b.exists(_.toSeq == Seq[Byte](2))
      )
    },
    test("a restarted engine restores snapshot lastSeq for vote freshness checks") {
      for {
        dir   <- tempDir
        _     <- ZIO.scoped {
                   makeEngineWithMachine(Some(dir)).flatMap { case (_, engine) =>
                     engine.submit(setElement("a", Array[Byte](1))).either *>
                       engine.submit(setElement("b", Array[Byte](2))).either *>
                       engine.takeAndPersistSnapshot
                   }
                 }
        pair  <- makeEngineWithMachine(Some(dir))
        (_, engine2) = pair
        vote  <- engine2.handleVote("behind-candidate", term = 2L, lastSeq = 1L)
      } yield assertTrue(
        !vote._1,
        vote._2 == 2L
      )
    },
    test("snapshotEvery=1 triggers an automatic snapshot after one apply") {
      for {
        dir          <- tempDir
        pair         <- makeEngineWithMachine(Some(dir), snapshotEvery = 1)
        (_, engine)   = pair
        _            <- engine.submit(setElement("auto", Array[Byte](7))).either
        // The snapshot save is forked, so give the background fiber a moment.
        verifier     <- StateMachineSnapshotStore.file(dir)
        loaded       <- verifier.load.repeatUntil(_.exists(_.entries.nonEmpty)).timeout(2.seconds)
      } yield assertTrue(
        loaded.flatten.exists(_.entries.exists(_.key == "auto"))
      )
    },
    test("snapshotEvery=0 disables automatic snapshots") {
      for {
        dir          <- tempDir
        pair         <- makeEngineWithMachine(Some(dir), snapshotEvery = 0)
        (_, engine)   = pair
        _            <- ZIO.foreachDiscard(1 to 50)(i => engine.submit(setElement(s"k$i", Array[Byte](i.toByte))).either)
        _            <- ZIO.sleep(100.millis)
        verifier     <- StateMachineSnapshotStore.file(dir)
        loaded       <- verifier.load
      } yield assertTrue(loaded.isEmpty)
    }
  ) @@ TestAspect.withLiveClock @@ TestAspect.sequential
}
