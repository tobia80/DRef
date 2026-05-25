package io.github.tobia80.dref.raft

import io.github.tobia80.dref.raft.proto.*
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

  private def singleNodeConfig(storage: Option[Path]): ProtoRaftConfig =
    ProtoRaftConfig(
      port = 0,
      bindAddress = Some("127.0.0.1:0"),
      nodeId = Some("node-under-test"),
      ttl = Some(5.seconds),
      connectionTimeout = 500.millis,
      electionTimeout = 5.seconds,   // long enough that no election fires during a test step
      heartbeatInterval = 1.second,
      initialEndpoints = List(NodeEndpoint("node-under-test", "127.0.0.1:0")),
      storageDir = storage
    )

  /** Build a consensus engine without spinning up its gRPC server. With a
    * single self-endpoint the peers map is empty, so no gRPC channels are
    * needed — we get to exercise the state machine logic directly.
    */
  private def makeEngine(storage: Option[Path]): ZIO[Scope, Throwable, ProtoConsensusEngine] =
    for {
      stateMachine <- ProtoStateMachine.make
      engine       <- ProtoConsensusEngine.make("node-under-test", stateMachine, singleNodeConfig(storage))
    } yield engine

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
    }
  ) @@ TestAspect.withLiveClock @@ TestAspect.sequential
}
