package io.github.tobia80.dref.raft

import io.github.tobia80.dref.raft.proto.{RaftPostgresConfig, RaftPostgresStorage, VoterState}
import zio.*
import zio.test.*

object RaftPostgresStorageSpec extends ZIOSpecDefault {

  private val postgresConfig: RaftPostgresConfig = RaftPostgresConfig(
    jdbcUrl = sys.env.getOrElse("DREF_TEST_POSTGRES_JDBC_URL", "jdbc:postgresql://localhost:5432/dref_test"),
    user = Some(sys.env.getOrElse("DREF_TEST_POSTGRES_USER", "postgres")),
    password = Some(sys.env.getOrElse("DREF_TEST_POSTGRES_PASSWORD", "test"))
  )

  private val postgresAvailable: UIO[Boolean] =
    RaftPostgresStorage
      .voterStateStore(postgresConfig, "postgres-probe")
      .fold(_ => false, _ => true)

  private def uniqueNodeId(prefix: String): UIO[String] =
    Random.nextUUID.map(id => s"$prefix-$id")

  private val postgresTests = suite("RaftPostgresStorage")(
    test("voter state save then load round-trips") {
      for {
        nodeId <- uniqueNodeId("voter")
        store  <- RaftPostgresStorage.voterStateStore(postgresConfig, nodeId)
        _      <- store.save(VoterState(42L, Some("node-7")))
        loaded <- store.load
      } yield assertTrue(loaded == VoterState(42L, Some("node-7")))
    },
    test("two node ids are isolated") {
      for {
        nodeA  <- uniqueNodeId("a")
        nodeB  <- uniqueNodeId("b")
        storeA <- RaftPostgresStorage.voterStateStore(postgresConfig, nodeA)
        storeB <- RaftPostgresStorage.voterStateStore(postgresConfig, nodeB)
        _      <- storeA.save(VoterState(10L, Some("alpha")))
        _      <- storeB.save(VoterState(20L, Some("beta")))
        readA  <- storeA.load
        readB  <- storeB.load
      } yield assertTrue(
        readA == VoterState(10L, Some("alpha")),
        readB == VoterState(20L, Some("beta"))
      )
    },
    test("snapshot save then load round-trips") {
      import io.github.tobia80.dref_consensus.ClusterSnapshot
      for {
        nodeId <- uniqueNodeId("snapshot")
        store  <- RaftPostgresStorage.snapshotStore(postgresConfig, nodeId)
        snap    = ClusterSnapshot(lastSeq = 99L)
        _      <- store.save(snap)
        loaded <- store.load
      } yield assertTrue(loaded.contains(snap))
    }
  ) @@ TestAspect.sequential

  override def spec: Spec[TestEnvironment, Any] = postgresTests.whenZIO(postgresAvailable)
}
