package io.github.tobia80.dref.raft.proto

import io.github.tobia80.dref_consensus.ClusterSnapshot
import zio.*

import java.sql.{Connection, DriverManager}
import java.util.Properties

object RaftPostgresStorage {

  private val VoterTable = "dref_raft_voter_state"
  private val SnapshotTable = "dref_raft_state_snapshot"

  private val SchemaSql =
    s"""CREATE TABLE IF NOT EXISTS $VoterTable (
       |  node_id TEXT PRIMARY KEY,
       |  payload BYTEA NOT NULL
       |);
       |CREATE TABLE IF NOT EXISTS $SnapshotTable (
       |  node_id TEXT PRIMARY KEY,
       |  payload BYTEA NOT NULL
       |);""".stripMargin

  def voterStateStore(config: RaftPostgresConfig, nodeId: String): Task[VoterStateStore] =
    open(config).map(new PostgresVoterStateStore(_, nodeId))

  def snapshotStore(config: RaftPostgresConfig, nodeId: String): Task[StateMachineSnapshotStore] =
    open(config).map(new PostgresSnapshotStore(_, nodeId))

  private def open(config: RaftPostgresConfig): Task[SimpleDataSource] =
    ZIO.attemptBlocking {
      Class.forName("org.postgresql.Driver")
      val ds = new SimpleDataSource(config)
      ds.ensureSchema()
      ds
    }

  private def connectionProperties(config: RaftPostgresConfig): Properties = {
    val props = new Properties()
    config.user.foreach(props.setProperty("user", _))
    config.password.foreach(props.setProperty("password", _))
    props
  }

  final private class SimpleDataSource(config: RaftPostgresConfig) {
    private val props = connectionProperties(config)

    def withConnection[A](f: Connection => A): A = {
      val conn = DriverManager.getConnection(config.jdbcUrl, props)
      try f(conn)
      finally conn.close()
    }

    def ensureSchema(): Unit =
      withConnection { conn =>
        val stmt = conn.createStatement()
        try stmt.execute(SchemaSql)
        finally stmt.close()
      }
  }

  final private class PostgresVoterStateStore(ds: SimpleDataSource, nodeId: String) extends VoterStateStore {

    private val upsertSql =
      s"INSERT INTO $VoterTable (node_id, payload) VALUES (?, ?) " +
        s"ON CONFLICT (node_id) DO UPDATE SET payload = EXCLUDED.payload"

    def load: Task[VoterState] =
      ZIO.attemptBlocking {
        ds.withConnection { conn =>
          val stmt = conn.prepareStatement(s"SELECT payload FROM $VoterTable WHERE node_id = ?")
          try {
            stmt.setString(1, nodeId)
            val rs = stmt.executeQuery()
            try
              if rs.next() then RaftStorageCodec.decodeVoterState(rs.getBytes(1))
              else VoterState.empty
            finally rs.close()
          } finally stmt.close()
        }
      }

    def save(state: VoterState): Task[Unit] =
      ZIO.attemptBlocking {
        val payload = RaftStorageCodec.encodeVoterState(state)
        ds.withConnection { conn =>
          conn.setAutoCommit(false)
          try {
            val stmt = conn.prepareStatement(upsertSql)
            try {
              stmt.setString(1, nodeId)
              stmt.setBytes(2, payload)
              stmt.executeUpdate()
            } finally stmt.close()
            conn.commit()
          } catch {
            case t: Throwable =>
              conn.rollback()
              throw t
          } finally conn.setAutoCommit(true)
        }
      }
  }

  final private class PostgresSnapshotStore(ds: SimpleDataSource, nodeId: String) extends StateMachineSnapshotStore {

    private val upsertSql =
      s"INSERT INTO $SnapshotTable (node_id, payload) VALUES (?, ?) " +
        s"ON CONFLICT (node_id) DO UPDATE SET payload = EXCLUDED.payload"

    def load: Task[Option[ClusterSnapshot]] =
      ZIO.attemptBlocking {
        ds.withConnection { conn =>
          val stmt = conn.prepareStatement(s"SELECT payload FROM $SnapshotTable WHERE node_id = ?")
          try {
            stmt.setString(1, nodeId)
            val rs = stmt.executeQuery()
            try
              if rs.next() then Some(RaftStorageCodec.decodeSnapshot(rs.getBytes(1)))
              else None
            finally rs.close()
          } finally stmt.close()
        }
      }

    def save(snapshot: ClusterSnapshot): Task[Unit] =
      ZIO.attemptBlocking {
        val payload = RaftStorageCodec.encodeSnapshot(snapshot)
        ds.withConnection { conn =>
          conn.setAutoCommit(false)
          try {
            val stmt = conn.prepareStatement(upsertSql)
            try {
              stmt.setString(1, nodeId)
              stmt.setBytes(2, payload)
              stmt.executeUpdate()
            } finally stmt.close()
            conn.commit()
          } catch {
            case t: Throwable =>
              conn.rollback()
              throw t
          } finally conn.setAutoCommit(true)
        }
      }
  }
}
