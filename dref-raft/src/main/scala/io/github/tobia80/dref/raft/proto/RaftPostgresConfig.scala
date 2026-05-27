package io.github.tobia80.dref.raft.proto

/** PostgreSQL-backed Raft persistence for multi-node clusters on ephemeral pods.
  *
  * Each node stores its voter state and state-machine snapshot under its stable `nodeId`, so a plain Kubernetes
  * `Deployment` (no per-pod volume) is safe as long as every replica uses a distinct `nodeId` and connects to the same
  * DB.
  */
final case class RaftPostgresConfig(
  jdbcUrl: String,
  user: Option[String] = None,
  password: Option[String] = None
)
