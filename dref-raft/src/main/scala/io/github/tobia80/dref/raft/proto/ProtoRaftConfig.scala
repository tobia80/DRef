package io.github.tobia80.dref.raft.proto

import zio.Duration

import java.nio.file.Path

case class ProtoRaftConfig(
  port: Int,
  bindAddress: Option[String] = None,
  nodeId: Option[String] = None,
  ttl: Option[Duration] = Some(zio.durationInt(10).seconds),
  connectionTimeout: Duration = zio.durationInt(500).millis,
  electionTimeout: Duration = zio.durationInt(400).millis,
  heartbeatInterval: Duration = zio.durationInt(80).millis,
  /** How often to refresh peer lists from an [[io.github.tobia80.dref.raft.IpProvider]]. */
  addressPollInterval: Duration = zio.durationInt(3).seconds,
  initialEndpoints: List[NodeEndpoint] = Nil,
  // Optional on-disk directory for Raft voter state (currentTerm + votedFor)
  // AND state-machine snapshots. Without it the node stays in-memory and
  // forgets its vote across restarts, which can violate Raft safety in
  // multi-node setups; set this for any cluster that needs to survive restarts.
  storageDir: Option[Path] = None,
  // Optional PostgreSQL persistence (one row per nodeId). Prefer this over
  // `storageDir` when running on ephemeral pods (e.g. a K8s Deployment) so
  // each replica can restart without a per-pod PersistentVolumeClaim.
  postgres: Option[RaftPostgresConfig] = None,
  // How many applied commands between automatic state-machine snapshot writes.
  // Only takes effect when `storageDir` or `postgres` is set. Lower = less restart catch-up
  // traffic at the cost of more disk I/O. The default is conservative; bump it
  // for write-heavy clusters where each snapshot would still be cheap to
  // rebuild from the leader.
  snapshotEvery: Int = 1000
)
