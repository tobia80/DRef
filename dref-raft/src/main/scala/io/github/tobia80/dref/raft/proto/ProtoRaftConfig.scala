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
  // Optional on-disk directory for Raft voter state (currentTerm + votedFor).
  // Without it the node stays in-memory and forgets its vote across restarts,
  // which can violate Raft safety in multi-node setups; set this for any
  // cluster that needs to survive restarts.
  storageDir: Option[Path] = None
)
