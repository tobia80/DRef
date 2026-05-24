package io.github.tobia80.dref.raft.proto

import zio.Duration

case class ProtoRaftConfig(
  port: Int,
  bindAddress: Option[String] = None,
  nodeId: Option[String] = None,
  ttl: Option[Duration] = Some(zio.durationInt(10).seconds),
  connectionTimeout: Duration = zio.durationInt(500).millis,
  electionTimeout: Duration = zio.durationInt(400).millis,
  heartbeatInterval: Duration = zio.durationInt(80).millis,
  initialEndpoints: List[NodeEndpoint] = Nil
)
