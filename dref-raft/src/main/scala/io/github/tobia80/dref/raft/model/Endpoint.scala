package io.github.tobia80.dref.raft.model

import io.microraft.RaftEndpoint

case class Endpoint(id: String, ip: String) extends RaftEndpoint {

  override def getId: AnyRef = id
}
