package io.github.tobia80.cref.raft

import io.microraft.RaftEndpoint

case class Endpoint(id: String, ip: String) extends RaftEndpoint {

  override def getId: AnyRef = id
}
