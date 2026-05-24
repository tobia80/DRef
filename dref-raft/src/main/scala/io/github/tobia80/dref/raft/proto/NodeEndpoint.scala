package io.github.tobia80.dref.raft.proto

case class NodeEndpoint(id: String, address: String)

object NodeEndpoint {
  def apply(id: String, host: String, port: Int): NodeEndpoint =
    NodeEndpoint(id, s"$host:$port")
}
