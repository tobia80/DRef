package io.github.tobia80.dref.raft

import com.google.protobuf.ByteString
import io.github.tobia80.dref.SendCommandRequest
import io.github.tobia80.dref.ZioDref.DRefRaftClient
import io.microraft.RaftEndpoint
import io.microraft.model.message.RaftMessage
import io.microraft.transport.Transport
import org.apache.commons.lang3.SerializationUtils
import zio.{Runtime, Unsafe}

class GrpcTransport(source: RaftEndpoint, runtime: Runtime[Any]) extends Transport {

  private var endpointsClient: Map[DRefRaftClient, List[RaftEndpoint]] = Map.empty

  override def send(target: RaftEndpoint, message: RaftMessage): Unit = {

    if source.equals(target) then {
      throw new IllegalArgumentException(source.getId.toString + " cannot send " + message + " to itself!")
    }
    val client =
      findClient(target).getOrElse(throw new IllegalArgumentException(s"No client found for target ${target.getId}"))
    val ip = target.asInstanceOf[Endpoint].ip
    Unsafe.unsafe { implicit unsafe =>
      val bytes = SerializationUtils.serialize(
        message
      ) // TODO use GRPC here or msgpack wrapping into case classes and transforming it and viceversa
      runtime.unsafe
        .run(client.sendCommand(SendCommandRequest(target.getId.asInstanceOf[String], ByteString.copyFrom(bytes))))
        .getOrThrow()
    }
  }

  private def findClient(target: RaftEndpoint): Option[DRefRaftClient] =
    endpointsClient.find { case (_, endpoints) => endpoints.contains(target) }.map(_._1)

  override def isReachable(endpoint: RaftEndpoint): Boolean = endpointsList.contains(endpoint)

  def endpointsList: Set[RaftEndpoint] = endpointsClient.values.flatMap(_.toList).toSet

  def updateEndpoints(updatedEndpoints: Map[DRefRaftClient, List[RaftEndpoint]]): Unit =
    endpointsClient = updatedEndpoints
}
