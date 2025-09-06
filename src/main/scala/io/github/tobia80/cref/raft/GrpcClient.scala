package io.github.tobia80.cref.raft

import io.github.tobia80.cref.GetEndpointsRequest
import io.github.tobia80.cref.ZioCref.CRefRaftClient
import io.microraft.RaftEndpoint
import zio.stream.ZStream
import zio.{Chunk, Ref, Task, ZIO}

class GrpcClient(clientsRef: Ref[Map[String, CRefRaftClient]]) {

  private def getClient(raftEndpoint: RaftEndpoint): Task[CRefRaftClient] =
    clientsRef.get.flatMap { clients =>
      clients.get(raftEndpoint.asInstanceOf[Endpoint].ip) match {
        case Some(client) => ZIO.succeed(client)
        case None         => ZIO.fail(new IllegalArgumentException(s"No client found for target ${raftEndpoint.getId}"))
      }
    }

  def retrieveEndpoints(): Task[Set[RaftEndpoint]] = for {
    clients <- clientsRef.get
    res     <- ZIO
                 .foreach(clients.toList) { ipAndClient =>
                   ipAndClient._2.getEndpoints(GetEndpointsRequest()).map { response =>
                     response.ids.map(id => Endpoint(id, ipAndClient._1))
                   }
                 }
                 .map(_.flatten.toSet)
    raftRes  = res.map(e => e.asInstanceOf[RaftEndpoint])
  } yield raftRes

  def sendCRefCommand(target: RaftEndpoint, command: Array[Byte]): Task[Unit] = for {
    client <- getClient(target)
    result <- client
                .sendCommand(
                  io.github.tobia80.cref
                    .SendCommandRequest(
                      target.getId.asInstanceOf[String],
                      com.google.protobuf.ByteString.copyFrom(command)
                    )
                )
                .unit
  } yield result

  def subscribeToAllEvents(target: RaftEndpoint): ZStream[Any, Throwable, Chunk[Byte]] = ???

  def setElement(target: RaftEndpoint, name: String, a: Array[Byte]): Task[Unit] = for {
    client <- getClient(target)
    result <- client
                .setElement(
                  io.github.tobia80.cref
                    .SetElementRequest(
                      target.getId.asInstanceOf[String],
                      name,
                      com.google.protobuf.ByteString.copyFrom(a)
                    )
                )
                .unit
  } yield result

  def setElementIfNotExist(target: RaftEndpoint, name: String, a: Array[Byte]): Task[Boolean] = for {
    client <- getClient(target)
    result <- client
                .setElementIfNotExist(
                  io.github.tobia80.cref
                    .SetElementIfNotExistRequest(
                      target.getId.asInstanceOf[String],
                      name,
                      com.google.protobuf.ByteString.copyFrom(a)
                    )
                )
  } yield result.created

  def getElement(target: RaftEndpoint, name: String): Task[Option[Array[Byte]]] = for {
    client <- getClient(target)
    result <- client
                .getElement(
                  io.github.tobia80.cref
                    .GetElementRequest(
                      target.getId.asInstanceOf[String],
                      name
                    )
                )
                .map(response => response.value.map(el => el.toByteArray))
  } yield result

  def deleteElement(target: RaftEndpoint, name: String): Task[Unit] = for {
    client <- getClient(target)
    result <- client
                .deleteElement(
                  io.github.tobia80.cref
                    .DeleteElementRequest(
                      target.getId.asInstanceOf[String],
                      name
                    )
                )
                .unit
  } yield result

}
