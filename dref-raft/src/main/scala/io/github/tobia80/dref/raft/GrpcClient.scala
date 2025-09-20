package io.github.tobia80.dref.raft

import io.github.tobia80.dref.GetEndpointsRequest
import io.github.tobia80.dref.ZioDref.DRefRaftClient
import io.microraft.RaftEndpoint
import zio.stream.ZStream
import zio.{durationInt, Chunk, Ref, Task, ZIO, *}

import java.util.concurrent.TimeUnit

class GrpcClient(clientsRef: Ref[Map[String, DRefRaftClient]], connectionTimeout: Duration) {

  private def getClient(raftEndpoint: RaftEndpoint): Task[DRefRaftClient] =
    clientsRef.get.flatMap { clients =>
      clients.get(raftEndpoint.asInstanceOf[Endpoint].ip) match {
        case Some(client) => ZIO.succeed(client)
        case None         => ZIO.fail(new IllegalArgumentException(s"No client found for target ${raftEndpoint.getId}"))
      }
    }

  def retrieveEndpoints(): Task[Set[RaftEndpoint]] = for {
    clients                   <- clientsRef.get
    res: Set[Endpoint]        <- ZIO
                                   .foreach(clients.toList) { ipAndClient =>
                                     ipAndClient._2
                                       .getEndpoints(GetEndpointsRequest())
                                       .map { response =>
                                         response.ids.map(id => Endpoint(id, ipAndClient._1))
                                       }
                                       .timeout(connectionTimeout)
                                       .tapError(err =>
                                         ZIO.logInfo(s"Cannot connect to ${ipAndClient._1} because of ${err.getMessage}")
                                       )
                                       .catchAll(_ => ZIO.none)
                                   }
                                   .map { elements =>
                                     elements
                                       .collect { case Some(value) =>
                                         value
                                       }
                                       .flatten
                                       .toSet
                                   }
    raftRes: Set[RaftEndpoint] = res.map(e => e.asInstanceOf[RaftEndpoint])
  } yield raftRes

  def sendDRefCommand(target: RaftEndpoint, command: Array[Byte]): Task[Unit] = for {
    client <- getClient(target)
    result <- client
                .sendCommand(
                  io.github.tobia80.dref
                    .SendCommandRequest(
                      target.getId.asInstanceOf[String],
                      com.google.protobuf.ByteString.copyFrom(command)
                    )
                )
                .unit
  } yield result

  def setElement(target: RaftEndpoint, name: String, a: Array[Byte], ttl: Option[Duration]): Task[Unit] = for {
    now     <- zio.Clock.currentTime(TimeUnit.MILLISECONDS)
    client  <- getClient(target)
    expireAt = ttl.map(t => now + t.toMillis)
    result  <- client
                 .setElement(
                   io.github.tobia80.dref
                     .SetElementRequest(
                       target.getId.asInstanceOf[String],
                       name,
                       com.google.protobuf.ByteString.copyFrom(a),
                       expireAt
                     )
                 )
                 .unit
  } yield result

  def expireElement(target: RaftEndpoint, name: String, ttl: Duration): Task[Unit] = for {
    now    <- zio.Clock.currentTime(TimeUnit.MILLISECONDS)
    client <- getClient(target)
    result <- client
                .expireElement(
                  io.github.tobia80.dref
                    .ExpireElementRequest(
                      target.getId.asInstanceOf[String],
                      name,
                      expireAt = now + ttl.toMillis
                    )
                )
                .unit
  } yield result

  def setElementIfNotExist(target: RaftEndpoint, name: String, a: Array[Byte], ttl: Option[Duration]): Task[Boolean] =
    for {
      now     <- zio.Clock.currentTime(TimeUnit.MILLISECONDS)
      client  <- getClient(target)
      expireAt = ttl.map(t => now + t.toMillis)
      result  <- client
                   .setElementIfNotExist(
                     io.github.tobia80.dref
                       .SetElementIfNotExistRequest(
                         target.getId.asInstanceOf[String],
                         name,
                         com.google.protobuf.ByteString.copyFrom(a),
                         expireAt
                       )
                   )
    } yield result.created

  def getElement(target: RaftEndpoint, name: String): Task[Option[Array[Byte]]] = for {
    client <- getClient(target)
    result <- client
                .getElement(
                  io.github.tobia80.dref
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
                  io.github.tobia80.dref
                    .DeleteElementRequest(
                      target.getId.asInstanceOf[String],
                      name
                    )
                )
                .unit
  } yield result

}
