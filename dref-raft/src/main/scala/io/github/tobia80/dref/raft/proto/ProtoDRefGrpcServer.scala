package io.github.tobia80.dref.raft.proto

import io.github.tobia80.dref.*
import io.github.tobia80.dref.ZioDref.ZDRefRaft
import io.grpc.{Status, StatusException}
import scalapb.zio_grpc.RequestContext
import zio.{IO, ZIO}

final class ProtoDRefGrpcServer(
  consensus: ProtoConsensusEngine,
  endpointIds: Seq[String]
) extends ZDRefRaft[RequestContext] {

  private def mapErr(error: ConsensusError): StatusException =
    error match {
      case ConsensusError.NotLeader(leaderId) =>
        new StatusException(
          Status.FAILED_PRECONDITION.withDescription(
            s"not-leader:${leaderId.getOrElse("unknown")}"
          )
        )
      case ConsensusError.NoLeader            =>
        new StatusException(Status.FAILED_PRECONDITION.withDescription("no-leader:unknown"))
      case ConsensusError.QuorumLost          =>
        new StatusException(Status.UNAVAILABLE.withDescription("quorum-lost"))
      case ConsensusError.Serialize(message)  =>
        new StatusException(Status.INTERNAL.withDescription(s"serialize: $message"))
      case ConsensusError.Transport(message)  =>
        new StatusException(Status.UNAVAILABLE.withDescription(message))
    }

  override def setElement(
    request: SetElementRequest,
    context: RequestContext
  ): IO[StatusException, SetElementResponse] =
    consensus
      .submit(StateCommands.setElement(request.name, request.value.toByteArray, request.expireAt))
      .mapBoth(mapErr, _ => SetElementResponse())

  override def setElementIfNotExist(
    request: SetElementIfNotExistRequest,
    context: RequestContext
  ): IO[StatusException, SetElementIfNotExistResponse] =
    consensus
      .submit(
        StateCommands.setElementIfNotExist(request.name, request.value.toByteArray, request.expireAt)
      )
      .mapBoth(
        mapErr,
        {
          case ApplyResult.Created(created) => SetElementIfNotExistResponse(created)
          case _                            => SetElementIfNotExistResponse(false)
        }
      )

  override def getElement(
    request: GetElementRequest,
    context: RequestContext
  ): IO[StatusException, GetElementResponse] =
    for {
      _        <- consensus.readIndex.mapError(mapErr)
      response <- consensus.stateMachine
                    .get(request.name)
                    .mapError { err =>
                      new StatusException(Status.INTERNAL.withDescription(err.getMessage))
                    }
                    .map { value =>
                      GetElementResponse(value.map(com.google.protobuf.ByteString.copyFrom))
                    }
    } yield response

  override def deleteElement(
    request: DeleteElementRequest,
    context: RequestContext
  ): IO[StatusException, DeleteElementResponse] =
    consensus
      .submit(StateCommands.deleteElement(request.name))
      .mapBoth(mapErr, _ => DeleteElementResponse())

  override def expireElement(
    request: ExpireElementRequest,
    context: RequestContext
  ): IO[StatusException, ExpireElementResponse] =
    consensus
      .submit(StateCommands.expireElement(request.name, request.expireAt))
      .mapBoth(mapErr, _ => ExpireElementResponse())

  override def getEndpoints(
    request: GetEndpointsRequest,
    context: RequestContext
  ): IO[StatusException, EndpointResponse] =
    ZIO.succeed(EndpointResponse(endpointIds))

  override def sendCommand(
    request: SendCommandRequest,
    context: RequestContext
  ): IO[StatusException, SendCommandResponse] =
    ZIO.succeed(SendCommandResponse())
}
