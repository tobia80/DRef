package io.github.tobia80.dref.raft

import com.google.protobuf.ByteString
import io.github.tobia80.dref.*
import io.github.tobia80.dref.ZioDref.ZDRefRaft
import io.github.tobia80.dref.raft.model.Endpoint
import io.grpc.{Status, StatusException}
import io.microraft.exception.NotLeaderException
import io.microraft.model.message.RaftMessage
import io.microraft.{QueryPolicy, RaftEndpoint, RaftNode}
import org.apache.commons.lang3.SerializationUtils
import scalapb.zio_grpc.RequestContext
import zio.{IO, Ref, ZIO}

import java.util.Optional

class DRefGrpcServer(
  myNodes: Ref[Map[String, NodeDescriptor]],
  endpoints: Ref[Set[RaftEndpoint]]
) extends ZDRefRaft[RequestContext] {

  // when receiving element, trigger the local term using the state machine, convert it back to bytes and return

  private def getLeaderNode: ZIO[Any, StatusException, RaftNode] = for {
    leader <- myNodes.get.flatMap { nodeMap =>
                nodeMap.values.find(el => el.node.getTerm.getLeaderEndpoint.getId == el.id) match {
                  case Some(leader) => ZIO.succeed(leader.node)
                  case None         =>
                    ZIO.fail(
                      new StatusException(io.grpc.Status.FAILED_PRECONDITION.withDescription("No leader found here"))
                    )
                }
              }
  } yield leader

  private def buildLeaderException(e: NotLeaderException): LeaderException = {
    val endpoint = e.getLeader.asInstanceOf[Endpoint]
    LeaderException(endpoint.id, endpoint.ip, io.grpc.Status.FAILED_PRECONDITION.withDescription("Not the leader"))
  }

  override def setElement(
    request: SetElementRequest,
    context: RequestContext
  ): IO[StatusException, SetElementResponse] = for {
    leader <- getLeaderNode
    res    <- ZIO
                .fromCompletionStage(leader.replicate(request)) // TODO manage NotLeaderException on the client side
                .mapBoth(
                  {
                    case e: NotLeaderException => buildLeaderException(e)
                    case err                   => new StatusException(io.grpc.Status.INTERNAL.withDescription(err.getMessage))
                  },
                  res => SetElementResponse()
                )
  } yield res

  override def setElementIfNotExist(
    request: SetElementIfNotExistRequest,
    context: RequestContext
  ): IO[StatusException, SetElementIfNotExistResponse] = for {
    leader <- getLeaderNode
    res    <- ZIO
                .fromCompletionStage(leader.replicate[Boolean](request))
                .mapBoth(
                  {
                    case e: NotLeaderException =>
                      buildLeaderException(e)
                    case err                   => new StatusException(io.grpc.Status.INTERNAL.withDescription(err.getMessage))
                  },
                  res => SetElementIfNotExistResponse(res.getResult)
                )
  } yield res

  override def getElement(
    request: GetElementRequest,
    context: RequestContext
  ): IO[StatusException, GetElementResponse] = for {
    leader <- getLeaderNode
    res    <- ZIO
                .fromCompletionStage(
                  leader.query[Array[Byte]](
                    request,
                    QueryPolicy.LINEARIZABLE,
                    Optional.empty[java.lang.Long](),
                    Optional.empty[java.time.Duration]()
                  )
                )
                .mapBoth(
                  {
                    case e: NotLeaderException => buildLeaderException(e)
                    case err                   => new StatusException(io.grpc.Status.INTERNAL.withDescription(err.getMessage))
                  },
                  res =>
                    if res == null then GetElementResponse(None)
                    else GetElementResponse(Option(ByteString.copyFrom(res.getResult)))
                )
  } yield res

  override def deleteElement(
    request: DeleteElementRequest,
    context: RequestContext
  ): IO[StatusException, DeleteElementResponse] = for {
    leader <- getLeaderNode
    res    <- ZIO
                .fromCompletionStage(leader.replicate(request))
                .mapBoth(
                  {
                    case e: NotLeaderException => buildLeaderException(e)
                    case err                   => new StatusException(io.grpc.Status.INTERNAL.withDescription(err.getMessage))
                  },
                  _ => DeleteElementResponse()
                )
  } yield res

  override def getEndpoints(
    request: GetEndpointsRequest,
    context: RequestContext
  ): IO[StatusException, EndpointResponse] = endpoints.get.map { eps =>
    EndpointResponse(eps.map(e => e.getId.asInstanceOf[String]).toSeq)
  }

  override def sendCommand(
    request: SendCommandRequest,
    context: RequestContext
  ): IO[StatusException, SendCommandResponse] = {
    val id = request.id
    val message = SerializationUtils.deserialize[RaftMessage](request.payload.toByteArray)
    for {
      nodeOpt <- myNodes.get.map(_.get(id))
      node    <- ZIO
                   .fromOption(nodeOpt)
                   .orElseFail(
                     new StatusException(io.grpc.Status.NOT_FOUND.withDescription(s"No node with id $id"))
                   )
      _       <- ZIO
                   .attempt(node.node.handle(message))
                   .mapError(err => new StatusException(io.grpc.Status.INTERNAL.withDescription(err.getMessage)))
    } yield SendCommandResponse()
  }

  override def expireElement(
    request: ExpireElementRequest,
    context: RequestContext
  ): IO[StatusException, ExpireElementResponse] = for {
    leader <- getLeaderNode
    res    <- ZIO
                .fromCompletionStage(leader.replicate(request))
                .mapBoth(
                  {
                    case e: NotLeaderException => buildLeaderException(e)
                    case err                   => new StatusException(io.grpc.Status.INTERNAL.withDescription(err.getMessage))
                  },
                  _ => ExpireElementResponse()
                )
  } yield res
}

case class LeaderException(leaderId: String, leaderAddress: String, status: Status) extends StatusException(status) {}

case class NodeDescriptor(id: String, node: RaftNode, transport: GrpcTransport)
