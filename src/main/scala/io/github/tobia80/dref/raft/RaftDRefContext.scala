package io.github.tobia80.dref.raft

import io.github.tobia80.dref.ZioDref.DRefRaftClient
import io.github.tobia80.dref.{ChangeEvent, DRefContext}
import io.grpc.ServerBuilder
import io.grpc.netty.NettyChannelBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import io.microraft.{MembershipChangeMode, RaftEndpoint, RaftNode}
import reactor.core.publisher.Sinks
import scalapb.zio_grpc.{ServerLayer, ServiceList, ZManagedChannel}
import zio.interop.reactivestreams.*
import zio.stream.{Take, ZStream}
import zio.{durationInt, Hub, Promise, Ref, Runtime, Schedule, Scope, Task, URIO, Unsafe, ZIO, ZLayer}

case class RaftConfig(port: Int)

object RaftDRefContext {

  import scala.jdk.CollectionConverters.*

  private def createTransport(endpoint: Endpoint, runtime: Runtime[Any]): GrpcTransport =
    GrpcTransport(endpoint, runtime)

  private def createRaftNode(
    endpoint: Endpoint,
    transport: GrpcTransport,
    initialEndpoints: Set[RaftEndpoint],
    sink: Sinks.Many[ChangeEvent]
  ): Task[RaftNode] =
    ZIO.attempt {
      val state = new DRefStateMachine(sink)
      val raftNode = RaftNode
        .newBuilder()
        .setGroupId("default")
        .setLocalEndpoint(endpoint)
        .setInitialGroupMembers(initialEndpoints.asJava)
        .setTransport(transport)
        .setStateMachine(state)
        .build()

      raftNode
    }

  private def buildZIOGrpcClient(address: String, port: Int): ZIO[Scope, Throwable, DRefRaftClient] =
    DRefRaftClient.scoped(
      ZManagedChannel(NettyChannelBuilder.forAddress(address, port).usePlaintext())
    )

  private def findLeaderNode(myNodes: Ref[Map[String, NodeDescriptor]]): Task[Option[RaftNode]] =
    myNodes.get.map { nodeMap =>
      nodeMap.values.find(el => el.node.getTerm.getLeaderEndpoint.getId == el.id).map(_.node)
    }

  private def leader(raftNode: RaftNode): URIO[Any, Option[RaftEndpoint]] =
    ZIO.attempt(Option(raftNode.getTerm.getLeaderEndpoint)).orElseSucceed(None)

  private def leaderEndpoint(raftNode: RaftNode): Task[RaftEndpoint] =
    leader(raftNode).flatMap {
      case Some(endpoint) => ZIO.succeed(endpoint)
      case _              => leaderEndpoint(raftNode).delay(20.millis)
    }

  trait RaftDRefContext extends DRefContext {

    def registerNode(id: String): Task[Unit]

    def unregisterNode(id: String): Task[Unit]

  }

  private def propagateMembership(
    leaderNode: RaftNode,
    currentEndpoints: Set[RaftEndpoint],
    additions: Set[RaftEndpoint],
    removals: Set[RaftEndpoint]
  ): Task[Unit] = {
    val addTasks = additions.toList.map { endpoint =>
      ZIO
        .fromCompletionStage(
          leaderNode.changeMembership(
            endpoint,
            MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER,
            leaderNode.getCommittedMembers.getLogIndex
          )
        ) // TODO what to put in commit index?
        .unit
        .tapError(err =>
          ZIO.logError(s"Error adding member $endpoint current endpoints are $currentEndpoints: ${err.getMessage}")
        )
    }
    val removeTasks = removals.toList.map { endpoint =>
      ZIO
        .fromCompletionStage(
          leaderNode
            .changeMembership(endpoint, MembershipChangeMode.REMOVE_MEMBER, leaderNode.getCommittedMembers.getLogIndex)
        )
        .unit
        .tapError(err =>
          ZIO.logError(s"Error removing member $endpoint current endpoints are $currentEndpoints: ${err.getMessage}")
        )
    }
    ZIO.collectAll(addTasks ++ removeTasks).unit
  }

  val live: ZLayer[IpProvider & RaftConfig & Scope, Throwable, RaftDRefContext] = ZLayer(for {
    config               <- ZIO.service[RaftConfig]
    ipProvider           <- ZIO.service[IpProvider]
    runtime              <- ZIO.runtime[Any]
    myIp                 <- ipProvider.findMyAddress()
    id                   <- ZIO.randomWith(_.nextLongBetween(0, 99999)).map(_.toString)
    myEndpoint            = Endpoint(id, myIp)
    clientMapRef         <- Ref.make[Map[String, DRefRaftClient]](Map.empty)
    endpointClientMapRef <- Ref.make[Map[DRefRaftClient, List[RaftEndpoint]]](Map.empty)
    grpcClient            = new GrpcClient(clientMapRef)
    myTransport           = createTransport(myEndpoint, runtime)
    transports           <- Ref.make[Map[String, GrpcTransport]](Map(id -> myTransport))
    endpointsOnThisJvm   <- Ref.make[Set[RaftEndpoint]](Set(myEndpoint))
    myNodes              <- Ref.make[Map[String, NodeDescriptor]](
                              Map.empty
                            )
    drefServer            = DRefGrpcServer(myNodes, endpointsOnThisJvm)
    builder               = ServerBuilder.forPort(config.port).addService(ProtoReflectionService.newInstance())
    services              = ServiceList.add(drefServer)
    logic                 = ServerLayer.fromServiceList(builder, services)
    ret                  <- logic.launch.forkScoped

    initialized                   <- Promise.make[Throwable, Unit]
    _                             <- ipProvider
                                       .findNodeAddresses()
                                       .flatMap(newAddresses =>
                                         for {
                                           addressClientMap <- ZIO
                                                                 .foreach(newAddresses) { address =>
                                                                   buildZIOGrpcClient(address, config.port).map(address -> _)
                                                                 }
                                                                 .map(_.toMap)
                                           _                <- clientMapRef.update(old => addressClientMap)

                                           oldEndpoints     <- endpointsOnThisJvm.get
                                           endpoints        <- grpcClient.retrieveEndpoints()
                                           _                <- ZIO.logInfo(s"Discovered endpoints: $endpoints")
                                           endpointClientMap = addressClientMap.map { case (ip, client) =>
                                                                 client -> endpoints.filter(_.asInstanceOf[Endpoint].ip == ip).toList
                                                               }

                                           _                   <- endpointClientMapRef.set(endpointClientMap)
                                           // check addition and removal and if leader, propagate the change
                                           additions            = endpoints.diff(oldEndpoints)
                                           removals             = oldEndpoints.diff(endpoints)
                                           _                   <- transports.get.map { inner =>
                                                                    inner.values.map(_.updateEndpoints(endpointClientMap))
                                                                  } // update all the transports not only mine
                                           leaderNodeHereMaybe <- findLeaderNode(myNodes)
                                           _                   <- leaderNodeHereMaybe match {
                                                                    case Some(leaderHere) =>
                                                                      propagateMembership(leaderHere, endpoints, additions, removals) // propagate changes
                                                                    case None             => ZIO.unit
                                                                  }
                                           _                   <- initialized.succeed(())
                                         } yield ()
                                       )
                                       .repeat(Schedule.fixed(3.seconds))
                                       .forkScoped
    hub                           <- Hub.bounded[Take[Throwable, ChangeEvent]](64)
    sinks: Sinks.Many[ChangeEvent] = Sinks.many().multicast().onBackpressureBuffer[ChangeEvent]()
    fiber                         <- sinks
                                       .asFlux()
                                       .toZIOStream(qSize = 16)
                                       .runForeach { event =>
                                         ZIO.logDebug(s"Publishing event $event to hub") *> hub
                                           .publish(Take.single(event))
                                       }
                                       .forkScoped

    myNode <-
      initialized.await *> createRaftNode(myEndpoint, myTransport, myTransport.endpointsList, sinks).tap { node =>
        val nodeDescriptor = NodeDescriptor(myEndpoint.id, node, myTransport)
        myNodes.update(old => old + (myEndpoint.id -> nodeDescriptor)) *>
          ZIO.logInfo(s"Starting node $myEndpoint") *> ZIO.fromCompletableFuture(node.start()) *> ZIO.logInfo(
            s"Node $myEndpoint started"
          )
      }
  } yield new RaftDRefContext {

    private def retryOnLeaderException[T](task: Task[T]) =
      task.retry(
        Schedule.forever.whileInput {
          case LeaderException(_, _, _) => true // retry on LeaderException
          case _                        => false // do not retry on other exceptions
        }
      )

    override def setElement(name: String, value: Array[Byte]): Task[Unit] =
      retryOnLeaderException(
        leaderEndpoint(myNode)
          .flatMap { leaderEndpoint =>
            grpcClient.setElement(leaderEndpoint, name, value) // What if not leader from server? repeat until success?
          }
      )

    override def setElementIfNotExist(name: String, value: Array[Byte]): Task[Boolean] =
      retryOnLeaderException(leaderEndpoint(myNode).flatMap { leaderEndpoint =>
        grpcClient.setElementIfNotExist(leaderEndpoint, name, value)
      })

    override def keepAliveStream(name: String): ZStream[Any, Throwable, Unit] = ZStream.empty

    override def getElement(name: String): Task[Option[Array[Byte]]] =
      retryOnLeaderException(leaderEndpoint(myNode).flatMap { leaderEndpoint =>
        grpcClient.getElement(leaderEndpoint, name)
      })

    override def deleteElement(name: String): Task[Unit] =
      retryOnLeaderException(leaderEndpoint(myNode).flatMap { leaderEndpoint =>
        grpcClient.deleteElement(leaderEndpoint, name)
      })

    override def onChangeStream(name: String): ZStream[Any, Throwable, ChangeEvent] =
      ZStream.logInfo(s"Subscribing to $name") *>
        ZStream.fromHub(hub).flattenTake.collect { case c if c.name == name => c }

    override def registerNode(id: String): Task[Unit] = {
      // add raft node, update the ref and handle the change in members
      val endpoint = Endpoint(id, myIp)
      val transport = createTransport(endpoint, runtime)
      for {
        endpointList      <- endpointsOnThisJvm.get
        endpointClientMap <- endpointClientMapRef.get
        _                  = transport.updateEndpoints(endpointClientMap)
        raftNode          <- createRaftNode(endpoint, transport, endpointList, sinks)
        _                 <- ZIO.logInfo(s"Starting node $endpoint")
        _                 <- ZIO.fromCompletableFuture(raftNode.start())
        report            <- ZIO.fromCompletionStage(raftNode.getReport)
        _                 <- ZIO.logInfo(
                               s"Node $endpoint started, effective members are ${report.getResult.getCommittedMembers.getMembers}"
                             )
        _                 <- endpointsOnThisJvm.update(old => old + endpoint) *> myNodes.update(old =>
                               old + (id -> NodeDescriptor(id, raftNode, transport))
                             )
      } yield ()
    }

    override def unregisterNode(nodeId: String): Task[Unit] = for {
      _ <- myNodes.get.flatMap(nodeMap =>
             nodeMap.get(nodeId).fold(ZIO.unit)(el => ZIO.fromCompletableFuture(el.node.terminate()).unit)
           )
      _ <- myNodes.update(old => old - nodeId)
      _ <- endpointsOnThisJvm.update(old => old.filterNot(_.getId == nodeId))
    } yield ()
  })
}
