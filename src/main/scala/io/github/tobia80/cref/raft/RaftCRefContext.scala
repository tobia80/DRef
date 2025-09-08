package io.github.tobia80.cref.raft

import io.github.tobia80.cref.ZioCref.CRefRaftClient
import io.github.tobia80.cref.{CRefContext, ChangeEvent, DeleteElement, SetElement}
import io.grpc.ServerBuilder
import io.grpc.netty.NettyChannelBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import io.microraft.{RaftEndpoint, RaftNode}
import reactor.core.publisher.Sinks
import scalapb.zio_grpc.{ServerLayer, ServiceList, ZManagedChannel}
import zio.interop.reactivestreams.*
import zio.stream.{Take, ZStream}
import zio.{durationInt, Hub, Promise, Ref, Runtime, Schedule, Scope, Task, URIO, Unsafe, ZIO, ZLayer}

case class RaftConfig(port: Int)

object RaftCRefContext {

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
      val state = new CRefStateMachine(sink)
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

  private def buildZIOGrpcClient(address: String, port: Int): ZIO[Scope, Throwable, CRefRaftClient] =
    CRefRaftClient.scoped(
      ZManagedChannel(NettyChannelBuilder.forAddress(address, port).usePlaintext())
    )

  private def leader(raftNode: RaftNode): URIO[Any, Option[RaftEndpoint]] =
    ZIO.attempt(Option(raftNode.getTerm.getLeaderEndpoint)).orElseSucceed(None)

  private def leaderEndpoint(raftNode: RaftNode): Task[RaftEndpoint] =
    leader(raftNode).flatMap {
      case Some(endpoint) => ZIO.succeed(endpoint)
      case _              => leaderEndpoint(raftNode).delay(20.millis)
    }

  trait RaftCRefContext extends CRefContext {

    def registerNode(id: String): Task[Unit]

    def unregisterNode(id: String): Task[Unit]

  }

  val live: ZLayer[IpProvider & RaftConfig & Scope, Throwable, RaftCRefContext] = ZLayer(for {
    config          <- ZIO.service[RaftConfig]
    ipProvider      <- ZIO.service[IpProvider]
    runtime         <- ZIO.runtime[Any]
    myIp            <- ipProvider.findMyAddress()
    id              <- ZIO.randomWith(_.nextLongBetween(0, 99999)).map(_.toString)
    myEndpoint       = Endpoint(id, myIp)
    clientMapRef    <- Ref.make[Map[String, CRefRaftClient]](Map.empty)
    grpcClient       = new GrpcClient(clientMapRef)
    myTransport      = createTransport(myEndpoint, runtime)
    endpointListRef <- Ref.make[Set[RaftEndpoint]](Set(myEndpoint))
    myNodes         <- Ref.make[Map[String, NodeDescriptor]](
                         Map.empty
                       )
    crefServer       = CRefGrpcServer(myNodes, endpointListRef)
    builder          = ServerBuilder.forPort(config.port).addService(ProtoReflectionService.newInstance())
    services         = ServiceList.add(crefServer)
    logic            = ServerLayer.fromServiceList(builder, services)
    ret             <- logic.launch.forkScoped

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

                                           endpoints        <- grpcClient.retrieveEndpoints()
                                           endpointClientMap = addressClientMap.map { case (ip, client) =>
                                                                 client -> endpoints.filter(_.asInstanceOf[Endpoint].ip == ip).toList
                                                               }
                                           _                 = myTransport.updateEndpoints(endpointClientMap)
                                           // discovering leader
                                           _                <- initialized.succeed(())
                                         } yield ()
                                       )
                                       .repeat(Schedule.fixed(3.seconds))
                                       .fork
    hub                           <- Hub.bounded[Take[Throwable, ChangeEvent]](64)
    sinks: Sinks.Many[ChangeEvent] = Sinks.many().multicast().onBackpressureBuffer[ChangeEvent]()
    fiber                         <- sinks
                                       .asFlux()
                                       .toZIOStream(qSize = 16)
                                       .runForeach { event =>
                                         ZIO.logInfo(s"Publishing event $event to hub") *> hub
                                           .publish(Take.single(event))
                                       }
                                       .forkDaemon

    myNode <-
      initialized.await *> createRaftNode(myEndpoint, myTransport, myTransport.endpointsList, sinks).tap { node =>
        val nodeDescriptor = NodeDescriptor(myEndpoint.id, node)
        myNodes.update(old => old + (myEndpoint.id -> nodeDescriptor)) *>
          ZIO.logInfo(s"Starting node $myEndpoint") *> ZIO.fromCompletableFuture(node.start()) *> ZIO.logInfo(
            s"Node $myEndpoint started"
          )
      }
  } yield new RaftCRefContext {

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
        endpointList <- endpointListRef.get
        raftNode     <- createRaftNode(endpoint, transport, endpointList, sinks)
        _            <- ZIO.logInfo(s"Starting node $endpoint")
        _            <- ZIO.fromCompletableFuture(raftNode.start())
        _            <- ZIO.logInfo(
                          s"Node $endpoint started"
                        )
        _            <- endpointListRef.update(old => old + endpoint) *> myNodes.update(old =>
                          old + (id -> NodeDescriptor(id, raftNode))
                        )
      } yield ()
    }

    override def unregisterNode(nodeId: String): Task[Unit] = for {
      _ <- myNodes.get.flatMap(nodeMap =>
             nodeMap.get(nodeId).fold(ZIO.unit)(el => ZIO.fromCompletableFuture(el.node.terminate()).unit)
           )
      _ <- myNodes.update(old => old - nodeId)
      _ <- endpointListRef.update(old => old.filterNot(_.getId == nodeId))
    } yield ()
  })
}
