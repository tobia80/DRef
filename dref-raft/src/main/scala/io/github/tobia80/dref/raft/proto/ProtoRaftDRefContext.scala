package io.github.tobia80.dref.raft.proto

import com.google.protobuf.ByteString
import io.github.tobia80.dref.*
import io.github.tobia80.dref.ZioDref.DRefRaftClient
import io.github.tobia80.dref.raft.IpProvider
import io.grpc.protobuf.services.ProtoReflectionService
import io.grpc.{ServerBuilder, StatusException}
import scalapb.zio_grpc.{ServerLayer, ServiceList}
import zio.stream.ZStream
import zio.{durationInt, *}

import java.util.concurrent.TimeUnit

trait ProtoRaftDRefContext extends DRefContext {
  def nodeId: String
  def isLeader: Task[Boolean]
  def leaderId: Task[Option[String]]
}

object ProtoRaftDRefContext {

  private final class ProtoDRefClient(
    ipClientsRef: Ref[Map[String, DRefRaftClient]],
    aliasRef: Ref[Map[String, DRefRaftClient]]
  ) {

    private def fromStatus(ex: StatusException): ClientError = {
      val desc = Option(ex.getStatus.getDescription).getOrElse("")
      if desc.startsWith("not-leader:") then
        val hint = desc.stripPrefix("not-leader:").trim
        ClientError.NotLeader(if hint.isEmpty || hint == "unknown" then None else Some(hint))
      else if desc.startsWith("no-leader:") then ClientError.NotLeader(None)
      else if ex.getStatus.getCode == io.grpc.Status.Code.UNAVAILABLE ||
        ex.getStatus.getCode == io.grpc.Status.Code.DEADLINE_EXCEEDED
      then ClientError.Transport(desc)
      else ClientError.Other(desc)
    }

    private def call[A](targetId: String)(op: DRefRaftClient => IO[StatusException, A]): IO[ClientError, A] =
      (aliasRef.get zip ipClientsRef.get).flatMap { case (aliases, ipClients) =>
        aliases.get(targetId).orElse(ipClients.get(targetId)) match {
          case None         => ZIO.fail(ClientError.UnknownNode(targetId))
          case Some(client) => op(client).mapError(fromStatus)
        }
      }

    def setElement(
      targetId: String,
      name: String,
      value: Array[Byte],
      expireAt: Option[Long]
    ): IO[ClientError, Unit] =
      call(targetId) {
        _.setElement(
          SetElementRequest(id = targetId, name = name, value = ByteString.copyFrom(value), expireAt = expireAt)
        ).unit
      }

    def setElementIfNotExist(
      targetId: String,
      name: String,
      value: Array[Byte],
      expireAt: Option[Long]
    ): IO[ClientError, Boolean] =
      call(targetId) {
        _.setElementIfNotExist(
          SetElementIfNotExistRequest(
            id = targetId,
            name = name,
            value = ByteString.copyFrom(value),
            expireAt = expireAt
          )
        ).map(_.created)
      }

    def getElement(targetId: String, name: String): IO[ClientError, Option[Array[Byte]]] =
      call(targetId) {
        _.getElement(GetElementRequest(id = targetId, name = name)).map(_.value.map(_.toByteArray))
      }

    def deleteElement(targetId: String, name: String): IO[ClientError, Unit] =
      call(targetId) {
        _.deleteElement(DeleteElementRequest(id = targetId, name = name)).unit
      }

    def expireElement(targetId: String, name: String, expireAt: Long): IO[ClientError, Unit] =
      call(targetId) {
        _.expireElement(ExpireElementRequest(id = targetId, name = name, expireAt = expireAt)).unit
      }
  }

  private enum ClientError {
    case NotLeader(leaderId: Option[String])
    case Transport(message: String)
    case UnknownNode(targetId: String)
    case Other(message: String)
  }

  /** Start a Raft node and periodically refresh peers from `ipProvider`. */
  def startWithAddressPolling(
    config: ProtoRaftConfig,
    ipProvider: IpProvider,
    leaderWait: Duration = 500.millis
  ): ZIO[Scope, Throwable, ProtoRaftDRefContext] =
    for {
      ips         <- ipProvider.findNodeAddresses()
      myIp        <- ipProvider.findMyAddress()
      port         = config.port
      bindAddress  = config.bindAddress.getOrElse(s"$myIp:$port")
      endpoints    = ips.map(ip => NodeEndpoint(ip, s"$ip:$port"))
      ctx         <- start(config.copy(bindAddress = Some(bindAddress), initialEndpoints = endpoints), leaderWait)
      _           <- addressPollLoop(ipProvider, ctx, config.addressPollInterval).forkScoped
    } yield ctx

  def start(config: ProtoRaftConfig, leaderWait: Duration = 500.millis): ZIO[Scope, Throwable, ProtoRaftDRefContext] =
    for {
      nodeId <- config.nodeId match {
                  case Some(id) => ZIO.succeed(id)
                  case None       => Random.nextLongBetween(0L, 99_999L).map(_.toString)
                }
      bindAddress = config.bindAddress.getOrElse(s"127.0.0.1:${config.port}")
      // IP-based discovery uses the IP itself as a placeholder endpoint id,
      // so our own address would otherwise stay in the peer list (whose
      // self-skip matches on nodeId) and inflate the quorum. Drop any
      // endpoint pointing at our bind, then add ourselves once under nodeId.
      endpoints   = config.initialEndpoints.filterNot(_.address == bindAddress) :+
                      NodeEndpoint(nodeId, bindAddress)
      memberIds   = endpoints.map(_.id)
      stateMachine <- ProtoStateMachine.make
      resolvedConfig = config.copy(nodeId = Some(nodeId), initialEndpoints = endpoints)
      consensus    <- ProtoConsensusEngine.make(nodeId, stateMachine, resolvedConfig)
      drefServer    = new ProtoDRefGrpcServer(consensus, memberIds)
      consensusServer = new DRefConsensusGrpcServer(consensus)
      builder       = ServerBuilder.forPort(config.port).addService(ProtoReflectionService.newInstance())
      services      = ServiceList.add(drefServer).add(consensusServer)
      _            <- ServerLayer.fromServiceList(builder, services).launch.forkScoped
      _            <- waitForLocalGrpcServer(config.port)
      clients      <- ZIO.foreach(endpoints) { ep =>
                        DRefRaftClient
                          .scoped(GrpcChannels.managedChannel(ep.address))
                          .map(ep.id -> _)
                      }.map(_.toMap)
      clientsRef   <- Ref.make(clients)
      // IP-based discovery keys peers by IP, but consensus identifies them by
      // real nodeId (heartbeats carry leader_id = randomly-chosen string).
      // Probe each peer's GetEndpoints — by convention the last entry in the
      // response is the peer's own nodeId — and alias that id to the same
      // gRPC client so forwarding writes to the elected leader works.
      //
      // Refresh on a schedule: at compose startup peers may not yet be serving
      // gRPC when we probe them, and one-shot discovery would leave the alias
      // permanently missing. Periodic refresh also covers rolling restarts
      // that hand a peer a new random nodeId.
      aliasRef     <- Ref.make(Map.empty[String, DRefRaftClient])
      _            <- refreshAliases(clientsRef, nodeId, aliasRef)
      _            <- refreshAliases(clientsRef, nodeId, aliasRef)
                        .repeat(Schedule.spaced(1.second))
                        .forkScoped
      client         = new ProtoDRefClient(clientsRef, aliasRef)
      _            <- consensus.spawnDrivers
      _            <- ttlReaper(consensus).forkScoped
      _            <- ProtoConsensusEngine.waitForLeader(consensus, leaderWait)
    } yield new Impl(nodeId, resolvedConfig, consensus, stateMachine, client, clientsRef, aliasRef, memberIds)

  private def addressPollLoop(
    ipProvider: IpProvider,
    ctx: ProtoRaftDRefContext,
    interval: Duration
  ): ZIO[Scope, Nothing, Unit] =
    val impl = ctx.asInstanceOf[Impl]
    val poll =
      for {
        ips           <- ipProvider.findNodeAddresses()
        port           = impl.config.port
        bindAddress    = impl.config.bindAddress.getOrElse(s"127.0.0.1:$port")
        endpoints      = ips.map(ip => NodeEndpoint(ip, s"$ip:$port"))
        peerEndpoints  = endpoints.filterNot(_.address == bindAddress)
        allEndpoints   = peerEndpoints :+ NodeEndpoint(impl.nodeId, bindAddress)
        _             <- impl.consensus.syncPeers(peerEndpoints, impl.nodeId, bindAddress)
        _             <- syncRaftClients(impl.clientsRef, allEndpoints)
        _             <- refreshAliases(impl.clientsRef, impl.nodeId, impl.aliasRef)
      } yield ()
    poll.catchAll(_ => ZIO.unit).repeat(Schedule.spaced(interval)).unit

  private def syncRaftClients(
    clientsRef: Ref[Map[String, DRefRaftClient]],
    endpoints: List[NodeEndpoint]
  ): ZIO[Scope, Throwable, Unit] =
    for {
      current <- clientsRef.get
      desired  = endpoints.map(ep => ep.id -> ep.address).toMap
      toRemove = current.keySet -- desired.keySet
      toAdd    = endpoints.filter(ep => !current.contains(ep.id))
      _       <- ZIO.foreachDiscard(toRemove)(id => clientsRef.update(_ - id))
      added   <- ZIO.foreach(toAdd) { ep =>
                   DRefRaftClient
                     .scoped(GrpcChannels.managedChannel(ep.address))
                     .map(ep.id -> _)
                 }
      _ <- clientsRef.update(_ ++ added.toMap).when(added.nonEmpty)
    } yield ()

  private def refreshAliases(
    ipClientsRef: Ref[Map[String, DRefRaftClient]],
    selfNodeId: String,
    aliasRef: Ref[Map[String, DRefRaftClient]]
  ): UIO[Unit] =
    ipClientsRef.get.flatMap { ipClients =>
      ZIO
        .foreach(ipClients.toList) { case (id, raftClient) =>
        if id == selfNodeId then ZIO.succeed(None)
        else
          raftClient
            .getEndpoints(GetEndpointsRequest())
            .either
            .map {
              case Right(resp) if resp.ids.nonEmpty =>
                val realId = resp.ids.last
                if realId != id && realId != selfNodeId then Some(realId -> raftClient)
                else None
              case _ => None
            }
      }
        .flatMap { pairs =>
          val discovered = pairs.flatten.toMap
          // Merge rather than replace so a transient unreachable peer doesn't
          // drop a previously-learned alias mid-flight.
          aliasRef.update(_ ++ discovered)
        }
    }

  private final class Impl(
    override val nodeId: String,
    val config: ProtoRaftConfig,
    val consensus: ProtoConsensusEngine,
    stateMachine: ProtoStateMachine,
    client: ProtoDRefClient,
    val clientsRef: Ref[Map[String, DRefRaftClient]],
    val aliasRef: Ref[Map[String, DRefRaftClient]],
    memberIds: Seq[String]
  ) extends ProtoRaftDRefContext {

    override def isLeader: Task[Boolean] = consensus.isLeader

    override def leaderId: Task[Option[String]] = consensus.leaderId

    override def defaultTtl: Duration = config.ttl.getOrElse(20.seconds)

    override def setElement(name: String, value: Array[Byte], ttl: Option[Duration]): Task[Unit] =
      withLeader(ttl) { (leader, expireAt) =>
        client.setElement(leader, name, value, expireAt)
      }

    override def setElementIfNotExist(name: String, value: Array[Byte], ttl: Option[Duration]): Task[Boolean] =
      withLeader(ttl) { (leader, expireAt) =>
        client.setElementIfNotExist(leader, name, value, expireAt)
      }

    override def getElement(name: String): Task[Option[Array[Byte]]] =
      withLeader(None) { (leader, _) =>
        client.getElement(leader, name)
      }

    override def deleteElement(name: String): Task[Unit] =
      withLeader(None) { (leader, _) =>
        client.deleteElement(leader, name)
      }

    override def keepAliveStream(name: String, ttl: Duration): ZStream[Any, Throwable, Unit] = {
      val period = Duration.fromScala(ttl.asFiniteDuration / 1.25)
      ZStream.repeatZIOWithSchedule(
        Clock.currentTime(TimeUnit.MILLISECONDS).flatMap { now =>
          withLeader(None) { (leader, _) =>
            client.expireElement(leader, name, now + ttl.toMillis)
          }
        },
        Schedule.fixed(period)
      )
    }

    override def onChangeStream(name: String): ZStream[Any, Throwable, ChangeEvent] =
      ZStream.fromHub(stateMachine.changeHub).filter(_.name == name)

    override def detectDeletionFromUnderlyingStream(name: String): ZStream[Any, Throwable, DeleteElement] =
      ZStream
        .repeatZIOWithSchedule(
          getElement(name).map {
            case None => Some(DeleteElement(name))
            case _    => None
          },
          Schedule.fixed(500.millis)
        )
        .collect { case Some(deleted) => deleted }

    override def detectStolenElement(name: String, value: Array[Byte]): ZStream[Any, Throwable, StolenElement] =
      ZStream
        .repeatZIOWithSchedule(
          getElement(name).map {
            case Some(current) if !java.util.Arrays.equals(current, value) => Some(StolenElement(name))
            case None                                                      => Some(StolenElement(name))
            case _                                                         => None
          },
          Schedule.fixed(500.millis)
        )
        .collect { case Some(stolen) => stolen }

    private def withLeader[A](
      ttl: Option[Duration]
    )(op: (String, Option[Long]) => IO[ClientError, A]): Task[A] = {
      def loop(transportAttempts: Int, unknownAttempts: Int): Task[A] =
        for {
          expireAt <- ttlToExpireAt(ttl)
          leader   <- currentLeader
          result   <- op(leader, expireAt).foldZIO(
                        {
                          case ClientError.NotLeader(_) =>
                            ZIO.sleep(30.millis) *> loop(0, 0)
                          case ClientError.Transport(_) if transportAttempts < 20 =>
                            ZIO.sleep(50.millis) *> loop(transportAttempts + 1, unknownAttempts)
                          case ClientError.Transport(msg) =>
                            ZIO.fail(new RuntimeException(s"transport error talking to leader: $msg"))
                          // Followers learn the leader's random nodeId via
                          // heartbeats before the alias-refresh fiber maps
                          // it to a gRPC channel. Wait briefly so refresh
                          // can catch up, then retry rather than failing
                          // the caller with a transient routing miss.
                          case ClientError.UnknownNode(_) if unknownAttempts < 20 =>
                            ZIO.sleep(100.millis) *> loop(transportAttempts, unknownAttempts + 1)
                          case ClientError.UnknownNode(target) =>
                            ZIO.fail(new RuntimeException(s"unknown node id $target"))
                          case ClientError.Other(msg) =>
                            ZIO.fail(new RuntimeException(msg))
                        },
                        ZIO.succeed(_)
                      )
        } yield result
      loop(0, 0)
    }

    private def ttlToExpireAt(ttl: Option[Duration]): Task[Option[Long]] =
      ttl match {
        case None    => ZIO.none
        case Some(d) =>
          Clock.currentTime(TimeUnit.MILLISECONDS).map(now => Some(now + d.toMillis))
      }

    private def currentLeader: Task[String] =
      consensus.leaderId.flatMap {
        case Some(id) => ZIO.succeed(id)
        case None     =>
          ProtoConsensusEngine.waitForLeader(consensus, 1.second).flatMap {
            case Some(id) => ZIO.succeed(id)
            case None       => ZIO.fail(new RuntimeException("no leader elected"))
          }
      }
  }

  private def ttlReaper(consensus: ProtoConsensusEngine): UIO[Unit] =
    ZStream
      .repeatZIOWithSchedule(
        for {
          isLeader <- consensus.isLeader
          _        <- ZIO.when(isLeader) {
                        for {
                          now   <- Clock.currentTime(TimeUnit.MILLISECONDS)
                          table <- consensus.stateMachine.expirationTable
                          _     <- ZIO.foreachDiscard(table) { case (name, expireAt) =>
                                     consensus
                                       .submit(StateCommands.deleteIfExpired(name, now))
                                       .catchAll(_ => ZIO.unit)
                                       .when(expireAt <= now)
                                   }
                        } yield ()
                      }
        } yield (),
        Schedule.spaced(200.millis)
      )
      .runDrain

  private def waitForLocalGrpcServer(port: Int): Task[Unit] =
    ZIO
      .attemptBlocking {
        val socket = new java.net.Socket()
        try socket.connect(new java.net.InetSocketAddress("127.0.0.1", port), 500)
        finally socket.close()
      }
      .retry(Schedule.spaced(50.millis) && Schedule.recurs(200))
      .unit
}
