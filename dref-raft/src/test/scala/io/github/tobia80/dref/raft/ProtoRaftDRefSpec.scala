package io.github.tobia80.dref.raft

import io.github.tobia80.dref.raft.proto.{NodeEndpoint, ProtoRaftConfig, ProtoRaftDRefContext}
import zio.*
import zio.test.*

import java.net.ServerSocket

object ProtoRaftDRefSpec extends ZIOSpecDefault {

  private def freePort: UIO[Int] =
    ZIO
      .attemptBlocking {
        val socket = new ServerSocket(0)
        val port   = socket.getLocalPort
        socket.close()
        port
      }
      .orDie

  private def makeClusterConfig(ports: List[Int], idx: Int, nodeId: String): ProtoRaftConfig = {
    val endpoints = ports.zipWithIndex.map { case (port, i) =>
      NodeEndpoint(s"node-$i", s"127.0.0.1:$port")
    }
    ProtoRaftConfig(
      port = ports(idx),
      bindAddress = Some(s"127.0.0.1:${ports(idx)}"),
      nodeId = Some(nodeId),
      ttl = Some(5.seconds),
      connectionTimeout = 500.millis,
      electionTimeout = 400.millis,
      heartbeatInterval = 80.millis,
      initialEndpoints = endpoints
    )
  }

  private def startCluster(size: Int): ZIO[Scope, Throwable, List[ProtoRaftDRefContext]] =
    for {
      ports <- ZIO.foreach(List.fill(size)(()))(_ => freePort)
      nodes <- ZIO.foreach(ports.zipWithIndex) { case (port, idx) =>
                 ProtoRaftDRefContext.start(makeClusterConfig(ports, idx, s"node-$idx"), 2.seconds)
               }
      _     <- waitForSingleLeader(nodes)
    } yield nodes

  private def waitForSingleLeader(nodes: List[ProtoRaftDRefContext]): Task[Unit] =
    ZIO
      .foreach(nodes)(_.isLeader)
      .flatMap { leaders =>
        if leaders.count(identity) == 1 then ZIO.unit
        else ZIO.sleep(100.millis) *> waitForSingleLeader(nodes)
      }
      .timeoutFail(new RuntimeException("no leader elected"))(10.seconds)
      .unit

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("Proto Raft DRef")(
    test("single node cluster writes and reads") {
      for {
        port   <- freePort
        config  = makeClusterConfig(List(port), 0, "node-0")
        ctx    <- ProtoRaftDRefContext.start(config, 500.millis)
        leader <- ctx.isLeader
        _      <- ctx.setElement("hello", "world".getBytes, None)
        value  <- ctx.getElement("hello")
        _      <- ctx.deleteElement("hello")
        gone   <- ctx.getElement("hello")
      } yield assertTrue(
        leader,
        value.exists(_.sameElements("world".getBytes)),
        gone.isEmpty
      )
    },
    test("three node cluster elects exactly one leader") {
      for {
        nodes   <- startCluster(3)
        leaders <- ZIO.foreach(nodes)(_.isLeader)
      } yield assertTrue(leaders.count(identity) == 1)
    },
    test("write through any node propagates to all nodes") {
      for {
        nodes  <- startCluster(3)
        writer <- ZIO
                    .foreach(nodes)(n => n.isLeader.map(b => (n, b)))
                    .map(_.find(!_._2).map(_._1).getOrElse(nodes.head))
        _      <- writer.setElement("propagated", "yes".getBytes, None)
        _      <- ZIO.sleep(300.millis)
        values <- ZIO.foreach(nodes)(_.getElement("propagated"))
      } yield assertTrue(values.forall(_.exists(_.sameElements("yes".getBytes))))
    },
    test("on_change_stream observes replicated writes") {
      for {
        nodes    <- startCluster(3)
        writer    = nodes.head
        listener  = nodes(1)
        fiber    <- listener.onChangeStream("events").take(1).runCollect.fork
        _        <- ZIO.sleep(50.millis)
        _        <- writer.setElement("events", "observed".getBytes, None)
        events   <- fiber.join.timeout(5.seconds).some
      } yield assertTrue(
        events.length == 1,
        events.head match {
          case io.github.tobia80.dref.SetElement("events", value) => value.sameElements("observed".getBytes)
          case _                                                  => false
        }
      )
    }
  ) @@ TestAspect.withLiveClock @@ TestAspect.sequential
}
