package io.tobia80.dref

import io.github.tobia80.dref.DRef
import io.github.tobia80.dref.DRef.*
import io.github.tobia80.dref.DRef.auto.*
import io.github.tobia80.dref.raft.IpProvider
import io.github.tobia80.dref.raft.proto.{NodeEndpoint, ProtoRaftConfig, ProtoRaftDRefContext}
import io.github.tobia80.dref.DRefContext
import zio.*

import scala.Console.{BLUE, CYAN, RESET}

object Main extends ZIOAppDefault {

//  override val bootstrap = Runtime.removeDefaultLoggers

  private case class DRefMessage(name: String, message: String)

  private val DefaultPort = 8082
  private val LeaderWait = 60.seconds
  private val NodesAddresses = "DREF_NODE_ADDRESSES"
  private val NodesServices = "DREF_NODE_SERVICES"
  private val PortEnvironment = "DREF_PORT"
  private val K8sService = "DREF_K8S_SERVICE"
  private val K8sNamespace = "DREF_K8S_NAMESPACE"

  private val ipProviderLayer: ZLayer[Any, Throwable, IpProvider] = {
    def parse(name: String): Option[Seq[String]] =
      sys.env
        .get(name)
        .map(_.split(',').map(_.trim).filter(_.nonEmpty).toSeq)
        .filter(_.nonEmpty)

    (for {
      service   <- sys.env.get(K8sService)
      namespace <- sys.env.get(K8sNamespace)
    } yield IpProvider.k8s(service, namespace))
      .orElse(parse(NodesAddresses).map(addresses => IpProvider.static(addresses*)))
      .orElse(parse(NodesServices).map(services => IpProvider.dnsBased(services*)))
      .getOrElse(IpProvider.local)
  }

  private val raftContextLayer: ZLayer[IpProvider, Throwable, DRefContext] =
    ZLayer.scoped {
      for {
        ipProvider <- ZIO.service[IpProvider]
        port        = sys.env.get(PortEnvironment).flatMap(_.toIntOption).getOrElse(DefaultPort)
        peerIps    <- ipProvider.findNodeAddresses()
        myIp       <- ipProvider.findMyAddress()
        bindAddress = s"$myIp:$port"
        endpoints   = peerIps.map(ip => NodeEndpoint(ip, s"$ip:$port")).toList
        config      = ProtoRaftConfig(
                        port = port,
                        bindAddress = Some(bindAddress),
                        initialEndpoints = endpoints
                      )
        ctx        <- ProtoRaftDRefContext.start(config, LeaderWait)
      } yield ctx: DRefContext
    }

  private def printReadMessageAndSend(str: String) =
    for {
      dref <- DRef.make[Option[DRefMessage]](None)
      _    <- dref.onChange {
                case Some(DRefMessage(name, message)) =>
                  Console.printLine(s"\n$CYAN<<< ($name): $message$RESET\n").when(name != str)
                case None                             =>
                  ZIO.unit
              }
      _    <- ZIO.iterate("")(_.toLowerCase != "exit") { _ =>
                for {
                  _            <- Console.printLine(BLUE + s" Enter a message: $RESET")
                  valueMessage <- Console.readLine
                  drefMessage   = DRefMessage(str, valueMessage)
                  _            <- dref.set(Some(drefMessage)).when(valueMessage.toLowerCase != "exit")
                } yield valueMessage
              }
    } yield ()

  override def run = {
    val program =
      for {
        // Materialise the Raft engine before blocking on stdin so consensus
        // starts immediately even when no one is attached to the container.
        _    <- ZIO.service[DRefContext]
        _    <- Console.print("Please enter your name: ")
        name <- Console.readLine
        _    <-
          Console.printLine(
            s"Hello, $name! Every message you type will be echoed back to you and to all subscribers. Type 'exit' to quit."
          )
        _    <- printReadMessageAndSend(name)
      } yield ()

    program.provide(raftContextLayer, ipProviderLayer)
  }
}
