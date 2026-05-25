package io.tobia80.dref

import io.github.tobia80.dref.DRef.msgpack.{*, given}
import io.github.tobia80.dref.raft.IpProvider
import io.github.tobia80.dref.raft.proto.{NodeEndpoint, ProtoRaftConfig, ProtoRaftDRefContext}
import io.github.tobia80.dref.{DRef, DRefContext, ManualId}
import zio.*

import scala.Console.{BLUE, CYAN, GREEN, RESET, YELLOW}

/** Cross-language interop demo: a Scala Raft node that joins the same cluster as the Rust `interop-node` binary under
  * `rust/interop-node`.
  *
  * Both sides agree on:
  *
  *   - the protobuf services from `proto/dref_consensus.proto` and `proto/dref.proto` (Raft replication is
  *     wire-compatible);
  *   - the MsgPack named-map encoding for `DRefMessage` (zio-schema-msg-pack on Scala produces the same bytes as
  *     rmp-serde on Rust for a struct of two `String` fields);
  *   - the manual id `interop-chat-message`, so the key is looked up in the same Raft log slot regardless of language.
  *
  * Driven by the standard `DREF_*` env vars (see `docker-compose.interop.yml`).
  */
object InteropMain extends ZIOAppDefault {

  /** Field names + order must stay aligned with the Rust definition. */
  private case class DRefMessage(name: String, message: String)

  private val SharedKey = "interop-chat-message"
  private val DefaultPort = 8082
  private val LeaderWait = 60.seconds

  private val ipProviderLayer: ZLayer[Any, Throwable, IpProvider] = {
    def parse(name: String): Option[Seq[String]] =
      sys.env
        .get(name)
        .map(_.split(',').map(_.trim).filter(_.nonEmpty).toSeq)
        .filter(_.nonEmpty)

    (for {
      service   <- sys.env.get("DREF_K8S_SERVICE")
      namespace <- sys.env.get("DREF_K8S_NAMESPACE")
    } yield IpProvider.k8s(service, namespace))
      .orElse(parse("DREF_NODE_ADDRESSES").map(addresses => IpProvider.static(addresses*)))
      .orElse(parse("DREF_NODE_SERVICES").map(services => IpProvider.dnsBased(services*)))
      .getOrElse(IpProvider.local)
  }

  /** Builds a [[ProtoRaftDRefContext]] (the gRPC-based engine that is wire-compatible with the Rust `interop-node`
    * crate) by resolving peer addresses through the configured [[IpProvider]] before starting the consensus stack.
    */
  private val raftContextLayer: ZLayer[IpProvider, Throwable, DRefContext] =
    ZLayer.scoped {
      for {
        ipProvider <- ZIO.service[IpProvider]
        port        = sys.env.get("DREF_PORT").flatMap(_.toIntOption).getOrElse(DefaultPort)
        peerIps    <- ipProvider.findNodeAddresses()
        myIp       <- ipProvider.findMyAddress()
        bindAddress = s"$myIp:$port"
        // Each discovered IP becomes a placeholder endpoint keyed by the IP itself;
        // ProtoRaftDRefContext.start strips the one matching our bind and re-adds it
        // under the real nodeId so we don't appear in our own peer list.
        endpoints   = peerIps.map(ip => NodeEndpoint(ip, s"$ip:$port")).toList
        config      = ProtoRaftConfig(
                        port = port,
                        bindAddress = Some(bindAddress),
                        initialEndpoints = endpoints
                      )
        ctx        <- ProtoRaftDRefContext.start(config, LeaderWait)
      } yield ctx: DRefContext
    }

  private val nodeLabel: String = {
    val role = sys.env.getOrElse("DREF_NODE_LABEL", "scala")
    val host = sys.env.get("HOSTNAME").getOrElse("unknown")
    s"$role/$host"
  }

  private def chat(displayName: String) =
    for {
      dref <- DRef.make[DRefMessage](DRefMessage("", ""), ManualId(SharedKey))
      _    <- Console.printLine(
                s"$GREEN[$nodeLabel] joined cluster as '$displayName'. Type messages, 'exit' to quit.$RESET"
              )
      _    <- dref.onChange { msg =>
                Console
                  .printLine(s"$CYAN<<< (${msg.name}) ${msg.message} [seen by $nodeLabel]$RESET")
                  .when(msg.name.nonEmpty && msg.name != displayName)
              }
      _    <- ZIO.iterate("")(_.toLowerCase != "exit") { _ =>
                for {
                  _       <- Console.print(s"$BLUE[$displayName] message ('exit' to quit): $RESET")
                  message <- Console.readLine
                  trimmed  = Option(message).map(_.trim).getOrElse("")
                  _       <- dref
                               .set(DRefMessage(displayName, trimmed))
                               .when(trimmed.nonEmpty && trimmed.toLowerCase != "exit")
                } yield trimmed
              }
    } yield ()

  /** Non-interactive demo: broadcast a timestamped message every few seconds
    * and log everything observed. Triggered by `DREF_AUTO_NAME` so the
    * default compose setup can show the cluster working without `docker
    * attach`. Logs from `docker compose logs -f` should show messages from
    * every other node crossing the language boundary. */
  private def autoDemo(displayName: String) =
    for {
      dref <- DRef.make[DRefMessage](DRefMessage("", ""), ManualId(SharedKey))
      _    <- Console.printLine(
                s"$GREEN[$nodeLabel] auto-demo joined as '$displayName' — broadcasting every 3s.$RESET"
              )
      _    <- dref
                .onChange { msg =>
                  Console
                    .printLine(s"$CYAN<<< (${msg.name}) ${msg.message} [seen by $nodeLabel]$RESET")
                    .when(msg.name.nonEmpty && msg.name != displayName)
                }
      _    <- (for {
                now <- Clock.currentDateTime
                msg  = s"hello @${now.toLocalTime}"
                _   <- Console.printLine(s"$BLUE[$displayName] >>> $msg$RESET")
                _   <- dref.set(DRefMessage(displayName, msg))
              } yield ()).schedule(Schedule.spaced(3.seconds)).forever
    } yield ()

  override def run = {
    val program =
      for {
        // Force the Raft engine to materialise BEFORE blocking on stdin —
        // otherwise the Scala node would only start serving consensus RPCs
        // once a human types a display name, which never happens in the
        // compose setup until someone `docker attach`es.
        ctx     <- ZIO.service[DRefContext]
        _       <- Console.printLine(s"$GREEN[$nodeLabel] Raft node ready (DRefContext=$ctx).$RESET")
        // Suffix the auto-name with a short hostname so each replica is
        // distinguishable in the chat log (otherwise scala-node-1 and
        // scala-node-2 both publish as "scala-auto" and the receiver
        // filter hides every Scala-to-Scala message).
        hostSuffix = sys.env.get("HOSTNAME").map(_.take(6)).filter(_.nonEmpty)
        autoOpt  = sys.env.get("DREF_AUTO_NAME").map(_.trim).filter(_.nonEmpty).map { base =>
                     hostSuffix.fold(base)(h => s"$base-$h")
                   }
        _       <- autoOpt match {
                     case Some(name) => autoDemo(name)
                     case None       =>
                       for {
                         _     <- Console.print(s"$YELLOW[$nodeLabel] enter your display name: $RESET")
                         name0 <- Console.readLine
                         name   = Option(name0).map(_.trim).filter(_.nonEmpty).getOrElse(nodeLabel)
                         _     <- chat(name)
                       } yield ()
                   }
      } yield ()

    program.provide(
      raftContextLayer,
      ipProviderLayer
    )
  }
}
