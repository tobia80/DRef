package io.tobia80.dref

import io.github.tobia80.dref.DRef.msgpack.{*, given}
import io.github.tobia80.dref.raft.{IpProvider, RaftConfig, RaftDRefContext}
import io.github.tobia80.dref.{DRef, ManualId}
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

  private val raftConfigLayer: ZLayer[Any, Nothing, RaftConfig] = {
    val port = sys.env.get("DREF_PORT").flatMap(_.toIntOption).getOrElse(DefaultPort)
    ZLayer.succeed(RaftConfig(port))
  }

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

  override def run = {
    val program =
      for {
        _     <- Console.print(s"$YELLOW[$nodeLabel] enter your display name: $RESET")
        name0 <- Console.readLine
        name   = Option(name0).map(_.trim).filter(_.nonEmpty).getOrElse(nodeLabel)
        _     <- chat(name)
      } yield ()

    program.provide(
      RaftDRefContext.live,
      raftConfigLayer,
      ipProviderLayer,
      Scope.default
    )
  }
}
