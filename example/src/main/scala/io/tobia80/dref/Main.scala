package io.tobia80.dref

import io.github.tobia80.dref.DRef
import io.github.tobia80.dref.DRef.*
import io.github.tobia80.dref.DRef.auto.*
import io.github.tobia80.dref.raft.{IpProvider, RaftConfig, RaftDRefContext}
import zio.*

import scala.Console.{BLUE, CYAN, RESET}

object Main extends ZIOAppDefault {

//  override val bootstrap = Runtime.removeDefaultLoggers

  private case class DRefMessage(name: String, message: String)

  private val DefaultPort     = 8082
  private val NodesAddresses  = "DREF_NODE_ADDRESSES"
  private val NodesServices   = "DREF_NODE_SERVICES"
  private val PortEnvironment = "DREF_PORT"

  private val raftConfigLayer: ZLayer[Any, Nothing, RaftConfig] = {
    val port = sys.env
      .get(PortEnvironment)
      .flatMap(_.toIntOption)
      .getOrElse(DefaultPort)

    ZLayer.succeed(RaftConfig(port))
  }

  private val ipProviderLayer: ZLayer[Any, Nothing, IpProvider] = {
    def parse(name: String): Option[Seq[String]] =
      sys.env
        .get(name)
        .map(_.split(',').map(_.trim).filter(_.nonEmpty).toSeq)
        .filter(_.nonEmpty)

    parse(NodesAddresses)
      .map(addresses => IpProvider.static(addresses*))
      .orElse(parse(NodesServices).map(services => IpProvider.dnsBased(services*)))
      .getOrElse(IpProvider.local)
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
    Console
      .print("Please enter your name: ")
  } *>
    Console.readLine
      .flatMap { name =>
        Console.printLine(
          s"Hello, $name! Every message you type will be echoed back to you and to all subscribers. Type 'exit' to quit."
        )
          *> printReadMessageAndSend(name)

      }
      .provide(
        RaftDRefContext.live,
        raftConfigLayer,
        ipProviderLayer,
        Scope.default
      )
}
