package io.tobia80.dref

import io.github.tobia80.dref.DRef.msgpack.{*, given}
import io.github.tobia80.dref.raft.IpProvider
import io.github.tobia80.dref.raft.proto.{ProtoRaftConfig, ProtoRaftDRefContext}
import io.github.tobia80.dref.{DRef, DRefContext, ManualId}
import zio.*

/** Scala side of the cross-language Raft demo (pairs with `rust/interop-node`). */
object InteropMain extends ZIOAppDefault {

  private case class ChatMsg(name: String, message: String)

  private val sharedKey = "interop-chat-message"
  private val port = sys.env.get("DREF_PORT").flatMap(_.toIntOption).getOrElse(8082)

  private val label =
    s"${sys.env.getOrElse("DREF_NODE_LABEL", "scala")}/${sys.env.getOrElse("HOSTNAME", "local")}"

  private def envList(name: String): Option[Seq[String]] =
    sys.env
      .get(name)
      .map(_.split(',').map(_.trim).filter(_.nonEmpty).toSeq)
      .filter(_.nonEmpty)

  private val ipProviderLayer: ZLayer[Any, Throwable, IpProvider] =
    (for {
      service   <- sys.env.get("DREF_K8S_SERVICE")
      namespace <- sys.env.get("DREF_K8S_NAMESPACE")
    } yield IpProvider.k8s(service, namespace))
      .orElse(envList("DREF_NODE_ADDRESSES").map(IpProvider.static(_*)))
      .orElse(envList("DREF_NODE_SERVICES").map(IpProvider.dnsBased(_*)))
      .getOrElse(IpProvider.local)

  private val raftContextLayer =
    ZLayer.scoped {
      for {
        ipProvider <- ZIO.service[IpProvider]
        myIp       <- ipProvider.findMyAddress()
        config      = ProtoRaftConfig(port = port, bindAddress = Some(s"$myIp:$port"))
        ctx        <- ProtoRaftDRefContext.startWithAddressPolling(config, ipProvider, 60.seconds)
      } yield ctx: DRefContext
    }

  private def logRemote(me: String)(msg: ChatMsg) =
    Console
      .printLine(s"\u001b[36m<<< (${msg.name}) ${msg.message} [$label]\u001b[0m")
      .when(msg.name.nonEmpty && msg.name != me)

  private def resolveDisplayName: Task[String] =
    sys.env.get("DREF_AUTO_NAME").map(_.trim).filter(_.nonEmpty) match {
      case Some(base) =>
        ZIO.succeed(sys.env.get("HOSTNAME").filter(_.nonEmpty).fold(base)(h => s"$base-${h.take(6)}"))
      case None       =>
        Console.print(s"\u001b[33m[$label] display name: \u001b[0m") *>
          Console.readLine.map(line => Option(line).map(_.trim).filter(_.nonEmpty).getOrElse(label))
    }

  private def broadcastSchedule: (Duration, Duration) = {
    val secs = sys.env.get("DREF_AUTO_INTERVAL_SECS").flatMap(_.toIntOption).filter(_ > 0).getOrElse(3)
    val id = sys.env.get("HOSTNAME").orElse(sys.env.get("DREF_NODE_LABEL")).getOrElse("node")
    val hash = id.foldLeft(5381L)((h, c) => (h << 5) + h + c.toLong)
    (math.abs(hash) % (secs * 1000L)).millis -> secs.seconds
  }

  private def demo(displayName: String, auto: Boolean) =
    for {
      dref <- DRef.make[ChatMsg](ChatMsg("", ""), ManualId(sharedKey))
      _    <- Console.printLine(
                if auto then s"\u001b[32m[$label] auto-demo as '$displayName'\u001b[0m"
                else s"\u001b[32m[$label] chat as '$displayName' (type exit to quit)\u001b[0m"
              )
      _    <- dref.onChange(logRemote(displayName))
      _    <-
        if auto then {
          val (stagger, every) = broadcastSchedule
          ZIO.sleep(stagger) *>
            (Clock.currentDateTime
              .flatMap { now =>
                val msg = s"hello @${now.toLocalTime}"
                Console.printLine(s"\u001b[34m[$displayName] >>> $msg\u001b[0m") *>
                  dref.set(ChatMsg(displayName, msg))
              })
              .schedule(Schedule.spaced(every))
              .forever
        } else {
          ZIO.iterate("")(_.toLowerCase != "exit") { _ =>
            Console.print(s"\u001b[34m[$displayName] > \u001b[0m") *>
              Console.readLine.flatMap { line =>
                val text = Option(line).map(_.trim).getOrElse("")
                dref.set(ChatMsg(displayName, text)).when(text.nonEmpty && text.toLowerCase != "exit") *> ZIO.succeed(
                  text
                )
              }
          }
        }
    } yield ()

  override def run =
    (for {
      _    <- ZIO.service[DRefContext]
      _    <- Console.printLine(s"\u001b[32m[$label] Raft ready on :$port\u001b[0m")
      name <- resolveDisplayName
      auto  = sys.env.get("DREF_AUTO_NAME").exists(_.trim.nonEmpty)
      _    <- demo(name, auto)
    } yield ()).provide(ipProviderLayer, raftContextLayer)
}
