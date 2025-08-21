import zio.cref.CRef.*
import zio.cref.CRef.auto.*
import zio.cref.{CRef, CRefContext, RedisCRefContext, RedisConfig}
import zio.{Console, ZIO, ZIOAppDefault, ZLayer, *}

object Main extends ZIOAppDefault {

  private case class CRefMessage(name: String, message: String)

  private def printReadMessageAndSend(str: String) =
    for {
      cref <- CRef.make[Option[CRefMessage]](None)
      _    <- cref.onChange {
                case Some(CRefMessage(name, message)) =>
                  Console.printLine(s"\n<<< ($name): $message\n").when(name != str)
                case None                             =>
                  ZIO.unit
              }
      _    <- ZIO.iterate("")(_.toLowerCase != "exit") { _ =>
                for {
                  _            <- Console.printLine("Enter a message: ")
                  valueMessage <- Console.readLine
                  crefMessage   = CRefMessage(str, valueMessage)
                  _            <- cref.set(Some(crefMessage)).when(valueMessage.toLowerCase != "exit")
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
        RedisCRefContext.live,
        ZLayer.succeed(
          RedisConfig("giulionatta.duckdns.org", 6379, 0, None, None, None, Some(50.minutes.asFiniteDuration))
        ),
        Scope.default
      )
}
