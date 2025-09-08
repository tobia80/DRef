package io.github.tobia80.cref.raft

import io.github.tobia80.cref.CRef
import io.github.tobia80.cref.CRef.*
import io.github.tobia80.cref.raft.RaftCRefContext.RaftCRefContext
import io.github.tobia80.cref.redis.RedisCRefSpec.test
import zio.*
import zio.test.{assertTrue, Spec, TestAspect, TestEnvironment, ZIOSpecDefault}

object RaftCRefSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("CRef Raft")(
    test("should be able to create a CRef") {
      for {
        context <- ZIO.service[RaftCRefContext]
        _       <- context.registerNode("other1")
        _       <- context.registerNode("other2")
        _       <- context.registerNode("other3")
        aRef    <- CRef.make("hi")
        _       <- aRef.set("hello")
        value   <- aRef.get
      } yield assertTrue(value == "hello")
    },
    test("should be able to listen for changes") {
      for {
        context       <- ZIO.service[RaftCRefContext]
        _             <- context.registerNode("other1")
        _             <- context.registerNode("other2")
        _             <- context.registerNode("other3")
        aRef          <- CRef.make("hi")
        _             <- ZIO.sleep(50.millis) // wait few milliseconds to ensure the change is not caught
        elementsFiber <- aRef.changeStream.interruptAfter(2.seconds).runCollect.forkScoped
        _             <- ZIO.sleep(50.millis)
        _             <- aRef.set("hello")
        _             <- aRef.set("changed again")
        mutations     <- elementsFiber.join
      } yield assertTrue(mutations == Chunk("hello", "changed again"))
    },
    test("locks should work") { // two fibers trying to lock the same resource, waiting 1 seconds, queue is written and sleep, when 2 seconds pass, queue is 2 elements
      for {
        list              <- Ref.make[List[Int]](Nil)
        fiber             <- ZIO
                               .foreachParDiscard(List(100, 200)) { id =>
                                 (ZIO.logInfo(s"Starting $id") *> CRef
                                   .lock() {
                                     ZIO.logInfo(s"Executing $id") *> list.update(_ :+ id) *> ZIO
                                       .sleep(2.seconds)
                                   })
                                   .delay(id.millis)
                               }
                               .fork
        _                 <- ZIO.sleep(1000.millis)
        valueWithOneLock  <- list.get
        _                 <- fiber.join
        valueWithTwoLocks <- list.get
      } yield assertTrue(valueWithOneLock == List(100) && valueWithTwoLocks == List(100, 200))
    }
  ).provide(
    RaftCRefContext.live,
    ZLayer.succeed(RaftConfig(8082)),
    IpProvider.static("127.0.0.1", List("127.0.0.1")),
    Scope.default
  ) @@ TestAspect.withLiveClock @@ TestAspect.sequential @@ TestAspect.debug
}
