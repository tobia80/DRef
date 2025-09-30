package io.github.tobia80.dref.raft

import io.github.tobia80.dref.DRef
import io.github.tobia80.dref.DRef.*
import io.github.tobia80.dref.DRef.auto.*
import io.github.tobia80.dref.raft.RaftDRefContext.RaftDRefContext
import zio.*
import zio.test.{assertTrue, Spec, TestAspect, TestEnvironment, ZIOSpecDefault}

object RaftDRefSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("DRef Raft")(
    test("should be able to create a DRef") {
      for {
        context <- ZIO.service[RaftDRefContext]
        _       <- context.registerNode("other1")
        _       <- context.registerNode("other2")
        _       <- context.registerNode("other3")
        aRef    <- DRef.make("hi")
        _       <- aRef.set("hello")
        value   <- aRef.get
      } yield assertTrue(value == "hello")
    },
    test("should be able to listen for changes") {
      for {
        context       <- ZIO.service[RaftDRefContext]
        _             <- context.registerNode("other1")
        _             <- context.registerNode("other2")
        _             <- context.registerNode("other3")
        aRef          <- DRef.make("hi")
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
                                 (ZIO.logInfo(s"Starting $id") *> DRef
                                   .lock() {
                                     ZIO.logInfo(s"Executing $id") *> list.update(_ :+ id) *> ZIO
                                       .sleep(1.seconds)
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
    RaftDRefContext.live,
    ZLayer.succeed(RaftConfig(8082)),
    IpProvider.local,
    Scope.default
  ) @@ TestAspect.withLiveClock @@ TestAspect.sequential @@ TestAspect.debug
}
