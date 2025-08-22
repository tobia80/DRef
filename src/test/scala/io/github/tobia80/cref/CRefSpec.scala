package io.github.tobia80.cref

import CRef.*
import RedisCRefSpec.test
import zio.test.{assertTrue, Spec, TestAspect, TestClock, TestEnvironment, ZIOSpecDefault}
import zio.{durationInt, Ref, Scope, ZIO}

object CRefSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("CRef")(
    test("should be able to create a CRef") {
      for {
        aRef  <- CRef.make("hi")
        _     <- aRef.set("hello")
        value <- aRef.get
      } yield assertTrue(value == "hello")
    },
    test("should be able to listen for changes") {
      for {
        aRef          <- CRef.make("hi")
        elementsFiber <- aRef.changeStream.interruptAfter(2.seconds).runCollect.fork
        _             <- aRef.set("hello")
        _             <- aRef.set("changed again")
        _             <- TestClock.adjust(2.seconds)
        mutations     <- elementsFiber.join
      } yield assertTrue(mutations == List("hello", "changed again"))
    },
    test("locks should work") { // two fibers trying to lock the same resource, waiting 1 seconds, queue is written and sleep, when 2 seconds pass, queue is 2 elements
      for {
        list              <- Ref.make[List[Int]](Nil)
        fiber             <- ZIO
                               .foreachParDiscard(List(100, 200)) { id =>
                                 (ZIO.logInfo(s"Starting $id") *> CRef
                                   .lock() {
                                     ZIO.logInfo(s"Executing $id") *> list.update(_ :+ id) *> ZIO
                                       .sleep(1.seconds)
                                   })
                                   .delay(id.millis)
                               }
                               .fork
        _                 <- ZIO.sleep(500.millis)
        valueWithOneLock  <- list.get
        _                 <- fiber.join
        valueWithTwoLocks <- list.get
      } yield assertTrue(valueWithOneLock == List(100) && valueWithTwoLocks == List(100, 200))
    } @@ TestAspect.withLiveClock
  ).provide(CRefContext.local)
}
