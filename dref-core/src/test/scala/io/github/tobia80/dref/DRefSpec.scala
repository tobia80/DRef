package io.github.tobia80.dref

import DRef.*
import DRef.auto.*
import zio.test.{assertTrue, Spec, TestAspect, TestClock, TestEnvironment, ZIOSpecDefault}
import zio.{durationInt, Promise, Ref, Scope, ZIO}

object DRefSpec extends QuietZIOSpec {

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("DRef memory")(
    test("should be able to create a DRef") {
      for {
        aRef  <- DRef.make("hi")
        _     <- aRef.set("hello")
        value <- aRef.get
      } yield assertTrue(value == "hello")
    },
    test("should be able to listen for changes") {
      for {
        aRef          <- DRef.make("hi")
        elementsFiber <- aRef.changeStream.interruptAfter(2.seconds).runCollect.fork
        _             <- aRef.set("hello")
        _             <- aRef.set("changed again")
        _             <- TestClock.adjust(2.seconds)
        mutations     <- elementsFiber.join
      } yield assertTrue(mutations == List("hello", "changed again"))
    },
    test("locks should work") {
      for {
        list        <- Ref.make[List[Int]](Nil)
        release200  <- Promise.make[Nothing, Unit]
        fiber       <- (
                       DRef
                         .lock(ManualId("locks-should-work")) {
                           for {
                             _ <- list.update(_ :+ 100)
                             _ <- release200.succeed(())
                             _ <- ZIO.sleep(1.seconds)
                           } yield ()
                         } *>
                         release200.await *>
                         DRef.lock(ManualId("locks-should-work")) {
                           list.update(_ :+ 200) *> ZIO.sleep(1.seconds)
                         }
                     ).fork
        _                 <- release200.await
        valueWithOneLock  <- list.get
        _                 <- fiber.join
        valueWithTwoLocks <- list.get
      } yield assertTrue(valueWithOneLock == List(100) && valueWithTwoLocks == List(100, 200))
    } @@ TestAspect.withLiveClock
  ).provideSome[Scope](DRefContext.local)
}
