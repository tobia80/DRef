package io.github.tobia80.dref

import DRef.*
import DRef.auto.*
import zio.test.{assertTrue, Spec, TestAspect, TestClock, TestEnvironment, ZIOSpecDefault}
import zio.{durationInt, Promise, Ref, Scope, ZIO}

object DRefSpec extends ZIOSpecDefault {

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
        _                 <- ZIO.sleep(500.millis)
        valueWithOneLock  <- list.get
        _                 <- fiber.join
        valueWithTwoLocks <- list.get
      } yield assertTrue(valueWithOneLock == List(100) && valueWithTwoLocks == List(100, 200))
    } @@ TestAspect.withLiveClock,
    test("lock release should not remove a lock reacquired by another owner") {
      val lockName = "lock:test-ownership"
      val lockId = ManualId(lockName)
      for {
        context        <- ZIO.service[DRefContext]
        firstAcquired  <- Promise.make[Nothing, Unit]
        releaseFirst   <- Promise.make[Nothing, Unit]
        secondAcquired <- Promise.make[Nothing, Unit]
        releaseSecond  <- Promise.make[Nothing, Unit]
        firstFiber     <- DRef
                            .lockWithContext(context, lockId) {
                              firstAcquired.succeed(()) *> releaseFirst.await
                            }
                            .fork
        _              <- firstAcquired.await
        _              <- context.deleteElement(lockName)
        secondFiber    <- DRef
                            .lockWithContext(context, lockId) {
                              secondAcquired.succeed(()) *> releaseSecond.await
                            }
                            .fork
        _              <- secondAcquired.await
        _              <- firstFiber.interrupt
        _              <- firstFiber.await.ignore
        stored         <- context.getElement(lockName)
        _              <- releaseSecond.succeed(())
        _              <- secondFiber.join
      } yield assertTrue(stored.isDefined)
    }
  ).provideSome[Scope](DRefContext.local)
}
