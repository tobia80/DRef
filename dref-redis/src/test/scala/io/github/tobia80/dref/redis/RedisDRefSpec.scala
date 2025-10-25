package io.github.tobia80.dref.redis

import io.github.tobia80.dref.{DRef, DRefContext, LockStolenException, ManualId}
import io.github.tobia80.dref.DRef.*
import io.github.tobia80.dref.DRef.auto.*
import zio.*
import zio.test.{assertTrue, Spec, TestAspect, TestEnvironment, ZIOSpecDefault}

object RedisDRefSpec extends ZIOSpecDefault {

  case class Wrapper(value: String)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("DRef Redis")(
    test("should be able to create a DRef") {
      for {
        aRef  <- DRef.make(Wrapper("hi"))
        _     <- aRef.set(Wrapper("hello"))
        value <- aRef.get
      } yield assertTrue(value == Wrapper("hello"))
    },
    test("should be able to listen for changes") {
      for {
        aRef          <- DRef.make("hi")
        _             <- ZIO.sleep(50.millis) // wait few milliseconds to ensure the change is not caught
        elementsFiber <- aRef.changeStream.interruptAfter(2.seconds).runCollect.forkScoped
        _             <- ZIO.sleep(50.millis)
        _             <- aRef.set("hello")
        _             <- aRef.set("changed again")
        mutations     <- elementsFiber.join
      } yield assertTrue(mutations == Chunk("hello", "changed again"))
    } @@ TestAspect.withLiveClock,
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
    test("stolen lock should throw LockStolenException") {
      for {
        context         <- ZIO.service[DRefContext]
        lockFiber       <- DRef
                             .lockWithContext(context, ManualId("stolen-lock-test")) {
                               (ZIO.logInfo("Original lock acquired, sleeping...") *>
                                 ZIO.sleep(3.seconds)).as("original-lock-completed")
                             }
                             .fork
        // Wait for the lock to be acquired and the stolen lock detection to start
        _               <- ZIO.sleep(500.millis)
        // Manually set a different value for the same key to simulate it being stolen
        _               <- ZIO.logInfo("Setting different value to simulate theft") *>
                             context.setElement("stolen-lock-test", "stolen-value".getBytes, None)
        // Wait for the original lock to fail and get the result
        originalResult  <- lockFiber.join.either
        _               <- ZIO.logInfo(s"Original lock result: $originalResult")
        // Verify that another client can now acquire the lock
        newLockResult   <- DRef
                             .lockWithContext(context, ManualId("stolen-lock-test")) {
                               ZIO.succeed("new-lock-acquired")
                             }
        // Original lock should have failed with LockStolenException
        originalFailed  <- originalResult match {
                             case Left(LockStolenException(name, _)) =>
                               ZIO.logInfo(s"LockStolenException caught for lock: $name").as(true)
                             case Left(other)                        =>
                               ZIO.logInfo(s"Other exception caught: $other").as(false)
                             case Right(_)                           =>
                               ZIO.logInfo("Lock completed successfully (unexpected)").as(false)
                           }
        // New lock should succeed
        newLockSucceeded = newLockResult == "new-lock-acquired"
      } yield assertTrue(originalFailed && newLockSucceeded)
    } @@ TestAspect.withLiveClock
  ).provide(
    RedisDRefContext.live,
    Scope.default,
    ZLayer.succeed(
      RedisConfig(
        host = "localhost",
        port = 6379,
        database = 0,
        username = None,
        password = None,
        caCert = None,
        ttl = Some(5.seconds)
      )
    )
  ) @@ TestAspect.debug
}
