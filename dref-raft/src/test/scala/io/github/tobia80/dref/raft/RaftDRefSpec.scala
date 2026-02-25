package io.github.tobia80.dref.raft

import io.github.tobia80.dref.{DRef, LockStolenException, ManualId}
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
    },
    test("stolen lock should throw LockStolenException") {
      for {
        context        <- ZIO.service[RaftDRefContext]
        _              <- context.registerNode("other1")
        _              <- context.registerNode("other2")
        _              <- context.registerNode("other3")
        _              <- context.deleteElement("stolen-lock-test").ignore
        lockFiber      <- DRef
                            .lockWithContext(context, ManualId("stolen-lock-test")) {
                              (ZIO.logInfo("Original lock acquired, sleeping...") *>
                                ZIO.sleep(5.seconds)).as("original-lock-completed")
                            }
                            .fork
        _              <- ZIO.sleep(1.seconds)
        _              <- ZIO.logInfo("Setting different value to simulate theft") *>
                            context.setElement("stolen-lock-test", "stolen-value".getBytes, None)
        originalResult <- lockFiber.join.either
        originalFailed <- originalResult match {
                            case Left(LockStolenException(name, _)) =>
                              ZIO.logInfo(s"LockStolenException caught for lock: $name").as(true)
                            case Left(other) =>
                              ZIO.logInfo(s"Other exception caught: $other").as(false)
                            case Right(_) =>
                              ZIO.logInfo("Lock completed successfully (unexpected)").as(false)
                          }
      } yield assertTrue(originalFailed)
    }
  ).provide(
    RaftDRefContext.live,
    ZLayer.succeed(RaftConfig(8082)),
    IpProvider.local,
    Scope.default
  ) @@ TestAspect.withLiveClock @@ TestAspect.sequential @@ TestAspect.debug
}
