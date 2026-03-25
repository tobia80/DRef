package io.github.tobia80.dref

import DRef.*
import DRef.auto.*
import zio.test.{assertTrue, Spec, TestAspect, TestClock, TestEnvironment, ZIOSpecDefault}
import zio.{durationInt, Chunk, Ref, Scope, ZIO}

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
      } yield assertTrue(mutations == Chunk("hello", "changed again"))
    },
    test("locks should work") {
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
    test("modify should atomically read and update") {
      for {
        aRef   <- DRef.make(10, ManualId("modify-test"))
        result <- aRef.modify(v => (v * 2, v + 1))
        value  <- aRef.get
      } yield assertTrue(result == 20, value == 11)
    } @@ TestAspect.withLiveClock,
    test("update should apply function to current value") {
      for {
        aRef  <- DRef.make(5, ManualId("update-test"))
        _     <- aRef.update(_ + 10)
        value <- aRef.get
      } yield assertTrue(value == 15)
    } @@ TestAspect.withLiveClock,
    test("getAndUpdate should return old value and apply update") {
      for {
        aRef     <- DRef.make(42, ManualId("getAndUpdate-test"))
        oldValue <- aRef.getAndUpdate(_ + 8)
        newValue <- aRef.get
      } yield assertTrue(oldValue == 42, newValue == 50)
    } @@ TestAspect.withLiveClock,
    test("updateAndGet should apply update and return new value") {
      for {
        aRef     <- DRef.make(10, ManualId("updateAndGet-test"))
        newValue <- aRef.updateAndGet(_ * 3)
        current  <- aRef.get
      } yield assertTrue(newValue == 30, current == 30)
    } @@ TestAspect.withLiveClock,
    test("setIfNotExist should only set when absent") {
      for {
        aRef    <- DRef.make("first", ManualId("setIfNotExist-test"))
        result1 <- aRef.setIfNotExist("second")
        value   <- aRef.get
      } yield assertTrue(!result1, value == "first")
    },
    test("modifyZIO should work with effectful functions") {
      for {
        aRef   <- DRef.make(100, ManualId("modifyZIO-test"))
        result <- aRef.modifyZIO(v => ZIO.succeed((v.toString, v + 1)))
        value  <- aRef.get
      } yield assertTrue(result == "100", value == 101)
    } @@ TestAspect.withLiveClock,
    test("updateZIO should work with effectful functions") {
      for {
        aRef  <- DRef.make(7, ManualId("updateZIO-test"))
        _     <- aRef.updateZIO(v => ZIO.succeed(v * 2))
        value <- aRef.get
      } yield assertTrue(value == 14)
    } @@ TestAspect.withLiveClock,
    test("get on non-existent element should fail") {
      for {
        context <- ZIO.service[DRefContext]
        _       <- context.deleteElement("ghost-element")
        ref      = new DRefImpl[String](context)("ghost-element")
        result  <- ref.get.either
      } yield assertTrue(result.isLeft)
    } @@ TestAspect.withLiveClock
  ).provideSome[Scope](DRefContext.local)
}
