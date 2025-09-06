package io.github.tobia80.cref.raft

import io.github.tobia80.cref.CRef
import io.github.tobia80.cref.CRef.*
import io.github.tobia80.cref.raft.RaftCRefContext.RaftCRefContext
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
    }
  ).provide(
    RaftCRefContext.live,
    ZLayer.succeed(RaftConfig(8081)),
    IpProvider.static("127.0.0.1", List("127.0.0.1")),
    Scope.default
  ) @@ TestAspect.withLiveClock
}
