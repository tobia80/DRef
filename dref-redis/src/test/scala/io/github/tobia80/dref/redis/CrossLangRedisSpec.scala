package io.github.tobia80.dref.redis

import io.github.tobia80.dref.{DRef, DRefContext, ManualId}
import io.github.tobia80.dref.DRef.msgpack.*
import zio.*
import zio.schema.Schema
import zio.test.*

/** Writes MsgPack-encoded values to Redis under stable keys so the Rust port
  * can verify it reads the same bytes (`crosslang_redis_tests.rs`).
  *
  * Run as part of the cross-language suite:
  *   ./scripts/crosslang-redis-compat.sh
  */
object CrossLangRedisSpec extends ZIOSpecDefault {

  case class Wrapper(value: String)
  given Schema[Wrapper] = zio.schema.DeriveSchema.gen[Wrapper]

  val CrossLangKey = "dref:compat:crosslang-test"
  val CrossLangLockKey = "dref:compat:crosslang-lock"

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("Cross-language Redis (Scala writer)")(
    test("write MsgPack value for Rust reader") {
      for {
        context <- ZIO.service[DRefContext]
        _       <- context.deleteElement(CrossLangKey)
        aRef    <- DRef.make(Wrapper("from-scala"), ManualId(CrossLangKey))
        _       <- aRef.set(Wrapper("scala-updated"))
        raw     <- context.getElement(CrossLangKey)
      } yield assertTrue(raw.isDefined)
    },
    test("write lock token bytes for Rust stolen-lock detection") {
      for {
        context <- ZIO.service[DRefContext]
        _       <- context.deleteElement(CrossLangLockKey)
        // 8-byte big-endian lock token written by Scala, read by Rust.
        token    = io.github.tobia80.dref.LockValue.toBytes(0x1234567890abcdefL)
        acquired <- context.setElementIfNotExist(CrossLangLockKey, token, Some(30.seconds))
      } yield assertTrue(acquired)
    }
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
  )
}
