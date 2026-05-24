package io.github.tobia80.dref

import zio.*
import zio.test.{TestEnvironment, ZIOSpecDefault, testEnvironment}

/** ZIO test base that suppresses INFO/DEBUG application logs during test runs. */
trait QuietZIOSpec extends ZIOSpecDefault {
  private val quietLogging: ZLayer[Any, Nothing, Unit] =
    Runtime.removeDefaultLoggers >>> Runtime.addLogger(
      ZLogger.default.filterLogLevel(_ >= LogLevel.Warning)
    )

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] =
    quietLogging >>> testEnvironment
}
