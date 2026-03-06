package io.github.tobia80.dref.redis

import io.lettuce.core.{RedisCommandExecutionException, RedisCommandTimeoutException, RedisConnectionException}
import zio.*
import zio.test.{assertTrue, Spec, TestAspect, TestEnvironment, ZIOSpecDefault}

import java.io.IOException
import java.net.SocketException
import java.util.concurrent.CompletionException

object RedisClientSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("RedisClient")(
    isTransientSuite,
    retryBehaviorSuite
  )

  private val isTransientSuite = suite("isTransient")(
    test("RedisCommandTimeoutException is transient") {
      assertTrue(RedisClient.isTransient(new RedisCommandTimeoutException("timed out")))
    },
    test("RedisCommandExecutionException is not transient") {
      assertTrue(!RedisClient.isTransient(new RedisCommandExecutionException("WRONGTYPE")))
    },
    test("RedisConnectionException is transient") {
      assertTrue(RedisClient.isTransient(new RedisConnectionException("Connection refused")))
    },
    test("IOException is transient") {
      assertTrue(RedisClient.isTransient(new IOException("broken pipe")))
    },
    test("SocketException is transient") {
      assertTrue(RedisClient.isTransient(new SocketException("Connection reset")))
    },
    test("CompletionException wrapping transient error is transient") {
      val wrapped = new CompletionException(new IOException("connection lost"))
      assertTrue(RedisClient.isTransient(wrapped))
    },
    test("CompletionException wrapping non-transient error is not transient") {
      val wrapped = new CompletionException(new RedisCommandExecutionException("ERR syntax"))
      assertTrue(!RedisClient.isTransient(wrapped))
    },
    test("CompletionException with null cause is not transient") {
      assertTrue(!RedisClient.isTransient(new CompletionException(null)))
    },
    test("arbitrary exception is not transient") {
      assertTrue(!RedisClient.isTransient(new IllegalArgumentException("bad arg")))
    }
  )

  private val retryBehaviorSuite = suite("retry behavior")(
    test("transient errors are retried") {
      val config   = RetryConfig(maxRetries = 2, initialDelay = 1.millis, maxDelay = 10.millis)
      val schedule = RedisClient.retrySchedule(config)
      for {
        counter <- Ref.make(0)
        result  <- counter
                     .updateAndGet(_ + 1)
                     .flatMap { n =>
                       if (n <= 2) ZIO.fail(new RedisConnectionException("Connection refused"))
                       else ZIO.succeed(n)
                     }
                     .retry(schedule)
      } yield assertTrue(result == 3)
    },
    test("non-transient errors fail immediately without retrying") {
      val config   = RetryConfig(maxRetries = 3, initialDelay = 1.millis, maxDelay = 10.millis)
      val schedule = RedisClient.retrySchedule(config)
      for {
        counter <- Ref.make(0)
        result  <- counter
                     .updateAndGet(_ + 1)
                     .flatMap(_ => ZIO.fail(new RedisCommandExecutionException("WRONGTYPE")))
                     .retry(schedule)
                     .either
        count   <- counter.get
      } yield assertTrue(result.isLeft && count == 1)
    },
    test("retries stop after maxRetries even for transient errors") {
      val config   = RetryConfig(maxRetries = 2, initialDelay = 1.millis, maxDelay = 10.millis)
      val schedule = RedisClient.retrySchedule(config)
      for {
        counter <- Ref.make(0)
        result  <- counter
                     .updateAndGet(_ + 1)
                     .flatMap(_ => ZIO.fail(new IOException("always fails")))
                     .retry(schedule)
                     .either
        count   <- counter.get
      } yield assertTrue(result.isLeft && count == 3) // 1 initial + 2 retries
    }
  ) @@ TestAspect.withLiveClock
}
