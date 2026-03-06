package io.github.tobia80.dref.redis

import io.lettuce.core
import io.lettuce.core.{RedisCommandExecutionException, RedisCommandTimeoutException, RedisException, SetArgs}
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands
import io.lettuce.core.support.{AsyncConnectionPoolSupport, BoundedPoolConfig}
import io.lettuce.core.support.AsyncPool
import reactor.core.publisher.Mono
import zio.stream.ZStream
import zio.{Chunk, Scope, Task, ZIO}
import zio.*

trait RedisClient {

  def set(name: Chunk[Byte], value: Chunk[Byte], ttl: Option[Duration]): Task[String]

  def setNx(name: Chunk[Byte], value: Chunk[Byte], ttl: Option[Duration]): Task[Boolean]

  def get(name: Chunk[Byte]): Task[Option[Chunk[Byte]]]

  def del(name: Chunk[Byte]): Task[Long]

  def expire(name: Chunk[Byte], ttl: Duration): Task[Boolean]

  def publish(channel: Chunk[Byte], message: Chunk[Byte]): Task[Long]

  def subscribe(channel: Chunk[Byte]): ZStream[Any, Throwable, Chunk[Byte]]

}

object RedisClient {

  private[redis] def isTransient(e: Throwable): Boolean = e match {
    case _: RedisCommandTimeoutException                                    => true
    case _: RedisCommandExecutionException                                  => false
    case _: RedisException                                                  => true
    case _: java.io.IOException                                             => true
    case c: java.util.concurrent.CompletionException if c.getCause != null  => isTransient(c.getCause)
    case _                                                                  => false
  }

  private[redis] def retrySchedule(config: RetryConfig): Schedule[Any, Throwable, Any] =
    (Schedule.recurWhile[Throwable](isTransient) &&
      (Schedule.exponential(config.initialDelay, 2.0) || Schedule.fixed(config.maxDelay)) &&
      Schedule.recurs(config.maxRetries)).unit

  def make(config: RedisConfig): ZIO[Scope, Throwable, RedisClient] = {
    val schedule = retrySchedule(config.retry)

    ZIO.acquireRelease {
      {
        for {
          client <- ZIO.attempt {
                      val c = core.RedisClient.create(config.toRedisURI)
                      c.setOptions(config.toOptions)
                      c
                    }
          poolCfg = BoundedPoolConfig.builder()
                      .minIdle(config.pool.minIdle)
                      .maxIdle(config.pool.maxIdle)
                      .maxTotal(config.pool.maxTotal)
                      .build()
          result <- {
            for {
              pool   <- ZIO.fromCompletionStage(
                          AsyncConnectionPoolSupport.createBoundedObjectPoolAsync(
                            () => client.connectAsync(ByteArrayCodec.INSTANCE, config.toRedisURI),
                            poolCfg
                          )
                        )
              pubsub <- ZIO.attempt(client.connectPubSub(ByteArrayCodec.INSTANCE).reactive())
                          .tapError(_ => ZIO.fromCompletionStage(pool.closeAsync()).ignore)
            } yield PooledLettuceRedisClient(client, pool, pubsub, schedule)
          }.tapError(_ => ZIO.attempt(client.close()).ignore)
        } yield result
      }.retry(schedule)
    }(_.close().ignore)
  }
}

private class PooledLettuceRedisClient(
  client: core.RedisClient,
  pool: AsyncPool[StatefulRedisConnection[Array[Byte], Array[Byte]]],
  pubsub: RedisPubSubReactiveCommands[Array[Byte], Array[Byte]],
  schedule: Schedule[Any, Throwable, Any]
) extends RedisClient {

  import zio.interop.reactivestreams.*

  import scala.language.implicitConversions

  private def withPooledConnection[A](f: RedisAsyncCommands[Array[Byte], Array[Byte]] => Task[A]): Task[A] =
    ZIO.acquireReleaseWith(
      ZIO.fromCompletionStage(pool.acquire())
    )(conn => ZIO.fromCompletionStage(pool.release(conn)).ignore)(conn => f(conn.async()))

  private def withRetry[A](f: RedisAsyncCommands[Array[Byte], Array[Byte]] => Task[A]): Task[A] =
    withPooledConnection(f).retry(schedule)

  override def set(name: Chunk[Byte], value: Chunk[Byte], ttl: Option[Duration]): Task[String] =
    withRetry { async =>
      ZIO.fromCompletionStage(
        ttl.fold(async.set(name.toArray, value.toArray))(ttlValue =>
          async.set(name.toArray, value.toArray, SetArgs.Builder.ex(ttlValue.toSeconds))
        )
      )
    }

  // No retry: setNx is used for distributed locking, retrying could cause double-acquisition.
  override def setNx(name: Chunk[Byte], value: Chunk[Byte], ttl: Option[Duration]): Task[Boolean] =
    withPooledConnection { async =>
      ttl match {
        case Some(duration) =>
          val setArgs = SetArgs.Builder.nx.ex(duration.getSeconds)
          ZIO.fromCompletionStage(async.set(name.toArray, value.toArray, setArgs)).map(res => res != null)
        case None           => ZIO.fromCompletionStage(async.setnx(name.toArray, value.toArray)).map(res => res.booleanValue())
      }
    }

  override def get(name: Chunk[Byte]): Task[Option[Chunk[Byte]]] =
    withRetry { async =>
      ZIO.fromCompletionStage(async.get(name.toArray)).map(Option(_).map(Chunk.fromArray))
    }

  override def del(name: Chunk[Byte]): Task[Long] =
    withRetry { async =>
      ZIO.fromCompletionStage(async.del(name.toArray)).map(_.longValue())
    }

  override def expire(name: Chunk[Byte], ttl: Duration): Task[Boolean] =
    withRetry { async =>
      ZIO.fromCompletionStage(async.expire(name.toArray, ttl.toSeconds)).map(_.booleanValue())
    }

  override def publish(channel: Chunk[Byte], message: Chunk[Byte]): Task[Long] =
    monoToZio(pubsub.publish(channel.toArray, message.toArray)).map(_.longValue())

  override def subscribe(channel: Chunk[Byte]): ZStream[Any, Throwable, Chunk[Byte]] =
    ZStream.from(pubsub.subscribe(channel.toArray).subscribe()) *> pubsub
      .observeChannels()
      .toZIOStream()
      .map(el => Chunk.fromArray(el.getMessage))

  def close(): Task[Unit] =
    ZIO.fromCompletionStage(pool.closeAsync()).unit.ignore *>
      ZIO.attempt(client.close())

  private def monoToZio[A](mono: Mono[A]): Task[A] =
    ZIO.async { callback =>
      mono.subscribe(
        a => callback(ZIO.succeed(a)),
        e => callback(ZIO.fail(e)),
        () => () // Mono completes; no action needed here
      )
    }
}
