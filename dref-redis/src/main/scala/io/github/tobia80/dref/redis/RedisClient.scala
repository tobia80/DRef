package io.github.tobia80.dref.redis

import io.lettuce.core
import io.lettuce.core.SetArgs
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands
import reactor.core.publisher.Mono
import zio.stream.ZStream
import zio.{Chunk, Scope, Task, ZIO}
import zio.*

import scala.concurrent.duration.FiniteDuration

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

  def make(config: RedisConfig): ZIO[Scope, Throwable, RedisClient] =
    ZIO.acquireRelease {
      ZIO.attempt {
        val client: core.RedisClient = core.RedisClient.create(config.toRedisURI)
        val asyncClient = client.connect(ByteArrayCodec.INSTANCE).async
        val reactive = client.connectPubSub(ByteArrayCodec.INSTANCE).reactive()
        LettuceRedisClient(client, asyncClient, reactive)
      }
    }(client => client.close().ignore)
}

class LettuceRedisClient(
  client: core.RedisClient,
  async: RedisAsyncCommands[Array[Byte], Array[Byte]],
  pubsub: RedisPubSubReactiveCommands[Array[Byte], Array[Byte]]
) extends RedisClient {

  import zio.interop.reactivestreams.*

  import scala.language.implicitConversions

  override def set(name: Chunk[Byte], value: Chunk[Byte], ttl: Option[Duration]): Task[String] = ZIO
    .fromCompletionStage(
      ttl
        .fold(async.set(name.toArray, value.toArray))(ttlValue =>
          async.set(name.toArray, value.toArray, SetArgs.Builder.ex(ttlValue.toSeconds)).toCompletableFuture
        )
    )

  override def setNx(name: Chunk[Byte], value: Chunk[Byte], ttl: Option[Duration]): Task[Boolean] =
    ttl match {
      case Some(duration) =>
        val setArgs = SetArgs.Builder.nx.ex(duration.getSeconds)
        ZIO.fromCompletionStage(async.set(name.toArray, value.toArray, setArgs)).map(res => res != null)
      case None           => ZIO.fromCompletionStage(async.setnx(name.toArray, value.toArray)).map(res => res.booleanValue())
    }

  override def get(name: Chunk[Byte]): Task[Option[Chunk[Byte]]] =
    ZIO.fromCompletionStage(async.get(name.toArray)).map(Option(_).map(Chunk.fromArray))

  override def del(name: Chunk[Byte]): Task[Long] = ZIO.fromCompletionStage(async.del(name.toArray)).map(_.longValue())

  override def expire(name: Chunk[Byte], ttl: Duration): Task[Boolean] =
    ZIO.fromCompletionStage(async.expire(name.toArray, ttl.toSeconds)).map(_.booleanValue())

  override def publish(channel: Chunk[Byte], message: Chunk[Byte]): Task[Long] =
    monoToZio(pubsub.publish(channel.toArray, message.toArray)).map(_.longValue())

  override def subscribe(channel: Chunk[Byte]): ZStream[Any, Throwable, Chunk[Byte]] =
    ZStream.from(pubsub.subscribe(channel.toArray).subscribe()) *> pubsub
      .observeChannels()
      .toZIOStream()
      .map(el => Chunk.fromArray(el.getMessage))

  def close(): Task[Unit] = ZIO.attempt(client.close())

  private def monoToZio[A](mono: Mono[A]): Task[A] =
    ZIO.async { callback =>
      mono.subscribe(
        a => callback(ZIO.succeed(a)),
        e => callback(ZIO.fail(e)),
        () => () // Mono completes; no action needed here
      )
    }
}
