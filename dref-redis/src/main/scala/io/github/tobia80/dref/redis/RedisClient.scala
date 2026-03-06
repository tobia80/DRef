package io.github.tobia80.dref.redis

import io.lettuce.core
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.SetArgs
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.event.Event
import io.lettuce.core.event.connection.{
  ConnectedEvent,
  ConnectionActivatedEvent,
  ConnectionDeactivatedEvent,
  DisconnectedEvent,
  ReconnectAttemptEvent,
  ReconnectFailedEvent
}
import io.lettuce.core.protocol.RedisCommand
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands
import zio.stream.ZStream
import zio.{Chunk, Hub, Scope, Task, ZIO}
import zio.*

sealed trait RedisConnectionEvent

object RedisConnectionEvent {

  case object Connected extends RedisConnectionEvent

  case object Disconnected extends RedisConnectionEvent

  case object Activated extends RedisConnectionEvent

  case object Deactivated extends RedisConnectionEvent

  final case class ReconnectAttempt(attempt: Int, delay: Duration) extends RedisConnectionEvent

  final case class ReconnectFailed(attempt: Int, cause: Throwable) extends RedisConnectionEvent
}

trait RedisClient {

  def set(name: Chunk[Byte], value: Chunk[Byte], ttl: Option[Duration]): Task[String]

  def setNx(name: Chunk[Byte], value: Chunk[Byte], ttl: Option[Duration]): Task[Boolean]

  def get(name: Chunk[Byte]): Task[Option[Chunk[Byte]]]

  def del(name: Chunk[Byte]): Task[Long]

  def expire(name: Chunk[Byte], ttl: Duration): Task[Boolean]

  def publish(channel: Chunk[Byte], message: Chunk[Byte]): Task[Long]

  def subscribe(channel: Chunk[Byte]): ZStream[Any, Throwable, Chunk[Byte]]

  def reconnections: ZStream[Any, Nothing, Unit]

}

object RedisClient {

  def make(config: RedisConfig): ZIO[Scope, Throwable, RedisClient] =
    for {
      resources <- ZIO.acquireRelease(ZIO.attempt(config.toClientResources)) { resources =>
                     ZIO.attempt(resources.shutdown()).ignore
                   }
      client    <- ZIO.acquireRelease {
                     ZIO.attempt {
                       val redisClient: core.RedisClient = core.RedisClient.create(resources, config.toRedisURI)
                       redisClient.setOptions(config.toOptions)
                       redisClient
                     }
                   }(client => ZIO.attempt(client.shutdown()).ignore)
      commands  <- ZIO.acquireRelease(ZIO.attempt(client.connect(ByteArrayCodec.INSTANCE))) { connection =>
                     ZIO.attempt(connection.close()).ignore
                   }
      pubSub    <- ZIO.acquireRelease(ZIO.attempt(client.connectPubSub(ByteArrayCodec.INSTANCE))) { connection =>
                     ZIO.attempt(connection.close()).ignore
                   }
      hub       <- Hub.unbounded[RedisConnectionEvent]
      _         <- eventStream(resources.eventBus().get())
                     .mapZIO { event =>
                       logConnectionEvent(event) *> hub.publish(event).unit
                     }
                     .runDrain
                     .forkScoped
    } yield LettuceRedisClient(client, commands, pubSub, hub)

  private def eventStream(events: org.reactivestreams.Publisher[Event]): ZStream[Any, Throwable, RedisConnectionEvent] = {
    import zio.interop.reactivestreams.*

    events.toZIOStream().collect {
      case _: ConnectedEvent                 => RedisConnectionEvent.Connected
      case _: DisconnectedEvent             => RedisConnectionEvent.Disconnected
      case _: ConnectionActivatedEvent      => RedisConnectionEvent.Activated
      case _: ConnectionDeactivatedEvent    => RedisConnectionEvent.Deactivated
      case event: ReconnectAttemptEvent     => RedisConnectionEvent.ReconnectAttempt(
          event.getAttempt(),
          Duration.fromJava(event.getDelay())
        )
      case event: ReconnectFailedEvent      => RedisConnectionEvent.ReconnectFailed(event.getAttempt(), event.getCause())
    }
  }

  private def logConnectionEvent(event: RedisConnectionEvent): UIO[Unit] =
    event match {
      case RedisConnectionEvent.Connected               =>
        ZIO.logInfo("Redis connection established")
      case RedisConnectionEvent.Disconnected            =>
        ZIO.logWarning("Redis connection disconnected")
      case RedisConnectionEvent.Activated               =>
        ZIO.logInfo("Redis connection activated")
      case RedisConnectionEvent.Deactivated             =>
        ZIO.logWarning("Redis connection deactivated")
      case RedisConnectionEvent.ReconnectAttempt(attempt, delay) =>
        ZIO.logWarning(s"Redis reconnect attempt #$attempt in ${delay.render}")
      case RedisConnectionEvent.ReconnectFailed(attempt, cause)  =>
        ZIO.logError(s"Redis reconnect attempt #$attempt failed: ${cause.getMessage}")
    }
}

class LettuceRedisClient(
  client: core.RedisClient,
  commandsConnection: StatefulRedisConnection[Array[Byte], Array[Byte]],
  pubSubConnection: StatefulRedisPubSubConnection[Array[Byte], Array[Byte]],
  connectionEventsHub: Hub[RedisConnectionEvent]
) extends RedisClient {

  import zio.interop.reactivestreams.*

  import scala.language.implicitConversions

  private val async: RedisAsyncCommands[Array[Byte], Array[Byte]] = commandsConnection.async()

  private val pubsub: RedisPubSubReactiveCommands[Array[Byte], Array[Byte]] = pubSubConnection.reactive()

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
    ZIO.fromCompletionStage(async.publish(channel.toArray, message.toArray)).map(_.longValue())

  override def subscribe(channel: Chunk[Byte]): ZStream[Any, Throwable, Chunk[Byte]] =
    ZStream.fromZIO(ZIO.fromCompletionStage(pubSubConnection.async().subscribe(channel.toArray)).unit) *> pubsub
      .observeChannels()
      .toZIOStream()
      .map(el => Chunk.fromArray(el.getMessage))

  override def reconnections: ZStream[Any, Nothing, Unit] =
    ZStream
      .fromHub(connectionEventsHub)
      .mapAccum(false) {
        case (_, RedisConnectionEvent.Disconnected | RedisConnectionEvent.Deactivated) => (true, false)
        case (true, RedisConnectionEvent.Activated)                                    => (false, true)
        case (wasDisconnected, _)                                                      => (wasDisconnected, false)
      }
      .filter(identity)
      .as(())

  def close(): Task[Unit] = ZIO.attempt(client.close())
}
