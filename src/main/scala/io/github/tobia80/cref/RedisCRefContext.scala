package io.github.tobia80.cref

import dev.profunktor.redis4cats.connection.RedisClient
import dev.profunktor.redis4cats.data.{RedisChannel, RedisCodec}
import dev.profunktor.redis4cats.effect.MkRedis.*
import dev.profunktor.redis4cats.effect.{Log, MkRedis}
import dev.profunktor.redis4cats.effects.SetArg.Ttl
import dev.profunktor.redis4cats.effects.SetArgs
import dev.profunktor.redis4cats.pubsub.PubSub
import dev.profunktor.redis4cats.{Redis, RedisCommands}
import fs2.Stream
import io.github.vigoo.desert.zioschema.DerivedBinaryCodec
import io.github.vigoo.desert.{deserializeFromArray, serializeToArray, BinaryCodec, DesertFailure}
import io.lettuce.core.{ClientOptions, RedisURI, SslOptions}
import zio.*
import zio.interop.catz.*
import zio.schema.{DeriveSchema, Schema}
import zio.stream.interop.fs2z.*
import zio.stream.{Take, ZStream}

import scala.concurrent.duration.FiniteDuration

case class RedisConfig(
  host: String,
  port: Int,
  database: Int,
  username: Option[String],
  password: Option[String],
  caCert: Option[String],
  ttl: Option[FiniteDuration]
) {

  def toRedisURI: String = {
    val builder = RedisURI.Builder
      .redis(host, port)
      .withDatabase(database)
      .withSsl(caCert.isDefined)

    (username, password) match {
      case (Some(u), Some(p)) => builder.withAuthentication(u, p.toCharArray)
      case (None, Some(p))    => builder.withPassword(p.toCharArray)
      case _                  => ()
    }

    builder
      .build()
      .toURI
      .toString
  }

  def toOptions: ClientOptions = {
    val clientBuilder = ClientOptions.builder()
    caCert.foreach { cert =>
      clientBuilder.sslOptions(
        SslOptions.builder().trustManager(SslOptions.Resource.from(java.io.File(cert))).build()
      )
    }
    clientBuilder.build()
  }
}

object RedisConfig {

  def get: URIO[RedisConfig, RedisConfig] = ZIO.service[RedisConfig]
}

object RedisCRefContext {

  given Log[Task] = Log.NoOp.instance

  private case class ChangePayload(name: Chunk[Byte], value: Chunk[Byte], delete: Boolean = false) {

    def nameString: String =
      new String(name.toArray)
  }

  type AStream[A] = fs2.Stream[Task, A]

  private given Schema[ChangePayload] = DeriveSchema.gen[ChangePayload]
  private given BinaryCodec[ChangePayload] = DerivedBinaryCodec.derive

  val live: ZLayer[zio.Scope & RedisConfig, Throwable, CRefContext] = ZLayer(
    for {
      config                                               <- ZIO.service[RedisConfig]
      redis: RedisCommands[Task, Array[Byte], Array[Byte]] <-
        Redis[Task].withOptions(config.toRedisURI, config.toOptions, RedisCodec.Bytes).toScopedZIO
      client                                               <- RedisClient[Task].from(config.toRedisURI).toScopedZIO
      redisStream                                          <-
        PubSub
          .mkPubSubConnection[Task, Array[Byte], Array[Byte]](client, RedisCodec.Bytes)
          .toScopedZIO // use only one channel to notify for every one
      redisReceiver                                        <- PubSub.mkPubSubConnection[Task, String, String](client, RedisCodec.Utf8).toScopedZIO
      ttl                                                   = config.ttl
      hub                                                  <- Hub.bounded[Take[Throwable, ChangePayload]](64)

      notificationChannel = RedisChannel("cref-change".getBytes)
      receiverChannel     = RedisChannel("cref-change")
      _                  <- redisReceiver
                              .subscribe(receiverChannel)
                              .toZStream()
                              .mapZIO { value =>
                                ZIO
                                  .fromEither(deserializeFromArray[ChangePayload](value.getBytes))
                                  .tapError { error =>
                                    ZIO.logError(s"Cannot decode notification ($value) (Error: $error)")
                                  }
                                  .either
                              }
                              .collect { case Right(notification) => notification }
                              .runIntoHub(hub)
                              .forkDaemon
    } yield new CRefContext {
      private val publish: (AStream[Array[Byte]] => AStream[Unit]) = redisStream.publish(notificationChannel)

      override def setElement(name: String, a: Array[Byte]): Task[Unit] = {
        val change = ChangePayload(name = Chunk.fromArray(name.getBytes), value = Chunk.fromArray(a))
        val result: Either[DesertFailure, Array[Byte]] = serializeToArray[ChangePayload](change)
        result match {
          case Right(value) =>
            for {
              _ <- redis.set(name.getBytes, a, SetArgs(None, ttl.map(value => Ttl.Ex(value))))
              _ <- publish(Stream.apply(value)).toZStream().runDrain
            } yield ()
          case Left(error)  => ZIO.fail(new Exception(s"Cannot serialize notification ($change) (Error: $error)"))
        }
      }

      override def deleteElement(name: String): Task[Unit] = {
        val delete = ChangePayload(name = Chunk.fromArray(name.getBytes), value = Chunk.empty, delete = true)
        val result: Either[DesertFailure, Array[Byte]] = serializeToArray[ChangePayload](delete)
        result match {
          case Right(value) =>
            for {
              _ <- redis.del(name.getBytes)
              _ <- publish(Stream.apply(value)).toZStream().runDrain
            } yield ()
          case Left(error)  => ZIO.fail(new Exception(s"Cannot serialize notification ($delete) (Error: $error)"))
        }
      }

      import zio.Duration
      override def keepAliveStream(name: String): ZStream[Any, Throwable, Unit] =
        ttl.fold(ZStream.empty)(ttlValue =>
          val ttlZio = Duration.fromScala(ttlValue / 1.25)
          ZStream.repeatZIOWithSchedule(keepAlive(name), Schedule.fixed(ttlZio))
        )

      private def keepAlive(name: String): Task[Unit] =
        ttl.fold(ZIO.unit)(ttlValue => redis.expire(name.getBytes, ttlValue).unit)

      override def setElementIfNotExist(name: String, value: Array[Byte]): Task[Boolean] = {
        val change = ChangePayload(name = Chunk.fromArray(name.getBytes), value = Chunk.fromArray(value))
        val result: Either[DesertFailure, Array[Byte]] = serializeToArray[ChangePayload](change)
        result match {
          case Right(value) =>
            for {
              result <- redis.setNx(name.getBytes, value)
              _      <- keepAlive(name).when(result)
              _      <- publish(Stream.apply(value)).toZStream().runDrain.when(result)
            } yield result
          case Left(error)  => ZIO.fail(new Exception(s"Cannot serialize notification ($change) (Error: $error)"))
        }
      }

      override def getElement(name: String): Task[Option[Array[Byte]]] = redis.get(name.getBytes)

      override def onChangeStream(name: String): ZStream[Any, Throwable, ChangeEvent] =
        ZStream.fromHub(hub).flattenTake.collect {
          case c @ ChangePayload(_, value, false) if c.nameString == name => SetElement(name, value.toArray)
          case c @ ChangePayload(_, value, true) if c.nameString == name  => DeleteElement(name)
        }
    }
  )
}
