package io.github.tobia80.dref.redis

import io.github.tobia80.dref.*
import io.github.vigoo.desert.zioschema.DerivedBinaryCodec
import io.github.vigoo.desert.{deserializeFromArray, serializeToArray, BinaryCodec, DesertFailure}
import io.lettuce.core.{ClientOptions, RedisURI, SslOptions}
import zio.*
import zio.schema.{DeriveSchema, Schema}
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

object RedisDRefContext {

  private case class ChangePayload(name: Chunk[Byte], value: Chunk[Byte], delete: Boolean = false) {

    def nameString: String =
      new String(name.toArray)
  }

  private given Schema[ChangePayload] = DeriveSchema.gen[ChangePayload]
  private given BinaryCodec[ChangePayload] = DerivedBinaryCodec.derive

  private val channel = Chunk.fromArray("dref-change".getBytes)

  val live: ZLayer[zio.Scope & RedisConfig, Throwable, DRefContext] = ZLayer(
    for {
      config           <- ZIO.service[RedisConfig]
      redisClient      <- RedisClient.make(config)
      redisSubscription = redisClient.subscribe(channel)
      ttl               = config.ttl
      hub              <- Hub.bounded[Take[Throwable, ChangePayload]](64)
      _                <- redisSubscription
                            .mapZIO { value =>
                              ZIO
                                .fromEither(deserializeFromArray[ChangePayload](value.toArray))
                                .tapError { error =>
                                  ZIO.logError(s"Cannot decode notification ($value) (Error: $error)")
                                }
                                .either
                            }
                            .collect { case Right(notification) => notification }
                            .runForeach { event =>
                              ZIO.logDebug(s"Publishing event $event to hub") *> hub
                                .publish(Take.single(event))
                            }
                            .forkDaemon
    } yield new DRefContext {
      private def publish(message: Chunk[Byte]) = redisClient.publish(channel, message)

      override def setElement(name: String, a: Array[Byte]): Task[Unit] = {
        val change = ChangePayload(name = Chunk.fromArray(name.getBytes), value = Chunk.fromArray(a))
        val result: Either[DesertFailure, Array[Byte]] = serializeToArray[ChangePayload](change)
        result match {
          case Right(value) =>
            for {
              _ <- redisClient.set(Chunk.fromArray(name.getBytes), Chunk.fromArray(a), ttl)
              _ <- publish(Chunk.fromArray(value))
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
              _ <- redisClient.del(Chunk.fromArray(name.getBytes))
              _ <- publish(Chunk.fromArray(value))
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
        ttl.fold(ZIO.unit)(ttlValue => redisClient.expire(Chunk.fromArray(name.getBytes), ttlValue).unit)

      override def setElementIfNotExist(name: String, value: Array[Byte]): Task[Boolean] = {
        val change = ChangePayload(name = Chunk.fromArray(name.getBytes), value = Chunk.fromArray(value))
        val result: Either[DesertFailure, Array[Byte]] = serializeToArray[ChangePayload](change)
        result match {
          case Right(value) =>
            for {
              result <- redisClient.setNx(Chunk.fromArray(name.getBytes), Chunk.fromArray(value))
              _      <- keepAlive(name).when(result)
              _      <- publish(Chunk.fromArray(value)).when(result)
            } yield result
          case Left(error)  => ZIO.fail(new Exception(s"Cannot serialize notification ($change) (Error: $error)"))
        }
      }

      override def getElement(name: String): Task[Option[Array[Byte]]] =
        redisClient.get(Chunk.fromArray(name.getBytes)).map(_.map(_.toArray))

      override def onChangeStream(name: String): ZStream[Any, Throwable, ChangeEvent] =
        ZStream.logInfo(s"Subscribing to $name") *> ZStream.fromHub(hub).flattenTake.collect {
          case c @ ChangePayload(_, value, false) if c.nameString == name => SetElement(name, value.toArray)
          case c @ ChangePayload(_, value, true) if c.nameString == name  => DeleteElement(name)
        }
    }
  )
}
